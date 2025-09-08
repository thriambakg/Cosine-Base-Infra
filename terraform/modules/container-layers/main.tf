# Container-based Lambda Layer Management Module
# This module creates a container that builds and manages Lambda layers
# Cost-effective approach: container only runs during deployment, not execution

# ECR Repository for Layer Container
resource "aws_ecr_repository" "layer_builder" {
  name                 = "${var.project_name}-layer-builder-${var.environment}"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = var.tags
}

# ECR Repository for Layer Artifacts
resource "aws_ecr_repository" "layer_artifacts" {
  name                 = "${var.project_name}-layer-artifacts-${var.environment}"
  image_tag_mutability = "IMMUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = var.tags
}

# IAM Role for Layer Builder Container
resource "aws_iam_role" "layer_builder_role" {
  name = "${var.project_name}-layer-builder-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

# IAM Policy for Layer Builder
resource "aws_iam_role_policy" "layer_builder_policy" {
  name = "${var.project_name}-layer-builder-policy-${var.environment}"
  role = aws_iam_role.layer_builder_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken",
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage",
          "ecr:PutImage",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${var.s3_bucket_arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "lambda:PublishLayerVersion",
          "lambda:GetLayerVersion"
        ]
        Resource = "*"
      }
    ]
  })
}

# S3 Bucket for Layer Artifacts
resource "aws_s3_bucket" "layer_artifacts" {
  bucket = "${var.project_name}-layer-artifacts-${var.environment}"

  tags = var.tags
}

# S3 Bucket Public Access Block
resource "aws_s3_bucket_public_access_block" "layer_artifacts" {
  bucket = aws_s3_bucket.layer_artifacts.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "layer_artifacts" {
  bucket = aws_s3_bucket.layer_artifacts.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = var.kms_key_id != null ? "aws:kms" : "AES256"
      kms_master_key_id = var.kms_key_id
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_versioning" "layer_artifacts" {
  bucket = aws_s3_bucket.layer_artifacts.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "layer_artifacts" {
  bucket = aws_s3_bucket.layer_artifacts.id

  rule {
    id     = "cleanup_old_versions"
    status = "Enabled"

    filter {
      prefix = "layers/"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

# Build Lambda Layers using Container
resource "null_resource" "build_layers" {
  for_each = var.layer_definitions

  provisioner "local-exec" {
    working_dir = path.module
    command     = <<-EOT
      # Get AWS Account ID
      AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
      ECR_REGISTRY="$${AWS_ACCOUNT_ID}.dkr.ecr.${var.aws_region}.amazonaws.com"
      ECR_REPOSITORY="${var.project_name}-layer-builder-${var.environment}"
      
      # Login to ECR
      aws ecr get-login-password --region ${var.aws_region} | docker login --username AWS --password-stdin $ECR_REGISTRY
      
      # Build and push container if not exists
      if ! docker manifest inspect $ECR_REGISTRY/$ECR_REPOSITORY:latest >/dev/null 2>&1; then
        echo "Building and pushing container image..."
        docker build -t $ECR_REGISTRY/$ECR_REPOSITORY:latest .
        docker push $ECR_REGISTRY/$ECR_REPOSITORY:latest
      fi
      
      # Run container to build specific layer
      echo "Running container for layer: ${each.key}"
      echo "Requirements file: ${each.value.requirements_file}"
      docker run --rm \
        -e AWS_DEFAULT_REGION=${var.aws_region} \
        -e S3_BUCKET_NAME=${aws_s3_bucket.layer_artifacts.bucket} \
        -e LAYER_NAME=${each.key} \
        -e REQUIREMENTS_FILE=${each.value.requirements_file} \
        -v $(pwd):/app \
        $ECR_REGISTRY/$ECR_REPOSITORY:latest
    EOT
  }

  depends_on = [
    aws_ecr_repository.layer_builder,
    aws_s3_bucket.layer_artifacts
  ]
}

# Lambda Layer Resources (created by container)
resource "aws_lambda_layer_version" "layers" {
  for_each = var.layer_definitions

  layer_name          = "${var.project_name}-${each.key}-${var.environment}"
  description         = each.value.description
  compatible_runtimes = each.value.compatible_runtimes

  # Use S3 for large layers
  s3_bucket = aws_s3_bucket.layer_artifacts.bucket
  s3_key    = "layers/${each.key}-layer.zip"

  depends_on = [
    aws_s3_bucket.layer_artifacts,
    null_resource.build_layers
  ]
}
