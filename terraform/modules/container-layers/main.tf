# Container-based Lambda Layer Management Module
# This module creates a container that builds and manages Lambda layers
# Cost-effective approach: container only runs during deployment, not execution

# ECR Repository for Layer Container
resource "aws_ecr_repository" "layer_builder" {
  name                 = "${var.project_name}-layer-builder-${var.environment}"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = var.tags
}

# ECR Repository for Layer Artifacts
resource "aws_ecr_repository" "layer_artifacts" {
  name                 = "${var.project_name}-layer-artifacts-${var.environment}"
  image_tag_mutability = "MUTABLE"

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

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
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

  depends_on = [aws_s3_bucket.layer_artifacts]
}
