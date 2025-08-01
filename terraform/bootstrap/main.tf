# Bootstrap Infrastructure for Terraform State Management

# This creates the S3 bucket and DynamoDB table needed for remote state

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Get current AWS account info
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# S3 bucket for Terraform state
resource "aws_s3_bucket" "terraform_state" {
  bucket = var.state_bucket_name

  tags = merge(var.tags, {
    Name        = var.state_bucket_name
    Environment = "shared"
    Purpose     = "TerraformState"
  })
}

# KMS key for S3 bucket encryption
resource "aws_kms_key" "terraform_state" {
  description             = "KMS key for Terraform state S3 buckets"
  enable_key_rotation     = true
  deletion_window_in_days = 7
}

# S3 bucket for access logs (in primary region)
resource "aws_s3_bucket" "terraform_state_logs" {
  bucket = "${var.state_bucket_name}-logs"

  tags = merge(var.tags, {
    Name        = "${var.state_bucket_name}-logs"
    Environment = "shared"
    Purpose     = "TerraformStateAccessLogs"
  })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state_logs" {
  bucket = aws_s3_bucket.terraform_state_logs.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.terraform_state.arn
    }
    bucket_key_enabled = true
  }
}


# SNS Topic for S3 notifications
resource "aws_sns_topic" "s3_notifications" {
  name = "terraform-state-s3-notifications"
}

# SNS Topic Policy to allow S3 buckets to publish
resource "aws_sns_topic_policy" "s3_notifications_policy" {
  arn = aws_sns_topic.s3_notifications.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { Service = "s3.amazonaws.com" }
        Action    = "sns:Publish"
        Resource  = aws_sns_topic.s3_notifications.arn
        Condition = {
          ArnLike = {
            "aws:SourceArn" = [
              aws_s3_bucket.terraform_state.arn,
              aws_s3_bucket.terraform_state_replica.arn
            ]
          }
        }
      }
    ]
  })
}

# SNS Email Subscription
resource "aws_sns_topic_subscription" "s3_notifications_email" {
  topic_arn = aws_sns_topic.s3_notifications.arn
  protocol  = "email"
  endpoint  = "investcosine@gmail.com"
}

# S3 Event Notification for terraform_state bucket (SNS)
resource "aws_s3_bucket_notification" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  topic {
    topic_arn = aws_sns_topic.s3_notifications.arn
    events    = ["s3:ObjectCreated:*"]
  }
}

# S3 Access Logging for terraform_state bucket
resource "aws_s3_bucket_logging" "terraform_state" {
  bucket        = aws_s3_bucket.terraform_state.id
  target_bucket = aws_s3_bucket.terraform_state_logs.id
  target_prefix = "terraform-state/"
}

# S3 bucket in another region for replication destination
provider "aws" {
  alias  = "replica"
  region = var.replica_region
}

resource "aws_s3_bucket" "terraform_state_replica" {
  provider = aws.replica
  bucket   = "${var.state_bucket_name}-replica"

  tags = merge(var.tags, {
    Name        = "${var.state_bucket_name}-replica"
    Environment = "shared"
    Purpose     = "TerraformStateReplica"
  })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state_replica" {
  provider = aws.replica
  bucket   = aws_s3_bucket.terraform_state_replica.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.terraform_state.arn
    }
    bucket_key_enabled = true
  }
}

# S3 Access Logging for terraform_state_replica bucket (logs to primary region logs bucket)
resource "aws_s3_bucket_logging" "terraform_state_replica" {
  provider      = aws.replica
  bucket        = aws_s3_bucket.terraform_state_replica.id
  target_bucket = aws_s3_bucket.terraform_state_logs.id
  target_prefix = "terraform-state-replica/"
}

# S3 Event Notification for terraform_state_replica bucket (SNS)
resource "aws_s3_bucket_notification" "terraform_state_replica" {
  provider = aws.replica
  bucket   = aws_s3_bucket.terraform_state_replica.id

  topic {
    topic_arn = aws_sns_topic.s3_notifications.arn
    events    = ["s3:ObjectCreated:*"]
  }
}

# S3 bucket versioning
resource "aws_s3_bucket_versioning" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 bucket encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.terraform_state.arn
    }
    bucket_key_enabled = true
  }
}

# S3 bucket public access block
resource "aws_s3_bucket_public_access_block" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 bucket lifecycle configuration
resource "aws_s3_bucket_lifecycle_configuration" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    id     = "terraform_state_lifecycle"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = var.state_retention_days
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# IAM Role for S3 Replication
resource "aws_iam_role" "terraform_state_replication" {
  name = "${var.state_bucket_name}-replication-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

# IAM Policy for S3 Replication
resource "aws_iam_role_policy" "terraform_state_replication_policy" {
  name = "${var.state_bucket_name}-replication-policy"
  role = aws_iam_role.terraform_state_replication.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetReplicationConfiguration",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.terraform_state.arn,
          aws_s3_bucket.terraform_state_replica.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObjectVersion",
          "s3:GetObjectVersionAcl",
          "s3:GetObjectVersionTagging"
        ]
        Resource = [
          "${aws_s3_bucket.terraform_state.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ReplicateObject",
          "s3:ReplicateDelete",
          "s3:ReplicateTags",
          "s3:GetObjectVersionForReplication",
          "s3:ObjectOwnerOverrideToBucketOwner"
        ]
        Resource = [
          "${aws_s3_bucket.terraform_state_replica.arn}/*"
        ]
      }
    ]
  })
}

# S3 Bucket Replication Configuration (cross-region)
resource "aws_s3_bucket_replication_configuration" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id
  role   = aws_iam_role.terraform_state_replication.arn

  rule {
    id     = "replicate-all"
    status = "Enabled"

    filter {}

    destination {
      bucket        = aws_s3_bucket.terraform_state_replica.arn
      storage_class = "STANDARD"
    }
  }
}

# DynamoDB table for state locking
resource "aws_dynamodb_table" "terraform_locks" {
  name         = var.lock_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  server_side_encryption {
    enabled = true
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = merge(var.tags, {
    Name        = var.lock_table_name
    Environment = "shared"
    Purpose     = "TerraformStateLocking"
  })
}
