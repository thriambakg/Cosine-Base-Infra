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

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow use of the key for S3"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey*"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow use of the key for DynamoDB"
        Effect = "Allow"
        Principal = {
          Service = "dynamodb.amazonaws.com"
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey*"
        ]
        Resource = "*"
      },
      {
        Sid    = "Allow use of the key for SNS"
        Effect = "Allow"
        Principal = {
          Service = "sns.amazonaws.com"
        }
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey*"
        ]
        Resource = "*"
      }
    ]
  })
}

# KMS key alias
resource "aws_kms_alias" "terraform_state" {
  name          = "alias/terraform-state-bootstrap"
  target_key_id = aws_kms_key.terraform_state.key_id
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


# S3 bucket lifecycle policies for logs bucket
resource "aws_s3_bucket_lifecycle_configuration" "logs_lifecycle" {
  bucket = aws_s3_bucket.terraform_state_logs.id

  rule {
    id     = "logs_lifecycle"
    status = "Enabled"

    expiration {
      days = 90
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# S3 bucket lifecycle policies for replica bucket
resource "aws_s3_bucket_lifecycle_configuration" "replica_lifecycle" {
  provider = aws.replica
  bucket   = aws_s3_bucket.terraform_state_replica.id

  rule {
    id     = "replica_lifecycle"
    status = "Enabled"

    expiration {
      days = 90
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# S3 bucket public access block for logs bucket
resource "aws_s3_bucket_public_access_block" "terraform_state_logs_pab" {
  bucket = aws_s3_bucket.terraform_state_logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 bucket public access block for replica bucket
resource "aws_s3_bucket_public_access_block" "terraform_state_replica_pab" {
  provider = aws.replica
  bucket   = aws_s3_bucket.terraform_state_replica.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 bucket notifications for logs bucket
resource "aws_s3_bucket_notification" "terraform_state_logs_notification" {
  bucket = aws_s3_bucket.terraform_state_logs.id

  topic {
    topic_arn = aws_sns_topic.s3_notifications.arn
    events    = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
  }

  depends_on = [aws_sns_topic_policy.s3_notifications_policy]
}

# SNS topic for state notifications
resource "aws_sns_topic" "s3_notifications" {
  name              = "terraform-state-s3-notifications"
  kms_master_key_id = aws_kms_key.terraform_state.arn
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

# S3 bucket versioning for logs bucket
resource "aws_s3_bucket_versioning" "terraform_state_logs_versioning" {
  bucket = aws_s3_bucket.terraform_state_logs.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 bucket versioning for replica bucket
resource "aws_s3_bucket_versioning" "terraform_state_replica_versioning" {
  provider = aws.replica
  bucket   = aws_s3_bucket.terraform_state_replica.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 bucket lifecycle policies
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

  depends_on = [
    aws_s3_bucket_versioning.terraform_state,
    aws_s3_bucket_versioning.terraform_state_replica_versioning
  ]
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
    enabled     = true
    kms_key_arn = aws_kms_key.terraform_state.arn
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

# S3 bucket for logs replica (cross-region)
resource "aws_s3_bucket" "terraform_state_logs_replica" {
  provider = aws.replica
  bucket   = "${var.state_bucket_name}-logs-replica"

  tags = merge(var.tags, {
    Name        = "${var.state_bucket_name}-logs-replica"
    Environment = "shared"
    Purpose     = "TerraformStateLogsReplica"
  })
}

# Versioning for logs replica bucket
resource "aws_s3_bucket_versioning" "terraform_state_logs_replica_versioning" {
  provider = aws.replica
  bucket   = aws_s3_bucket.terraform_state_logs_replica.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Encryption for logs replica bucket
resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state_logs_replica" {
  provider = aws.replica
  bucket   = aws_s3_bucket.terraform_state_logs_replica.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.terraform_state.arn
    }
    bucket_key_enabled = true
  }
}

# Public access block for logs replica bucket
resource "aws_s3_bucket_public_access_block" "terraform_state_logs_replica_pab" {
  provider = aws.replica
  bucket   = aws_s3_bucket.terraform_state_logs_replica.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM Role for logs replication
resource "aws_iam_role" "terraform_state_logs_replication" {
  name = "${var.state_bucket_name}-logs-replication-role"

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

# IAM Policy for logs replication
resource "aws_iam_role_policy" "terraform_state_logs_replication_policy" {
  name = "${var.state_bucket_name}-logs-replication-policy"
  role = aws_iam_role.terraform_state_logs_replication.id

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
          aws_s3_bucket.terraform_state_logs.arn,
          aws_s3_bucket.terraform_state_logs_replica.arn
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
          "${aws_s3_bucket.terraform_state_logs.arn}/*"
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
          "${aws_s3_bucket.terraform_state_logs_replica.arn}/*"
        ]
      }
    ]
  })
}

# S3 Bucket Replication Configuration for logs (cross-region)
resource "aws_s3_bucket_replication_configuration" "terraform_state_logs" {
  bucket = aws_s3_bucket.terraform_state_logs.id
  role   = aws_iam_role.terraform_state_logs_replication.arn

  rule {
    id     = "replicate-logs"
    status = "Enabled"

    filter {}

    destination {
      bucket        = aws_s3_bucket.terraform_state_logs_replica.arn
      storage_class = "STANDARD"
    }
  }

  depends_on = [aws_s3_bucket_versioning.terraform_state_logs_versioning]
}

# Lifecycle configuration for logs replica bucket
resource "aws_s3_bucket_lifecycle_configuration" "terraform_state_logs_replica_lifecycle" {
  provider = aws.replica
  bucket   = aws_s3_bucket.terraform_state_logs_replica.id

  rule {
    id     = "logs_replica_lifecycle"
    status = "Enabled"

    expiration {
      days = 90
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}
