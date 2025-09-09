# ============================================================================
# SESSION MANAGEMENT INFRASTRUCTURE
# ============================================================================
# This module provides the base infrastructure for multi-session context management
# Includes DynamoDB tables, S3 buckets, and IAM policies

# ============================================================================
# DYNAMODB TABLES
# ============================================================================

# Sessions Table - Stores session metadata and basic information
resource "aws_dynamodb_table" "sessions" {
  name         = "${var.project_name}-sessions-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PK"
  range_key    = "SK"

  attribute {
    name = "PK"
    type = "S"
  }

  attribute {
    name = "SK"
    type = "S"
  }

  # Global Secondary Index for querying sessions by user
  global_secondary_index {
    name            = "UserSessionsIndex"
    hash_key        = "GSI1PK"
    range_key       = "GSI1SK"
    projection_type = "ALL"
  }

  # Global Secondary Index for querying active sessions
  global_secondary_index {
    name            = "ActiveSessionsIndex"
    hash_key        = "GSI2PK"
    range_key       = "GSI2SK"
    projection_type = "ALL"
  }

  # TTL for automatic cleanup of old sessions
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  # Point-in-time recovery for data protection
  point_in_time_recovery {
    enabled = true
  }

  # Server-side encryption
  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  tags = merge(var.common_tags, {
    Name      = "${var.project_name}-sessions-${var.environment}"
    Component = "session-management"
  })
}

# Session Context Table - Stores detailed context data for each session
resource "aws_dynamodb_table" "session_context" {
  name         = "${var.project_name}-session-context-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PK"
  range_key    = "SK"

  attribute {
    name = "PK"
    type = "S"
  }

  attribute {
    name = "SK"
    type = "S"
  }

  # Global Secondary Index for querying context by session
  global_secondary_index {
    name            = "SessionContextIndex"
    hash_key        = "GSI1PK"
    range_key       = "GSI1SK"
    projection_type = "ALL"
  }

  # TTL for automatic cleanup of old context data
  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  # Point-in-time recovery for data protection
  point_in_time_recovery {
    enabled = true
  }

  # Server-side encryption
  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  tags = merge(var.common_tags, {
    Name      = "${var.project_name}-session-context-${var.environment}"
    Component = "session-management"
  })
}

# ============================================================================
# S3 BUCKETS
# ============================================================================

# Session Archives Bucket - For storing large context data and conversation archives
resource "aws_s3_bucket" "session_archives" {
  bucket = "${var.project_name}-session-archives-${var.environment}"

  tags = merge(var.common_tags, {
    Name      = "${var.project_name}-session-archives-${var.environment}"
    Component = "session-management"
  })
}

# S3 Bucket Versioning
resource "aws_s3_bucket_versioning" "session_archives" {
  bucket = aws_s3_bucket.session_archives.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Bucket Server-Side Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "session_archives" {
  bucket = aws_s3_bucket.session_archives.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

# S3 Bucket Public Access Block
resource "aws_s3_bucket_public_access_block" "session_archives" {
  bucket = aws_s3_bucket.session_archives.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket Lifecycle Configuration
resource "aws_s3_bucket_lifecycle_configuration" "session_archives" {
  bucket = aws_s3_bucket.session_archives.id

  rule {
    id     = "session_archives_lifecycle"
    status = "Enabled"

    filter {}

    # Transition to IA after 30 days
    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    # Transition to Glacier after 90 days
    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    # Delete after 1 year
    expiration {
      days = 365
    }

    # Clean up incomplete multipart uploads
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# ============================================================================
# IAM POLICIES
# ============================================================================

# IAM Policy for Session Management Operations
resource "aws_iam_policy" "session_management_policy" {
  name_prefix = "${var.project_name}-session-management-policy-${var.environment}-"
  description = "Policy for session management operations"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem"
        ]
        Resource = [
          aws_dynamodb_table.sessions.arn,
          "${aws_dynamodb_table.sessions.arn}/index/*",
          aws_dynamodb_table.session_context.arn,
          "${aws_dynamodb_table.session_context.arn}/index/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.session_archives.arn,
          "${aws_s3_bucket.session_archives.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = [
          var.kms_key_arn
        ]
      }
    ]
  })

  tags = var.common_tags
}

# ============================================================================
# CLOUDWATCH LOG GROUPS
# ============================================================================

# CloudWatch Log Group for Session Management
resource "aws_cloudwatch_log_group" "session_management" {
  name              = "/aws/session-management/${var.project_name}-${var.environment}"
  retention_in_days = 14

  tags = merge(var.common_tags, {
    Name      = "${var.project_name}-session-management-logs-${var.environment}"
    Component = "session-management"
  })
}
