# S3 Module - Security Compliant by Default
# modules/s3/main.tf

# Data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}


# Primary S3 bucket
resource "aws_s3_bucket" "this" {
  bucket        = var.bucket_name
  force_destroy = var.force_destroy

  tags = merge(var.tags, {
    Name        = var.bucket_name
    Environment = var.environment
    Purpose     = var.purpose
  })
}

# Versioning - Always enabled for compliance
resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption - Always enabled
resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

# Public access block - Configurable for CloudFront compatibility
resource "aws_s3_bucket_public_access_block" "this" {
  bucket = aws_s3_bucket.this.id

  block_public_acls       = var.block_public_acls
  block_public_policy     = var.allow_cloudfront_oac ? false : var.block_public_policy
  ignore_public_acls      = var.ignore_public_acls
  restrict_public_buckets = var.allow_cloudfront_oac ? false : var.restrict_public_buckets
}

# Lifecycle configuration - Always configured
resource "aws_s3_bucket_lifecycle_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  rule {
    id     = "security_compliance"
    status = "Enabled"

    filter {
      prefix = ""
    }

    # Abort incomplete multipart uploads (CKV_AWS_300)
    abort_incomplete_multipart_upload {
      days_after_initiation = var.abort_incomplete_multipart_upload_days
    }

    # Expire old versions
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }

    # Optional: Transition to IA and Glacier
    dynamic "transition" {
      for_each = var.enable_lifecycle_transitions ? [1] : []
      content {
        days          = var.transition_to_ia_days
        storage_class = "STANDARD_IA"
      }
    }

    dynamic "transition" {
      for_each = var.enable_lifecycle_transitions ? [1] : []
      content {
        days          = var.transition_to_glacier_days
        storage_class = "GLACIER"
      }
    }

    # Optional: Expiration
    dynamic "expiration" {
      for_each = var.enable_expiration ? [1] : []
      content {
        days = var.expiration_days
      }
    }
  }

  # Additional lifecycle rules (e.g., for specific prefixes)
  dynamic "rule" {
    for_each = var.lifecycle_rules
    content {
      id     = rule.value.id
      status = rule.value.status

      # Filter block - required for lifecycle rules
      filter {
        prefix = rule.value.filter != null && rule.value.filter.prefix != null ? rule.value.filter.prefix : null
        dynamic "tag" {
          for_each = rule.value.filter != null && rule.value.filter.tags != null ? rule.value.filter.tags : {}
          content {
            key   = tag.key
            value = tag.value
          }
        }
      }

      # Expiration
      dynamic "expiration" {
        for_each = rule.value.expiration != null ? [rule.value.expiration] : []
        content {
          days = expiration.value.days
        }
      }

      # Noncurrent version expiration
      dynamic "noncurrent_version_expiration" {
        for_each = rule.value.noncurrent_version_expiration != null ? [rule.value.noncurrent_version_expiration] : []
        content {
          noncurrent_days = noncurrent_version_expiration.value.noncurrent_days
        }
      }

      # Abort incomplete multipart upload
      dynamic "abort_incomplete_multipart_upload" {
        for_each = rule.value.abort_incomplete_multipart_upload != null ? [rule.value.abort_incomplete_multipart_upload] : []
        content {
          days_after_initiation = abort_incomplete_multipart_upload.value.days_after_initiation
        }
      }
    }
  }

  depends_on = [aws_s3_bucket_versioning.this]
}

# CORS configuration - Enable for buckets that need browser uploads
resource "aws_s3_bucket_cors_configuration" "this" {
  bucket = aws_s3_bucket.this.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "POST", "PUT", "HEAD"]
    allowed_origins = ["*"] # In production, restrict to specific origins
    expose_headers  = ["ETag"]
    max_age_seconds = 3000
  }
}

# Notification configuration - Optional
resource "aws_s3_bucket_notification" "this" {
  count  = var.notification_topic_arn != "" ? 1 : 0
  bucket = aws_s3_bucket.this.id

  topic {
    topic_arn     = var.notification_topic_arn
    events        = var.notification_events
    filter_prefix = var.notification_filter_prefix
  }
}

# Access logging - Optional but recommended
resource "aws_s3_bucket_logging" "this" {
  count         = var.access_log_bucket != "" ? 1 : 0
  bucket        = aws_s3_bucket.this.id
  target_bucket = var.access_log_bucket
  target_prefix = var.access_log_prefix
}

# Cross-region replication - Optional
resource "aws_s3_bucket" "replica" {
  count    = var.enable_cross_region_replication ? 1 : 0
  provider = aws.replica
  bucket   = "${var.bucket_name}-replica"

  tags = merge(var.tags, {
    Name        = "${var.bucket_name}-replica"
    Environment = var.environment
    Purpose     = "${var.purpose}-replica"
  })
}

resource "aws_s3_bucket_versioning" "replica" {
  count    = var.enable_cross_region_replication ? 1 : 0
  provider = aws.replica
  bucket   = aws_s3_bucket.replica[0].id
  versioning_configuration {
    status = "Enabled"
  }
}


resource "aws_s3_bucket_server_side_encryption_configuration" "replica" {
  count    = var.enable_cross_region_replication ? 1 : 0
  provider = aws.replica
  bucket   = aws_s3_bucket.replica[0].id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = var.kms_key_arn
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "replica" {
  count    = var.enable_cross_region_replication ? 1 : 0
  provider = aws.replica
  bucket   = aws_s3_bucket.replica[0].id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# CKV2_AWS_61: Lifecycle configuration for replica bucket
resource "aws_s3_bucket_lifecycle_configuration" "replica" {
  count    = var.enable_cross_region_replication ? 1 : 0
  provider = aws.replica
  bucket   = aws_s3_bucket.replica[0].id

  rule {
    id     = "replica_lifecycle_rule"
    status = "Enabled"

    filter {
      prefix = ""
    }

    # Delete incomplete multipart uploads after 7 days (CKV_AWS_300)
    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }

    # Expire old versions
    noncurrent_version_expiration {
      noncurrent_days = var.noncurrent_version_expiration_days
    }
  }

  depends_on = [aws_s3_bucket_versioning.replica]
}

# CKV_AWS_18: Access logging for replica bucket
resource "aws_s3_bucket_logging" "replica" {
  count    = var.enable_cross_region_replication && var.access_log_bucket != "" ? 1 : 0
  provider = aws.replica
  bucket   = aws_s3_bucket.replica[0].id

  target_bucket = var.access_log_bucket
  target_prefix = "replica-access-logs/${var.bucket_name}-replica/"
}

# CKV2_AWS_62: Event notifications for replica bucket
resource "aws_s3_bucket_notification" "replica" {
  count    = var.enable_cross_region_replication && var.notification_topic_arn != "" ? 1 : 0
  provider = aws.replica
  bucket   = aws_s3_bucket.replica[0].id

  topic {
    topic_arn = var.notification_topic_arn
    events    = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
  }
}

# IAM role for replication
resource "aws_iam_role" "replication" {
  count = var.enable_cross_region_replication ? 1 : 0
  name  = "${var.bucket_name}-replication-role"

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

  tags = var.tags
}

resource "aws_iam_role_policy" "replication" {
  count = var.enable_cross_region_replication ? 1 : 0
  name  = "${var.bucket_name}-replication-policy"
  role  = aws_iam_role.replication[0].id

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
          aws_s3_bucket.this.arn,
          aws_s3_bucket.replica[0].arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObjectVersionForReplication",
          "s3:GetObjectVersionAcl",
          "s3:GetObjectVersionTagging"
        ]
        Resource = [
          "${aws_s3_bucket.this.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ReplicateObject",
          "s3:ReplicateDelete",
          "s3:ReplicateTags"
        ]
        Resource = [
          "${aws_s3_bucket.replica[0].arn}/*"
        ]
      }
    ]
  })
}

# Replication configuration
resource "aws_s3_bucket_replication_configuration" "this" {
  count  = var.enable_cross_region_replication ? 1 : 0
  bucket = aws_s3_bucket.this.id
  role   = aws_iam_role.replication[0].arn

  rule {
    id     = "replicate-all"
    status = "Enabled"

    filter {}

    destination {
      bucket        = aws_s3_bucket.replica[0].arn
      storage_class = "STANDARD"
    }
  }

  depends_on = [
    aws_s3_bucket_versioning.this,
    aws_s3_bucket_versioning.replica
  ]
}

# Upload static files to the bucket
resource "aws_s3_object" "static_files" {
  for_each = {
    for idx, file in var.static_files : file.s3_key => file
  }

  bucket       = aws_s3_bucket.this.id
  key          = each.value.s3_key
  source       = each.value.source_path
  content_type = each.value.content_type != null ? each.value.content_type : null

  # Use file hash to detect changes and trigger updates
  etag = filemd5(each.value.source_path)

  # Server-side encryption inherited from bucket configuration

  tags = merge(var.tags, {
    Name    = each.value.s3_key
    Purpose = "StaticFile"
    Source  = each.value.source_path
  })

  depends_on = [aws_s3_bucket.this]
}
