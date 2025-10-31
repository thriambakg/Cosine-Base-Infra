# S3 Module Variables - Security Compliant Defaults
# modules/s3/variables.tf

variable "bucket_name" {
  description = "Name of the S3 bucket"
  type        = string
}

variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
}

variable "purpose" {
  description = "Purpose of the S3 bucket"
  type        = string
  default     = "general"
}

variable "tags" {
  description = "Tags to apply to the S3 bucket"
  type        = map(string)
  default     = {}
}

variable "force_destroy" {
  description = "Allow bucket to be destroyed even if it contains objects"
  type        = bool
  default     = false
}

variable "kms_key_arn" {
  description = "ARN of the KMS key for S3 encryption"
  type        = string
}

# Lifecycle configuration variables
variable "abort_incomplete_multipart_upload_days" {
  description = "Days after initiation to abort incomplete multipart uploads (CKV_AWS_300 compliance)"
  type        = number
  default     = 7

  validation {
    condition     = var.abort_incomplete_multipart_upload_days >= 1 && var.abort_incomplete_multipart_upload_days <= 30
    error_message = "Abort incomplete multipart upload days must be between 1 and 30."
  }
}

variable "noncurrent_version_expiration_days" {
  description = "Days to retain noncurrent object versions"
  type        = number
  default     = 30

  validation {
    condition     = var.noncurrent_version_expiration_days >= 1
    error_message = "Noncurrent version expiration days must be at least 1."
  }
}

variable "enable_lifecycle_transitions" {
  description = "Enable automatic transitions to IA and Glacier storage classes"
  type        = bool
  default     = false
}

variable "transition_to_ia_days" {
  description = "Days before transitioning objects to Infrequent Access"
  type        = number
  default     = 30
}

variable "transition_to_glacier_days" {
  description = "Days before transitioning objects to Glacier"
  type        = number
  default     = 90
}

variable "enable_expiration" {
  description = "Enable automatic object expiration"
  type        = bool
  default     = false
}

variable "expiration_days" {
  description = "Days before objects expire and are deleted"
  type        = number
  default     = 365
}

# Notification variables
variable "notification_topic_arn" {
  description = "SNS topic ARN for S3 event notifications (CKV2_AWS_62 compliance)"
  type        = string
  default     = ""
}

variable "notification_events" {
  description = "S3 events to send notifications for"
  type        = list(string)
  default     = ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"]
}

variable "notification_filter_prefix" {
  description = "S3 object key prefix filter for notifications (e.g., 'users/' for user files only)"
  type        = string
  default     = ""
}

# Access logging variables
variable "access_log_bucket" {
  description = "S3 bucket for access logs (CKV_AWS_18 compliance)"
  type        = string
  default     = ""
}

variable "access_log_prefix" {
  description = "Prefix for access log objects"
  type        = string
  default     = "access-logs/"
}

# Cross-region replication variables
variable "enable_cross_region_replication" {
  description = "Enable cross-region replication (CKV_AWS_144 compliance)"
  type        = bool
  default     = false
}

variable "replica_region" {
  description = "AWS region for the replica bucket"
  type        = string
  default     = ""
}

variable "lifecycle_rules" {
  description = "Lifecycle rules for S3 bucket"
  type = list(object({
    id     = string
    status = string
    filter = optional(object({
      prefix = optional(string)
      tags   = optional(map(string))
    }))
    expiration = optional(object({
      days = number
    }))
    noncurrent_version_expiration = optional(object({
      noncurrent_days = number
    }))
    abort_incomplete_multipart_upload = optional(object({
      days_after_initiation = number
    }))
  }))
  default = []
}

# Public Access Block Configuration
variable "block_public_acls" {
  description = "Whether Amazon S3 should block public ACLs for this bucket"
  type        = bool
  default     = true
}

variable "ignore_public_acls" {
  description = "Whether Amazon S3 should ignore public ACLs for this bucket"
  type        = bool
  default     = true
}

variable "block_public_policy" {
  description = "Whether Amazon S3 should block public bucket policies for this bucket"
  type        = bool
  default     = true
}

variable "restrict_public_buckets" {
  description = "Whether Amazon S3 should restrict public bucket policies for this bucket"
  type        = bool
  default     = true
}

variable "allow_cloudfront_oac" {
  description = "Allow CloudFront Origin Access Control by setting block_public_policy and restrict_public_buckets to false"
  type        = bool
  default     = false
}

# Static file uploads
variable "static_files" {
  description = "List of static files to upload to the bucket. Each file object should have source_path (relative to Terraform root), s3_key (destination key), and optional content_type"
  type = list(object({
    source_path  = string
    s3_key       = string
    content_type = optional(string, null)
  }))
  default = []
}
