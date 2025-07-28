# Base Infrastructure Variables
# variables.tf

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "cosine"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.project_name))
    error_message = "Project name must contain only lowercase letters, numbers, and hyphens."
  }
}

variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
  default     = "staging"

  validation {
    condition = contains([
      "development",
      "staging",
      "production"
    ], var.environment)
    error_message = "Environment must be development, staging, or production."
  }
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Project    = "cosine"
    ManagedBy  = "terraform"
    Repository = "Cosine-Base-Infra"
  }
}

# KMS Configuration
variable "enable_key_rotation" {
  description = "Enable automatic key rotation for KMS keys"
  type        = bool
  default     = true
}

variable "kms_deletion_window_in_days" {
  description = "KMS key deletion window in days"
  type        = number
  default     = 7

  validation {
    condition     = var.kms_deletion_window_in_days >= 7 && var.kms_deletion_window_in_days <= 30
    error_message = "Deletion window must be between 7 and 30 days."
  }
}

variable "kms_key_administrators" {
  description = "List of IAM ARNs that can administer KMS keys"
  type        = list(string)
  default     = []
}

variable "kms_allowed_services" {
  description = "List of AWS services allowed to use KMS keys"
  type        = list(string)
  default = [
    "dynamodb.amazonaws.com",
    "logs.amazonaws.com",
    "s3.amazonaws.com",
    "lambda.amazonaws.com"
  ]
}

# Cognito Configuration
variable "cognito_mfa_configuration" {
  description = "MFA configuration for Cognito User Pool"
  type        = string
  default     = "OPTIONAL"

  validation {
    condition     = contains(["OFF", "ON", "OPTIONAL"], var.cognito_mfa_configuration)
    error_message = "MFA configuration must be OFF, ON, or OPTIONAL."
  }
}

variable "cognito_advanced_security_mode" {
  description = "Advanced security mode for Cognito User Pool"
  type        = string
  default     = "ENFORCED"

  validation {
    condition     = contains(["OFF", "AUDIT", "ENFORCED"], var.cognito_advanced_security_mode)
    error_message = "Advanced security mode must be OFF, AUDIT, or ENFORCED."
  }
}

variable "cognito_callback_urls" {
  description = "List of callback URLs for Cognito User Pool Client"
  type        = list(string)
  default     = ["http://localhost:3000"]
}

variable "cognito_logout_urls" {
  description = "List of logout URLs for Cognito User Pool Client"
  type        = list(string)
  default     = ["http://localhost:3000"]
}

variable "cognito_access_token_validity" {
  description = "Access token validity duration in minutes"
  type        = number
  default     = 60

  validation {
    condition     = var.cognito_access_token_validity >= 5 && var.cognito_access_token_validity <= 1440
    error_message = "Access token validity must be between 5 and 1440 minutes."
  }
}

variable "cognito_id_token_validity" {
  description = "ID token validity duration in minutes"
  type        = number
  default     = 60

  validation {
    condition     = var.cognito_id_token_validity >= 5 && var.cognito_id_token_validity <= 1440
    error_message = "ID token validity must be between 5 and 1440 minutes."
  }
}

variable "cognito_refresh_token_validity" {
  description = "Refresh token validity duration in days"
  type        = number
  default     = 30

  validation {
    condition     = var.cognito_refresh_token_validity >= 1 && var.cognito_refresh_token_validity <= 3650
    error_message = "Refresh token validity must be between 1 and 3650 days."
  }
}

variable "cognito_domain_name" {
  description = "Domain name for Cognito User Pool (optional)"
  type        = string
  default     = ""
}

# DynamoDB Configuration
variable "dynamodb_billing_mode" {
  description = "DynamoDB billing mode"
  type        = string
  default     = "PAY_PER_REQUEST"

  validation {
    condition     = contains(["PROVISIONED", "PAY_PER_REQUEST"], var.dynamodb_billing_mode)
    error_message = "Billing mode must be PROVISIONED or PAY_PER_REQUEST."
  }
}

variable "dynamodb_read_capacity" {
  description = "DynamoDB read capacity units (only used with PROVISIONED billing)"
  type        = number
  default     = 5
}

variable "dynamodb_write_capacity" {
  description = "DynamoDB write capacity units (only used with PROVISIONED billing)"
  type        = number
  default     = 5
}

variable "dynamodb_gsi_read_capacity" {
  description = "DynamoDB GSI read capacity units (only used with PROVISIONED billing)"
  type        = number
  default     = 5
}

variable "dynamodb_gsi_write_capacity" {
  description = "DynamoDB GSI write capacity units (only used with PROVISIONED billing)"
  type        = number
  default     = 5
}

variable "dynamodb_stream_enabled" {
  description = "Enable DynamoDB streams"
  type        = bool
  default     = true
}

variable "dynamodb_stream_view_type" {
  description = "DynamoDB stream view type"
  type        = string
  default     = "NEW_AND_OLD_IMAGES"

  validation {
    condition     = contains(["KEYS_ONLY", "NEW_IMAGE", "OLD_IMAGE", "NEW_AND_OLD_IMAGES"], var.dynamodb_stream_view_type)
    error_message = "Stream view type must be KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, or NEW_AND_OLD_IMAGES."
  }
}

variable "dynamodb_point_in_time_recovery_enabled" {
  description = "Enable point-in-time recovery for DynamoDB tables"
  type        = bool
  default     = true
}

variable "dynamodb_deletion_protection_enabled" {
  description = "Enable deletion protection for DynamoDB tables"
  type        = bool
  default     = false
}

variable "dynamodb_ttl_enabled" {
  description = "Enable TTL for appropriate DynamoDB tables"
  type        = bool
  default     = true
}

variable "dynamodb_ttl_attribute_name" {
  description = "TTL attribute name for DynamoDB tables"
  type        = string
  default     = "expires_at"
}

# CloudWatch Configuration
variable "cloudwatch_security_log_retention_days" {
  description = "CloudWatch security log retention in days"
  type        = number
  default     = 365
}

variable "cloudwatch_auth_log_retention_days" {
  description = "CloudWatch authentication log retention in days"
  type        = number
  default     = 90
}

variable "cloudwatch_application_log_retention_days" {
  description = "CloudWatch application log retention in days"
  type        = number
  default     = 30
}

variable "cloudwatch_lambda_log_retention_days" {
  description = "CloudWatch Lambda log retention in days"
  type        = number
  default     = 14
}

variable "cloudwatch_api_gateway_log_retention_days" {
  description = "CloudWatch API Gateway log retention in days"
  type        = number
  default     = 30
}

variable "cloudwatch_failed_login_threshold" {
  description = "Threshold for failed login attempts alarm"
  type        = number
  default     = 5
}

variable "cloudwatch_suspicious_activity_threshold" {
  description = "Threshold for suspicious activity alarm"
  type        = number
  default     = 10
}

variable "cloudwatch_alarm_notification_topic_arn" {
  description = "SNS topic ARN for CloudWatch alarm notifications (optional)"
  type        = string
  default     = ""
}