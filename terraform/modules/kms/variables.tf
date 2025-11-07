# KMS Module Variables
# modules/kms/variables.tf

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "enable_key_rotation" {
  description = "Enable automatic key rotation"
  type        = bool
  default     = true
}

variable "deletion_window_in_days" {
  description = "Number of days before key deletion (7-30)"
  type        = number
  default     = 7

  validation {
    condition     = var.deletion_window_in_days >= 7 && var.deletion_window_in_days <= 30
    error_message = "Deletion window must be between 7 and 30 days."
  }
}

variable "key_administrators" {
  description = "List of ARNs that can administer the KMS keys"
  type        = list(string)
  default     = []
}

variable "allowed_services" {
  description = "List of AWS services allowed to use the keys"
  type        = list(string)
  default = [
    "lambda.amazonaws.com",
    "dynamodb.amazonaws.com",
    "logs.amazonaws.com",
    "s3.amazonaws.com",
    "cloudfront.amazonaws.com",
    "glue.amazonaws.com"
  ]
}

variable "additional_role_arns" {
  description = "List of additional IAM role ARNs that need KMS access (e.g., Glue job roles)"
  type        = list(string)
  default     = []
}
