# SNS Module Variables
# modules/sns/variables.tf

variable "topic_name" {
  description = "Name of the SNS topic"
  type        = string

  validation {
    condition     = length(var.topic_name) > 0 && length(var.topic_name) <= 256
    error_message = "Topic name must be between 1 and 256 characters."
  }
}

variable "display_name" {
  description = "Display name for the SNS topic"
  type        = string
  default     = null
}

variable "kms_key_arn" {
  description = "ARN of KMS key for encryption. If not provided, default encryption will be used."
  type        = string
  default     = null
}

variable "purpose" {
  description = "Purpose/description of the SNS topic"
  type        = string
  default     = "General notifications"
}

variable "tags" {
  description = "Tags to apply to SNS topic"
  type        = map(string)
  default     = {}
}

# Permissions configuration
variable "allow_s3_publish" {
  description = "Allow S3 buckets to publish to this topic"
  type        = bool
  default     = false
}

variable "s3_bucket_arns" {
  description = "List of S3 bucket ARNs allowed to publish to this topic (only used if allow_s3_publish is true)"
  type        = list(string)
  default     = null
}

variable "allow_cloudwatch_publish" {
  description = "Allow CloudWatch to publish to this topic"
  type        = bool
  default     = false
}

variable "allow_lambda_publish" {
  description = "Allow Lambda functions to publish to this topic"
  type        = bool
  default     = false
}

# Subscription configuration
variable "email_addresses" {
  description = "List of email addresses to subscribe to the topic"
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for email in var.email_addresses : can(regex("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", email))
    ])
    error_message = "All email addresses must be valid email format."
  }
}

variable "phone_numbers" {
  description = "List of phone numbers (E.164 format) to subscribe to the topic"
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for phone in var.phone_numbers : can(regex("^\\+[1-9]\\d{1,14}$", phone))
    ])
    error_message = "All phone numbers must be in E.164 format (e.g., +1234567890)."
  }
}

variable "sqs_subscriptions" {
  description = "Map of SQS subscriptions to create"
  type = map(object({
    queue_arn     = string
    filter_policy = optional(map(any))
  }))
  default = {}
}

variable "lambda_function_arns" {
  description = "List of Lambda function ARNs to subscribe to the topic"
  type        = list(string)
  default     = []
}

variable "http_endpoints" {
  description = "List of HTTP endpoints to subscribe to the topic"
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for url in var.http_endpoints : can(regex("^http://", url))
    ])
    error_message = "All HTTP endpoints must start with http://."
  }
}

variable "https_endpoints" {
  description = "List of HTTPS endpoints to subscribe to the topic"
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for url in var.https_endpoints : can(regex("^https://", url))
    ])
    error_message = "All HTTPS endpoints must start with https://."
  }
}
