# Lambda Module Variables - Security Compliant Defaults
# modules/lambda/variables.tf

variable "function_name" {
  description = "Name of the Lambda function"
  type        = string
}

variable "purpose" {
  description = "Purpose of the Lambda function"
  type        = string
  default     = "general"
}

variable "tags" {
  description = "Tags to apply to Lambda resources"
  type        = map(string)
  default     = {}
}

# Function configuration
variable "handler" {
  description = "Function entrypoint in your code"
  type        = string
  default     = "index.handler"
}

variable "runtime" {
  description = "Runtime for the Lambda function"
  type        = string
  default     = "python3.11"

  validation {
    condition = contains([
      "python3.8", "python3.9", "python3.10", "python3.11", "python3.12",
      "nodejs18.x", "nodejs20.x",
      "java11", "java17", "java21",
      "dotnet6", "dotnet8",
      "go1.x",
      "ruby3.2", "ruby3.3"
    ], var.runtime)
    error_message = "Runtime must be a supported AWS Lambda runtime."
  }
}

variable "timeout" {
  description = "Amount of time your Lambda function has to run in seconds"
  type        = number
  default     = 30

  validation {
    condition     = var.timeout >= 1 && var.timeout <= 900
    error_message = "Timeout must be between 1 and 900 seconds."
  }
}

variable "memory_size" {
  description = "Amount of memory in MB your Lambda function can use at runtime"
  type        = number
  default     = 128

  validation {
    condition     = var.memory_size >= 128 && var.memory_size <= 10240
    error_message = "Memory size must be between 128 and 10240 MB."
  }
}

variable "publish" {
  description = "Whether to publish creation/change as new Lambda Function Version"
  type        = bool
  default     = false
}

# Source code
variable "filename" {
  description = "Path to the function's deployment package within the local filesystem"
  type        = string
  default     = null
}

variable "source_code_hash" {
  description = "Used to trigger updates. Must be set to a base64-encoded SHA256 hash of the package file"
  type        = string
  default     = null
}

variable "layers" {
  description = "List of Lambda Layer Version ARNs to attach to your Lambda Function"
  type        = list(string)
  default     = []
}

# Environment
variable "environment_variables" {
  description = "Map of environment variables for the Lambda function"
  type        = map(string)
  default     = {}
  sensitive   = true
}

# Encryption
variable "lambda_kms_key_arn" {
  description = "ARN of KMS key for Lambda environment variable encryption"
  type        = string
  default     = ""
}

variable "cloudwatch_kms_key_arn" {
  description = "ARN of KMS key for CloudWatch logs encryption"
  type        = string
}

# Logging (CKV_AWS_338 compliance)
variable "log_retention_days" {
  description = "CloudWatch log retention in days (minimum 365 for compliance)"
  type        = number
  default     = 365

  validation {
    condition     = var.log_retention_days >= 365
    error_message = "Log retention must be at least 365 days for security compliance (CKV_AWS_338)."
  }
}

# VPC configuration
variable "vpc_config" {
  description = "VPC configuration for the Lambda function"
  type = object({
    subnet_ids         = list(string)
    security_group_ids = list(string)
  })
  default = null
}

# Dead letter queue
variable "dead_letter_queue_arn" {
  description = "ARN of the SQS queue or SNS topic for dead letter queue"
  type        = string
  default     = ""
}

# Tracing
variable "tracing_mode" {
  description = "Tracing mode for Lambda function (Active or PassThrough)"
  type        = string
  default     = "PassThrough"

  validation {
    condition     = contains(["Active", "PassThrough"], var.tracing_mode)
    error_message = "Tracing mode must be Active or PassThrough."
  }
}

# Concurrency
variable "reserved_concurrent_executions" {
  description = "Amount of reserved concurrent executions for this Lambda function"
  type        = number
  default     = -1
}

# Custom IAM policy
variable "custom_policy_json" {
  description = "JSON policy document for additional Lambda permissions"
  type        = string
  default     = ""
}

# Aliasing
variable "create_alias" {
  description = "Whether to create a Lambda alias"
  type        = bool
  default     = false
}

variable "alias_name" {
  description = "Name of the Lambda alias"
  type        = string
  default     = "live"
}

# Monitoring
variable "enable_error_monitoring" {
  description = "Enable CloudWatch error monitoring and metrics"
  type        = bool
  default     = true
}

variable "error_threshold" {
  description = "Error count threshold for CloudWatch alarm"
  type        = number
  default     = 5
}

variable "alarm_sns_topic_arn" {
  description = "SNS topic ARN for CloudWatch alarms"
  type        = string
  default     = ""
}

# Code Signing Configuration - CKV_AWS_272
variable "code_signing_config_arn" {
  description = "ARN of the Code Signing Config"
  type        = string
  default     = null
}
