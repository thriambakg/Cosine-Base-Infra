variable "job_name" {
  description = "Name of the Glue job"
  type        = string
}

variable "script_location" {
  description = "S3 path to the Glue job script (e.g., s3://bucket/path/to/script.py)"
  type        = string
}

variable "python_version" {
  description = "Python version for the Glue job (3 or 3.9)"
  type        = string
  default     = "3"
}

variable "glue_version" {
  description = "Glue version (e.g., 4.0, 3.0, 2.0)"
  type        = string
  default     = "4.0"
}

variable "max_retries" {
  description = "Maximum number of retries for the job"
  type        = number
  default     = 1
}

variable "timeout" {
  description = "Job timeout in minutes"
  type        = number
  default     = 2880 # 48 hours
}

variable "worker_type" {
  description = "Worker type (G.1X, G.2X, G.4X, G.8X, Standard, or Z.2X). If set, max_capacity must be null."
  type        = string
  default     = null
}

variable "number_of_workers" {
  description = "Number of workers (only used when worker_type is set). Required when worker_type is G.1X, G.2X, G.4X, G.8X, or Z.2X."
  type        = number
  default     = null
}

variable "max_capacity" {
  description = "Maximum number of DPUs (Data Processing Units) for the job. Use this when worker_type is null (Standard worker type)."
  type        = number
  default     = null
}

variable "job_bookmark_option" {
  description = "Job bookmark option (job-bookmark-enable, job-bookmark-disable, job-bookmark-pause)"
  type        = string
  default     = "job-bookmark-disable"
}

variable "s3_bucket_arn" {
  description = "ARN of the primary S3 bucket for data access"
  type        = string
}

variable "additional_s3_bucket_arns" {
  description = "List of additional S3 bucket ARNs for jobs that need access to multiple buckets"
  type        = list(string)
  default     = null
}

variable "spark_logs_bucket" {
  description = "S3 bucket name (ID) for Spark UI logs"
  type        = string
}

variable "temp_bucket" {
  description = "S3 bucket name (ID) for temporary files"
  type        = string
}

variable "dynamodb_table_arn" {
  description = "ARN of DynamoDB table (optional, for jobs that write to DynamoDB). If provided, must be known at plan time (not from module output) to avoid count evaluation errors."
  type        = string
  default     = null

  # Note: When this is set from a module output (unknown at plan time), Terraform cannot
  # evaluate count expressions that depend on it. Workaround: Use -target to create the table first.
}

variable "kms_key_arn" {
  description = "ARN of KMS key for encryption (optional, deprecated - use additional_kms_key_arns instead)"
  type        = string
  default     = null
}

variable "additional_kms_key_arns" {
  description = "List of additional KMS key ARNs for encryption (e.g., DynamoDB key, S3 key, etc.)"
  type        = list(string)
  default     = []
}

variable "default_arguments" {
  description = "Additional default arguments for the Glue job"
  type        = map(string)
  default     = {}
}

variable "additional_policies" {
  description = "Map of additional IAM policies to create and attach (key = policy name, value = policy JSON document)"
  type        = map(string)
  default     = {}
}

variable "additional_policy_arns" {
  description = "List of additional IAM policy ARNs to attach (for shared/managed policies)"
  type        = list(string)
  default     = []
}

variable "concurrent_executions" {
  description = "Maximum number of concurrent runs for this Glue job (default: 1, max: 10)"
  type        = number
  default     = 1
  validation {
    condition     = var.concurrent_executions >= 1 && var.concurrent_executions <= 10
    error_message = "concurrent_executions must be between 1 and 10"
  }
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}
