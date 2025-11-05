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
  description = "Worker type (G.1X, G.2X, G.4X, G.8X, Standard, or Z.2X)"
  type        = string
  default     = "G.1X"
}

variable "number_of_workers" {
  description = "Number of workers (for Standard worker type, use max_capacity instead)"
  type        = number
  default     = null
}

variable "max_capacity" {
  description = "Maximum number of DPUs (Data Processing Units) for the job"
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
  description = "ARN of DynamoDB table (optional, for jobs that write to DynamoDB)"
  type        = string
  default     = null
}

variable "kms_key_arn" {
  description = "ARN of KMS key for encryption (optional)"
  type        = string
  default     = null
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

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}

