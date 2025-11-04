variable "job_name" {
  description = "Name of the Glue job"
  type        = string
}

variable "description" {
  description = "Description of the Glue job"
  type        = string
  default     = "Glue ETL job created by Terraform"
}

variable "script_location" {
  description = "S3 path to the Glue script (s3://bucket/path/to/script.py). If source_code_dir is provided, this will be auto-constructed."
  type        = string
  default     = null
}

variable "python_version" {
  description = "Python version for Glue job (default: 3.9)"
  type        = string
  default     = "3"
}

variable "glue_version" {
  description = "Glue version (e.g., '4.0', '3.0')"
  type        = string
  default     = "4.0"
}

variable "max_retries" {
  description = "Maximum number of retries for the job"
  type        = number
  default     = 0
}

variable "timeout" {
  description = "Job timeout in minutes"
  type        = number
  default     = 60
}

variable "max_capacity" {
  description = "Maximum number of DPUs (Data Processing Units) for standard workers. Set to null to use worker_type instead"
  type        = number
  default     = null
}

variable "worker_type" {
  description = "Type of worker (G.1X, G.2X, G.4X, G.8X, Standard, Z.2X). Required if max_capacity is null"
  type        = string
  default     = null
}

variable "number_of_workers" {
  description = "Number of workers for G.1X, G.2X, G.4X, G.8X, or Z.2X workers. Not used with Standard worker type"
  type        = number
  default     = null
}

variable "default_arguments" {
  description = "Default arguments for the Glue job (key-value pairs)"
  type        = map(string)
  default     = {}
}

variable "environment_variables" {
  description = "Environment variables for the Glue job"
  type        = map(string)
  default     = {}
}

# IAM and Permissions
variable "s3_bucket_ids" {
  description = "List of S3 bucket IDs/names that the job needs access to (for IAM policies)"
  type        = list(string)
  default     = []
}

variable "s3_paths" {
  description = "List of S3 path patterns the job needs access to (e.g., ['trades/*', 'lists/*'])"
  type        = list(string)
  default     = []
}

variable "dynamodb_table_names" {
  description = "List of DynamoDB table names that the job needs access to (for IAM policies)"
  type        = list(string)
  default     = []
}

variable "additional_policy_statements" {
  description = "Additional IAM policy statements (JSON) for the Glue job role"
  type = list(object({
    Effect   = string
    Action   = list(string)
    Resource = list(string)
  }))
  default = []
}

variable "kms_key_arns" {
  description = "List of KMS key ARNs the job needs access to (for encryption)"
  type        = list(string)
  default     = []
}

# Source Code Management
variable "source_code_dir" {
  description = "Local directory containing the Glue job script (will be uploaded to S3)"
  type        = string
  default     = null
}

variable "script_s3_prefix" {
  description = "S3 prefix/folder where the script will be stored (e.g., 'glue-scripts/')"
  type        = string
  default     = "glue-scripts"
}

variable "script_s3_bucket_id" {
  description = "S3 bucket ID where the script will be stored. If null, uses first bucket from s3_bucket_ids"
  type        = string
  default     = null
}

# Tags
variable "tags" {
  description = "A map of tags to assign to the resource"
  type        = map(string)
  default     = {}
}

# CloudWatch Logs
variable "enable_cloudwatch_logs" {
  description = "Enable CloudWatch logs for the Glue job"
  type        = bool
  default     = true
}

variable "log_group_name" {
  description = "CloudWatch log group name (defaults to /aws-glue/jobs/{job_name})"
  type        = string
  default     = null
}

variable "log_retention_days" {
  description = "Number of days to retain CloudWatch logs"
  type        = number
  default     = 7
}

