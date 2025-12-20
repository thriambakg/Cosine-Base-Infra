variable "state_machine_name" {
  description = "Name of the Step Functions state machine"
  type        = string
}

variable "definition" {
  description = "JSON definition of the state machine"
  type        = string
}

variable "lambda_function_arns" {
  description = "List of Lambda function ARNs that the state machine can invoke"
  type        = list(string)
  default     = []
}

variable "glue_job_names" {
  description = "List of Glue job names that the state machine can invoke"
  type        = list(string)
  default     = []
}

variable "additional_policy_arns" {
  description = "Additional IAM policy ARNs to attach to the Step Functions role"
  type        = list(string)
  default     = []
}

variable "environment" {
  description = "Environment name (development, staging, production)"
  type        = string
}

variable "log_level" {
  description = "Logging level for Step Functions (ALL, ERROR, FATAL, OFF)"
  type        = string
  default     = "ERROR"
}

variable "log_retention_days" {
  description = "Number of days to retain Step Functions logs"
  type        = number
  default     = 7
}

variable "include_execution_data" {
  description = "Whether to include execution data in logs"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Additional tags for resources"
  type        = map(string)
  default     = {}
}

variable "enable_s3_history" {
  description = "Enable S3 execution history logging to avoid 25k event limit"
  type        = bool
  default     = false
}

variable "s3_bucket_id" {
  description = "S3 bucket ID for execution history (required if enable_s3_history is true)"
  type        = string
  default     = ""
}

