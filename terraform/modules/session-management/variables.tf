# ============================================================================
# SESSION MANAGEMENT MODULE VARIABLES
# ============================================================================

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name (development, staging, production)"
  type        = string
}

variable "kms_key_arn" {
  description = "ARN of the KMS key for encryption"
  type        = string
}

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

# ============================================================================
# OPTIONAL CONFIGURATION VARIABLES
# ============================================================================

variable "session_ttl_days" {
  description = "Number of days to keep session data (TTL)"
  type        = number
  default     = 30
}

variable "context_ttl_days" {
  description = "Number of days to keep context data (TTL)"
  type        = number
  default     = 7
}

variable "enable_point_in_time_recovery" {
  description = "Enable point-in-time recovery for DynamoDB tables"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Number of days to retain CloudWatch logs"
  type        = number
  default     = 14
}
