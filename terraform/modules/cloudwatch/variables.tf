# CloudWatch Module Variables
# modules/cloudwatch/variables.tf

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

variable "kms_key_id" {
  description = "KMS key ID for CloudWatch Logs encryption"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

# Log retention periods
variable "security_log_retention_days" {
  description = "Retention period for security logs in days"
  type        = number
  default     = 365
}

variable "auth_log_retention_days" {
  description = "Retention period for authentication logs in days"
  type        = number
  default     = 365
}

variable "application_log_retention_days" {
  description = "Retention period for application logs in days"
  type        = number
  default     = 365
}

variable "lambda_log_retention_days" {
  description = "Retention period for Lambda logs in days"
  type        = number
  default     = 365
}

variable "api_gateway_log_retention_days" {
  description = "Retention period for API Gateway logs in days"
  type        = number
  default     = 365
}

# Dashboard configuration
variable "cognito_user_pool_id" {
  description = "Cognito User Pool ID for dashboard metrics"
  type        = string
  default     = ""
}

variable "user_profiles_table_name" {
  description = "DynamoDB user profiles table name for dashboard metrics"
  type        = string
  default     = ""
}

# Alarm configuration
variable "failed_login_threshold" {
  description = "Threshold for failed login attempts alarm"
  type        = number
  default     = 10
}

variable "suspicious_activity_threshold" {
  description = "Threshold for suspicious activity alarm"
  type        = number
  default     = 1
}

variable "alarm_notification_topic_arn" {
  description = "SNS topic ARN for alarm notifications"
  type        = string
  default     = ""
}
