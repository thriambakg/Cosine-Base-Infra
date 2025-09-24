# EventBridge Scheduler Module Variables
# modules/eventbridge-scheduler/variables.tf

variable "rule_name" {
  description = "Name of the EventBridge rule"
  type        = string
}

variable "rule_description" {
  description = "Description of the EventBridge rule"
  type        = string
  default     = ""
}

variable "schedule_expression" {
  description = "Schedule expression for the rule (e.g., 'rate(5 minutes)', 'cron(0 12 * * ? *)')"
  type        = string
}

variable "enabled" {
  description = "Whether the scheduler is enabled"
  type        = bool
  default     = true
}

variable "target_arn" {
  description = "ARN of the target resource (Lambda, SQS, SNS, etc.)"
  type        = string
}

variable "target_id" {
  description = "Unique identifier for the target"
  type        = string
}

variable "target_type" {
  description = "Type of target (lambda, sqs, sns, etc.)"
  type        = string
  default     = "lambda"
}

variable "target_function_name" {
  description = "Name of the Lambda function (required when target_type is lambda)"
  type        = string
  default     = null
}

variable "target_input" {
  description = "Input to pass to the target (JSON string)"
  type        = string
  default     = null
}

variable "target_role_arn" {
  description = "IAM role ARN for cross-account access (optional)"
  type        = string
  default     = null
}

variable "purpose" {
  description = "Purpose of the scheduler for tagging"
  type        = string
  default     = "ScheduledExecution"
}

variable "environment" {
  description = "Environment name for tagging"
  type        = string
  default     = ""
}

variable "tags" {
  description = "Additional tags to apply to resources"
  type        = map(string)
  default     = {}
}
