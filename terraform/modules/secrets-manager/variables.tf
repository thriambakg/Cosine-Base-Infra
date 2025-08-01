# Secrets Manager Module Variables
# modules/secrets-manager/variables.tf

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name (e.g., dev, staging, prod)"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}

variable "secrets" {
  description = "Map of secrets to create"
  type = map(object({
    description = string
    secret_data = map(string)
  }))
  default = {}
}

variable "kms_key_id" {
  description = "KMS key ID for encrypting secrets (optional)"
  type        = string
  default     = null
}

variable "recovery_window_days" {
  description = "Number of days to recover deleted secrets"
  type        = number
  default     = 7
  validation {
    condition     = var.recovery_window_days >= 0 && var.recovery_window_days <= 30
    error_message = "Recovery window must be between 0 and 30 days."
  }
}

variable "automatic_rotation" {
  description = "Configuration for automatic secret rotation"
  type = map(object({
    rotation_lambda_arn = string
    rotation_rules = object({
      automatically_after_days = number
    })
  }))
  default = {}

  validation {
    condition = alltrue([
      for config in var.automatic_rotation :
      config.rotation_rules.automatically_after_days <= 90
    ])
    error_message = "Automatic rotation must be configured for 90 days or less to meet compliance requirements."
  }
}
