# Cognito Module Variables
# modules/cognito/variables.tf

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

variable "mfa_configuration" {
  description = "MFA configuration for the user pool (OFF, ON, OPTIONAL)"
  type        = string
  default     = "OPTIONAL"

  validation {
    condition     = contains(["OFF", "ON", "OPTIONAL"], var.mfa_configuration)
    error_message = "MFA configuration must be OFF, ON, or OPTIONAL."
  }
}

variable "advanced_security_mode" {
  description = "Advanced security mode for the user pool (OFF, AUDIT, ENFORCED)"
  type        = string
  default     = "AUDIT"

  validation {
    condition     = contains(["OFF", "AUDIT", "ENFORCED"], var.advanced_security_mode)
    error_message = "Advanced security mode must be OFF, AUDIT, or ENFORCED."
  }
}

variable "callback_urls" {
  description = "List of allowed callback URLs for the user pool client"
  type        = list(string)
  default = [
    "http://localhost:3000/api/auth/callback",
    "https://localhost:3000/api/auth/callback"
  ]
}

variable "logout_urls" {
  description = "List of allowed logout URLs for the user pool client"
  type        = list(string)
  default = [
    "http://localhost:3000",
    "https://localhost:3000"
  ]
}

variable "access_token_validity" {
  description = "Time limit for access tokens in minutes"
  type        = number
  default     = 60
}

variable "id_token_validity" {
  description = "Time limit for ID tokens in minutes"
  type        = number
  default     = 60
}

variable "refresh_token_validity" {
  description = "Time limit for refresh tokens in days"
  type        = number
  default     = 30
}

variable "domain_name" {
  description = "Domain name for the Cognito user pool (optional)"
  type        = string
  default     = ""
}
