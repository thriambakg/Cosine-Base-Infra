# Cognito Module Variables
# modules/cognito/variables.tf

# Basic Configuration
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

# Cognito User Pool Configuration
variable "mfa_configuration" {
  description = "Multi-factor authentication configuration"
  type        = string
  default     = "OPTIONAL"
  validation {
    condition     = contains(["OFF", "ON", "OPTIONAL"], var.mfa_configuration)
    error_message = "MFA configuration must be OFF, ON, or OPTIONAL."
  }
}

variable "advanced_security_mode" {
  description = "Advanced security mode for Cognito User Pool"
  type        = string
  default     = "ENFORCED"
  validation {
    condition     = contains(["OFF", "AUDIT", "ENFORCED"], var.advanced_security_mode)
    error_message = "Advanced security mode must be OFF, AUDIT, or ENFORCED."
  }
}

# Cognito User Pool Client Configuration
variable "callback_urls" {
  description = "List of allowed callback URLs for the client"
  type        = list(string)
  default     = ["http://localhost:3000"]
}

variable "logout_urls" {
  description = "List of allowed logout URLs for the client"
  type        = list(string)
  default     = ["http://localhost:3000"]
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

# Domain Configuration
variable "domain_name" {
  description = "Domain name for Cognito hosted UI (optional)"
  type        = string
  default     = ""
}

# Google Identity Provider Configuration
variable "enable_google_provider" {
  description = "Enable Google identity provider"
  type        = bool
  default     = false
}

variable "google_client_id" {
  description = "Google OAuth client ID"
  type        = string
  default     = ""
  sensitive   = true
}

variable "google_client_secret" {
  description = "Google OAuth client secret"
  type        = string
  default     = ""
  sensitive   = true
}



# Secrets Manager Integration
variable "use_secrets_manager" {
  description = "Retrieve OAuth credentials from Secrets Manager instead of variables"
  type        = bool
  default     = false
}

variable "secrets_manager_secret_name" {
  description = "Name of the Secrets Manager secret containing OAuth credentials"
  type        = string
  default     = ""
}

# Email Template Configuration
variable "verification_link_placeholder" {
  description = "Cognito placeholder for email verification link"
  type        = string
  default     = "{##Verify My Email Address##}"
}

# Lambda Trigger Configuration
variable "post_authentication_lambda_arn" {
  description = "ARN of the Lambda function to trigger after authentication"
  type        = string
  default     = ""
}


