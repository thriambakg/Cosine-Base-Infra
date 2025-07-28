# Enterprise Extensions for Base Infrastructure
# Add these variables to your existing variables.tf for enterprise features

# ====================================
# ENTERPRISE IDENTITY PROVIDER SUPPORT
# ====================================

# SAML Identity Providers (Enterprise SSO)
variable "enable_saml_providers" {
  description = "Enable SAML identity providers for enterprise SSO"
  type        = bool
  default     = false
}

variable "saml_providers" {
  description = "SAML identity providers configuration"
  type = map(object({
    metadata_url         = optional(string)
    metadata_file        = optional(string)
    display_name         = string
    attribute_mapping    = optional(map(string), {})
  }))
  default = {}
}

# OIDC Identity Providers (Additional Enterprise Options)
variable "enable_oidc_providers" {
  description = "Enable OIDC identity providers"
  type        = bool
  default     = false
}

variable "oidc_providers" {
  description = "OIDC identity providers configuration"
  type = map(object({
    issuer_url           = string
    client_id            = string
    client_secret        = string
    display_name         = string
    authorize_scopes     = optional(string, "email openid profile")
    attribute_mapping    = optional(map(string), {})
  }))
  default   = {}
  sensitive = true
}

# ====================================
# ADVANCED SECURITY & COMPLIANCE
# ====================================

# Account Takeover Protection
variable "enable_account_takeover_protection" {
  description = "Enable advanced account takeover protection"
  type        = bool
  default     = false
}

variable "account_takeover_protection_config" {
  description = "Account takeover protection configuration"
  type = object({
    notify_configuration = object({
      block_email = object({
        subject   = string
        html_body = string
      })
      mfa_email = optional(object({
        subject   = string
        html_body = string
      }))
    })
    actions = object({
      low_action    = string
      medium_action = string
      high_action   = string
    })
  })
  default = {
    notify_configuration = {
      block_email = {
        subject   = "Account Security Alert"
        html_body = "We detected and blocked a suspicious sign-in attempt to your account."
      }
    }
    actions = {
      low_action    = "NO_ACTION"
      medium_action = "MFA_IF_CONFIGURED"
      high_action   = "BLOCK"
    }
  }
}

# Enhanced Password Policy
variable "use_enterprise_password_policy" {
  description = "Use enhanced password policy for enterprise compliance"
  type        = bool
  default     = false
}

variable "enterprise_password_policy" {
  description = "Enhanced password policy configuration"
  type = object({
    minimum_length                   = number
    require_lowercase                = bool
    require_numbers                  = bool
    require_symbols                  = bool
    require_uppercase                = bool
    temporary_password_validity_days = number
    password_history_size           = optional(number, 24)
  })
  default = {
    minimum_length                   = 12
    require_lowercase                = true
    require_numbers                  = true
    require_symbols                  = true
    require_uppercase                = true
    temporary_password_validity_days = 1
    password_history_size           = 24
  }
}

# ====================================
# MULTI-TENANT SUPPORT
# ====================================

variable "enable_multi_tenant" {
  description = "Enable multi-tenant architecture"
  type        = bool
  default     = false
}

variable "tenant_isolation_mode" {
  description = "Tenant isolation strategy"
  type        = string
  default     = "user_groups"
  validation {
    condition     = contains(["user_groups", "user_pool_per_tenant"], var.tenant_isolation_mode)
    error_message = "Tenant isolation mode must be user_groups or user_pool_per_tenant."
  }
}

# ====================================
# COMPLIANCE & GOVERNANCE
# ====================================

variable "compliance_mode" {
  description = "Compliance framework requirements"
  type        = string
  default     = "standard"
  validation {
    condition     = contains(["standard", "sox", "pci", "hipaa", "fedramp"], var.compliance_mode)
    error_message = "Compliance mode must be one of: standard, sox, pci, hipaa, fedramp."
  }
}

variable "enable_detailed_audit_logging" {
  description = "Enable comprehensive audit logging for compliance"
  type        = bool
  default     = false
}

variable "data_residency_requirements" {
  description = "Data residency and sovereignty requirements"
  type = object({
    required_region     = optional(string, "")
    cross_border_data   = optional(bool, true)
    encryption_required = optional(bool, true)
  })
  default = {}
}

# ====================================
# PERFORMANCE & SCALING
# ====================================

variable "expected_peak_rps" {
  description = "Expected peak requests per second for capacity planning"
  type        = number
  default     = 100
}

variable "expected_monthly_active_users" {
  description = "Expected monthly active users"
  type        = number
  default     = 10000
}

# ====================================
# DISASTER RECOVERY
# ====================================

variable "enable_cross_region_backup" {
  description = "Enable cross-region backup for disaster recovery"
  type        = bool
  default     = false
}

variable "backup_regions" {
  description = "Additional regions for backup and disaster recovery"
  type        = list(string)
  default     = []
}

variable "disaster_recovery_rto_minutes" {
  description = "Recovery Time Objective in minutes"
  type        = number
  default     = 60
}

variable "disaster_recovery_rpo_minutes" {
  description = "Recovery Point Objective in minutes"
  type        = number
  default     = 15
}
