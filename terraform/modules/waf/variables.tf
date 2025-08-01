# WAF Module Variables
# modules/waf/variables.tf

variable "web_acl_name" {
  description = "Name of the WAF Web ACL"
  type        = string

  validation {
    condition     = length(var.web_acl_name) > 0 && length(var.web_acl_name) <= 128
    error_message = "Web ACL name must be between 1 and 128 characters."
  }
}

variable "scope" {
  description = "Scope of the WAF Web ACL. Valid values: CLOUDFRONT, REGIONAL"
  type        = string
  default     = "REGIONAL"

  validation {
    condition     = contains(["CLOUDFRONT", "REGIONAL"], var.scope)
    error_message = "Scope must be either CLOUDFRONT or REGIONAL."
  }
}

variable "rate_limit" {
  description = "Rate limit for the rate limiting rule (requests per 5 minutes from single IP)"
  type        = number
  default     = 2000

  validation {
    condition     = var.rate_limit >= 100 && var.rate_limit <= 2000000000
    error_message = "Rate limit must be between 100 and 2,000,000,000."
  }
}

variable "tags" {
  description = "Tags to apply to WAF resources"
  type        = map(string)
  default     = {}
}

variable "enable_logging" {
  description = "Whether to enable WAF logging"
  type        = bool
  default     = true
}

variable "log_retention_days" {
  description = "Number of days to retain WAF logs"
  type        = number
  default     = 365

  validation {
    condition = contains([
      1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653
    ], var.log_retention_days)
    error_message = "Log retention days must be a valid CloudWatch Logs retention period."
  }
}

variable "kms_key_arn" {
  description = "ARN of KMS key for log encryption"
  type        = string
  default     = null
}
