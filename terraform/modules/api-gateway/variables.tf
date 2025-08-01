# API Gateway Module Variables
# modules/api-gateway/variables.tf

# Basic API Configuration
variable "api_name" {
  description = "Name of the API Gateway"
  type        = string

  validation {
    condition     = length(var.api_name) > 0 && length(var.api_name) <= 255
    error_message = "API name must be between 1 and 255 characters."
  }
}

variable "api_description" {
  description = "Description of the API Gateway"
  type        = string
  default     = null
}

variable "endpoint_type" {
  description = "Type of endpoint. Valid values: EDGE, REGIONAL, PRIVATE. Note: EDGE and REGIONAL require waf_web_acl_arn for compliance"
  type        = string
  default     = "REGIONAL"

  validation {
    condition     = contains(["EDGE", "REGIONAL", "PRIVATE"], var.endpoint_type)
    error_message = "Endpoint type must be one of: EDGE, REGIONAL, PRIVATE."
  }
}

variable "vpc_endpoint_ids" {
  description = "List of VPC endpoint IDs (required for PRIVATE endpoint type)"
  type        = list(string)
  default     = null
}

variable "disable_execute_api_endpoint" {
  description = "Whether to disable the execute-api endpoint"
  type        = bool
  default     = false
}

variable "binary_media_types" {
  description = "List of binary media types supported by the API"
  type        = list(string)
  default     = []
}

variable "minimum_compression_size" {
  description = "Minimum response size to compress for the API (in bytes)"
  type        = number
  default     = 1024

  validation {
    condition     = var.minimum_compression_size >= 0 && var.minimum_compression_size <= 10485760
    error_message = "Minimum compression size must be between 0 and 10485760 bytes."
  }
}

variable "api_key_source" {
  description = "Source of the API key for requests. Valid values: HEADER, AUTHORIZER"
  type        = string
  default     = "HEADER"

  validation {
    condition     = contains(["HEADER", "AUTHORIZER"], var.api_key_source)
    error_message = "API key source must be either HEADER or AUTHORIZER."
  }
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}

# Logging Configuration
variable "log_retention_days" {
  description = "Number of days to retain CloudWatch logs"
  type        = number
  default     = 365

  validation {
    condition = contains([
      1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922, 3288, 3653
    ], var.log_retention_days)
    error_message = "Log retention days must be a valid CloudWatch Logs retention period."
  }
}

variable "cloudwatch_kms_key_arn" {
  description = "ARN of KMS key for CloudWatch log encryption"
  type        = string
  default     = null
}

variable "create_api_gateway_account" {
  description = "Whether to create API Gateway account for CloudWatch logging"
  type        = bool
  default     = false
}

# Deployment Configuration
variable "create_deployment" {
  description = "Whether to create a deployment and stage"
  type        = bool
  default     = true
}

variable "stage_name" {
  description = "Name of the API Gateway stage"
  type        = string
  default     = "prod"
}

variable "deployment_triggers" {
  description = "Map of arbitrary keys and values that, when changed, will trigger a redeployment"
  type        = map(string)
  default     = {}
}

# Monitoring and Tracing
variable "xray_tracing_enabled" {
  description = "Whether to enable X-Ray tracing"
  type        = bool
  default     = true
}

variable "enable_access_logging" {
  description = "Whether to enable access logging"
  type        = bool
  default     = true
}

variable "detailed_metrics_enabled" {
  description = "Whether to enable detailed CloudWatch metrics"
  type        = bool
  default     = true
}

variable "data_trace_enabled" {
  description = "Whether to enable data trace logging"
  type        = bool
  default     = false
}

variable "logging_level" {
  description = "Logging level for method settings. Valid values: ERROR, INFO (CKV2_AWS_4 compliance - OFF is not allowed)"
  type        = string
  default     = "INFO" # Compliance default for detailed logging

  validation {
    condition     = contains(["ERROR", "INFO"], var.logging_level)
    error_message = "CKV2_AWS_4: Logging level must be ERROR or INFO for compliance. OFF is not allowed for production APIs as it violates security logging requirements."
  }
}

# Throttling Configuration
variable "throttling_rate_limit" {
  description = "API Gateway throttling rate limit (requests per second)"
  type        = number
  default     = 1000

  validation {
    condition     = var.throttling_rate_limit >= 0
    error_message = "Throttling rate limit must be non-negative."
  }
}

variable "throttling_burst_limit" {
  description = "API Gateway throttling burst limit"
  type        = number
  default     = 2000

  validation {
    condition     = var.throttling_burst_limit >= 0
    error_message = "Throttling burst limit must be non-negative."
  }
}

# Caching Configuration - CKV_AWS_120 & CKV_AWS_225
variable "cache_cluster_enabled" {
  description = "Whether to enable API Gateway cache cluster"
  type        = bool
  default     = true # Enable by default for compliance
}

variable "cache_cluster_size" {
  description = "Size of the API Gateway cache cluster"
  type        = string
  default     = "0.5"

  validation {
    condition = contains([
      "0.5", "1.6", "6.1", "13.5", "28.4", "58.2", "118", "237"
    ], var.cache_cluster_size)
    error_message = "Cache cluster size must be a valid API Gateway cache size."
  }
}

variable "caching_enabled" {
  description = "Whether to enable method-level caching"
  type        = bool
  default     = true # Enable by default for compliance CKV_AWS_225
}

variable "cache_ttl_in_seconds" {
  description = "TTL for cache entries in seconds"
  type        = number
  default     = 300

  validation {
    condition     = var.cache_ttl_in_seconds >= 0 && var.cache_ttl_in_seconds <= 3600
    error_message = "Cache TTL must be between 0 and 3600 seconds."
  }
}

variable "cache_data_encrypted" {
  description = "Whether to encrypt cache data"
  type        = bool
  default     = true
}

variable "require_authorization_for_cache_control" {
  description = "Whether to require authorization for cache control"
  type        = bool
  default     = true
}

variable "unauthorized_cache_control_header_strategy" {
  description = "Strategy for unauthorized cache control headers"
  type        = string
  default     = "SUCCEED_WITH_RESPONSE_HEADER"

  validation {
    condition = contains([
      "FAIL_WITH_403", "SUCCEED_WITH_RESPONSE_HEADER", "SUCCEED_WITHOUT_RESPONSE_HEADER"
    ], var.unauthorized_cache_control_header_strategy)
    error_message = "Unauthorized cache control header strategy must be a valid value."
  }
}

# Security Configuration
variable "waf_web_acl_arn" {
  description = "ARN of WAF Web ACL to associate with the API Gateway stage. REQUIRED for EDGE/REGIONAL endpoints (CKV2_AWS_29 compliance)"
  type        = string
  default     = null

  validation {
    condition = (
      var.waf_web_acl_arn == null ||
      (var.waf_web_acl_arn != "" && can(regex("^arn:aws:wafv2:[a-z0-9-]+:[0-9]+:.*", var.waf_web_acl_arn)))
    )
    error_message = "WAF Web ACL ARN must be a valid ARN format when provided. Cannot be empty string."
  }
}

# WAF is required by default for CKV2_AWS_29 compliance - no option to disable
# If you need to bypass this, use external WAF management at the ALB/CloudFront level

# Custom Domain Configuration
variable "domain_name" {
  description = "Custom domain name for the API Gateway"
  type        = string
  default     = null
}

variable "certificate_arn" {
  description = "ARN of the ACM certificate for the custom domain"
  type        = string
  default     = null
}

variable "domain_endpoint_type" {
  description = "Type of endpoint for custom domain. Valid values: EDGE, REGIONAL"
  type        = string
  default     = "REGIONAL"

  validation {
    condition     = contains(["EDGE", "REGIONAL"], var.domain_endpoint_type)
    error_message = "Domain endpoint type must be either EDGE or REGIONAL."
  }
}

variable "security_policy" {
  description = "Security policy for the custom domain"
  type        = string
  default     = "TLS_1_2"

  validation {
    condition     = contains(["TLS_1_0", "TLS_1_2"], var.security_policy)
    error_message = "Security policy must be either TLS_1_0 or TLS_1_2."
  }
}

variable "base_path" {
  description = "Base path for the custom domain mapping"
  type        = string
  default     = null
}

# Usage Plan and API Keys
variable "create_usage_plan" {
  description = "Whether to create a usage plan"
  type        = bool
  default     = false
}

variable "quota_settings" {
  description = "Quota settings for the usage plan"
  type = object({
    limit  = number
    period = string
    offset = optional(number, 0)
  })
  default = null

  validation {
    condition = var.quota_settings == null || contains([
      "DAY", "WEEK", "MONTH"
    ], var.quota_settings.period)
    error_message = "Quota period must be one of: DAY, WEEK, MONTH."
  }
}

variable "throttle_settings" {
  description = "Throttle settings for the usage plan"
  type = object({
    rate_limit  = number
    burst_limit = number
  })
  default = null
}

variable "api_keys" {
  description = "Map of API keys to create"
  type = map(object({
    description = optional(string, "")
    enabled     = optional(bool, true)
    value       = optional(string, null)
  }))
  default = {}
}

# Client Certificate Configuration - CKV2_AWS_51
variable "client_certificate_id" {
  description = "ID of the client certificate for the stage"
  type        = string
  default     = null
}
