# Production Environment Configuration
# production.auto.tfvars

# Basic configuration
environment = "production"
aws_region  = "us-east-1"

# Common tags
common_tags = {
  Project     = "cosine"
  Environment = "production"
  ManagedBy   = "terraform"
  Repository  = "Cosine-Base-Infra"
  CostCenter  = "production"
  Compliance  = "required"
}

# KMS Configuration (maximum security)
enable_key_rotation         = true
kms_deletion_window_in_days = 30 # Maximum retention

# Cognito Configuration (maximum security)
cognito_mfa_configuration      = "ON"       # Required MFA
cognito_advanced_security_mode = "ENFORCED" # Full security enforcement
cognito_callback_urls = [
  "https://investcosine.com",
  "https://investcosine.com/app",
  "https://investcosine.com/auth/callback",
  "https://www.investcosine.com",
  "https://www.investcosine.com/app",
  "https://www.investcosine.com/auth/callback"
]
cognito_logout_urls = [
  "https://investcosine.com",
  "https://investcosine.com/auth/logout",
  "https://www.investcosine.com",
  "https://www.investcosine.com/auth/logout"
]
cognito_access_token_validity  = 60 # 60 minutes (1 hour)
cognito_id_token_validity      = 60 # 60 minutes (1 hour)  
cognito_refresh_token_validity = 30 # 30 days

# Temporarily disable Google/Microsoft providers until OAuth secrets are configured
cognito_enable_google_provider    = false
cognito_enable_microsoft_provider = false
cognito_domain_name               = "cosine-production"
# Temporarily disable OAuth secrets to fix deployment
oauth_secrets_enabled = false

# DynamoDB Configuration (maximum durability)
dynamodb_billing_mode                   = "PAY_PER_REQUEST" # Can switch to PROVISIONED if predictable load
dynamodb_stream_enabled                 = true
dynamodb_stream_view_type               = "NEW_AND_OLD_IMAGES"
dynamodb_point_in_time_recovery_enabled = true # Critical for production
dynamodb_deletion_protection_enabled    = true # Prevent accidental deletion
dynamodb_ttl_enabled                    = true

# Alternative provisioned capacity settings (uncomment if switching to PROVISIONED)
# dynamodb_billing_mode     = "PROVISIONED"
# dynamodb_read_capacity    = 10
# dynamodb_write_capacity   = 10
# dynamodb_gsi_read_capacity = 5
# dynamodb_gsi_write_capacity = 5

# CloudWatch Configuration (maximum retention)

cloudwatch_security_log_retention_days    = 365 # 1 year
cloudwatch_auth_log_retention_days        = 365 # 1 year
cloudwatch_application_log_retention_days = 365 # 1 year
cloudwatch_lambda_log_retention_days      = 365 # 1 year
cloudwatch_api_gateway_log_retention_days = 365 # 1 year

# Alert thresholds (strict for production)
cloudwatch_failed_login_threshold        = 3 # Very sensitive
cloudwatch_suspicious_activity_threshold = 5 # Very sensitive

# Secrets Manager automatic rotation (CKV_AWS_304 compliance)
# Note: OAuth credentials typically don't require automatic rotation as they are manually managed
# If rotation is needed, create a Lambda function and provide its ARN below
automatic_secret_rotation = {
  # oauth-credentials = {
  #   rotation_lambda_arn = "arn:aws:lambda:us-east-1:676206904242:function:cosine-oauth-rotation-production"
  #   rotation_rules = {
  #     automatically_after_days = 30 # More frequent rotation for production
  #   }
  # }
  # Add other secrets here as needed
}

# SNS topic for critical alerts (to be created separately or referenced)
# cloudwatch_alarm_notification_topic_arn = "arn:aws:sns:us-east-1:123456789012:critical-alerts"

# ============================================================================
# VPC Configuration
# ============================================================================

vpc_cidr = "10.0.0.0/16"
az_count = 2

# ============================================================================
# ALB Configuration
# ============================================================================

# SSL Certificate for production domain
certificate_arn = "arn:aws:acm:us-east-1:676206904242:certificate/7f8d2b7b-d9d7-4ba8-9795-ddd3c11d8361"

# HTTPS configuration
enable_https = true

# ALB Access Logs (disabled by default, enable if S3 bucket exists)
enable_alb_access_logs = false
alb_access_logs_bucket = ""

# WAF Configuration (strict security for production)
waf_rate_limit        = 1000 # Stricter rate limiting for production
waf_blocked_countries = ["CN", "RU", "KP", "IR", "SY", "CU"]
enable_waf_logging    = true
