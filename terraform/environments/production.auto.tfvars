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
cognito_mfa_configuration      = "OPTIONAL" # Optional MFA
cognito_advanced_security_mode = "OFF"      # Cognito Plus (ENFORCED) bills per MAU even when idle-ish
cognito_callback_urls = [
  "https://fingov.ai",
  "https://fingov.ai/app",
  "https://fingov.ai/auth/callback",
  "https://fingov.ai/auth/verify",
  "https://www.fingov.ai",
  "https://www.fingov.ai/app",
  "https://www.fingov.ai/auth/callback",
  "https://www.fingov.ai/auth/verify",
  "https://investcosine.com",
  "https://investcosine.com/app",
  "https://investcosine.com/auth/callback",
  "https://investcosine.com/auth/verify",
  "https://www.investcosine.com",
  "https://www.investcosine.com/app",
  "https://www.investcosine.com/auth/callback",
  "https://www.investcosine.com/auth/verify"
]
cognito_logout_urls = [
  "https://fingov.ai",
  "https://fingov.ai/auth/logout",
  "https://fingov.ai/?",
  "https://www.fingov.ai",
  "https://www.fingov.ai/auth/logout",
  "https://www.fingov.ai/?",
  "https://investcosine.com",
  "https://investcosine.com/auth/logout",
  "https://investcosine.com/?",
  "https://www.investcosine.com",
  "https://www.investcosine.com/auth/logout",
  "https://www.investcosine.com/?"
]
cognito_access_token_validity  = 60 # 1 hour
cognito_id_token_validity      = 60 # 1 hour  
cognito_refresh_token_validity = 1  # 1 day

# Temporarily disable Google provider until OAuth secrets are configured
cognito_enable_google_provider = true
cognito_domain_name            = "cosine-production"
# Temporarily disable OAuth secrets to fix deployment
oauth_secrets_enabled = true

# DynamoDB (hibernate / cost): on-demand billing; streams & PITR off — app Lambdas use Query/GetItem only
dynamodb_billing_mode                   = "PAY_PER_REQUEST"
dynamodb_stream_enabled                 = false
dynamodb_stream_view_type               = "NEW_AND_OLD_IMAGES" # unused when stream_enabled = false
dynamodb_point_in_time_recovery_enabled = false
dynamodb_deletion_protection_enabled    = false

# Keep indexed tables/buckets in Terraform (empty is fine; storage is $0).
# Set false only if you want Terraform to destroy the search tables.
enable_indexed_data = true

# No reserved concurrency carve-out for writer Lambdas (null = use account default pool)
lambda_reserved_concurrency_default = null

# Alternative provisioned capacity settings (uncomment if switching to PROVISIONED)
# dynamodb_billing_mode     = "PROVISIONED"
# dynamodb_read_capacity    = 10
# dynamodb_write_capacity   = 10
# dynamodb_gsi_read_capacity = 5
# dynamodb_gsi_write_capacity = 5

# CloudWatch Configuration (short retention — idle cost save)

cloudwatch_security_log_retention_days    = 7
cloudwatch_auth_log_retention_days        = 7
cloudwatch_application_log_retention_days = 7
cloudwatch_lambda_log_retention_days      = 7
cloudwatch_api_gateway_log_retention_days = 7

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
# VPC, ALB, and WAF Configuration
# ============================================================================
# Note: These variables are not currently used in the base infrastructure
# They are reserved for future use when VPC, ALB, and WAF modules are added
# 
# vpc_cidr = "10.0.0.0/16"
# az_count = 2
# certificate_arn = "arn:aws:acm:us-east-1:676206904242:certificate/7f8d2b7b-d9d7-4ba8-9795-ddd3c11d8361"
# enable_https = true
# enable_alb_access_logs = false
# alb_access_logs_bucket = ""
# waf_rate_limit = 1000
# waf_blocked_countries = ["CN", "RU", "KP", "IR", "SY", "CU"]
# enable_waf_logging = true
