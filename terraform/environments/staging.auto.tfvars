# Staging Environment Configuration
# staging.auto.tfvars

# Basic configuration
environment = "staging"
aws_region  = "us-east-1"


# Common tags
common_tags = {
  Project     = "cosine"
  Environment = "staging"
  ManagedBy   = "terraform"
  Repository  = "Cosine-Base-Infra"
  CostCenter  = "staging"
}

# KMS Configuration (production-like)
enable_key_rotation         = true
kms_deletion_window_in_days = 10

# Cognito Configuration (production-like)
cognito_mfa_configuration      = "OPTIONAL" # Optional MFA for testing
cognito_advanced_security_mode = "OFF"
cognito_callback_urls = [
  "https://d5b4qcbiesv5t.cloudfront.net/dashboard",
  "https://d5b4qcbiesv5t.cloudfront.net/auth/callback",
  "https://d5b4qcbiesv5t.cloudfront.net/app",
  "https://d5b4qcbiesv5t.cloudfront.net/auth/verify",
  "https://www.d5b4qcbiesv5t.cloudfront.net",
  "https://www.d5b4qcbiesv5t.cloudfront.net/app",
  "https://www.d5b4qcbiesv5t.cloudfront.net/auth/callback",
  "https://www.d5b4qcbiesv5t.cloudfront.net/auth/verify",
  "http://localhost:3000/auth/callback"
]
cognito_logout_urls = [
  "https://d5b4qcbiesv5t.cloudfront.net/",
  "https://d5b4qcbiesv5t.cloudfront.net/auth/logout",
  "https://www.d5b4qcbiesv5t.cloudfront.net",
  "https://www.d5b4qcbiesv5t.cloudfront.net/auth/logout",
  "http://localhost:3000"
]
cognito_access_token_validity  = 60 # 1 hour
cognito_id_token_validity      = 60 # 1 hour
cognito_refresh_token_validity = 30 # 30 days

# DynamoDB (hibernate / cost)
dynamodb_billing_mode                   = "PAY_PER_REQUEST"
dynamodb_stream_enabled                 = false
dynamodb_stream_view_type               = "NEW_AND_OLD_IMAGES"
dynamodb_point_in_time_recovery_enabled = false
dynamodb_deletion_protection_enabled    = false
enable_indexed_data                     = true
enable_kms                              = false
retain_kms_keys                         = false
enable_sqs                              = false

# CloudWatch Configuration (short retention - idle cost save)
cloudwatch_security_log_retention_days    = 7
cloudwatch_auth_log_retention_days        = 7
cloudwatch_application_log_retention_days = 7
cloudwatch_lambda_log_retention_days      = 7
cloudwatch_api_gateway_log_retention_days = 7

# Secrets Manager automatic rotation (disabled for manual console management)
automatic_secret_rotation = {}

# Alert thresholds (production-like)
cloudwatch_failed_login_threshold        = 5
cloudwatch_suspicious_activity_threshold = 10

# Federated Authentication Configuration
cognito_enable_google_provider = true
cognito_domain_name            = "cosine-auth-staging"

# OAuth Secrets Manager Integration (console-managed secrets)
oauth_secrets_enabled = true # Create empty secret resource for console population

# No reserved pool carve-out (hibernate / rare traffic)
lambda_reserved_concurrency_default = null
