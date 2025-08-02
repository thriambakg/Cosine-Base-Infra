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
cognito_advanced_security_mode = "ENFORCED" # Full security
cognito_callback_urls = [
  "http://cosine-alb-v2-staging-1054813572.us-east-1.elb.amazonaws.com",
  "http://cosine-alb-v2-staging-1054813572.us-east-1.elb.amazonaws.com/auth/callback"
]
cognito_logout_urls = [
  "http://cosine-alb-v2-staging-1054813572.us-east-1.elb.amazonaws.com",
  "http://cosine-alb-v2-staging-1054813572.us-east-1.elb.amazonaws.com/auth/logout"
]
cognito_access_token_validity  = 60 # 1 hour
cognito_id_token_validity      = 60 # 1 hour
cognito_refresh_token_validity = 30 # 30 days

# DynamoDB Configuration (production-like)
dynamodb_billing_mode                   = "PAY_PER_REQUEST"
dynamodb_stream_enabled                 = true
dynamodb_stream_view_type               = "NEW_AND_OLD_IMAGES"
dynamodb_point_in_time_recovery_enabled = true # Enabled for data protection
dynamodb_deletion_protection_enabled    = true # Protect against accidental deletion
dynamodb_ttl_enabled                    = true

# CloudWatch Configuration (moderate retention)
cloudwatch_security_log_retention_days    = 365 # 1 year
cloudwatch_auth_log_retention_days        = 365 # 1 year
cloudwatch_application_log_retention_days = 365 # 1 year
cloudwatch_lambda_log_retention_days      = 365 # 1 year
cloudwatch_api_gateway_log_retention_days = 365 # 1 year

# Secrets Manager automatic rotation (disabled for manual console management)
automatic_secret_rotation = {}

# Alert thresholds (production-like)
cloudwatch_failed_login_threshold        = 5
cloudwatch_suspicious_activity_threshold = 10

# Federated Authentication Configuration
cognito_enable_google_provider    = true
cognito_enable_microsoft_provider = true
cognito_domain_name               = "cosine-auth-staging"

# OAuth Secrets Manager Integration (console-managed secrets)
oauth_secrets_enabled = true # Create empty secret resource for console population
