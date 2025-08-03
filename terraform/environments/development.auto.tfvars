# Development Environment Configuration
# development.auto.tfvars

# Basic configuration
environment = "development"
aws_region  = "us-east-1"

# Common tags
common_tags = {
  Project     = "cosine"
  Environment = "development"
  ManagedBy   = "terraform"
  Repository  = "Cosine-Base-Infra"
  CostCenter  = "development"
}

# KMS Configuration (less restrictive for development)
enable_key_rotation         = false # Disabled for cost savings in dev
kms_deletion_window_in_days = 7     # Minimum for faster iteration

# Cognito Configuration (development-friendly)
cognito_mfa_configuration      = "OFF"   # Disabled for easier testing
cognito_advanced_security_mode = "AUDIT" # Less restrictive
cognito_callback_urls = [
  "http://localhost:3000",
  "http://localhost:3000/auth/callback"
]
cognito_logout_urls = [
  "http://localhost:3000",
  "http://localhost:3000/auth/logout"
]
cognito_access_token_validity  = 120 # 2 hours for development
cognito_id_token_validity      = 120 # 2 hours for development
cognito_refresh_token_validity = 7   # 7 days for development

# DynamoDB Configuration (cost-optimized)
dynamodb_billing_mode                   = "PAY_PER_REQUEST" # Cost-effective for low usage
dynamodb_stream_enabled                 = true
dynamodb_stream_view_type               = "NEW_AND_OLD_IMAGES"
dynamodb_point_in_time_recovery_enabled = false # Disabled for cost savings
dynamodb_deletion_protection_enabled    = false # Allow easy cleanup in dev
dynamodb_ttl_enabled                    = true

# CloudWatch Configuration (shorter retention for cost savings)

cloudwatch_security_log_retention_days    = 365 # 1 year
cloudwatch_auth_log_retention_days        = 365 # 1 year
cloudwatch_application_log_retention_days = 365 # 1 year
cloudwatch_lambda_log_retention_days      = 365 # 1 year
cloudwatch_api_gateway_log_retention_days = 365 # 1 year

# Secrets Manager automatic rotation (CKV_AWS_304 compliance)
# Note: OAuth credentials typically don't require automatic rotation as they are manually managed
# If rotation is needed, create a Lambda function and provide its ARN below
automatic_secret_rotation = {
  # oauth-credentials = {
  #   rotation_lambda_arn = "arn:aws:lambda:us-east-1:676206904242:function:cosine-oauth-rotation-development"
  #   rotation_rules = {
  #     automatically_after_days = 90 # Maximum allowed for compliance
  #   }
  # }
  # Add other secrets here as needed
}

# Alert thresholds (more lenient for development)
cloudwatch_failed_login_threshold        = 10 # Higher threshold
cloudwatch_suspicious_activity_threshold = 20 # Higher threshold
