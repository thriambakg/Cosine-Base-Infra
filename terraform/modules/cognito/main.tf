# Cognito User Pool Module
# modules/cognito/main.tf

# Data source for retrieving OAuth credentials from Secrets Manager
data "aws_secretsmanager_secret" "oauth_credentials" {
  count = var.use_secrets_manager ? 1 : 0
  name  = var.secrets_manager_secret_name
}

data "aws_secretsmanager_secret_version" "oauth_credentials" {
  count     = var.use_secrets_manager ? 1 : 0
  secret_id = data.aws_secretsmanager_secret.oauth_credentials[0].id
}

# Local values to handle both direct variables and Secrets Manager
locals {
  oauth_secrets = var.use_secrets_manager ? jsondecode(data.aws_secretsmanager_secret_version.oauth_credentials[0].secret_string) : {}

  # Google credentials
  google_client_id     = var.use_secrets_manager ? lookup(local.oauth_secrets, "google_client_id", "") : var.google_client_id
  google_client_secret = var.use_secrets_manager ? lookup(local.oauth_secrets, "google_client_secret", "") : var.google_client_secret

  # Microsoft credentials
  microsoft_client_id     = var.use_secrets_manager ? lookup(local.oauth_secrets, "microsoft_client_id", "") : var.microsoft_client_id
  microsoft_client_secret = var.use_secrets_manager ? lookup(local.oauth_secrets, "microsoft_client_secret", "") : var.microsoft_client_secret
}

# Cognito User Pool
resource "aws_cognito_user_pool" "main" {
  name = "${var.project_name}-user-pool-${var.environment}"

  # Username configuration - use alias_attributes instead of username_attributes
  alias_attributes = ["email", "preferred_username"]

  # Password policy
  password_policy {
    minimum_length                   = 8
    require_lowercase                = true
    require_numbers                  = true
    require_symbols                  = true
    require_uppercase                = true
    temporary_password_validity_days = 7
  }

  # Account recovery setting
  account_recovery_setting {
    recovery_mechanism {
      name     = "verified_email"
      priority = 1
    }
  }

  # Auto verification
  auto_verified_attributes = ["email"]

  # Email configuration
  email_configuration {
    email_sending_account = "COGNITO_DEFAULT"
  }

  # MFA configuration
  mfa_configuration = var.mfa_configuration

  software_token_mfa_configuration {
    enabled = var.mfa_configuration != "OFF"
  }

  # Device configuration
  device_configuration {
    challenge_required_on_new_device      = true
    device_only_remembered_on_user_prompt = false
  }

  # User pool add-ons
  user_pool_add_ons {
    advanced_security_mode = var.advanced_security_mode
  }

  # Verification message templates
  verification_message_template {
    default_email_option = "CONFIRM_WITH_CODE"
    email_subject        = "Verify your ${var.project_name} account"
    email_message        = "Your verification code is {####}"
  }

  # User attribute update settings
  user_attribute_update_settings {
    attributes_require_verification_before_update = ["email"]
  }

  # Schema
  schema {
    attribute_data_type      = "String"
    developer_only_attribute = false
    mutable                  = true
    name                     = "email"
    required                 = true

    string_attribute_constraints {
      min_length = 1
      max_length = 256
    }
  }

  schema {
    attribute_data_type      = "String"
    developer_only_attribute = false
    mutable                  = true
    name                     = "given_name"
    required                 = false

    string_attribute_constraints {
      min_length = 1
      max_length = 256
    }
  }

  schema {
    attribute_data_type      = "String"
    developer_only_attribute = false
    mutable                  = true
    name                     = "family_name"
    required                 = false

    string_attribute_constraints {
      min_length = 1
      max_length = 256
    }
  }

  schema {
    attribute_data_type      = "String"
    developer_only_attribute = false
    mutable                  = true
    name                     = "phone_number"
    required                 = false

    string_attribute_constraints {
      min_length = 1
      max_length = 256
    }
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-user-pool-${var.environment}"
    Type = "Authentication"
  })

  lifecycle {
    prevent_destroy = true
  }
}

# Cognito User Pool Client
resource "aws_cognito_user_pool_client" "main" {
  name         = "${var.project_name}-client-${var.environment}"
  user_pool_id = aws_cognito_user_pool.main.id

  # Client settings
  generate_secret = false

  # Allowed OAuth flows
  allowed_oauth_flows                  = ["code", "implicit"]
  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_scopes                 = ["email", "openid", "profile", "aws.cognito.signin.user.admin"]

  # Callback URLs
  callback_urls = var.callback_urls
  logout_urls   = var.logout_urls

  # Explicit auth flows
  explicit_auth_flows = [
    "ALLOW_USER_SRP_AUTH",
    "ALLOW_REFRESH_TOKEN_AUTH",
    "ALLOW_USER_PASSWORD_AUTH"
  ]

  # Supported identity providers
  supported_identity_providers = concat(
    ["COGNITO"],
    var.enable_google_provider ? ["Google"] : [],
    var.enable_microsoft_provider ? ["Microsoft"] : []
  )

  # Token validity
  access_token_validity  = var.access_token_validity
  id_token_validity      = var.id_token_validity
  refresh_token_validity = var.refresh_token_validity

  token_validity_units {
    access_token  = "minutes"
    id_token      = "minutes"
    refresh_token = "days"
  }

  # Read and write attributes
  read_attributes = [
    "email",
    "email_verified",
    "given_name",
    "family_name",
    "phone_number",
    "phone_number_verified"
  ]

  write_attributes = [
    "email",
    "given_name",
    "family_name",
    "phone_number"
  ]

  # Prevent user existence errors
  prevent_user_existence_errors = "ENABLED"

  depends_on = [
    aws_cognito_user_pool.main,
    aws_cognito_identity_provider.google,
    aws_cognito_identity_provider.microsoft
  ]
}

# Cognito User Pool Domain (optional)
resource "aws_cognito_user_pool_domain" "main" {
  count        = var.domain_name != "" ? 1 : 0
  domain       = var.domain_name
  user_pool_id = aws_cognito_user_pool.main.id
}

# Google Identity Provider
resource "aws_cognito_identity_provider" "google" {
  count         = var.enable_google_provider ? 1 : 0
  user_pool_id  = aws_cognito_user_pool.main.id
  provider_name = "Google"
  provider_type = "Google"

  provider_details = {
    client_id        = local.google_client_id
    client_secret    = local.google_client_secret
    authorize_scopes = "email openid profile"
  }

  attribute_mapping = {
    email       = "email"
    given_name  = "given_name"
    family_name = "family_name"
    username    = "sub"
  }
}

# Microsoft Identity Provider
resource "aws_cognito_identity_provider" "microsoft" {
  count         = var.enable_microsoft_provider ? 1 : 0
  user_pool_id  = aws_cognito_user_pool.main.id
  provider_name = "Microsoft"
  provider_type = "OIDC"

  provider_details = {
    client_id                 = local.microsoft_client_id
    client_secret             = local.microsoft_client_secret
    attributes_request_method = "GET"
    oidc_issuer               = "https://login.microsoftonline.com/common/v2.0"
    authorize_scopes          = "email openid profile"
  }

  attribute_mapping = {
    email       = "email"
    given_name  = "given_name"
    family_name = "family_name"
    username    = "sub"
  }
}
