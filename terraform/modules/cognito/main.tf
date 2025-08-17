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


}

# Cognito User Pool
resource "aws_cognito_user_pool" "main" {
  name = "${var.project_name}-user-pool-auth-${var.environment}"

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

  # Verification message templates - using LINK for better UX
  verification_message_template {
    default_email_option  = "CONFIRM_WITH_LINK"
    email_subject         = "Welcome to Cosine! Please verify your email"
    email_subject_by_link = "Welcome to Cosine! Please verify your email"
    email_message_by_link = <<-EOT
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Verify Your Email - Cosine</title>
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif; background-color: #f8fafc;">
    <table role="presentation" style="width: 100%; border-collapse: collapse;">
        <tr>
            <td style="padding: 40px 20px;">
                <table role="presentation" style="max-width: 600px; margin: 0 auto; background-color: white; border-radius: 12px; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
                    <!-- Header with Branding -->
                    <tr>
                        <td style="padding: 40px 40px 20px; text-align: center; background: linear-gradient(135deg, #1e3a8a 0%, #581c87 50%, #3730a3 100%); border-radius: 12px 12px 0 0;">
                            <div style="display: inline-flex; align-items: center; font-family: system-ui, -apple-system, sans-serif;">
                                <!-- Cosine logo image -->
                                <img src="https://investcosine.com/email-logo.png" 
                                     width="48" 
                                     height="48" 
                                     alt="Cosine Logo" 
                                     style="margin-right: 15px; display: inline-block; vertical-align: middle;">
                                
                                <!-- Company name and tagline -->
                                <div style="text-align: left;">
                                    <div style="color: white; font-size: 24px; font-weight: 600; line-height: 1.2; text-shadow: 0 2px 4px rgba(0,0,0,0.2);">
                                        Cosine
                                    </div>
                                    <div style="color: rgba(255,255,255,0.7); font-size: 11px; font-weight: 400; line-height: 1.2;">
                                        AI-Powered Trading Intelligence
                                    </div>
                                </div>
                            </div>
                        </td>
                    </tr>
                    
                    <!-- Content -->
                    <tr>
                        <td style="padding: 40px;">
                            <h2 style="margin: 0 0 20px; color: #1e293b; font-size: 24px; font-weight: 600;">Welcome to Cosine!</h2>
                            
                            <p style="margin: 0 0 20px; color: #475569; font-size: 16px; line-height: 1.6;">
                                Thank you for joining our AI-powered trading platform. To get started and ensure the security of your account, please verify your email address.
                            </p>
                            
                            <div style="text-align: center; margin: 40px 0;">
                                <p style="margin: 0; font-size: 24px; font-weight: 700; color: #1e3a8a;">${var.verification_link_placeholder}</p>
                            </div>
                            
                            <hr style="margin: 32px 0; border: none; border-top: 1px solid #e2e8f0;">
                            
                            <p style="margin: 0; color: #64748b; font-size: 14px; line-height: 1.6;">
                                <strong>What's next?</strong><br>
                                • Access real-time market analysis<br>
                                • Get AI-powered trading insights<br>
                                • Optimize your portfolio with advanced tools<br>
                                • Connect with other traders in our community
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="padding: 20px 40px; background-color: #f8fafc; border-radius: 0 0 12px 12px; text-align: center;">
                            <p style="margin: 0; color: #64748b; font-size: 12px;">
                                This email was sent by Cosine Trading Platform<br>
                                If you didn't create an account, you can safely ignore this email.
                            </p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
EOT
    email_message         = "Your verification code is {####}. Use this code to verify your Cosine account."
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

  # Schema attributes to match existing User Pool configuration
  # These were likely added manually in AWS console and need to be defined in Terraform

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
    name                     = "custom_termsaccept"
    required                 = false
    string_attribute_constraints {
      min_length = 1
      max_length = 10
    }
  }

  schema {
    attribute_data_type      = "String"
    developer_only_attribute = false
    mutable                  = true
    name                     = "custom_markconsent"
    required                 = false
    string_attribute_constraints {
      min_length = 1
      max_length = 10
    }
  }

  schema {
    attribute_data_type      = "String"
    developer_only_attribute = false
    mutable                  = true
    name                     = "custom_role"
    required                 = false
    string_attribute_constraints {
      min_length = 1
      max_length = 20
    }
  }

  schema {
    attribute_data_type      = "String"
    developer_only_attribute = false
    mutable                  = true
    name                     = "custom_subplan"
    required                 = false
    string_attribute_constraints {
      min_length = 1
      max_length = 20
    }
  }

  schema {
    attribute_data_type      = "String"
    developer_only_attribute = false
    mutable                  = true
    name                     = "custom_substatus"
    required                 = false
    string_attribute_constraints {
      min_length = 1
      max_length = 20
    }
  }

  tags = merge(var.tags, {
    Name = "${var.project_name}-user-pool-${var.environment}"
    Type = "Authentication"
  })

  lifecycle {
    prevent_destroy = false
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

  # Supported identity providers (social providers first)
  supported_identity_providers = concat(
    var.enable_google_provider ? ["Google"] : [],

    ["COGNITO"]
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

  # Read and write attributes - enable access to existing User Pool attributes
  read_attributes = [
    "email",
    "given_name",
    "family_name",
    "custom:custom_termsaccept",
    "custom:custom_markconsent",
    "custom:custom_role",
    "custom:custom_subplan",
    "custom:custom_substatus"
  ]

  write_attributes = [
    "email",
    "given_name",
    "family_name",
    "custom:custom_termsaccept",
    "custom:custom_markconsent",
    "custom:custom_role",
    "custom:custom_subplan",
    "custom:custom_substatus"
  ]

  # Prevent user existence errors
  prevent_user_existence_errors = "ENABLED"

  depends_on = [
    aws_cognito_user_pool.main,
    aws_cognito_identity_provider.google,

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


