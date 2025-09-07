# Cosine Base Infrastructure
# Main Terraform configuration for shared resources

terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = var.common_tags
  }
}

# Temporary provider for replica region to clean up cross-region replication resources
provider "aws" {
  alias  = "replica"
  region = "us-west-2" # Different region for replication

  default_tags {
    tags = var.common_tags
  }
}

# KMS keys for encryption
module "kms" {
  source = "./modules/kms"

  project_name            = var.project_name
  environment             = var.environment
  tags                    = var.common_tags
  enable_key_rotation     = var.enable_key_rotation
  deletion_window_in_days = var.kms_deletion_window_in_days
  key_administrators      = var.kms_key_administrators
  allowed_services        = var.kms_allowed_services
}

# Secrets Manager for OAuth credentials
module "secrets_manager" {
  source = "./modules/secrets-manager"

  project_name         = var.project_name
  environment          = var.environment
  tags                 = var.common_tags
  kms_key_id           = module.kms.main_key_id
  recovery_window_days = var.secrets_recovery_window_days

  # Only enable automatic rotation if secrets are enabled
  automatic_rotation = var.oauth_secrets_enabled ? var.automatic_secret_rotation : {}

  # Create empty secret for console population
  secrets = var.oauth_secrets_enabled ? {
    oauth-gaz = {
      description = "OAuth provider credentials for federated authentication (populated manually)"
      secret_data = {
        # Placeholder values - will be updated manually in console
        google_client_id     = "PLACEHOLDER_GOOGLE_CLIENT_ID"
        google_client_secret = "PLACEHOLDER_GOOGLE_CLIENT_SECRET"

      }
    }
  } : {}
}

# Cognito User Pool for authentication
module "cognito" {
  source = "./modules/cognito"

  project_name           = var.project_name
  environment            = var.environment
  tags                   = var.common_tags
  mfa_configuration      = var.cognito_mfa_configuration
  advanced_security_mode = var.cognito_advanced_security_mode
  callback_urls          = var.cognito_callback_urls
  logout_urls            = var.cognito_logout_urls
  access_token_validity  = var.cognito_access_token_validity
  id_token_validity      = var.cognito_id_token_validity
  refresh_token_validity = var.cognito_refresh_token_validity
  domain_name            = var.cognito_domain_name

  # Google Identity Provider
  enable_google_provider = var.cognito_enable_google_provider
  google_client_id       = var.oauth_secrets_enabled ? "" : var.cognito_google_client_id
  google_client_secret   = var.oauth_secrets_enabled ? "" : var.cognito_google_client_secret

  # Lambda trigger for user profile creation
  post_authentication_lambda_arn = module.user_profile_creation_lambda.function_arn

  # Secrets Manager Integration
  use_secrets_manager         = var.oauth_secrets_enabled
  secrets_manager_secret_name = var.oauth_secrets_enabled ? module.secrets_manager.secret_names["oauth-gaz"] : ""

  depends_on = [module.secrets_manager, module.user_profile_creation_lambda]
}

# DynamoDB tables for user data
module "dynamodb" {
  source = "./modules/dynamodb"

  project_name                   = var.project_name
  environment                    = var.environment
  tags                           = var.common_tags
  kms_key_id                     = module.kms.dynamodb_key_arn
  billing_mode                   = var.dynamodb_billing_mode
  read_capacity                  = var.dynamodb_read_capacity
  write_capacity                 = var.dynamodb_write_capacity
  gsi_read_capacity              = var.dynamodb_gsi_read_capacity
  gsi_write_capacity             = var.dynamodb_gsi_write_capacity
  stream_enabled                 = var.dynamodb_stream_enabled
  stream_view_type               = var.dynamodb_stream_view_type
  point_in_time_recovery_enabled = var.dynamodb_point_in_time_recovery_enabled
  deletion_protection_enabled    = var.dynamodb_deletion_protection_enabled
  ttl_enabled                    = var.dynamodb_ttl_enabled
  ttl_attribute_name             = var.dynamodb_ttl_attribute_name

  depends_on = [module.kms]
}

# User Profile Creation Lambda Function (Cognito Trigger)
module "user_profile_creation_lambda" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-user-profile-creation-${var.environment}"
  description   = "Lambda function for creating user profiles on Cognito signup"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11"
  timeout       = 30
  memory_size   = 256

  # Source directory
  source_dir = "../backend_app/src/user_profile_creation/app"

  # Environment variables
  environment_variables = {
    USER_PROFILES_TABLE_NAME = module.dynamodb.user_profiles_table_name
  }

  # IAM policies for DynamoDB access
  additional_policy_arns = [
    module.dynamodb.user_profiles_table_policy_arn
  ]

  tags = var.common_tags

  depends_on = [module.dynamodb]
}

# Lambda permission for Cognito to invoke the user profile creation function
resource "aws_lambda_permission" "cognito_user_profile_creation" {
  statement_id  = "AllowCognitoInvoke"
  action        = "lambda:InvokeFunction"
  function_name = module.user_profile_creation_lambda.function_name
  principal     = "cognito-idp.amazonaws.com"
  source_arn    = module.cognito.user_pool_arn

  depends_on = [module.user_profile_creation_lambda, module.cognito]
}

# CloudWatch logging and monitoring
module "cloudwatch" {
  source = "./modules/cloudwatch"

  project_name                   = var.project_name
  environment                    = var.environment
  tags                           = var.common_tags
  kms_key_id                     = module.kms.cloudwatch_key_arn
  aws_region                     = var.aws_region
  cognito_user_pool_id           = module.cognito.user_pool_id
  user_profiles_table_name       = module.dynamodb.user_profiles_table_name
  security_log_retention_days    = var.cloudwatch_security_log_retention_days
  auth_log_retention_days        = var.cloudwatch_auth_log_retention_days
  application_log_retention_days = var.cloudwatch_application_log_retention_days
  lambda_log_retention_days      = var.cloudwatch_lambda_log_retention_days
  api_gateway_log_retention_days = var.cloudwatch_api_gateway_log_retention_days
  failed_login_threshold         = var.cloudwatch_failed_login_threshold
  suspicious_activity_threshold  = var.cloudwatch_suspicious_activity_threshold
  alarm_notification_topic_arn   = var.cloudwatch_alarm_notification_topic_arn

  depends_on = [module.kms, module.cognito, module.dynamodb]
}



# Multiple Lambda Layers for shared dependencies
# This approach splits dependencies into logical layers to stay under 64MB limit per layer

# Core Dependencies Layer - Essential packages for all Lambda functions
module "lambda_layer_core" {
  source = "./modules/lambda-layer"

  project_name        = var.project_name
  environment         = var.environment
  requirements_file   = "core-dependencies.txt"
  layer_name_suffix   = "core"
  layer_description   = "Core dependencies layer for ${var.project_name} Lambda functions (requests, boto3, essential libraries)"
  compatible_runtimes = ["python3.11"]
  python_command      = "python3.11"
  s3_bucket_name      = module.lambda_layers_bucket.bucket_id
}

# Financial Dependencies Layer - Data analysis and financial packages
module "lambda_layer_financial" {
  source = "./modules/lambda-layer"

  project_name        = var.project_name
  environment         = var.environment
  requirements_file   = "financial-dependencies.txt"
  layer_name_suffix   = "financial"
  layer_description   = "Financial data processing dependencies for ${var.project_name} Lambda functions (yfinance, numpy, pandas)"
  compatible_runtimes = ["python3.11"]
  python_command      = "python3.11"
  s3_bucket_name      = module.lambda_layers_bucket.bucket_id
}

# AI Core Dependencies Layer - CONSOLIDATED INTO CORE LAYER
# module "lambda_layer_ai_core" {
#   source = "./modules/lambda-layer"
#
#   project_name        = var.project_name
#   environment         = var.environment
#   requirements_file   = "ai-core-dependencies.txt"
#   layer_name_suffix   = "ai-core"
#   layer_description   = "Core AI dependencies for ${var.project_name} Lambda functions (aiohttp, pyjwt, tenacity, etc.)"
#   compatible_runtimes = ["python3.11"]
#   python_command      = "python3.11"
#   s3_bucket_name      = module.lambda_layers_bucket.bucket_id
# }

# Strands Core Dependencies Layer - Core AI agent framework
module "lambda_layer_strands" {
  source = "./modules/lambda-layer"

  project_name        = var.project_name
  environment         = var.environment
  requirements_file   = "strands-dependencies.txt"
  layer_name_suffix   = "strands"
  layer_description   = "Strands Agents core framework for ${var.project_name} Lambda functions (strands-agents)"
  compatible_runtimes = ["python3.11"]
  python_command      = "python3.11"
  s3_bucket_name      = module.lambda_layers_bucket.bucket_id
}

# Strands Tools Dependencies Layer - AI agent tools
module "lambda_layer_strands_tools" {
  source = "./modules/lambda-layer"

  project_name        = var.project_name
  environment         = var.environment
  requirements_file   = "strands-tools-dependencies.txt"
  layer_name_suffix   = "strands-tools"
  layer_description   = "Strands Agents tools for ${var.project_name} Lambda functions (strands-agents-tools)"
  compatible_runtimes = ["python3.11"]
  python_command      = "python3.11"
  s3_bucket_name      = module.lambda_layers_bucket.bucket_id
}

# Utility Dependencies Layer - Optional utility packages
module "lambda_layer_utility" {
  source = "./modules/lambda-layer"

  project_name        = var.project_name
  environment         = var.environment
  requirements_file   = "utility-dependencies.txt"
  layer_name_suffix   = "utility"
  layer_description   = "Utility and optional dependencies for ${var.project_name} Lambda functions (pillow, sympy, rich, etc.)"
  compatible_runtimes = ["python3.11"]
  python_command      = "python3.11"
  s3_bucket_name      = module.lambda_layers_bucket.bucket_id
}

# S3 bucket for Lambda layers (large files >50MB)
module "lambda_layers_bucket" {
  source = "./modules/s3"

  bucket_name                     = "${var.project_name}-lambda-layers-${var.environment}"
  environment                     = var.environment
  purpose                         = "lambda-layers-storage"
  enable_cross_region_replication = false # Disable replication to avoid aws.replica provider requirement
  force_destroy                   = true
  kms_key_arn                     = module.kms.main_key_arn
  tags                            = var.common_tags
  lifecycle_rules = [
    {
      id     = "cleanup_old_layers"
      status = "Enabled"
      expiration = {
        days = 30
      }
    }
  ]

  # Provide aws.replica provider (even though replication is disabled)
  providers = {
    aws.replica = aws
  }
}

# S3 bucket for static website hosting
module "static_hosting_bucket" {
  source = "./modules/s3"

  bucket_name                     = "${var.project_name}-static-hosting-${var.environment}"
  environment                     = var.environment
  purpose                         = "static-website-hosting"
  force_destroy                   = true
  kms_key_arn                     = module.kms.main_key_arn
  tags                            = var.common_tags
  enable_cross_region_replication = false # Explicitly disable replication
  allow_cloudfront_oac            = true  # Enable CloudFront OAC compatibility

  providers = {
    aws.replica = aws.replica
  }

  depends_on = [module.kms]
}
