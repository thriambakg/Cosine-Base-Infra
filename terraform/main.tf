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
  policy_name_suffix   = "oauth"

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

# Secrets Manager for Alpha Vantage API key
module "alpha_vantage_secrets_manager" {
  source = "./modules/secrets-manager"

  project_name         = var.project_name
  environment          = var.environment
  tags                 = var.common_tags
  kms_key_id           = module.kms.main_key_id
  recovery_window_days = var.secrets_recovery_window_days
  policy_name_suffix   = "alpha-vantage"

  # No automatic rotation for API keys
  automatic_rotation = {}

  # Create empty secret for console population
  secrets = {
    alpha-vantage-api = {
      description = "Alpha Vantage API key for stock data (populated manually)"
      secret_data = {
        # Placeholder value - will be updated manually in console
        api_key = "PLACEHOLDER_ALPHA_VANTAGE_API_KEY"
      }
    }
  }

  depends_on = [module.kms]
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

# REMOVED: Session Management Infrastructure - consolidated into chat_sessions table

# Core Dependencies Layer for Lambda functions
module "core_layer" {
  source = "./modules/lambda-layer"

  project_name        = var.project_name
  environment         = var.environment
  layer_name_suffix   = "core"
  layer_description   = "Core dependencies (boto3, requests, common utilities)"
  requirements_file   = "core-dependencies.txt"
  compatible_runtimes = ["python3.11", "python3.12"]
  s3_bucket_name      = module.static_hosting_bucket.bucket_id
  python_command      = "python3.11"

  depends_on = [module.static_hosting_bucket]
}

# Financial Dependencies Layer for Lambda functions
module "financial_layer" {
  source = "./modules/lambda-layer"

  project_name        = var.project_name
  environment         = var.environment
  layer_name_suffix   = "financial"
  layer_description   = "Financial analysis dependencies (yfinance, numpy, pandas, scipy)"
  requirements_file   = "financial-dependencies.txt"
  compatible_runtimes = ["python3.11", "python3.12"]
  s3_bucket_name      = module.static_hosting_bucket.bucket_id
  python_command      = "python3.11"

  depends_on = [module.static_hosting_bucket]
}

# Crypto Dependencies Layer for Lambda functions
module "crypto_layer" {
  source = "./modules/lambda-layer"

  project_name        = var.project_name
  environment         = var.environment
  layer_name_suffix   = "crypto"
  layer_description   = "Cryptocurrency data dependencies (ccxt, cryptocompare, requests)"
  requirements_file   = "crypto-dependencies.txt"
  compatible_runtimes = ["python3.11", "python3.12"]
  s3_bucket_name      = module.static_hosting_bucket.bucket_id
  python_command      = "python3.11"

  depends_on = [module.static_hosting_bucket]
}

# SQS Queue for Stock Data Processing
module "stock_data_queue" {
  source = "./modules/sqs"

  project_name = var.project_name
  environment  = var.environment
  queue_name   = "stock-data"
  purpose      = "StockDataProcessing"

  # Queue configuration
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 60      # 1 minute
  max_receive_count          = 3
  enable_dlq                 = true

  # Encryption
  kms_key_id = module.kms.main_key_id

  tags = var.common_tags
}

# Stock Data Batch Fetcher Lambda
module "stock_data_batch_fetcher" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-stock-data-batch-fetcher-${var.environment}"
  description   = "Fetches stock data from external APIs and sends to SQS"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 300 # 5 minutes
  memory_size   = 1024

  source_dir = "${path.module}/../backend_app/src/stock_data_batch_fetcher/app"

  # Environment variables
  environment_variables = {
    SQS_QUEUE_URL = module.stock_data_queue.queue_url
    ENVIRONMENT   = var.environment
  }

  # Lambda layers
  layers = [
    module.core_layer.layer_arn,
    module.financial_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    module.stock_data_queue.sqs_access_policy_arn,
    module.alpha_vantage_secrets_manager.secret_access_policy_arn,
    module.kms.kms_access_policy_arn
  ]

  tags = var.common_tags
}

# Stock Data Processor Lambda
module "stock_data_processor" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-stock-data-processor-${var.environment}"
  description   = "Processes stock data from SQS and stores in DynamoDB"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 60 # 1 minute
  memory_size   = 512

  source_dir = "${path.module}/../backend_app/src/stock_data_processor/app"

  # Environment variables
  environment_variables = {
    SQS_QUEUE_URL       = module.stock_data_queue.queue_url
    DYNAMODB_TABLE_NAME = module.dynamodb.stock_data_table_name
    ENVIRONMENT         = var.environment
  }

  # Lambda layers
  layers = [
    module.core_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    module.stock_data_queue.sqs_access_policy_arn,
    module.dynamodb.stock_data_table_policy_arn
  ]

  tags = var.common_tags
}

# EventBridge Rules for Stock Data Batch Fetcher - DISABLED FOR NOW
# TODO: Re-enable once API testing is complete
# 
# # EventBridge Rule for Stock Data Batch Fetcher (High Priority - every 5 minutes)
# resource "aws_cloudwatch_event_rule" "stock_data_batch_fetcher_high_priority" {
#   name                = "${var.project_name}-stock-data-batch-fetcher-high-${var.environment}"
#   description         = "Trigger stock data batch fetcher for high priority stocks every 5 minutes"
#   schedule_expression = "rate(5 minutes)"
# 
#   tags = var.common_tags
# }
# 
# resource "aws_cloudwatch_event_target" "stock_data_batch_fetcher_high_priority" {
#   rule      = aws_cloudwatch_event_rule.stock_data_batch_fetcher_high_priority.name
#   target_id = "StockDataBatchFetcherHighPriority"
#   arn       = module.stock_data_batch_fetcher.function_arn
# 
#   input = jsonencode({
#     priority_tier = "high"
#     timeframe     = "1d"
#   })
# }
# 
# resource "aws_lambda_permission" "allow_eventbridge_high_priority" {
#   statement_id  = "AllowExecutionFromEventBridgeHighPriority"
#   action        = "lambda:InvokeFunction"
#   function_name = module.stock_data_batch_fetcher.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.stock_data_batch_fetcher_high_priority.arn
# }
# 
# # EventBridge Rule for Stock Data Batch Fetcher (Medium Priority - every 15 minutes)
# resource "aws_cloudwatch_event_rule" "stock_data_batch_fetcher_medium_priority" {
#   name                = "${var.project_name}-stock-data-batch-fetcher-medium-${var.environment}"
#   description         = "Trigger stock data batch fetcher for medium priority stocks every 15 minutes"
#   schedule_expression = "rate(15 minutes)"
# 
#   tags = var.common_tags
# }
# 
# resource "aws_cloudwatch_event_target" "stock_data_batch_fetcher_medium_priority" {
#   rule      = aws_cloudwatch_event_rule.stock_data_batch_fetcher_medium_priority.name
#   target_id = "StockDataBatchFetcherMediumPriority"
#   arn       = module.stock_data_batch_fetcher.function_arn
# 
#   input = jsonencode({
#     priority_tier = "medium"
#     timeframe     = "1d"
#   })
# }
# 
# resource "aws_lambda_permission" "allow_eventbridge_medium_priority" {
#   statement_id  = "AllowExecutionFromEventBridgeMediumPriority"
#   action        = "lambda:InvokeFunction"
#   function_name = module.stock_data_batch_fetcher.function_name
#   principal     = "events.amazonaws.com"
#   source_arn    = aws_cloudwatch_event_rule.stock_data_batch_fetcher_medium_priority.arn
# }

# SQS Event Source Mapping for Stock Data Processor
resource "aws_lambda_event_source_mapping" "stock_data_processor_sqs" {
  event_source_arn                   = module.stock_data_queue.queue_arn
  function_name                      = module.stock_data_processor.function_arn
  batch_size                         = 10
  maximum_batching_window_in_seconds = 5
}
