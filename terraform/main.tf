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

# Secrets Manager for NewsData API key
module "newsdata_secrets_manager" {
  source = "./modules/secrets-manager"

  project_name         = var.project_name
  environment          = var.environment
  tags                 = var.common_tags
  kms_key_id           = module.kms.main_key_id
  recovery_window_days = var.secrets_recovery_window_days
  policy_name_suffix   = "newsdata"

  # No automatic rotation for API keys
  automatic_rotation = {}

  # Create empty secret for console population
  secrets = {
    newsdata-api = {
      description = "NewsData.io API key for news data (populated manually)"
      secret_data = {
        # Placeholder value - will be updated manually in console
        api_key = "PLACEHOLDER_NEWSDATA_API_KEY"
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

# ============================================================================
# DYNAMODB TABLES - Individual table modules
# ============================================================================

# User Profiles Table
module "user_profiles_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "user-profiles"

  hash_key  = "user_id"
  range_key = null

  attributes = [
    { name = "user_id", type = "S" },
    { name = "email", type = "S" },
    { name = "created_at", type = "S" }
  ]

  global_secondary_indexes = [
    {
      name            = "EmailIndex"
      hash_key        = "email"
      range_key       = null
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "CreatedAtIndex"
      hash_key        = "created_at"
      range_key       = null
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    }
  ]

  billing_mode                   = var.dynamodb_billing_mode
  read_capacity                  = var.dynamodb_read_capacity
  write_capacity                 = var.dynamodb_write_capacity
  stream_enabled                 = var.dynamodb_stream_enabled
  stream_view_type               = var.dynamodb_stream_view_type
  point_in_time_recovery_enabled = var.dynamodb_point_in_time_recovery_enabled
  deletion_protection_enabled    = var.dynamodb_deletion_protection_enabled
  ttl_enabled                    = var.dynamodb_ttl_enabled
  ttl_attribute_name             = var.dynamodb_ttl_attribute_name

  kms_key_arn = module.kms.dynamodb_key_arn

  table_type    = "UserData"
  table_purpose = "UserProfiles"

  tags = var.common_tags

  depends_on = [module.kms]
}

# Security Events Table
module "security_events_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "security-events"

  hash_key  = "event_id"
  range_key = "timestamp"

  attributes = [
    { name = "event_id", type = "S" },
    { name = "timestamp", type = "S" },
    { name = "user_id", type = "S" },
    { name = "event_type", type = "S" }
  ]

  global_secondary_indexes = [
    {
      name            = "UserIndex"
      hash_key        = "user_id"
      range_key       = "timestamp"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "EventTypeIndex"
      hash_key        = "event_type"
      range_key       = "timestamp"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    }
  ]

  billing_mode                   = var.dynamodb_billing_mode
  read_capacity                  = var.dynamodb_read_capacity
  write_capacity                 = var.dynamodb_write_capacity
  stream_enabled                 = var.dynamodb_stream_enabled
  stream_view_type               = var.dynamodb_stream_view_type
  point_in_time_recovery_enabled = var.dynamodb_point_in_time_recovery_enabled
  deletion_protection_enabled    = var.dynamodb_deletion_protection_enabled
  ttl_enabled                    = true
  ttl_attribute_name             = "expires_at"

  kms_key_arn = module.kms.dynamodb_key_arn

  table_type    = "SecurityData"
  table_purpose = "AuditLogs"

  tags = var.common_tags

  depends_on = [module.kms]
}

# Alerts Table
module "alerts_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "alerts"

  hash_key  = "alert_status"
  range_key = "created_at"

  attributes = [
    { name = "alert_status", type = "S" },
    { name = "created_at", type = "S" },
    { name = "alert_id", type = "S" },
    { name = "user_email", type = "S" }
  ]

  global_secondary_indexes = [
    {
      name            = "AlertIdIndex"
      hash_key        = "alert_id"
      range_key       = null
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "UserAlertsIndex"
      hash_key        = "user_email"
      range_key       = "created_at"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    }
  ]

  billing_mode                   = var.dynamodb_billing_mode
  read_capacity                  = var.dynamodb_read_capacity
  write_capacity                 = var.dynamodb_write_capacity
  stream_enabled                 = var.dynamodb_stream_enabled
  stream_view_type               = var.dynamodb_stream_view_type
  point_in_time_recovery_enabled = var.dynamodb_point_in_time_recovery_enabled
  deletion_protection_enabled    = var.dynamodb_deletion_protection_enabled
  ttl_enabled                    = true
  ttl_attribute_name             = "expires_at"

  kms_key_arn = module.kms.dynamodb_key_arn

  table_type    = "AlertData"
  table_purpose = "StockAlerts"

  tags = var.common_tags

  depends_on = [module.kms]
}

# Chat Connections Table
module "chat_connections_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "chat-connections"

  hash_key  = "connection_id"
  range_key = null

  attributes = [
    { name = "connection_id", type = "S" },
    { name = "user_id", type = "S" },
    { name = "session_id", type = "S" }
  ]

  global_secondary_indexes = [
    {
      name            = "UserConnectionsIndex"
      hash_key        = "user_id"
      range_key       = null
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "SessionConnectionsIndex"
      hash_key        = "session_id"
      range_key       = null
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    }
  ]

  billing_mode                   = var.dynamodb_billing_mode
  read_capacity                  = var.dynamodb_read_capacity
  write_capacity                 = var.dynamodb_write_capacity
  stream_enabled                 = var.dynamodb_stream_enabled
  stream_view_type               = var.dynamodb_stream_view_type
  point_in_time_recovery_enabled = var.dynamodb_point_in_time_recovery_enabled
  deletion_protection_enabled    = var.dynamodb_deletion_protection_enabled
  ttl_enabled                    = true
  ttl_attribute_name             = "expires_at"

  kms_key_arn = module.kms.dynamodb_key_arn

  table_type    = "ConnectionData"
  table_purpose = "WebSocketConnections"

  tags = var.common_tags

  depends_on = [module.kms]
}

# Chat Sessions Table
module "chat_sessions_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "chat-sessions"

  hash_key  = "user_id"
  range_key = "session_id"

  attributes = [
    { name = "user_id", type = "S" },
    { name = "session_id", type = "S" },
    { name = "created_at", type = "N" }
  ]

  global_secondary_indexes = [
    {
      name            = "CreatedAtIndex"
      hash_key        = "user_id"
      range_key       = "created_at"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    }
  ]

  billing_mode                   = var.dynamodb_billing_mode
  read_capacity                  = var.dynamodb_read_capacity
  write_capacity                 = var.dynamodb_write_capacity
  stream_enabled                 = var.dynamodb_stream_enabled
  stream_view_type               = var.dynamodb_stream_view_type
  point_in_time_recovery_enabled = var.dynamodb_point_in_time_recovery_enabled
  deletion_protection_enabled    = false
  ttl_enabled                    = true
  ttl_attribute_name             = "expires_at"

  kms_key_arn = module.kms.dynamodb_key_arn

  table_type    = "ChatData"
  table_purpose = "CompleteChatSessions"

  tags = var.common_tags

  depends_on = [module.kms]
}

# Stock Data Table
module "stock_data_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "stock-data"

  hash_key  = "PK"
  range_key = "SK"

  attributes = [
    { name = "PK", type = "S" },     # STOCK#{symbol}
    { name = "SK", type = "S" },     # {timeframe}#CURRENT (e.g., "1d#CURRENT", "7d#CURRENT")
    { name = "GSI1PK", type = "S" }, # INDUSTRY#{industry}#{timeframe}
    { name = "GSI1SK", type = "N" }, # Volatility (numeric)
    { name = "GSI2PK", type = "S" }, # VOLATILITY#{timeframe}
    { name = "GSI2SK", type = "N" }, # Volatility value (numeric)
    { name = "GSI3PK", type = "S" }, # PRICE_CHANGE#{timeframe}
    { name = "GSI3SK", type = "N" }, # Price change % (numeric)
    { name = "GSI4PK", type = "S" }, # MARKET_CAP#{timeframe}
    { name = "GSI4SK", type = "N" }, # Market cap (numeric)
    { name = "GSI5PK", type = "S" }, # PRICE#{timeframe}
    { name = "GSI5SK", type = "N" }  # Price (numeric)
  ]

  global_secondary_indexes = [
    {
      name            = "IndustryVolatilityIndex"
      hash_key        = "GSI1PK"
      range_key       = "GSI1SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "VolatilityRangeIndex"
      hash_key        = "GSI2PK"
      range_key       = "GSI2SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "PriceChangeRangeIndex"
      hash_key        = "GSI3PK"
      range_key       = "GSI3SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "MarketCapRangeIndex"
      hash_key        = "GSI4PK"
      range_key       = "GSI4SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "PriceRangeIndex"
      hash_key        = "GSI5PK"
      range_key       = "GSI5SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    }
  ]

  billing_mode                   = var.dynamodb_billing_mode
  read_capacity                  = var.dynamodb_read_capacity
  write_capacity                 = var.dynamodb_write_capacity
  stream_enabled                 = var.dynamodb_stream_enabled
  stream_view_type               = var.dynamodb_stream_view_type
  point_in_time_recovery_enabled = var.dynamodb_point_in_time_recovery_enabled
  deletion_protection_enabled    = var.dynamodb_deletion_protection_enabled
  ttl_enabled                    = var.dynamodb_ttl_enabled
  ttl_attribute_name             = "expires_at"

  kms_key_arn = module.kms.dynamodb_key_arn

  table_type    = "StockData"
  table_purpose = "RealTimeStockData"

  iam_policy_actions = [
    "dynamodb:GetItem",
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:DeleteItem",
    "dynamodb:Query",
    "dynamodb:Scan",
    "dynamodb:BatchGetItem",
    "dynamodb:BatchWriteItem"
  ]

  tags = var.common_tags

  depends_on = [module.kms]
}

# News Table - for storing financial news articles
module "news_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "news"

  hash_key  = "PK"
  range_key = "SK"

  attributes = [
    { name = "PK", type = "S" },
    { name = "SK", type = "S" },
    { name = "GSI1PK", type = "S" },
    { name = "GSI1SK", type = "S" },
    { name = "GSI2PK", type = "S" },
    { name = "GSI2SK", type = "S" },
    { name = "GSI3PK", type = "S" },
    { name = "GSI3SK", type = "S" },
    { name = "GSI4PK", type = "S" },
    { name = "GSI4SK", type = "S" },
    { name = "GSI5PK", type = "S" },
    { name = "GSI5SK", type = "S" }
  ]

  global_secondary_indexes = [
    {
      name            = "GSI1"
      hash_key        = "GSI1PK"
      range_key       = "GSI1SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "GSI2"
      hash_key        = "GSI2PK"
      range_key       = "GSI2SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "GSI3"
      hash_key        = "GSI3PK"
      range_key       = "GSI3SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "GSI4"
      hash_key        = "GSI4PK"
      range_key       = "GSI4SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "GSI5"
      hash_key        = "GSI5PK"
      range_key       = "GSI5SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    }
  ]

  billing_mode                   = var.dynamodb_billing_mode
  read_capacity                  = var.dynamodb_read_capacity
  write_capacity                 = var.dynamodb_write_capacity
  stream_enabled                 = var.dynamodb_stream_enabled
  stream_view_type               = var.dynamodb_stream_view_type
  point_in_time_recovery_enabled = var.dynamodb_point_in_time_recovery_enabled
  deletion_protection_enabled    = var.dynamodb_deletion_protection_enabled
  ttl_enabled                    = true
  ttl_attribute_name             = "ttl"

  kms_key_arn = module.kms.dynamodb_key_arn

  table_type    = "NewsData"
  table_purpose = "FinancialNews"

  additional_iam_policy_statements = [
    {
      Effect = "Allow"
      Action = [
        "comprehend:DetectKeyPhrases",
        "comprehend:DetectEntities",
        "comprehend:DetectSentiment"
      ]
      Resource = "*"
    }
  ]

  tags = var.common_tags

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
    USER_PROFILES_TABLE_NAME = module.user_profiles_table.table_name
  }

  # IAM policies for DynamoDB access
  additional_policy_arns = [
    module.user_profiles_table.table_policy_arn
  ]

  tags = var.common_tags

  depends_on = [module.user_profiles_table]
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
  user_profiles_table_name       = module.user_profiles_table.table_name
  security_log_retention_days    = var.cloudwatch_security_log_retention_days
  auth_log_retention_days        = var.cloudwatch_auth_log_retention_days
  application_log_retention_days = var.cloudwatch_application_log_retention_days
  lambda_log_retention_days      = var.cloudwatch_lambda_log_retention_days
  api_gateway_log_retention_days = var.cloudwatch_api_gateway_log_retention_days
  failed_login_threshold         = var.cloudwatch_failed_login_threshold
  suspicious_activity_threshold  = var.cloudwatch_suspicious_activity_threshold
  alarm_notification_topic_arn   = var.cloudwatch_alarm_notification_topic_arn

  depends_on = [module.kms, module.cognito, module.user_profiles_table]
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

# News Dependencies Layer for Lambda functions (Python 3.9)
module "news_layer" {
  source = "./modules/lambda-layer"

  project_name        = var.project_name
  environment         = var.environment
  layer_name_suffix   = "news"
  layer_description   = "News data dependencies (newsdataapi, requests, urllib3)"
  requirements_file   = "news-dependencies.txt"
  compatible_runtimes = ["python3.9", "python3.10"]
  s3_bucket_name      = module.static_hosting_bucket.bucket_id
  python_command      = "python3.9"

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
    SQS_QUEUE_URL        = module.stock_data_queue.queue_url
    ENVIRONMENT          = var.environment
    BATCH_SIZE_HIGH      = "100" # Fortune 500 companies
    BATCH_SIZE_MEDIUM    = "75"  # Mid-cap stocks
    BATCH_SIZE_LOW       = "50"  # Small-cap stocks
    MAX_PARALLEL_THREADS = "10"  # Parallel HTTP requests
    REQUEST_RATE_LIMIT   = "1.0" # Requests per second per thread
  }

  # Lambda layers
  layers = [
    module.core_layer.layer_arn,
    module.financial_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    module.stock_data_queue.sqs_access_policy_arn,
    module.newsdata_secrets_manager.secret_access_policy_arn,
    module.kms.kms_access_policy_arn
  ]

  tags = var.common_tags
}

# Stock Data Processor Lambda - Updates S3 historical files bi-hourly
module "stock_data_processor" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-stock-data-processor-${var.environment}"
  description   = "Processes stock data from SQS and updates S3 historical files (bi-hourly during market hours)"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 300 # 5 minutes (may need to process many stocks)
  memory_size   = 1024

  source_dir = "${path.module}/../backend_app/src/stock_data_processor/app"

  # Environment variables
  environment_variables = {
    S3_BUCKET            = module.stock_data_historical_s3.bucket_id
    ENVIRONMENT          = var.environment
    MAX_PARALLEL_THREADS = "10"  # Parallel HTTP requests
    REQUEST_RATE_LIMIT   = "1.0" # Requests per second per thread
    BATCH_TIMEOUT        = "50"  # Seconds before timeout warning
  }

  # Lambda layers
  layers = [
    module.core_layer.layer_arn,
    module.financial_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    module.stock_data_queue.sqs_access_policy_arn,
    aws_iam_policy.stock_data_historical_s3_access.arn,
    module.kms.kms_access_policy_arn
  ]

  tags = var.common_tags

  depends_on = [module.stock_data_historical_s3]
}

# ==============================================================================
# STOCK DATA BI-HOURLY UPDATE SCHEDULERS (During Market Hours Only)
# ==============================================================================
# These schedulers trigger the stock_data_batch_fetcher to send batches to SQS
# The stock_data_processor Lambda (triggered by SQS) updates S3 historical files

# High Priority Stocks - Every 30 minutes during market hours (9:00 AM - 4:00 PM ET)
# Note: EventBridge uses UTC, so we need to adjust for ET timezone
# During DST (March-November): ET = UTC-4, so 9:00 AM ET = 1:00 PM UTC
# During Standard (November-March): ET = UTC-5, so 9:00 AM ET = 2:00 PM UTC
# Using DST times: 13:00-20:00 UTC (covers 9:00 AM - 4:00 PM ET during DST)
module "stock_data_high_priority_scheduler" {
  source = "./modules/eventbridge-scheduler"

  rule_name           = "${var.project_name}-stock-data-high-priority-${var.environment}"
  rule_description    = "Trigger high-priority stock data updates every 30 minutes during market hours (9:00 AM - 4:00 PM ET)"
  schedule_expression = "cron(0,30 13,14,15,16,17,18,19 ? * MON-FRI *)" # Every 30 min from 1:00 PM-8:00 PM UTC (9 AM-4 PM ET DST)
  enabled             = true

  target_arn           = module.stock_data_batch_fetcher.function_arn
  target_id            = "StockDataHighPriorityScheduler"
  target_type          = "lambda"
  target_function_name = module.stock_data_batch_fetcher.function_name
  target_input = jsonencode({
    priority = "high"
    source   = "scheduler-market-hours"
  })

  purpose     = "StockDataHighPriorityUpdate"
  environment = var.environment
  tags        = var.common_tags

  depends_on = [module.stock_data_batch_fetcher]
}

# Medium Priority Stocks - Every hour during market hours
module "stock_data_medium_priority_scheduler" {
  source = "./modules/eventbridge-scheduler"

  rule_name           = "${var.project_name}-stock-data-medium-priority-${var.environment}"
  rule_description    = "Trigger medium-priority stock data updates every hour during market hours (9:00 AM - 4:00 PM ET)"
  schedule_expression = "cron(0 13,14,15,16,17,18,19,20 ? * MON-FRI *)" # Every hour from 1:00 PM-8:00 PM UTC (9 AM-4 PM ET DST)
  enabled             = true

  target_arn           = module.stock_data_batch_fetcher.function_arn
  target_id            = "StockDataMediumPriorityScheduler"
  target_type          = "lambda"
  target_function_name = module.stock_data_batch_fetcher.function_name
  target_input = jsonencode({
    priority = "medium"
    source   = "scheduler-market-hours"
  })

  purpose     = "StockDataMediumPriorityUpdate"
  environment = var.environment
  tags        = var.common_tags

  depends_on = [module.stock_data_batch_fetcher]
}

# Low Priority Stocks - Every 2 hours during market hours
module "stock_data_low_priority_scheduler" {
  source = "./modules/eventbridge-scheduler"

  rule_name           = "${var.project_name}-stock-data-low-priority-${var.environment}"
  rule_description    = "Trigger low-priority stock data updates every 2 hours during market hours (9:00 AM - 4:00 PM ET)"
  schedule_expression = "cron(0 13,15,17,19 ? * MON-FRI *)" # Every 2 hours from 1:00 PM-7:00 PM UTC (9 AM-3 PM ET DST)
  enabled             = true

  target_arn           = module.stock_data_batch_fetcher.function_arn
  target_id            = "StockDataLowPriorityScheduler"
  target_type          = "lambda"
  target_function_name = module.stock_data_batch_fetcher.function_name
  target_input = jsonencode({
    priority = "low"
    source   = "scheduler-market-hours"
  })

  purpose     = "StockDataLowPriorityUpdate"
  environment = var.environment
  tags        = var.common_tags

  depends_on = [module.stock_data_batch_fetcher]
}

# SQS Queue for News Processing
module "news_queue" {
  source = "./modules/sqs"

  project_name = var.project_name
  environment  = var.environment
  queue_name   = "news"
  purpose      = "NewsProcessing"

  # Queue configuration
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 60      # 1 minute
  max_receive_count          = 3
  enable_dlq                 = true

  # Encryption
  kms_key_id = module.kms.main_key_id

  tags = var.common_tags
}

# News Fetcher Lambda (Python 3.9)
module "news_fetcher" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-news-fetcher-${var.environment}"
  description   = "Fetches financial news from NewsData.io and sends to SQS"
  runtime       = "python3.9"
  handler       = "lambda_function.lambda_handler"
  timeout       = 300 # 5 minutes
  memory_size   = 512

  source_dir = "${path.module}/../backend_app/src/news_fetcher/app"

  # Environment variables
  environment_variables = {
    SQS_QUEUE_URL = module.news_queue.queue_url
    PROJECT_NAME  = var.project_name
    ENVIRONMENT   = var.environment
  }

  # Lambda layers (Python 3.9)
  layers = [
    module.news_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    module.news_queue.sqs_access_policy_arn,
    module.newsdata_secrets_manager.secret_access_policy_arn,
    module.kms.kms_access_policy_arn
  ]

  tags = var.common_tags
}

# News Processor Lambda (Python 3.11)
module "news_processor" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-news-processor-${var.environment}"
  description   = "Processes news articles from SQS and stores in DynamoDB"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 60 # 1 minute
  memory_size   = 512

  source_dir = "${path.module}/../backend_app/src/news_processor/app"

  # Environment variables
  environment_variables = {
    NEWS_TABLE_NAME = module.news_table.table_name
  }

  # Lambda layers (Python 3.11)
  layers = [
    module.core_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    module.news_queue.sqs_access_policy_arn,
    module.news_table.table_policy_arn
  ]

  tags = var.common_tags
}

# SQS Event Source Mapping for Stock Data Processor
resource "aws_lambda_event_source_mapping" "stock_data_processor_sqs" {
  event_source_arn                   = module.stock_data_queue.queue_arn
  function_name                      = module.stock_data_processor.function_arn
  batch_size                         = 10
  maximum_batching_window_in_seconds = 5
}

# SQS Event Source Mapping for News Processor
resource "aws_lambda_event_source_mapping" "news_processor_sqs" {
  event_source_arn                   = module.news_queue.queue_arn
  function_name                      = module.news_processor.function_arn
  batch_size                         = 10
  maximum_batching_window_in_seconds = 5
}

# EventBridge Scheduler for News Fetcher (every 8 minutes)
module "news_fetcher_scheduler" {
  source = "./modules/eventbridge-scheduler"

  rule_name           = "${var.project_name}-news-fetcher-${var.environment}"
  rule_description    = "Trigger news fetcher every 8 minutes to distribute 200 credits across 24 hours"
  schedule_expression = "rate(8 minutes)"
  enabled             = true

  target_arn           = module.news_fetcher.function_arn
  target_id            = "NewsFetcherScheduler"
  target_type          = "lambda"
  target_function_name = module.news_fetcher.function_name
  target_input = jsonencode({
    source    = "scheduler"
    timestamp = "{{.Timestamp}}"
  })

  purpose     = "NewsFetching"
  environment = var.environment
  tags        = var.common_tags
}

# ==============================================================================
# STOCK DATA HISTORICAL LOADER (One-Time Load via Step Functions)
# ==============================================================================

# S3 Bucket for Historical Stock Data
module "stock_data_historical_s3" {
  source = "./modules/s3"

  # Required providers
  providers = {
    aws         = aws
    aws.replica = aws.replica
  }

  bucket_name = "${var.project_name}-stock-historical-${var.environment}"
  environment = var.environment
  purpose     = "StockHistoricalData"

  # Enable lifecycle transitions to Glacier
  enable_lifecycle_transitions = true
  transition_to_glacier_days   = 90

  # Enable expiration after 5 years
  enable_expiration = true
  expiration_days   = 1825 # 5 years

  # Abort incomplete multipart uploads after 7 days
  abort_incomplete_multipart_upload_days = 7

  # Noncurrent version expiration
  noncurrent_version_expiration_days = 30

  kms_key_arn = module.kms.main_key_arn
  tags        = var.common_tags
}

# Historical Loader Lambda (Python 3.11)
module "stock_data_historical_loader" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-stock-data-historical-loader-${var.environment}"
  description   = "Loads 5 years of historical stock data from Yahoo Finance to S3 (orchestrated by Step Functions)"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 900  # 15 minutes (max)
  memory_size   = 3008 # Max memory for faster processing

  source_dir = "${path.module}/../backend_app/src/stock_data_historical_loader/app"

  # Environment variables
  environment_variables = {
    S3_BUCKET   = module.stock_data_historical_s3.bucket_id
    RATE_LIMIT  = "2.0" # 2 requests/second to Yahoo Finance
    MAX_WORKERS = "5"   # Parallel workers for batch processing
  }

  # Lambda layers (Python 3.11)
  layers = [
    module.core_layer.layer_arn,
    module.financial_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    aws_iam_policy.stock_data_historical_s3_access.arn,
    module.kms.kms_access_policy_arn
  ]

  tags = var.common_tags
}

# IAM Policy for Lambda to access S3 historical data bucket
resource "aws_iam_policy" "stock_data_historical_s3_access" {
  name        = "${var.project_name}-stock-historical-s3-access-${var.environment}"
  description = "Allows Lambda to read/write to stock historical data S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          module.stock_data_historical_s3.bucket_arn,
          "${module.stock_data_historical_s3.bucket_arn}/*"
        ]
      }
    ]
  })

  tags = var.common_tags
}

# Step Functions State Machine for Historical Data Loading
module "stock_data_historical_loader_state_machine" {
  source = "./modules/step-functions"

  state_machine_name = "${var.project_name}-stock-historical-loader-${var.environment}"
  environment        = var.environment

  # Step Functions definition with self-contained batch generation
  definition = jsonencode({
    Comment = "Load historical stock data in parallel batches with automatic batch generation"
    StartAt = "GenerateBatches"
    States = {
      # Step 1: Generate batches (Lambda reads CSVs and creates batch configs)
      GenerateBatches = {
        Type       = "Task"
        Resource   = module.stock_data_historical_loader.function_arn
        Comment    = "Generate batches from CSV files (no input required)"
        ResultPath = "$.batchConfig"
        Next       = "ProcessBatches"
        Retry = [
          {
            ErrorEquals     = ["States.ALL"]
            IntervalSeconds = 5
            MaxAttempts     = 3
            BackoffRate     = 2.0
          }
        ]
      }

      # Step 2: Process batches in parallel (Map state)
      ProcessBatches = {
        Type           = "Map"
        ItemsPath      = "$.batchConfig.batches"
        MaxConcurrency = 5 # Process 5 batches at a time (rate limiting)
        ResultPath     = "$.results"

        Iterator = {
          StartAt = "LoadBatch"
          States = {
            LoadBatch = {
              Type           = "Task"
              Resource       = module.stock_data_historical_loader.function_arn
              Comment        = "Load historical data for a batch of stocks"
              TimeoutSeconds = 900 # 15 minutes per batch
              Retry = [
                {
                  ErrorEquals     = ["States.TaskFailed", "States.Timeout"]
                  IntervalSeconds = 60
                  MaxAttempts     = 3
                  BackoffRate     = 2.0
                }
              ]
              Catch = [
                {
                  ErrorEquals = ["States.ALL"]
                  ResultPath  = "$.error"
                  Next        = "BatchFailed"
                }
              ]
              End = true
            }

            BatchFailed = {
              Type = "Pass"
              Result = {
                status = "failed"
              }
              End = true
            }
          }
        }

        Next = "AggregateResults"
      }

      # Step 3: Aggregate results and log summary
      AggregateResults = {
        Type    = "Pass"
        Comment = "Summarize batch processing results"
        Parameters = {
          "totalBatches.$" = "$.batchConfig.total_batches"
          "totalSymbols.$" = "$.batchConfig.total_symbols"
          "results.$"      = "$.results"
          "completedAt.$"  = "$$.State.EnteredTime"
        }
        Next = "Success"
      }

      # Final state
      Success = {
        Type = "Succeed"
      }
    }
  })

  # Lambda ARNs for IAM permissions
  lambda_function_arns = [
    module.stock_data_historical_loader.function_arn
  ]

  # Logging configuration
  log_level              = var.environment == "production" ? "ERROR" : "ALL"
  log_retention_days     = 7
  include_execution_data = true

  tags = var.common_tags
}
