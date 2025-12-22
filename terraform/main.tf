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
  # Note: Glue role ARN will be added via separate aws_kms_key_policy resource to avoid circular dependency
  additional_role_arns = []
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

# Secrets Manager for Congress.gov API key
module "congress_api_secrets_manager" {
  source = "./modules/secrets-manager"

  project_name         = var.project_name
  environment          = var.environment
  tags                 = var.common_tags
  kms_key_id           = module.kms.main_key_id
  recovery_window_days = var.secrets_recovery_window_days
  policy_name_suffix   = "congress-api"

  # No automatic rotation for API keys
  automatic_rotation = {}

  # Create empty secret for console population
  secrets = {
    congress-api = {
      description = "Congress.gov API key for legislative data (populated manually)"
      secret_data = {
        # Placeholder value - will be updated manually in console
        api_key = "PLACEHOLDER_CONGRESS_API_KEY"
      }
    }
  }

  depends_on = [module.kms]
}

# Secrets Manager for LDA Senate API key
module "lda_api_secrets_manager" {
  source = "./modules/secrets-manager"

  project_name         = var.project_name
  environment          = var.environment
  tags                 = var.common_tags
  kms_key_id           = module.kms.main_key_id
  recovery_window_days = var.secrets_recovery_window_days
  policy_name_suffix   = "lda-api"

  # No automatic rotation for API keys
  automatic_rotation = {}

  # Create empty secret for console population
  secrets = {
    lda-api = {
      description = "LDA Senate API key for lobbying disclosure data (populated manually)"
      secret_data = {
        # Placeholder value - will be updated manually in console
        api_key = "PLACEHOLDER_LDA_API_KEY"
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
    { name = "GSI1PK", type = "S" }, # SECTOR#{sector}#{timeframe} (e.g., "SECTOR#Financials#1d")
    { name = "GSI1SK", type = "N" }, # Volatility (numeric)
    { name = "GSI2PK", type = "S" }, # VOLATILITY#{timeframe}
    { name = "GSI2SK", type = "N" }, # Volatility value (numeric)
    { name = "GSI3PK", type = "S" }, # PRICE_CHANGE#{timeframe}
    { name = "GSI3SK", type = "N" }, # Price change % (numeric)
    { name = "GSI4PK", type = "S" }, # MARKET_CAP#{timeframe}
    { name = "GSI4SK", type = "N" }, # Market cap (numeric)
    { name = "GSI5PK", type = "S" }, # PRICE#{timeframe}
    { name = "GSI5SK", type = "N" }, # Price (numeric)
    { name = "GSI6PK", type = "S" }, # PE_RATIO#{timeframe}
    { name = "GSI6SK", type = "N" }, # P/E ratio (numeric)
    { name = "GSI7PK", type = "S" }, # DIVIDEND_YIELD#{timeframe}
    { name = "GSI7SK", type = "N" }  # Dividend yield (numeric)
  ]

  global_secondary_indexes = [
    {
      name            = "SectorVolatilityIndex"
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
    },
    {
      name            = "PERatioRangeIndex"
      hash_key        = "GSI6PK"
      range_key       = "GSI6SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "DividendYieldRangeIndex"
      hash_key        = "GSI7PK"
      range_key       = "GSI7SK"
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

# Document/Image Processing Dependencies Layer for Lambda functions
module "document_processing_layer" {
  source = "./modules/lambda-layer"

  project_name        = var.project_name
  environment         = var.environment
  layer_name_suffix   = "document-processing"
  layer_description   = "Document and image processing dependencies (Pillow for GIF/PNG conversion, future LLM image processing)"
  requirements_file   = "document-processing-dependencies.txt"
  compatible_runtimes = ["python3.11", "python3.12"]
  s3_bucket_name      = module.static_hosting_bucket.bucket_id
  python_command      = "python3.11"

  depends_on = [module.static_hosting_bucket]
}

# Document Processing (PDF) Dependencies Layer for Lambda functions
module "docprocessing_layer" {
  source = "./modules/lambda-layer"

  project_name        = var.project_name
  environment         = var.environment
  layer_name_suffix   = "docprocessing"
  layer_description   = "PDF document processing dependencies (pypdf for parsing House PTR PDFs)"
  requirements_file   = "docprocessing-dependencies.txt"
  compatible_runtimes = ["python3.11", "python3.12"]
  s3_bucket_name      = module.static_hosting_bucket.bucket_id
  python_command      = "python3.11"

  depends_on = [module.static_hosting_bucket]
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

  source_dir = "${path.module}/../backend_app/src/NEWS/news_fetcher/app"

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

  source_dir = "${path.module}/../backend_app/src/NEWS/news_processor/app"

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
# COMMENTED OUT - Already ran, don't want to overwrite S3 data
# ==============================================================================

# S3 Bucket for Historical Stock Data (KEEP - needed by EOD aggregator)
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

# S3 Bucket for Chat File Uploads
module "chat_files_s3" {
  source = "./modules/s3"

  # Required providers
  providers = {
    aws         = aws
    aws.replica = aws.replica
  }

  bucket_name = "${var.project_name}-chat-files-${var.environment}"
  environment = var.environment
  purpose     = "ChatFileUploads"

  # Enable lifecycle transitions to IA and Glacier for cost optimization
  enable_lifecycle_transitions = true
  transition_to_ia_days        = 30 # Move to IA after 30 days (AWS minimum)
  transition_to_glacier_days   = 60 # Move to Glacier after 60 days

  # Enable expiration after 90 days (chat files TTL)
  # Files automatically deleted after 90 days to handle message editing scenarios
  # This prevents orphaned files when users edit/delete old messages
  enable_expiration = true
  expiration_days   = 90 # 90 days - files auto-delete after 3 months

  # Abort incomplete multipart uploads after 1 day
  abort_incomplete_multipart_upload_days = 1

  # Noncurrent version expiration
  noncurrent_version_expiration_days = 7

  # S3 notifications disabled - handled by separate notification resource below
  notification_topic_arn = ""

  kms_key_arn = module.kms.main_key_arn
  tags        = var.common_tags
}

# S3 Bucket Notification is now handled by the S3 module

# IAM Policy for Lambda to access S3 chat files bucket
resource "aws_iam_policy" "lambda_s3_chat_files_policy" {
  name        = "${var.project_name}-lambda-s3-chat-files-access-${var.environment}"
  description = "Allows Lambda to read/write to chat files S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
          "s3:GeneratePresignedUrl"
        ]
        Resource = [
          module.chat_files_s3.bucket_arn,
          "${module.chat_files_s3.bucket_arn}/*"
        ]
      }
    ]
  })

  tags = var.common_tags
}


# Historical Loader Lambda (Python 3.11) - UNCOMMENTED TO ADD P/E & DIVIDEND YIELD
# Loads 5 years of historical stock data with P/E ratio and dividend yield
module "stock_data_historical_loader" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-stock-data-historical-loader-${var.environment}"
  description   = "Loads 5 years of historical stock data from Yahoo Finance to S3 (orchestrated by Step Functions)"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 900  # 15 minutes (max)
  memory_size   = 3008 # Max memory for faster processing

  source_dir = "${path.module}/../backend_app/src/STOCK/stock_data_historical_loader/app"

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

# Step Functions State Machine for Historical Data Loading - UNCOMMENTED
# Re-running to populate S3 with P/E ratio and dividend yield data
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

# EventBridge Scheduler for Daily Historical Data Loading (4:30 PM ET after market close)
# Runs Monday-Friday at 4:30 PM ET to fetch EOD data from Yahoo Finance

# IAM Role for EventBridge to invoke Historical Loader Step Function
resource "aws_iam_role" "historical_loader_scheduler_role" {
  name = "${var.project_name}-historical-loader-scheduler-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.common_tags
}

# IAM Policy for EventBridge to start Historical Loader Step Function
resource "aws_iam_role_policy" "historical_loader_scheduler_policy" {
  name = "${var.project_name}-historical-loader-scheduler-policy-${var.environment}"
  role = aws_iam_role.historical_loader_scheduler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = module.stock_data_historical_loader_state_machine.state_machine_arn
      }
    ]
  })
}

module "historical_loader_scheduler" {
  source = "./modules/eventbridge-scheduler"

  rule_name           = "${var.project_name}-historical-loader-${var.environment}"
  rule_description    = "Trigger historical data loader daily at 4:30 PM ET (after market close) to fetch EOD data and update S3"
  schedule_expression = "cron(30 20 ? * MON-FRI *)" # 4:30 PM ET = 8:30 PM UTC during DST
  enabled             = true

  # Target is Step Functions state machine, not Lambda
  target_arn = module.stock_data_historical_loader_state_machine.state_machine_arn
  target_id  = "HistoricalLoaderScheduler"

  # For Step Functions, we need to provide a role
  target_type     = "stepfunctions"
  target_role_arn = aws_iam_role.historical_loader_scheduler_role.arn

  target_input = jsonencode({
    source    = "scheduler-eod"
    timestamp = "scheduled"
  })

  purpose     = "HistoricalDataLoading"
  environment = var.environment
  tags        = var.common_tags

  depends_on = [
    module.stock_data_historical_loader_state_machine,
    aws_iam_role.historical_loader_scheduler_role,
    aws_iam_role_policy.historical_loader_scheduler_policy
  ]
}

# ==============================================================================
# EOD (END OF DAY) AGGREGATOR SYSTEM
# ==============================================================================
# Runs daily at 5:00 PM ET (30 min after historical loader) to aggregate S3 data into DynamoDB
# Uses Step Functions for parallel batch processing

# EOD Batch Generator Lambda - Lists S3 files and creates batches
module "eod_batch_generator" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-eod-batch-generator-${var.environment}"
  description   = "Generates batches of stock symbols from S3 for EOD aggregator processing"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 60 # 1 minute
  memory_size   = 512

  source_dir = "${path.module}/../backend_app/src/EODSTOCK/eod_batch_generator/app"

  # Environment variables
  environment_variables = {
    S3_BUCKET  = module.stock_data_historical_s3.bucket_id
    BATCH_SIZE = "200" # Stocks per batch for parallel processing
  }

  # Lambda layers - includes financial layer for pandas_market_calendars
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

  depends_on = [module.stock_data_historical_s3]
}

# EOD Aggregator Lambda - Reads S3, calculates metrics, writes to DynamoDB
module "eod_aggregator" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-eod-aggregator-${var.environment}"
  description   = "Aggregates historical stock data from S3 into DynamoDB for querying (runs daily after market close)"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 300 # 5 minutes per batch
  memory_size   = 1024

  source_dir = "${path.module}/../backend_app/src/EODSTOCK/eod_aggregator/app"

  # Environment variables
  environment_variables = {
    S3_BUCKET           = module.stock_data_historical_s3.bucket_id
    DYNAMODB_TABLE_NAME = module.stock_data_table.table_name
  }

  # Lambda layers
  layers = [
    module.core_layer.layer_arn,
    module.financial_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    aws_iam_policy.stock_data_historical_s3_access.arn,
    module.stock_data_table.table_policy_arn,
    module.kms.kms_access_policy_arn
  ]

  tags = var.common_tags

  depends_on = [module.stock_data_historical_s3, module.stock_data_table]
}

# Step Functions State Machine for EOD Aggregation
module "eod_aggregator_state_machine" {
  source = "./modules/step-functions"

  state_machine_name = "${var.project_name}-eod-aggregator-${var.environment}"
  environment        = var.environment

  # Step Functions definition
  definition = jsonencode({
    Comment = "Daily EOD aggregator - reads S3 historical data and updates DynamoDB with calculated metrics"
    StartAt = "GenerateBatches"
    States = {
      # Step 1: Generate batches from S3 file list
      GenerateBatches = {
        Type       = "Task"
        Resource   = module.eod_batch_generator.function_arn
        Comment    = "List S3 stock files and create batches for parallel processing"
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

      # Step 2: Process batches in parallel (Map state - 10 concurrent executions)
      ProcessBatches = {
        Type           = "Map"
        ItemsPath      = "$.batchConfig.batches"
        MaxConcurrency = 10 # Reduced from 40 to avoid Lambda throttling (429 errors)
        ResultPath     = "$.results"

        Iterator = {
          StartAt = "AggregateBatch"
          States = {
            AggregateBatch = {
              Type           = "Task"
              Resource       = module.eod_aggregator.function_arn
              Comment        = "Read S3, calculate metrics for all timeframes, write to DynamoDB"
              TimeoutSeconds = 300 # 5 minutes per batch
              Retry = [
                {
                  ErrorEquals     = ["Lambda.TooManyRequestsException", "Lambda.ServiceException"]
                  IntervalSeconds = 5
                  MaxAttempts     = 5
                  BackoffRate     = 2.0
                },
                {
                  ErrorEquals     = ["States.TaskFailed", "States.Timeout"]
                  IntervalSeconds = 30
                  MaxAttempts     = 2
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

      # Step 3: Aggregate results
      AggregateResults = {
        Type    = "Pass"
        Comment = "Summarize EOD aggregation results"
        Parameters = {
          "totalBatches.$" = "$.batchConfig.total_batches"
          "totalStocks.$"  = "$.batchConfig.total_stocks"
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
    module.eod_batch_generator.function_arn,
    module.eod_aggregator.function_arn
  ]

  # Logging configuration
  log_level              = var.environment == "production" ? "ERROR" : "ALL"
  log_retention_days     = 7
  include_execution_data = true

  tags = var.common_tags

  depends_on = [module.eod_batch_generator, module.eod_aggregator]
}

# EventBridge Scheduler for Daily EOD Aggregation (5:00 PM ET, 30 min after historical loader)
# Runs Monday-Friday at 5:00 PM ET = 9:00 PM UTC (DST) or 10:00 PM UTC (Standard)

# IAM Role for EventBridge to invoke EOD Aggregator Step Function
resource "aws_iam_role" "eod_aggregator_scheduler_role" {
  name = "${var.project_name}-eod-aggregator-scheduler-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.common_tags
}

# IAM Policy for EventBridge to start EOD Aggregator Step Function
resource "aws_iam_role_policy" "eod_aggregator_scheduler_policy" {
  name = "${var.project_name}-eod-aggregator-scheduler-policy-${var.environment}"
  role = aws_iam_role.eod_aggregator_scheduler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = module.eod_aggregator_state_machine.state_machine_arn
      }
    ]
  })
}

module "eod_aggregator_scheduler" {
  source = "./modules/eventbridge-scheduler"

  rule_name           = "${var.project_name}-eod-aggregator-${var.environment}"
  rule_description    = "Trigger EOD aggregator daily at 5:00 PM ET (30 min after historical loader) to update DynamoDB from S3"
  schedule_expression = "cron(0 21 ? * MON-FRI *)" # 5:00 PM ET = 9:00 PM UTC during DST
  enabled             = true

  # Target is Step Functions state machine, not Lambda
  target_arn = module.eod_aggregator_state_machine.state_machine_arn
  target_id  = "EODAggregatorScheduler"

  # For Step Functions, we need to provide a role
  target_type     = "stepfunctions"
  target_role_arn = aws_iam_role.eod_aggregator_scheduler_role.arn

  target_input = jsonencode({
    source    = "scheduler-eod"
    timestamp = "scheduled"
  })

  purpose     = "EODDataAggregation"
  environment = var.environment
  tags        = var.common_tags

  depends_on = [
    module.eod_aggregator_state_machine,
    aws_iam_role.eod_aggregator_scheduler_role,
    aws_iam_role_policy.eod_aggregator_scheduler_policy
  ]
}


# ==============================================================================
# POLITICIAN TRADES AGGREGATION SYSTEM
# ==============================================================================
# Daily batch job that fetches SEC forms (3, 4, 5) and Congressional PTRs,
# matches trades to politicians, and stores in DynamoDB for dashboard querying

# S3 Bucket for SEC Forms and Politician Data
module "politician_trades_s3" {
  source = "./modules/s3"

  providers = {
    aws         = aws
    aws.replica = aws.replica
  }

  bucket_name = "${var.project_name}-politician-trades-${var.environment}"
  environment = var.environment
  purpose     = "PoliticianTradesData"

  # Enable lifecycle transitions to Glacier for cost optimization
  enable_lifecycle_transitions = true
  transition_to_ia_days        = 30
  transition_to_glacier_days   = 90

  # Enable expiration after 2 years (keep raw forms for compliance/audit)
  enable_expiration = true
  expiration_days   = 730 # 2 years

  # Abort incomplete multipart uploads after 7 days
  abort_incomplete_multipart_upload_days = 7

  # Noncurrent version expiration
  noncurrent_version_expiration_days = 30

  kms_key_arn = module.kms.main_key_arn

  # Upload static files (legislators CSV from cloned congress-legislators repo)
  # Note: Keep this file updated by running: scripts/update-legislators-csv.ps1
  static_files = [
    {
      source_path  = "${path.module}/../static-files/lists/congress-legislators.csv"
      s3_key       = "congress-legislators.csv"
      content_type = "text/csv"
    },
    {
      source_path  = "${path.module}/../static-files/mappings/house_ptr_asset_codes.csv"
      s3_key       = "house_ptr_asset_codes.csv"
      content_type = "text/csv"
    }
  ]

  tags = var.common_tags
}

# S3 Bucket for SEC Filings (separate from politician trades for scalability)
module "sec_filings_s3" {
  source = "./modules/s3"

  providers = {
    aws         = aws
    aws.replica = aws.replica
  }

  bucket_name = "${var.project_name}-sec-filings-${var.environment}"
  environment = var.environment
  purpose     = "SECFilingsData"

  # Enable lifecycle transitions to Glacier for cost optimization
  enable_lifecycle_transitions = true
  transition_to_ia_days        = 30
  transition_to_glacier_days   = 90

  # Enable expiration after 2 years (keep raw forms for compliance/audit)
  enable_expiration = true
  expiration_days   = 730 # 2 years

  # Abort incomplete multipart uploads after 7 days
  abort_incomplete_multipart_upload_days = 7

  # Noncurrent version expiration
  noncurrent_version_expiration_days = 30

  kms_key_arn = module.kms.main_key_arn

  # Upload static files (legislators CSV - same file as politician trades)
  # Note: Keep this file updated by running: scripts/update-legislators-csv.ps1
  static_files = [
    {
      source_path  = "${path.module}/../static-files/lists/congress-legislators.csv"
      s3_key       = "congress-legislators.csv"
      content_type = "text/csv"
    }
  ]

  tags = var.common_tags
}

# S3 Bucket for USAspending Award Details (Transactions and Subawards)
# Stores combined transaction and subaward data as gzipped JSON files
# Structure: award-details/{award_id}.json.gz
module "usaspending_data_s3" {
  source = "./modules/s3"

  providers = {
    aws         = aws
    aws.replica = aws.replica
  }

  bucket_name = "${var.project_name}-usaspending-data-${var.environment}"
  environment = var.environment
  purpose     = "USASpendingAwardDetails"

  # Enable lifecycle transitions to Glacier for cost optimization
  enable_lifecycle_transitions = true
  transition_to_ia_days        = 30
  transition_to_glacier_days   = 90

  # Enable expiration after 120 days (must be greater than glacier transition)
  # Award details are re-indexed on demand, so we can expire old files
  enable_expiration = true
  expiration_days   = 120 # 120 days - must be > transition_to_glacier_days (90)

  # Abort incomplete multipart uploads after 7 days
  abort_incomplete_multipart_upload_days = 7

  # Noncurrent version expiration
  noncurrent_version_expiration_days = 30

  kms_key_arn = module.kms.main_key_arn

  tags = var.common_tags
}

# IAM Policy for Lambda to access S3 politician trades bucket
resource "aws_iam_policy" "lambda_politician_trades_s3_policy" {
  name        = "${var.project_name}-lambda-politician-trades-s3-access-${var.environment}"
  description = "Allows Lambda to read/write to politician trades S3 bucket"

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
          module.politician_trades_s3.bucket_arn,
          "${module.politician_trades_s3.bucket_arn}/*"
        ]
      }
    ]
  })

  tags = var.common_tags
}

# IAM Policy for Lambda to access S3 USAspending data bucket
resource "aws_iam_policy" "lambda_usaspending_data_s3_policy" {
  name        = "${var.project_name}-lambda-usaspending-data-s3-access-${var.environment}"
  description = "Allows Lambda to read/write to USAspending award details S3 bucket"

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
          module.usaspending_data_s3.bucket_arn,
          "${module.usaspending_data_s3.bucket_arn}/*"
        ]
      }
    ]
  })

  tags = var.common_tags
}

# ==============================================================================
# USASPENDING DAILY BULK INDEXING SYSTEM
# ==============================================================================
# Daily Glue job that fetches all new contract awards from the previous day,
# downloads bulk data, and indexes awards, transactions, and subawards

# S3 Bucket for Glue Scripts
module "glue_scripts_s3" {
  source = "./modules/s3"

  providers = {
    aws         = aws
    aws.replica = aws.replica
  }

  bucket_name = "${var.project_name}-glue-scripts-${var.environment}"
  environment = var.environment
  purpose     = "GlueScripts"

  # No lifecycle transitions needed for scripts (they're small and rarely change)
  enable_lifecycle_transitions = false

  # No expiration for scripts (keep them indefinitely)
  enable_expiration = false

  # Abort incomplete multipart uploads after 1 day
  abort_incomplete_multipart_upload_days = 1

  # Noncurrent version expiration
  noncurrent_version_expiration_days = 30

  kms_key_arn = module.kms.main_key_arn

  # Upload Glue scripts to S3
  static_files = [
    {
      source_path  = "${path.module}/../backend_app/src/glue/govt_contracts/glue_script.py"
      s3_key       = "govt_contracts/glue_script.py"
      content_type = "text/x-python"
    },
    {
      source_path  = "${path.module}/../backend_app/src/glue/congress_bills/glue_script.py"
      s3_key       = "congress_bills/glue_script.py"
      content_type = "text/x-python"
    },
    {
      source_path  = "${path.module}/../backend_app/src/glue/congress_bills/backfill_bill_text.py"
      s3_key       = "congress_bills/backfill_bill_text.py"
      content_type = "text/x-python"
    },
    {
      source_path  = "${path.module}/../backend_app/src/glue/lda_disclosures/glue_script.py"
      s3_key       = "lda_disclosures/glue_script.py"
      content_type = "text/x-python"
    }
  ]

  tags = var.common_tags

  depends_on = [module.kms]
}

# Glue Job for USAspending Daily Bulk Indexing
module "usaspending_bulk_indexing_glue_job" {
  source = "./modules/glue-job"

  job_name = "${var.project_name}-usaspending-bulk-indexing-${var.environment}"

  # Script location - uploaded to glue scripts bucket
  script_location = "s3://${module.glue_scripts_s3.bucket_id}/govt_contracts/glue_script.py"
  python_version  = "3"
  glue_version    = "4.0"

  # Job configuration
  max_retries           = 1
  timeout               = 2880   # 48 hours (max is 10080 minutes = 7 days, bulk downloads can take time)
  concurrent_executions = 1      # Only allow 1 concurrent run to avoid conflicts
  worker_type           = "G.1X" # 16 GB memory per worker (downgraded from G.2X for daily runs)
  number_of_workers     = 5      # 5 × 16 GB = 80 GB total memory (sufficient for daily indexing)

  # S3 buckets
  s3_bucket_arn = module.glue_scripts_s3.bucket_arn
  additional_s3_bucket_arns = [
    module.usaspending_data_s3.bucket_arn,
    module.static_hosting_bucket.bucket_arn
  ]
  spark_logs_bucket = module.static_hosting_bucket.bucket_id
  temp_bucket       = module.static_hosting_bucket.bucket_id

  # DynamoDB access
  dynamodb_table_arn = module.usaspending_awards_index_table.table_arn

  # KMS for encryption
  kms_key_arn = module.kms.main_key_arn
  # Also include DynamoDB KMS key since the table is encrypted with it
  additional_kms_key_arns = [
    module.kms.dynamodb_key_arn
  ]

  # Job arguments
  default_arguments = {
    "--USASPENDING_BASE_URL"    = "https://api.usaspending.gov"
    "--USASPENDING_USER_AGENT"  = "Cosine Financial Platform (contact@cosine.financial)"
    "--AWARDS_TABLE_NAME"       = module.usaspending_awards_index_table.table_name
    "--S3_BUCKET_NAME"          = module.usaspending_data_s3.bucket_id
    "--REQUEST_TIMEOUT"         = "30"
    "--ORPHAN_SUBAWARD_SQS_URL" = module.usaspending_orphan_subaward_queue.queue_url
    "--DLQ_SQS_URL"             = module.usaspending_dlq_queue.queue_url
  }

  job_bookmark_option = "job-bookmark-disable"

  tags = var.common_tags

  depends_on = [
    module.glue_scripts_s3,
    module.static_hosting_bucket,
    module.usaspending_data_s3,
    module.usaspending_awards_index_table,
    module.kms,
    module.usaspending_orphan_subaward_queue
  ]
}

# Grant Glue job role access to DynamoDB KMS key
# This is needed because the DynamoDB table is encrypted with the DynamoDB-specific KMS key
resource "aws_kms_grant" "glue_dynamodb_key_access" {
  name              = "${var.project_name}-usaspending-bulk-indexing-${var.environment}-dynamodb-key-grant"
  key_id            = module.kms.dynamodb_key_id
  grantee_principal = module.usaspending_bulk_indexing_glue_job.role_arn
  operations = [
    "Decrypt",
    "Encrypt",
    "GenerateDataKey",
    "DescribeKey"
  ]

  depends_on = [
    module.usaspending_bulk_indexing_glue_job,
    module.kms
  ]
}


# SQS Queue for Orphan Subaward Processing
module "usaspending_orphan_subaward_queue" {
  source = "./modules/sqs"

  project_name = var.project_name
  environment  = var.environment
  queue_name   = "usaspending-orphan-subaward"
  purpose      = "Queue for orphan subawards that need parent awards fetched from API"

  message_retention_seconds     = 1209600 # 14 days
  visibility_timeout_seconds    = 300     # 5 minutes (enough for API call + DynamoDB write)
  max_receive_count             = 3
  enable_dlq                    = true
  dlq_message_retention_seconds = 1209600 # 14 days

  kms_key_id = module.kms.main_key_id

  tags = var.common_tags
}

# Lambda Function for USAspending Orphan Subaward Processor
module "usaspending_orphan_subaward_processor_lambda" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-usaspending-orphan-subaward-processor-${var.environment}"
  description   = "Processes orphan subawards by fetching parent awards from API and storing in DynamoDB"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11"
  timeout       = 300 # 5 minutes (enough for API call + DynamoDB write)
  # Memory calculation for 50 messages:
  # - Message payloads (worst case: 50 × 200KB if not in S3): ~10MB
  # - S3 downloads (decompressed, one at a time): ~5-10MB per message
  # - API responses + DynamoDB operations: ~5-10MB per message
  # - Python runtime overhead: ~100-150MB
  # - Safety margin: ~200MB
  # Total worst case: ~400-500MB, so 1024MB provides comfortable headroom
  memory_size = 1024

  source_dir = "${path.module}/../backend_app/src/CONTRACTS/usaspending_orphan_subaward_processor/app"

  layers = [
    module.core_layer.layer_arn
  ]

  environment_variables = {
    AWARDS_TABLE_NAME         = module.usaspending_awards_index_table.table_name
    S3_BUCKET_NAME            = module.usaspending_data_s3.bucket_id
    ORPHAN_SUBAWARD_QUEUE_URL = module.usaspending_orphan_subaward_queue.queue_url
    USASPENDING_BASE_URL      = "https://api.usaspending.gov"
    LOG_LEVEL                 = "INFO"
  }

  additional_policy_arns = [
    module.usaspending_awards_index_table.table_policy_arn,
    module.kms.kms_access_policy_arn,
    module.usaspending_orphan_subaward_queue.sqs_access_policy_arn,
    aws_iam_policy.lambda_usaspending_data_s3_policy.arn
  ]

  tags = var.common_tags

  depends_on = [
    module.usaspending_awards_index_table,
    module.usaspending_orphan_subaward_queue,
    module.kms,
    module.core_layer
  ]
}

# SQS Event Source Mapping for Lambda
resource "aws_lambda_event_source_mapping" "orphan_subaward_sqs_trigger" {
  event_source_arn                   = module.usaspending_orphan_subaward_queue.queue_arn
  function_name                      = module.usaspending_orphan_subaward_processor_lambda.function_arn
  batch_size                         = 50 # Process up to 50 messages per invocation
  maximum_batching_window_in_seconds = 3  # Wait up to 3 seconds to collect more messages (required when batch_size > 10)
  enabled                            = true

  depends_on = [
    module.usaspending_orphan_subaward_processor_lambda,
    module.usaspending_orphan_subaward_queue
  ]
}

# IAM Policy for Glue Job to send messages to orphan subaward queue
resource "aws_iam_policy" "glue_orphan_subaward_sqs_policy" {
  name        = "${var.project_name}-glue-orphan-subaward-sqs-${var.environment}"
  description = "Allows Glue job to send orphan subaward messages to SQS"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
          "sqs:GetQueueAttributes"
        ]
        Resource = [
          module.usaspending_orphan_subaward_queue.queue_arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = [
          module.kms.main_key_arn
        ]
      }
    ]
  })

  tags = var.common_tags
}

# Attach SQS policy to Glue job role
resource "aws_iam_role_policy_attachment" "glue_orphan_subaward_sqs" {
  role       = module.usaspending_bulk_indexing_glue_job.role_name
  policy_arn = aws_iam_policy.glue_orphan_subaward_sqs_policy.arn

  depends_on = [
    module.usaspending_bulk_indexing_glue_job,
    aws_iam_policy.glue_orphan_subaward_sqs_policy
  ]
}

# SQS Queue for Failed Awards DLQ (Dead Letter Queue)
module "usaspending_dlq_queue" {
  source = "./modules/sqs"

  project_name = var.project_name
  environment  = var.environment
  queue_name   = "usaspending-dlq"
  purpose      = "Queue for failed awards that need individual processing"

  message_retention_seconds     = 1209600 # 14 days
  visibility_timeout_seconds    = 5400    # 90 minutes (6x Lambda timeout of 900s as required by AWS)
  max_receive_count             = 3
  enable_dlq                    = true
  dlq_message_retention_seconds = 1209600 # 14 days

  kms_key_id = module.kms.main_key_id

  tags = var.common_tags
}

# Lambda Function for USAspending Individual Award Processor
module "usaspending_individual_award_processor_lambda" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-usaspending-individual-award-processor-${var.environment}"
  description   = "Processes individual awards from DLQ with full functionality including oversize support"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11"
  timeout       = 900  # 15 minutes (enough for CSV parsing + processing + DynamoDB writes)
  memory_size   = 1024 # Enough for CSV parsing and processing

  source_dir = "${path.module}/../backend_app/src/CONTRACTS/usaspending_individual_award_processor/app"

  layers = [
    module.core_layer.layer_arn
  ]

  environment_variables = {
    AWARDS_TABLE_NAME    = module.usaspending_awards_index_table.table_name
    S3_BUCKET_NAME       = module.usaspending_data_s3.bucket_id
    USASPENDING_BASE_URL = "https://api.usaspending.gov"
    LOG_LEVEL            = "INFO"
  }

  additional_policy_arns = [
    module.usaspending_awards_index_table.table_policy_arn,
    module.kms.kms_access_policy_arn,
    aws_iam_policy.lambda_usaspending_data_s3_policy.arn,
    module.usaspending_dlq_queue.sqs_access_policy_arn
  ]

  tags = var.common_tags

  depends_on = [
    module.usaspending_awards_index_table,
    module.usaspending_data_s3,
    module.kms,
    module.core_layer,
    module.usaspending_dlq_queue
  ]
}

# IAM Policy for Glue Job to send messages to DLQ
resource "aws_iam_policy" "glue_dlq_sqs_policy" {
  name        = "${var.project_name}-glue-dlq-sqs-${var.environment}"
  description = "Allows Glue job to send failed award messages to DLQ"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage"
        ]
        Resource = module.usaspending_dlq_queue.queue_arn
      }
    ]
  })
}

# Attach DLQ SQS policy to Glue job role
resource "aws_iam_role_policy_attachment" "glue_dlq_sqs" {
  role       = module.usaspending_bulk_indexing_glue_job.role_name
  policy_arn = aws_iam_policy.glue_dlq_sqs_policy.arn

  depends_on = [
    module.usaspending_bulk_indexing_glue_job,
    aws_iam_policy.glue_dlq_sqs_policy
  ]
}

# SQS Event Source Mapping for DLQ to Lambda (SQS triggers Lambda directly)
resource "aws_lambda_event_source_mapping" "dlq_sqs_trigger" {
  event_source_arn                   = module.usaspending_dlq_queue.queue_arn
  function_name                      = module.usaspending_individual_award_processor_lambda.function_arn
  batch_size                         = 1 # Process one award at a time
  maximum_batching_window_in_seconds = 0
  enabled                            = true

  depends_on = [
    module.usaspending_dlq_queue,
    module.usaspending_individual_award_processor_lambda
  ]
}

# Lambda Function for USAspending Bulk Router
module "usaspending_bulk_router_lambda" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-usaspending-bulk-router-${var.environment}"
  description   = "Routes USAspending bulk indexing to Lambda or Glue based on date range"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11"
  timeout       = 60 # 1 minute max
  memory_size   = 128

  source_dir = "${path.module}/../backend_app/src/CONTRACTS/usaspending_bulk_router/app"

  environment_variables = {}

  tags = var.common_tags
}

# Lambda Function for USAspending Bulk Fetcher (≤2 days)
module "usaspending_bulk_fetcher_lambda" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-usaspending-bulk-fetcher-${var.environment}"
  description   = "Fetches USAspending bulk contract data for date ranges ≤2 days"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11"
  timeout       = 900  # 15 minutes max
  memory_size   = 3008 # Max memory for processing very large CSV files (300k+ rows)

  source_dir = "${path.module}/../backend_app/src/CONTRACTS/usaspending_bulk_fetcher/app"

  environment_variables = {
    USASPENDING_BASE_URL   = "https://api.usaspending.gov"
    USASPENDING_USER_AGENT = "Cosine Financial Platform (contact@cosine.financial)"
    AWARDS_TABLE_NAME      = module.usaspending_awards_index_table.table_name
    S3_BUCKET_NAME         = module.usaspending_data_s3.bucket_id
    REQUEST_TIMEOUT        = "30"
    MAX_RETRIES            = "5"
    RETRY_BASE_DELAY       = "2.0"
  }

  additional_policy_arns = [
    module.usaspending_awards_index_table.table_policy_arn,
    module.kms.kms_access_policy_arn,
    aws_iam_policy.lambda_usaspending_data_s3_policy.arn
  ]

  tags = var.common_tags

  depends_on = [
    module.usaspending_awards_index_table,
    module.usaspending_data_s3,
    module.kms
  ]
}

# Step Functions State Machine for USAspending Daily Bulk Indexing
module "usaspending_bulk_indexing_state_machine" {
  source = "./modules/step-functions"

  state_machine_name = "${var.project_name}-usaspending-bulk-indexing-${var.environment}"
  environment        = var.environment

  # Step Functions definition - calculates date range and invokes Glue job
  # Input should include: JobName, AWARDS_TABLE_NAME, S3_BUCKET_NAME, START_DATE (optional), END_DATE (optional)
  # Router Lambda calculates date range (especially for scheduled mode) and always routes to Glue
  definition = jsonencode({
    Comment = "USAspending Bulk Indexing - Always uses Glue job"
    StartAt = "CalculateRoute"
    States = {
      CalculateRoute = {
        Type     = "Task"
        Resource = module.usaspending_bulk_router_lambda.function_arn
        Comment  = "Calculate date range (handles scheduled mode date calculation)"
        Parameters = {
          "JobName.$" : "$.JobName"
          "AWARDS_TABLE_NAME.$" : "$.AWARDS_TABLE_NAME"
          "S3_BUCKET_NAME.$" : "$.S3_BUCKET_NAME"
          "START_DATE.$?" : "$.START_DATE"
          "END_DATE.$?" : "$.END_DATE"
          "source.$?" : "$.source"
        }
        ResultPath = "$.route"
        Next       = "StartGlueJob"
      }
      StartGlueJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Comment  = "Start Glue job for date ranges >2 days (2 day timeout)"
        Parameters = {
          "JobName.$" : "$.route.JobName"
          "Arguments" : {
            "--USASPENDING_BASE_URL" : "https://api.usaspending.gov"
            "--USASPENDING_USER_AGENT" : "Cosine Financial Platform (contact@cosine.financial)"
            "--AWARDS_TABLE_NAME.$" : "$.route.AWARDS_TABLE_NAME"
            "--S3_BUCKET_NAME.$" : "$.route.S3_BUCKET_NAME"
            "--REQUEST_TIMEOUT" : "30"
            "--START_DATE.$" : "$.route.START_DATE"
            "--END_DATE.$" : "$.route.END_DATE"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.error"
            Next        = "HandleError"
          }
        ]
        Next = "Success"
      }
      Success = {
        Type    = "Succeed"
        Comment = "Bulk indexing completed successfully"
      }
      HandleError = {
        Type  = "Fail"
        Error = "BulkIndexingFailed"
        Cause = "The bulk indexing job failed. Check CloudWatch logs for details."
      }
    }
  })

  # Lambda function ARNs for IAM permissions
  lambda_function_arns = [
    module.usaspending_bulk_router_lambda.function_arn
  ]

  # Glue job name for IAM permissions
  glue_job_names = [
    module.usaspending_bulk_indexing_glue_job.job_name
  ]

  # Logging configuration
  log_level              = var.environment == "production" ? "ERROR" : "ALL"
  log_retention_days     = 7
  include_execution_data = true

  tags = var.common_tags

  depends_on = [
    module.usaspending_bulk_router_lambda,
    module.usaspending_bulk_indexing_glue_job
  ]
}

# IAM Role for EventBridge to invoke USAspending Bulk Indexing Step Function
resource "aws_iam_role" "usaspending_bulk_indexing_scheduler_role" {
  name = "${var.project_name}-usaspending-bulk-indexing-scheduler-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.common_tags
}

# IAM Policy for EventBridge to start USAspending Bulk Indexing Step Function
resource "aws_iam_role_policy" "usaspending_bulk_indexing_scheduler_policy" {
  name = "${var.project_name}-usaspending-bulk-indexing-scheduler-policy-${var.environment}"
  role = aws_iam_role.usaspending_bulk_indexing_scheduler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = module.usaspending_bulk_indexing_state_machine.state_machine_arn
      }
    ]
  })
}

# EventBridge Scheduler for Daily USAspending Bulk Indexing (9:00 AM EST)
# Runs daily at 9:00 AM EST to process previous day's contract updates
# Note: EST is UTC-5, EDT is UTC-4. Using 14:00 UTC = 9:00 AM EST (standard time) or 10:00 AM EDT (daylight time)
resource "aws_cloudwatch_event_rule" "usaspending_bulk_indexing_scheduler" {
  name                = "${var.project_name}-usaspending-bulk-indexing-daily-${var.environment}"
  description         = "Trigger daily bulk indexing of USAspending contracts at 9:00 AM EST (14:00 UTC) - processes previous day's contract updates"
  schedule_expression = "cron(0 14 * * ? *)" # 14:00 UTC = 9:00 AM EST (standard time) or 10:00 AM EDT (daylight time)
  state               = "ENABLED"

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-usaspending-bulk-indexing-daily-${var.environment}"
    Type        = "EventBridgeRule"
    Purpose     = "USASpendingBulkIndexing"
    Environment = var.environment
  })
}

# EventBridge Target for Step Function
resource "aws_cloudwatch_event_target" "usaspending_bulk_indexing_scheduler_target" {
  rule      = aws_cloudwatch_event_rule.usaspending_bulk_indexing_scheduler.name
  target_id = "USASpendingBulkIndexingScheduler"
  arn       = module.usaspending_bulk_indexing_state_machine.state_machine_arn
  role_arn  = aws_iam_role.usaspending_bulk_indexing_scheduler_role.arn

  # Input payload for Step Function - scheduled mode will set dates in router Lambda
  input = jsonencode({
    source            = "scheduler-daily"
    JobName           = module.usaspending_bulk_indexing_glue_job.job_name
    AWARDS_TABLE_NAME = module.usaspending_awards_index_table.table_name
    S3_BUCKET_NAME    = module.usaspending_data_s3.bucket_id
    # START_DATE and END_DATE omitted - router Lambda will set to previous day and current day in scheduled mode
  })

  depends_on = [
    aws_cloudwatch_event_rule.usaspending_bulk_indexing_scheduler,
    aws_iam_role.usaspending_bulk_indexing_scheduler_role,
    module.usaspending_bulk_indexing_state_machine
  ]
}

# ==============================================================================
# LDA SENATE LOBBYING DISCLOSURES INGESTION SYSTEM
# ==============================================================================
# System to fetch, index, and download all lobbying disclosure filings and contributions
# from LDA Senate API

# S3 Bucket for LDA Disclosures Documents
module "lda_disclosures_s3" {
  source = "./modules/s3"

  providers = {
    aws         = aws
    aws.replica = aws.replica
  }

  bucket_name = "${var.project_name}-lda-disclosures-${var.environment}"
  environment = var.environment
  purpose     = "LDADisclosures"

  # Enable lifecycle transitions for cost optimization
  enable_lifecycle_transitions = true
  transition_to_ia_days        = 90  # Move to IA after 90 days
  transition_to_glacier_days   = 180 # Move to Glacier after 180 days

  # No expiration - keep documents indefinitely
  enable_expiration = false

  # Abort incomplete multipart uploads after 1 day
  abort_incomplete_multipart_upload_days = 1

  # Noncurrent version expiration
  noncurrent_version_expiration_days = 30

  kms_key_arn = module.kms.main_key_arn
  tags        = var.common_tags
}


# ==============================================================================
# CONGRESS.GOV BILL DATA INGESTION SYSTEM
# ==============================================================================
# System to fetch comprehensive bill data from Congress.gov API
# Uses Lambda for short date ranges (≤2 days) and Glue for longer ranges (>2 days)

# S3 Bucket for Congress.gov Glue Scripts

# S3 Bucket for Congress.gov Raw Data
module "congress_bills_data_s3" {
  source = "./modules/s3"

  providers = {
    aws         = aws
    aws.replica = aws.replica
  }

  bucket_name = "${var.project_name}-congress-bills-data-${var.environment}"
  environment = var.environment
  purpose     = "CongressBillsData"

  kms_key_arn = module.kms.main_key_arn

  # Enable lifecycle transitions to Glacier for cost optimization
  enable_lifecycle_transitions = true
  transition_to_ia_days        = 30
  transition_to_glacier_days   = 90

  tags = var.common_tags
}

# Congress Bills DynamoDB Table
# Primary Key: bill_id (e.g., "119-HR-1949")
# GSIs for sorting by proposer, party, and dates
module "congress_bills_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "congress-bills"

  hash_key  = "bill_id"
  range_key = null

  attributes = [
    { name = "bill_id", type = "S" },
    { name = "sponsor_full_name", type = "S" },
    { name = "sponsor_party", type = "S" },
    { name = "sponsor_state", type = "S" },
    { name = "introduced_date", type = "S" },
    { name = "latest_action_date", type = "S" },
    { name = "congress", type = "N" },
    { name = "bill_type", type = "S" },
    { name = "bill_title", type = "S" },
    { name = "bill_number", type = "N" },
    { name = "bipartisan", type = "N" },
    { name = "policy_area", type = "S" }
  ]

  global_secondary_indexes = [
    {
      name            = "SponsorPartyDateIndex"
      hash_key        = "sponsor_party"
      range_key       = "introduced_date"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "SponsorNameDateIndex"
      hash_key        = "sponsor_full_name"
      range_key       = "introduced_date"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "CongressBillTypeIndex"
      hash_key        = "congress"
      range_key       = "bill_type"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "BillTypeDateIndex"
      hash_key        = "bill_type"
      range_key       = "introduced_date"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "BillTitleDateIndex"
      hash_key        = "bill_title"
      range_key       = "introduced_date"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "BillNumberDateIndex"
      hash_key        = "bill_number"
      range_key       = "introduced_date"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "BipartisanDateIndex"
      hash_key        = "bipartisan"
      range_key       = "introduced_date"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "LatestActionDateIndex"
      hash_key        = "latest_action_date"
      range_key       = null
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "PolicyAreaDateIndex"
      hash_key        = "policy_area"
      range_key       = "introduced_date"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "SponsorStateDateIndex"
      hash_key        = "sponsor_state"
      range_key       = "introduced_date"
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "IntroducedDateIndex"
      hash_key        = "introduced_date"
      range_key       = null
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    }
  ]

  billing_mode = "PAY_PER_REQUEST"
  kms_key_arn  = module.kms.dynamodb_key_arn

  deletion_protection_enabled = var.dynamodb_deletion_protection_enabled

  table_type    = "Data"
  table_purpose = "CongressBills"

  tags = var.common_tags
}


# Lambda Functions for Congress Bills Router and Fetcher - DEPRECATED
# Moved to deprecated folder - removed from infrastructure due to timeout limitations
# All bill fetching now handled by Glue job only

# Glue Job for Congress Bills Fetcher (>2 days)
module "congress_bills_fetcher_glue_job" {
  source = "./modules/glue-job"

  job_name = "${var.project_name}-congress-bills-fetcher-${var.environment}"

  # Script location - uploaded to glue scripts bucket
  script_location = "s3://${module.glue_scripts_s3.bucket_id}/congress_bills/glue_script.py"
  python_version  = "3"
  glue_version    = "4.0"

  # Job configuration
  max_retries           = 1
  timeout               = 2880 # 2 days (48 hours)
  concurrent_executions = 1    # Only allow 1 concurrent run
  worker_type           = "G.1X"
  number_of_workers     = 2

  # S3 buckets
  s3_bucket_arn = module.glue_scripts_s3.bucket_arn
  additional_s3_bucket_arns = [
    module.congress_bills_data_s3.bucket_arn,
    module.static_hosting_bucket.bucket_arn,
    module.politician_trades_s3.bucket_arn
  ]
  spark_logs_bucket = module.static_hosting_bucket.bucket_id
  temp_bucket       = module.static_hosting_bucket.bucket_id

  # DynamoDB access
  dynamodb_table_arn = module.congress_bills_table.table_arn

  # KMS for encryption
  kms_key_arn = module.kms.main_key_arn
  additional_kms_key_arns = [
    module.kms.dynamodb_key_arn
  ]

  # Additional IAM policies for Secrets Manager and S3 access
  additional_policy_arns = [
    module.congress_api_secrets_manager.secret_access_policy_arn,
    aws_iam_policy.lambda_politician_trades_s3_policy.arn
  ]

  # Job arguments
  default_arguments = {
    "--PROJECT_NAME"                = var.project_name
    "--ENVIRONMENT"                 = var.environment
    "--CONGRESS_API_BASE_URL"       = "https://api.congress.gov/v3"
    "--BILLS_TABLE_NAME"            = module.congress_bills_table.table_name
    "--S3_BUCKET_NAME"              = module.congress_bills_data_s3.bucket_id
    "--POLITICIAN_TRADES_S3_BUCKET" = module.politician_trades_s3.bucket_id
    "--REQUEST_TIMEOUT"             = "30"
    "--BILL_TEXT_SQS_URL"           = module.congress_bills_bill_text_queue.queue_url
  }

  job_bookmark_option = "job-bookmark-disable"

  tags = var.common_tags

  depends_on = [
    module.glue_scripts_s3,
    module.congress_bills_data_s3,
    module.static_hosting_bucket,
    module.congress_bills_table,
    module.kms,
    module.congress_api_secrets_manager,
    module.congress_bills_bill_text_queue
  ]
}

# Grant Glue job role access to DynamoDB KMS key
resource "aws_kms_grant" "congress_bills_glue_dynamodb_key_access" {
  name              = "${var.project_name}-congress-bills-fetcher-${var.environment}-dynamodb-key-grant"
  key_id            = module.kms.dynamodb_key_id
  grantee_principal = module.congress_bills_fetcher_glue_job.role_arn
  operations = [
    "Decrypt",
    "Encrypt",
    "GenerateDataKey",
    "DescribeKey"
  ]

  depends_on = [
    module.congress_bills_fetcher_glue_job,
    module.kms
  ]
}

# SQS Queue for Congress Bills Bill Text Downloads
module "congress_bills_bill_text_queue" {
  source = "./modules/sqs"

  project_name = var.project_name
  environment  = var.environment
  queue_name   = "congress-bills-bill-text"
  purpose      = "Queue for bill text downloads that need to be processed by Lambda"

  message_retention_seconds     = 1209600 # 14 days
  visibility_timeout_seconds    = 900     # 15 minutes (enough for API calls + S3 upload + DynamoDB write)
  max_receive_count             = 3
  enable_dlq                    = true
  dlq_message_retention_seconds = 1209600 # 14 days

  kms_key_id = module.kms.main_key_id

  tags = var.common_tags
}

# Lambda Function for Congress Bills Bill Text Processor
module "congress_bills_bill_text_processor_lambda" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-congress-bills-bill-text-processor-${var.environment}"
  description   = "Processes bill text download messages from SQS, downloads HTML bill text, and stores in S3/DynamoDB"
  handler       = "lambda_function.lambda_handler"
  runtime       = "python3.11"
  timeout       = 900 # 15 minutes (enough for sequential processing of 10 bills)
  memory_size   = 512 # Enough for API calls, downloads, and DynamoDB operations

  source_dir = "${path.module}/../backend_app/src/congress_bills_bill_text_processor/app"

  layers = [
    module.core_layer.layer_arn
  ]

  environment_variables = {
    BILLS_TABLE_NAME      = module.congress_bills_table.table_name
    S3_BUCKET_NAME        = module.congress_bills_data_s3.bucket_id
    PROJECT_NAME          = var.project_name
    ENVIRONMENT           = var.environment
    CONGRESS_API_BASE_URL = "https://api.congress.gov/v3"
    BILL_TEXT_QUEUE_URL   = module.congress_bills_bill_text_queue.queue_url
    REQUEST_TIMEOUT       = "30"
    LOG_LEVEL             = "INFO"
  }

  additional_policy_arns = [
    module.congress_bills_table.table_policy_arn,
    module.kms.kms_access_policy_arn,
    module.congress_api_secrets_manager.secret_access_policy_arn,
    module.congress_bills_bill_text_queue.sqs_access_policy_arn,
    aws_iam_policy.lambda_congress_bills_data_s3_policy.arn
  ]

  tags = var.common_tags

  depends_on = [
    module.congress_bills_table,
    module.congress_bills_data_s3,
    module.kms,
    module.core_layer,
    module.congress_bills_bill_text_queue,
    module.congress_api_secrets_manager
  ]
}

# IAM Policy for Lambda to access Congress Bills Data S3 bucket
resource "aws_iam_policy" "lambda_congress_bills_data_s3_policy" {
  name        = "${var.project_name}-lambda-congress-bills-data-s3-${var.environment}"
  description = "Allows Lambda to read/write Congress bills data in S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${module.congress_bills_data_s3.bucket_arn}/billtext/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = module.congress_bills_data_s3.bucket_arn
        Condition = {
          StringLike = {
            "s3:prefix" = "billtext/*"
          }
        }
      }
    ]
  })

  tags = var.common_tags
}

# SQS Event Source Mapping for Lambda (SQS triggers Lambda directly)
resource "aws_lambda_event_source_mapping" "congress_bills_bill_text_sqs_trigger" {
  event_source_arn                   = module.congress_bills_bill_text_queue.queue_arn
  function_name                      = module.congress_bills_bill_text_processor_lambda.function_arn
  batch_size                         = 10 # Process up to 10 messages per invocation (sequential processing)
  maximum_batching_window_in_seconds = 0  # No batching window - process immediately
  enabled                            = true

  depends_on = [
    module.congress_bills_bill_text_queue,
    module.congress_bills_bill_text_processor_lambda
  ]
}

# IAM Policy for Glue Job to send messages to bill text queue
resource "aws_iam_policy" "glue_congress_bills_bill_text_sqs_policy" {
  name        = "${var.project_name}-glue-congress-bills-bill-text-sqs-${var.environment}"
  description = "Allows Glue job to send bill text download messages to SQS"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
          "sqs:GetQueueAttributes"
        ]
        Resource = module.congress_bills_bill_text_queue.queue_arn
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = module.kms.main_key_arn
      }
    ]
  })

  tags = var.common_tags
}

# Attach SQS policy to Glue job role
resource "aws_iam_role_policy_attachment" "glue_congress_bills_bill_text_sqs" {
  role       = module.congress_bills_fetcher_glue_job.role_name
  policy_arn = aws_iam_policy.glue_congress_bills_bill_text_sqs_policy.arn

  depends_on = [
    module.congress_bills_fetcher_glue_job,
    aws_iam_policy.glue_congress_bills_bill_text_sqs_policy
  ]
}

# Step Functions State Machine for Congress Bills Fetcher
# Uses Glue job only (Lambda removed due to timeout limitations with large bill counts)
module "congress_bills_fetcher_state_machine" {
  source = "./modules/step-functions"

  state_machine_name = "${var.project_name}-congress-bills-fetcher-${var.environment}"
  environment        = var.environment

  # Step Functions definition - directly invokes Glue job
  # Input should include: start_date, end_date, congress (optional)
  definition = jsonencode({
    Comment = "Congress.gov Bill Data Fetcher - Uses Glue job only"
    StartAt = "StartGlueJob"
    States = {
      StartGlueJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Comment  = "Start Glue job for Congress bills fetching (2 day timeout)"
        Parameters = {
          "JobName" : module.congress_bills_fetcher_glue_job.job_name
          "Arguments" : {
            "--PROJECT_NAME" : var.project_name
            "--ENVIRONMENT" : var.environment
            "--CONGRESS_API_BASE_URL" : "https://api.congress.gov/v3"
            "--BILLS_TABLE_NAME" : module.congress_bills_table.table_name
            "--S3_BUCKET_NAME" : module.congress_bills_data_s3.bucket_id
            "--REQUEST_TIMEOUT" : "30"
            "--START_DATE.$" : "$.start_date"
            "--END_DATE.$" : "$.end_date"
            "--SOURCE.$" : "$.source"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.error"
            Next        = "HandleError"
          }
        ]
        Next = "Success"
      }
      Success = {
        Type    = "Succeed"
        Comment = "Congress bills fetched and stored successfully"
      }
      HandleError = {
        Type  = "Fail"
        Error = "CongressBillsFetchFailed"
        Cause = "The Congress.gov bill fetching job failed. Check CloudWatch logs for details."
      }
    }
  })

  # Lambda function ARNs for IAM permissions (removed - no longer using Lambda)
  lambda_function_arns = []

  # Glue job name for IAM permissions
  glue_job_names = [
    module.congress_bills_fetcher_glue_job.job_name
  ]

  # Logging configuration
  log_level              = var.environment == "production" ? "ERROR" : "ALL"
  log_retention_days     = 7
  include_execution_data = true

  tags = var.common_tags

  depends_on = [
    module.congress_bills_fetcher_glue_job
  ]
}

# Temporary Glue Job for Congress Bills Bill Text Backfill
# This job backfills bill_text_s3_key for existing bills in DynamoDB
module "congress_bills_bill_text_backfill_glue_job" {
  source = "./modules/glue-job"

  job_name = "${var.project_name}-congress-bills-bill-text-backfill-${var.environment}"

  # Script location - uploaded to glue scripts bucket
  script_location = "s3://${module.glue_scripts_s3.bucket_id}/congress_bills/backfill_bill_text.py"
  python_version  = "3"
  glue_version    = "4.0"

  # Job configuration
  max_retries           = 1
  timeout               = 2880 # 2 days (48 hours) - may take a while for large tables
  concurrent_executions = 1    # Only allow 1 concurrent run
  worker_type           = "G.1X"
  number_of_workers     = 2

  # S3 buckets
  s3_bucket_arn = module.glue_scripts_s3.bucket_arn
  additional_s3_bucket_arns = [
    module.congress_bills_data_s3.bucket_arn
  ]
  spark_logs_bucket = module.static_hosting_bucket.bucket_id
  temp_bucket       = module.static_hosting_bucket.bucket_id

  # DynamoDB access
  dynamodb_table_arn = module.congress_bills_table.table_arn

  # KMS for encryption
  kms_key_arn = module.kms.main_key_arn
  additional_kms_key_arns = [
    module.kms.dynamodb_key_arn
  ]

  # Additional IAM policies for Secrets Manager
  additional_policy_arns = [
    module.congress_api_secrets_manager.secret_access_policy_arn
  ]

  # Job arguments
  default_arguments = {
    "--PROJECT_NAME"          = var.project_name
    "--ENVIRONMENT"           = var.environment
    "--CONGRESS_API_BASE_URL" = "https://api.congress.gov/v3"
    "--BILLS_TABLE_NAME"      = module.congress_bills_table.table_name
    "--S3_BUCKET_NAME"        = module.congress_bills_data_s3.bucket_id
    "--REQUEST_TIMEOUT"       = "30"
  }

  job_bookmark_option = "job-bookmark-disable"

  tags = var.common_tags

  depends_on = [
    module.glue_scripts_s3,
    module.congress_bills_data_s3,
    module.static_hosting_bucket,
    module.congress_bills_table,
    module.kms,
    module.congress_api_secrets_manager
  ]
}

# Grant backfill Glue job role access to DynamoDB KMS key
resource "aws_kms_grant" "congress_bills_backfill_glue_dynamodb_key_access" {
  name              = "${var.project_name}-congress-bills-backfill-${var.environment}-dynamodb-key-grant"
  key_id            = module.kms.dynamodb_key_id
  grantee_principal = module.congress_bills_bill_text_backfill_glue_job.role_arn
  operations = [
    "Decrypt",
    "Encrypt",
    "GenerateDataKey",
    "DescribeKey"
  ]

  depends_on = [
    module.congress_bills_bill_text_backfill_glue_job,
    module.kms
  ]
}

# Step Functions State Machine for Congress Bills Bill Text Prefill
# Single stage that directly invokes the Glue job with EVENT="Begin prefill"
module "congress_bills_bill_text_prefill_state_machine" {
  source = "./modules/step-functions"

  state_machine_name = "${var.project_name}-congress-bill-text-prefill-${var.environment}"
  environment        = var.environment

  # Step Functions definition - single stage with Glue job
  definition = jsonencode({
    Comment = "Congress Bills Bill Text Prefill - Directly invokes Glue job with EVENT='Begin prefill'"
    StartAt = "StartGlueJob"
    States = {
      StartGlueJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Comment  = "Start Glue job for bill text prefill (crawls for bills with empty bill_text_html_s3_key)"
        Parameters = {
          "JobName" = module.congress_bills_bill_text_backfill_glue_job.job_name
          "Arguments" = {
            "--EVENT" = "Begin prefill"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.error"
            Next        = "HandleError"
          }
        ]
        Next = "Success"
      }
      Success = {
        Type    = "Succeed"
        Comment = "Bill text prefill completed successfully"
      }
      HandleError = {
        Type  = "Fail"
        Error = "BillTextPrefillFailed"
        Cause = "The bill text prefill job failed. Check CloudWatch logs for details."
      }
    }
  })

  # No Lambda functions needed
  lambda_function_arns = []

  # Glue job name for IAM permissions
  glue_job_names = [
    module.congress_bills_bill_text_backfill_glue_job.job_name
  ]

  # Logging configuration
  log_level              = var.environment == "production" ? "ERROR" : "ALL"
  log_retention_days     = 7
  include_execution_data = true

  tags = var.common_tags

  depends_on = [
    module.congress_bills_bill_text_backfill_glue_job
  ]
}

# EventBridge Scheduler for Daily Congress Bills Bill Text Prefill (2:00 PM UTC)
# Runs 3 hours after the bills fetcher to crawl for bills with empty bill_text_html_s3_key

# IAM Role for EventBridge to invoke Congress Bills Bill Text Prefill Step Function
resource "aws_iam_role" "congress_bills_bill_text_prefill_scheduler_role" {
  name = "${var.project_name}-congress-bill-text-prefill-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.common_tags
}

# IAM Policy for EventBridge to start Congress Bills Bill Text Prefill Step Function
resource "aws_iam_role_policy" "congress_bills_bill_text_prefill_scheduler_policy" {
  name = "${var.project_name}-congress-bill-text-prefill-policy-${var.environment}"
  role = aws_iam_role.congress_bills_bill_text_prefill_scheduler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = module.congress_bills_bill_text_prefill_state_machine.state_machine_arn
      }
    ]
  })
}

# ==============================================================================
# LDA SENATE LOBBYING DISCLOSURES INGESTION SYSTEM
# ==============================================================================
# System to fetch, index, and download all lobbying disclosure filings and contributions
# from LDA Senate API

# DynamoDB Table for LDA Filings (LD-1, LD-2)
module "lda_filings_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "lda-filings"

  hash_key  = "PK"
  range_key = "SK"

  attributes = [
    { name = "PK", type = "S" },
    { name = "SK", type = "S" },
    { name = "filing_year", type = "N" },
    { name = "dt_posted", type = "S" },
    { name = "filing_period", type = "S" },
    { name = "report_type", type = "S" },
    { name = "registrant_name", type = "S" },
    { name = "client_name", type = "S" },
    { name = "amount_reported", type = "N" },
    { name = "amount_bucket", type = "N" },
    { name = "state", type = "S" },
    { name = "contribution_item_type", type = "S" },
    { name = "is_foreign", type = "N" },
    { name = "pac", type = "N" },
    { name = "filer_type", type = "S" },
    { name = "item_type", type = "S" }
  ]

  global_secondary_indexes = [
    {
      name            = "YearPostedDateIndex"
      hash_key        = "filing_year"
      range_key       = "dt_posted"
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "PeriodPostedDateIndex"
      hash_key        = "filing_period"
      range_key       = "dt_posted"
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "ReportTypePostedDateIndex"
      hash_key        = "report_type"
      range_key       = "dt_posted"
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "RegistrantPostedDateIndex"
      hash_key        = "registrant_name"
      range_key       = "dt_posted"
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "ClientPostedDateIndex"
      hash_key        = "client_name"
      range_key       = "dt_posted"
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "AmountReportedIndex"
      hash_key        = "amount_bucket"
      range_key       = "amount_reported"
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "StatePostedDateIndex"
      hash_key        = "state"
      range_key       = "dt_posted"
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "ContributionItemTypePostedDateIndex"
      hash_key        = "contribution_item_type"
      range_key       = "dt_posted"
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "ForeignEntityPostedDateIndex"
      hash_key        = "is_foreign"
      range_key       = "dt_posted"
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "PACPostedDateIndex"
      hash_key        = "pac"
      range_key       = "dt_posted"
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "FilerTypePostedDateIndex"
      hash_key        = "filer_type"
      range_key       = "dt_posted"
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "ItemTypePostedDateIndex"
      hash_key        = "item_type"
      range_key       = "dt_posted"
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

  table_type    = "LobbyingData"
  table_purpose = "LDAFilings"

  tags = var.common_tags

  depends_on = [module.kms]
}

# Glue Job for LDA Disclosures Indexing
# Note: Parameter-filing mappings are stored in the same lda-filings table
# using PK = "PARAMETER_TYPE#VALUE" and SK = "FILING#{uuid}" or "CONTRIBUTION#{uuid}"
module "lda_disclosures_glue_job" {
  source = "./modules/glue-job"

  job_name = "${var.project_name}-lda-disclosures-indexing-${var.environment}"

  # Script location - uploaded to glue scripts bucket
  script_location = "s3://${module.glue_scripts_s3.bucket_id}/lda_disclosures/glue_script.py"
  python_version  = "3"
  glue_version    = "4.0"

  # Job configuration
  max_retries           = 1
  timeout               = 2880   # 48 hours (max is 10080 minutes = 7 days)
  concurrent_executions = 1      # Only allow 1 concurrent run
  worker_type           = "G.1X" # 16 GB memory, 4 vCPUs per worker
  # 25 workers = 100 vCPUs, 400 GB total memory
  # Processing: 25 pages × 25 items = 625 concurrent threads (I/O-bound)
  # ~25 threads per worker on average - sufficient for I/O-bound operations
  # Memory cleanup after each batch prevents accumulation
  number_of_workers = 25

  # S3 buckets
  s3_bucket_arn = module.glue_scripts_s3.bucket_arn
  additional_s3_bucket_arns = [
    module.lda_disclosures_s3.bucket_arn,
    module.static_hosting_bucket.bucket_arn
  ]
  spark_logs_bucket = module.static_hosting_bucket.bucket_id
  temp_bucket       = module.static_hosting_bucket.bucket_id

  # DynamoDB access
  dynamodb_table_arn = module.lda_filings_table.table_arn
  # Note: Parameter-filing mappings are stored in the same table using different PK/SK patterns

  # KMS for encryption
  kms_key_arn = module.kms.main_key_arn
  # Also include DynamoDB KMS key since the table is encrypted with it
  additional_kms_key_arns = [
    module.kms.dynamodb_key_arn
  ]

  # Additional IAM policies for Secrets Manager and SQS access
  additional_policy_arns = [
    module.lda_api_secrets_manager.secret_access_policy_arn,
    aws_iam_policy.lda_glue_pac_sqs_policy.arn
  ]

  # Job arguments
  default_arguments = {
    "--LDA_API_BASE_URL"   = "https://lda.senate.gov/api/v1"
    "--LDA_SECRET_NAME"    = module.lda_api_secrets_manager.secret_names["lda-api"]
    "--FILINGS_TABLE_NAME" = module.lda_filings_table.table_name
    "--S3_BUCKET_NAME"     = module.lda_disclosures_s3.bucket_id
    "--REQUEST_TIMEOUT"    = "30"
    "--RATE_LIMIT_DELAY"   = "0.5"
    "--PAC_QUEUE_URL"      = module.lda_pac_autocomplete_queue.queue_url
  }

  job_bookmark_option = "job-bookmark-disable"

  tags = var.common_tags

  depends_on = [
    module.lda_filings_table,
    module.lda_disclosures_s3,
    module.kms,
    module.glue_scripts_s3,
    module.lda_api_secrets_manager,
    module.lda_pac_autocomplete_queue
  ]
}

# IAM Policy for Glue Job to send messages to PAC autocomplete queue
resource "aws_iam_policy" "lda_glue_pac_sqs_policy" {
  name        = "${var.project_name}-lda-glue-pac-sqs-${var.environment}"
  description = "Allows Glue job to send PAC names to autocomplete queue"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage"
        ]
        Resource = module.lda_pac_autocomplete_queue.queue_arn
      }
    ]
  })

  tags = var.common_tags
}

# Attach PAC SQS policy to Glue job role
resource "aws_iam_role_policy_attachment" "lda_glue_pac_sqs" {
  role       = module.lda_disclosures_glue_job.role_name
  policy_arn = aws_iam_policy.lda_glue_pac_sqs_policy.arn

  depends_on = [
    module.lda_disclosures_glue_job,
    aws_iam_policy.lda_glue_pac_sqs_policy
  ]
}

# Step Functions State Machine for LDA Disclosures Indexing
module "lda_disclosures_state_machine" {
  source = "./modules/step-functions"

  state_machine_name = "${var.project_name}-lda-disclosures-indexing-${var.environment}"
  environment        = var.environment

  # Step Functions definition - Invokes Glue job
  definition = jsonencode({
    Comment = "LDA Senate Lobbying Disclosures Indexing - Glue Job"
    StartAt = "StartGlueJob"
    States = {
      StartGlueJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Comment  = "Start Glue job for LDA disclosures indexing"
        Parameters = {
          "JobName" = module.lda_disclosures_glue_job.job_name
          "Arguments" = {
            "--LDA_API_BASE_URL"   = "https://lda.senate.gov/api/v1"
            "--LDA_SECRET_NAME"    = module.lda_api_secrets_manager.secret_names["lda-api"]
            "--FILINGS_TABLE_NAME" = module.lda_filings_table.table_name
            "--S3_BUCKET_NAME"     = module.lda_disclosures_s3.bucket_id
            "--REQUEST_TIMEOUT"    = "30"
            "--RATE_LIMIT_DELAY"   = "0.5"
            "--PAC_QUEUE_URL"      = module.lda_pac_autocomplete_queue.queue_url
            "--START_DATE.$"       = "$.START_DATE"
            "--END_DATE.$"         = "$.END_DATE"
            "--TESTING.$"          = "$.TESTING"
          }
        }
        Catch = [
          {
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.error"
            Next        = "HandleError"
          }
        ]
        Next = "Success"
      }
      Success = {
        Type    = "Succeed"
        Comment = "LDA disclosures indexing completed successfully"
      }
      HandleError = {
        Type  = "Fail"
        Error = "LDADisclosuresIndexingFailed"
        Cause = "The LDA disclosures indexing job failed. Check CloudWatch logs for details."
      }
    }
  })

  # Glue job name for IAM permissions
  glue_job_names = [
    module.lda_disclosures_glue_job.job_name
  ]

  # Logging configuration
  log_level              = var.environment == "production" ? "ERROR" : "ALL"
  log_retention_days     = 7
  include_execution_data = true

  tags = var.common_tags

  depends_on = [
    module.lda_disclosures_glue_job
  ]
}


# SQS Queue for LDA PAC Autocomplete Processing
module "lda_pac_autocomplete_queue" {
  source = "./modules/sqs"

  project_name = var.project_name
  environment  = var.environment
  queue_name   = "lda-pac-autocomplete"
  purpose      = "LDA PAC Autocomplete Processing"

  # Queue configuration
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 60      # 1 minute
  max_receive_count          = 3
  enable_dlq                 = true

  # Encryption
  kms_key_id = module.kms.main_key_id

  tags = var.common_tags
}

# Lambda Function for LDA PAC Autocomplete Processor
module "lda_pac_autocomplete_processor" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-lda-pac-autocomplete-processor-${var.environment}"
  description   = "Processes autocomplete strings (PAC names, client names, lobbyist names, registrant names) from SQS and maintains sorted, deduplicated CSVs in S3"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 60  # 1 minute
  memory_size   = 256 # Lightweight - just CSV operations

  source_dir = "${path.module}/../backend_app/src/LDA/lda_pac_autocomplete_processor/app"

  # Environment variables
  environment_variables = {
    S3_BUCKET_NAME = module.lda_disclosures_s3.bucket_id
  }

  # Lambda layers
  layers = [
    module.core_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    module.lda_pac_autocomplete_queue.sqs_access_policy_arn,
    module.kms.kms_access_policy_arn,
    aws_iam_policy.lda_pac_autocomplete_s3_policy.arn
  ]

  tags = var.common_tags

  depends_on = [
    module.lda_pac_autocomplete_queue,
    module.lda_disclosures_s3,
    module.core_layer
  ]
}

# IAM Policy for Indexer Lambda to access S3 for document downloads
resource "aws_iam_policy" "lda_indexer_s3_policy" {
  name        = "${var.project_name}-lda-indexer-s3-${var.environment}"
  description = "Allows LDA indexer Lambda to read/write documents in S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${module.lda_disclosures_s3.bucket_arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = module.lda_disclosures_s3.bucket_arn
      }
    ]
  })

  tags = var.common_tags
}

# SQS Standard Queue for LDA Batch Processing
# Using standard queue (not FIFO) to enable full concurrency - order doesn't matter
module "lda_batch_queue" {
  source = "./modules/sqs"

  project_name = var.project_name
  environment  = var.environment
  queue_name   = "lda-batch-processing"
  purpose      = "LDA Batch Processing Queue"

  # Standard queue configuration (not FIFO - order doesn't matter)
  fifo_queue = false

  # Queue configuration
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 900     # 15 minutes (enough for batch processing)
  max_receive_count          = 3
  enable_dlq                 = true

  # Encryption
  kms_key_id = module.kms.main_key_id

  tags = var.common_tags
}

# LDA Disclosures Fetcher Lambda
module "lda_disclosures_fetcher" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-lda-disclosures-fetcher-${var.environment}"
  description   = "Fetches LDA API counts, creates batches, and sends to SQS queue"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 900  # 15 minutes (Lambda maximum)
  memory_size   = 1024 # Increased memory for better performance with high parallelism

  source_dir = "${path.module}/../backend_app/src/LDA/lda_disclosures_fetcher/app"

  # Environment variables
  environment_variables = {
    LDA_API_BASE_URL = "https://lda.senate.gov/api/v1"
    LDA_SECRET_NAME  = module.lda_api_secrets_manager.secret_names["lda-api"]
    REQUEST_TIMEOUT  = "30"
    BATCH_QUEUE_URL  = module.lda_batch_queue.queue_url
    S3_BUCKET_NAME   = module.lda_disclosures_s3.bucket_id
  }

  # Lambda layers
  layers = [
    module.core_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    module.lda_api_secrets_manager.secret_access_policy_arn,
    module.lda_batch_queue.sqs_access_policy_arn,
    module.kms.kms_access_policy_arn,
    aws_iam_policy.lda_fetcher_s3_policy.arn
  ]

  tags = var.common_tags

  depends_on = [
    module.lda_batch_queue,
    module.lda_api_secrets_manager,
    module.lda_disclosures_s3,
    module.core_layer
  ]
}

# LDA Disclosures Indexer Lambda
module "lda_disclosures_indexer" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-lda-disclosures-indexer-${var.environment}"
  description   = "Processes individual LDA pages from SQS queue with parallel item processing. Concurrency limit: 25 (25 pages processed in parallel, 25 items per page processed in parallel)"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 900  # 15 minutes (enough for parallel batch processing)
  memory_size   = 1024 # Increased from 512 - parallel processing of 25 items per page (API calls, document downloads, DynamoDB writes)

  source_dir = "${path.module}/../backend_app/src/LDA/lda_disclosures_indexer/app"

  # Environment variables
  environment_variables = {
    LDA_API_BASE_URL   = "https://lda.senate.gov/api/v1"
    LDA_SECRET_NAME    = module.lda_api_secrets_manager.secret_names["lda-api"]
    FILINGS_TABLE_NAME = module.lda_filings_table.table_name
    S3_BUCKET_NAME     = module.lda_disclosures_s3.bucket_id
    REQUEST_TIMEOUT    = "30"
    RATE_LIMIT_DELAY   = "0.5"
    PAC_QUEUE_URL      = module.lda_pac_autocomplete_queue.queue_url
    DLQ_QUEUE_URL      = module.lda_batch_queue.dlq_url
  }

  # Lambda layers
  layers = [
    module.core_layer.layer_arn
  ]

  # Reserved concurrency limit of 25
  reserved_concurrent_executions = 25

  # IAM policies
  additional_policy_arns = [
    module.lda_api_secrets_manager.secret_access_policy_arn,
    module.lda_filings_table.table_policy_arn,
    module.lda_batch_queue.sqs_access_policy_arn,
    module.lda_pac_autocomplete_queue.sqs_access_policy_arn,
    module.kms.kms_access_policy_arn,
    aws_iam_policy.lda_indexer_s3_policy.arn
  ]

  tags = var.common_tags

  depends_on = [
    module.lda_batch_queue,
    module.lda_filings_table,
    module.lda_disclosures_s3,
    module.lda_api_secrets_manager,
    module.lda_pac_autocomplete_queue,
    module.core_layer
  ]
}


# SQS Event Source Mapping for Indexer Lambda
resource "aws_lambda_event_source_mapping" "lda_batch_sqs_trigger" {
  event_source_arn                   = module.lda_batch_queue.queue_arn
  function_name                      = module.lda_disclosures_indexer.function_arn
  batch_size                         = 1 # Process 1 page at a time
  maximum_batching_window_in_seconds = 0 # Process immediately
  enabled                            = true

  # Standard queues scale naturally - reserved_concurrent_executions (25) on Lambda will limit concurrency
  # No scaling_config needed for standard queues

  depends_on = [
    module.lda_disclosures_indexer,
    module.lda_batch_queue
  ]
}

# IAM Policy for Lambda to access S3 for autocomplete CSVs
resource "aws_iam_policy" "lda_pac_autocomplete_s3_policy" {
  name        = "${var.project_name}-lda-pac-autocomplete-s3-${var.environment}"
  description = "Allows Lambda to read/write autocomplete CSVs (PACs, client names, lobbyist names, registrant names) in S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${module.lda_disclosures_s3.bucket_arn}/lists/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = module.lda_disclosures_s3.bucket_arn
        Condition = {
          StringLike = {
            "s3:prefix" = "lists/*"
          }
        }
      }
    ]
  })

  tags = var.common_tags
}

# IAM Policy for Fetcher Lambda to write constants to S3
resource "aws_iam_policy" "lda_fetcher_s3_policy" {
  name        = "${var.project_name}-lda-fetcher-s3-${var.environment}"
  description = "Allows Fetcher Lambda to write constants (general issues, government entities, countries) to S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject"
        ]
        Resource = "${module.lda_disclosures_s3.bucket_arn}/lists/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = module.lda_disclosures_s3.bucket_arn
        Condition = {
          StringLike = {
            "s3:prefix" = "lists/*"
          }
        }
      }
    ]
  })

  tags = var.common_tags
}

# SQS Event Source Mapping for PAC Autocomplete Processor
resource "aws_lambda_event_source_mapping" "lda_pac_autocomplete_sqs_trigger" {
  event_source_arn                   = module.lda_pac_autocomplete_queue.queue_arn
  function_name                      = module.lda_pac_autocomplete_processor.function_arn
  batch_size                         = 10
  maximum_batching_window_in_seconds = 5
  enabled                            = true

  depends_on = [
    module.lda_pac_autocomplete_queue,
    module.lda_pac_autocomplete_processor
  ]
}

# IAM Policy for Glue Job to send PAC names to SQS

# EventBridge Rule for Daily Bill Text Prefill
resource "aws_cloudwatch_event_rule" "congress_bills_bill_text_prefill_scheduler" {
  name                = "${var.project_name}-congress-bill-text-prefill-daily-${var.environment}"
  description         = "Trigger Congress bills bill text prefill daily at 2:00 PM UTC (3 hours after bills fetcher) to crawl for bills with empty bill_text_html_s3_key"
  schedule_expression = "cron(0 14 * * ? *)" # 2:00 PM UTC daily (9:00 AM EST / 10:00 AM EDT)
  state               = "ENABLED"

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-congress-bills-bill-text-prefill-daily-${var.environment}"
    Type        = "EventBridgeRule"
    Purpose     = "CongressBillsBillTextPrefill"
    Environment = var.environment
  })
}

# EventBridge Target for Step Function
resource "aws_cloudwatch_event_target" "congress_bills_bill_text_prefill_scheduler_target" {
  rule      = aws_cloudwatch_event_rule.congress_bills_bill_text_prefill_scheduler.name
  target_id = "CongressBillsBillTextPrefillScheduler"
  arn       = module.congress_bills_bill_text_prefill_state_machine.state_machine_arn
  role_arn  = aws_iam_role.congress_bills_bill_text_prefill_scheduler_role.arn

  # Input payload for Step Function (empty - Step Function will use hardcoded parameters)
  input = jsonencode({
    source = "scheduler"
  })

  depends_on = [
    aws_cloudwatch_event_rule.congress_bills_bill_text_prefill_scheduler,
    aws_iam_role.congress_bills_bill_text_prefill_scheduler_role,
    module.congress_bills_bill_text_prefill_state_machine
  ]
}

# EventBridge Scheduler for Daily Congress Bills Fetcher (11:00 AM UTC)

# IAM Role for EventBridge to invoke Congress Bills Fetcher Step Function
resource "aws_iam_role" "congress_bills_fetcher_scheduler_role" {
  name = "${var.project_name}-congress-bills-fetcher-scheduler-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.common_tags
}

# IAM Policy for EventBridge to start Congress Bills Fetcher Step Function
resource "aws_iam_role_policy" "congress_bills_fetcher_scheduler_policy" {
  name = "${var.project_name}-congress-bills-fetcher-scheduler-policy-${var.environment}"
  role = aws_iam_role.congress_bills_fetcher_scheduler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = module.congress_bills_fetcher_state_machine.state_machine_arn
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "congress_bills_fetcher_scheduler" {
  name                = "${var.project_name}-congress-bills-fetcher-daily-${var.environment}"
  description         = "Trigger Congress bills fetcher daily at 11:00 AM UTC (after Congress.gov's 10:00 AM data publication) to fetch yesterday's data"
  schedule_expression = "cron(0 11 * * ? *)" # 11:00 AM UTC daily (6:00 AM EST / 7:00 AM EDT)
  state               = "ENABLED"

  tags = merge(var.common_tags, {
    Name        = "${var.project_name}-congress-bills-fetcher-daily-${var.environment}"
    Type        = "EventBridgeRule"
    Purpose     = "CongressBillsFetching"
    Environment = var.environment
  })
}

# EventBridge Target for Step Function
resource "aws_cloudwatch_event_target" "congress_bills_fetcher_scheduler_target" {
  rule      = aws_cloudwatch_event_rule.congress_bills_fetcher_scheduler.name
  target_id = "CongressBillsFetcherScheduler"
  arn       = module.congress_bills_fetcher_state_machine.state_machine_arn
  role_arn  = aws_iam_role.congress_bills_fetcher_scheduler_role.arn

  # Input payload for scheduler: null dates with source="scheduler"
  # Router lambda will calculate yesterday's date
  input = jsonencode({
    start_date = null
    end_date   = null
    source     = "scheduler"
  })

  depends_on = [
    aws_iam_role.congress_bills_fetcher_scheduler_role,
    aws_iam_role_policy.congress_bills_fetcher_scheduler_policy
  ]
}

resource "aws_iam_policy" "lambda_politician_trades_textract_policy" {
  name        = "${var.project_name}-lambda-politician-trades-textract-access-${var.environment}"
  description = "Allows Lambda to use Textract for parsing PTR PDFs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "textract:DetectDocumentText",
          "textract:AnalyzeDocument",
          "textract:AnalyzeExpense",
          "textract:AnalyzeID",
          "textract:GetDocumentAnalysis",
          "textract:GetExpenseAnalysis",
          "textract:StartDocumentAnalysis",
          "textract:StartExpenseAnalysis"
        ]
        Resource = "*"
      }
    ]
  })

  tags = var.common_tags
}

# Politician Trades DynamoDB Table
module "politician_trades_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "politician-trades"

  hash_key  = "tradeId"
  range_key = null

  attributes = [
    { name = "tradeId", type = "S" },
    { name = "politicianName", type = "S" },
    { name = "party", type = "S" },
    { name = "position", type = "S" },
    { name = "securitySymbol", type = "S" },
    { name = "securityName", type = "S" },
    { name = "transactionType", type = "S" },
    { name = "transactionDate", type = "N" },
    { name = "amountMin", type = "N" },
    { name = "stateDistrict", type = "S" }
  ]

  global_secondary_indexes = [
    {
      name            = "PoliticianTradeDateIndex"
      hash_key        = "politicianName"
      range_key       = "transactionDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "PositionTradeDateIndex"
      hash_key        = "position"
      range_key       = "transactionDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "PartyTradeDateIndex"
      hash_key        = "party"
      range_key       = "transactionDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "SecurityTradeDateIndex"
      hash_key        = "securitySymbol"
      range_key       = "transactionDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "SecurityNameTradeDateIndex"
      hash_key        = "securityName"
      range_key       = "transactionDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "TransactionTypeTradeDateIndex"
      hash_key        = "transactionType"
      range_key       = "transactionDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "AmountRangeTradeDateIndex"
      hash_key        = "amountMin"
      range_key       = "transactionDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "StateDistrictTradeDateIndex"
      hash_key        = "stateDistrict"
      range_key       = "transactionDate"
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
  ttl_attribute_name             = var.dynamodb_ttl_attribute_name

  kms_key_arn = module.kms.dynamodb_key_arn

  table_type    = "TradeData"
  table_purpose = "PoliticianTrades"

  tags = var.common_tags

  depends_on = [module.kms]
}

# SEC Filings Cache DynamoDB Table
# Caches SEC filing data to avoid repeated web scraping
# Primary Key: filingId = {form}-{CIK}-{fileNumber}-{filmNumber}
# Note: documentUrls will be stored as a String Set (SS) in items but doesn't need to be in schema
module "sec_filings_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "sec-filings-cache"

  hash_key  = "filingId"
  range_key = null

  attributes = [
    { name = "filingId", type = "S" },
    { name = "form", type = "S" },
    { name = "cik", type = "S" },
    { name = "fileNumber", type = "S" },
    { name = "filmNumber", type = "S" },
    { name = "filingDate", type = "S" },
    { name = "reportingFor", type = "S" },
    { name = "filingEntity", type = "S" },
    { name = "located", type = "S" },
    { name = "incorporated", type = "S" }
  ]

  global_secondary_indexes = [
    {
      name            = "FormTypeFilingDateIndex"
      hash_key        = "form"
      range_key       = "filingDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "ReportingForFilingDateIndex"
      hash_key        = "reportingFor"
      range_key       = "filingDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "FilingEntityFilingDateIndex"
      hash_key        = "filingEntity"
      range_key       = "filingDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "LocationFilingDateIndex"
      hash_key        = "located"
      range_key       = "filingDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "IncorporatedFilingDateIndex"
      hash_key        = "incorporated"
      range_key       = "filingDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "CIKFilingDateIndex"
      hash_key        = "cik"
      range_key       = "filingDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "FileNumberFilingDateIndex"
      hash_key        = "fileNumber"
      range_key       = "filingDate"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "FilmNumberFilingDateIndex"
      hash_key        = "filmNumber"
      range_key       = "filingDate"
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
  ttl_enabled                    = true # Enable TTL for cache expiration
  ttl_attribute_name             = "ttl"

  kms_key_arn = module.kms.dynamodb_key_arn

  table_type    = "CacheData"
  table_purpose = "SECFilingsCache"

  # Add BatchGetItem and BatchWriteItem for cache operations
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

# USAspending Awards Index DynamoDB Table
# Stores indexed award/contract data from USAspending API
# Primary Key: award_id (unique per contract/award)
# Transactions and subawards stored in S3 (referenced via award_details_s3_key)
module "usaspending_awards_index_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "usaspending-awards-index"

  hash_key  = "award_id"
  range_key = null

  attributes = [
    { name = "award_id", type = "S" },
    { name = "recipient_name_normalized", type = "S" },
    { name = "awarding_agency_code", type = "S" },
    { name = "awarding_agency_name", type = "S" },
    { name = "fiscal_year", type = "N" },
    { name = "total_obligated_amount", type = "N" },
    { name = "period_start_date", type = "S" },
    { name = "period_end_date", type = "S" },
    { name = "recipient_location_state", type = "S" },
    { name = "award_type", type = "S" },
    { name = "is_assistance", type = "N" }
  ]

  global_secondary_indexes = [
    {
      name            = "AwardingAgencyCodeFiscalYearIndex"
      hash_key        = "awarding_agency_code"
      range_key       = "fiscal_year"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "AwardingAgencyNameFiscalYearIndex"
      hash_key        = "awarding_agency_name"
      range_key       = "fiscal_year"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "RecipientNameFiscalYearIndex"
      hash_key        = "recipient_name_normalized"
      range_key       = "fiscal_year"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "FiscalYearObligationIndex"
      hash_key        = "fiscal_year"
      range_key       = "total_obligated_amount"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "FiscalYearStartDateIndex"
      hash_key        = "fiscal_year"
      range_key       = "period_start_date"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "StateFiscalYearIndex"
      hash_key        = "recipient_location_state"
      range_key       = "fiscal_year"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "AwardTypeFiscalYearIndex"
      hash_key        = "award_type"
      range_key       = "fiscal_year"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "PeriodStartDateIndex"
      hash_key        = "fiscal_year"
      range_key       = "period_start_date"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "PeriodEndDateIndex"
      hash_key        = "fiscal_year"
      range_key       = "period_end_date"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "IsAssistanceFiscalYearIndex"
      hash_key        = "is_assistance"
      range_key       = "fiscal_year"
      projection_type = "KEYS_ONLY" # Changed from ALL to reduce write costs
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

  table_type    = "AwardData"
  table_purpose = "USASpendingAwardsIndex"

  # Add BatchGetItem and BatchWriteItem for efficient indexing operations
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

# SEC Search Query Cache DynamoDB Table
# Caches search queries to avoid re-running identical searches
# Primary Key: queryHash = hash of normalized search parameters (excluding reportingFor, incorporated, fileNumber, filmNumber)
# Stores job_id and S3 key for quick retrieval of cached results
module "sec_search_query_cache_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "sec-search-query-cache"

  hash_key  = "queryHash"
  range_key = null

  attributes = [
    { name = "queryHash", type = "S" },
    { name = "job_id", type = "S" },
    { name = "created_at", type = "S" }
  ]

  global_secondary_indexes = [
    {
      name            = "JobIdIndex"
      hash_key        = "job_id"
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
  stream_enabled                 = false
  stream_view_type               = null
  point_in_time_recovery_enabled = var.dynamodb_point_in_time_recovery_enabled
  deletion_protection_enabled    = var.dynamodb_deletion_protection_enabled
  ttl_enabled                    = true # Enable TTL for cache expiration
  ttl_attribute_name             = "ttl"

  kms_key_arn = module.kms.dynamodb_key_arn

  table_type    = "CacheData"
  table_purpose = "SECSearchQueryCache"

  # Add BatchGetItem and BatchWriteItem for cache operations
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

# Lambda 1: Fetch SEC Forms and Congressional PTRs
module "politician_trades_fetcher" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-pol-trades-fetcher-${var.environment}"
  description   = "Fetches SEC forms (3, 4, 5) and Congressional PTRs (House/Senate) and stores in S3"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 900 # 15 minutes (max)
  memory_size   = 1024

  source_dir = "${path.module}/../backend_app/src/POLITRADES/politician_trades_fetcher/app"

  # Environment variables
  environment_variables = {
    S3_BUCKET = module.politician_trades_s3.bucket_id
  }

  # Lambda layers
  layers = [
    module.core_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    aws_iam_policy.lambda_politician_trades_s3_policy.arn,
    aws_iam_policy.lambda_politician_trades_textract_policy.arn,
    module.kms.kms_access_policy_arn
  ]

  tags = var.common_tags

  depends_on = [module.politician_trades_s3]
}

# Lambda 2: Download SEC Forms (parallel processing)
module "politician_trades_downloader" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-pol-trades-downloader-${var.environment}"
  description   = "Downloads a single SEC form and stores it in S3 (invoked in parallel)"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 60 # 1 minute per form
  memory_size   = 512

  source_dir = "${path.module}/../backend_app/src/POLITRADES/politician_trades_downloader/app"

  # Environment variables
  environment_variables = {
    S3_BUCKET = module.politician_trades_s3.bucket_id
  }

  # Lambda layers
  layers = [
    module.core_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    aws_iam_policy.lambda_politician_trades_s3_policy.arn,
    module.kms.kms_access_policy_arn
  ]

  tags = var.common_tags

  depends_on = [module.politician_trades_s3]
}

# Lambda 3: Match Senate PTR Trades to Politicians (single file, parallel processing)
# Note: SEC matching is no longer handled by Glue job - removed
module "politician_trades_senate_matcher" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-pol-trades-senate-matcher-${var.environment}"
  description   = "Matches trades from a single Senate PTR to politicians (invoked in parallel)"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 300  # 5 minutes per file (for PDF parsing and Textract)
  memory_size   = 2048 # Higher memory for PDF parsing, text processing, and Textract

  source_dir = "${path.module}/../backend_app/src/POLITRADES/politician_trades_senate_matcher/app"

  # Environment variables
  environment_variables = {
    S3_BUCKET           = module.politician_trades_s3.bucket_id
    DYNAMODB_TABLE_NAME = module.politician_trades_table.table_name
  }

  # Lambda layers - includes document processing layer for Pillow (GIF to PNG conversion)
  layers = [
    module.core_layer.layer_arn,
    module.document_processing_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    aws_iam_policy.lambda_politician_trades_s3_policy.arn,
    aws_iam_policy.lambda_politician_trades_textract_policy.arn,
    module.politician_trades_table.table_policy_arn,
    module.kms.kms_access_policy_arn
  ]

  tags = var.common_tags

  depends_on = [module.politician_trades_s3, module.politician_trades_table, module.document_processing_layer]
}

# Lambda 4: Match House PTR Trades
module "politician_trades_house_matcher" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-pol-trades-house-matcher-${var.environment}"
  description   = "Parses House PTRs, matches trades to politicians using fuzzy name matching"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 900  # 15 minutes (max)
  memory_size   = 2048 # Higher memory for PDF parsing and text processing

  source_dir = "${path.module}/../backend_app/src/POLITRADES/politician_trades_house_matcher/app"

  # Environment variables
  environment_variables = {
    S3_BUCKET           = module.politician_trades_s3.bucket_id
    DYNAMODB_TABLE_NAME = module.politician_trades_table.table_name
  }

  # Lambda layers
  layers = [
    module.core_layer.layer_arn,
    module.docprocessing_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    aws_iam_policy.lambda_politician_trades_s3_policy.arn,
    aws_iam_policy.lambda_politician_trades_textract_policy.arn,
    module.politician_trades_table.table_policy_arn,
    module.kms.kms_access_policy_arn
  ]

  tags = var.common_tags

  depends_on = [module.politician_trades_s3, module.politician_trades_table, module.docprocessing_layer]
}

# Lambda 5: Save Trades to Database
module "politician_trades_saver" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-pol-trades-saver-${var.environment}"
  description   = "Batch writes matched politician trades to DynamoDB with idempotency"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 300 # 5 minutes
  memory_size   = 512

  source_dir = "${path.module}/../backend_app/src/POLITRADES/politician_trades_saver/app"

  # Environment variables
  environment_variables = {
    DYNAMODB_TABLE_NAME = module.politician_trades_table.table_name
    S3_BUCKET           = module.politician_trades_s3.bucket_id
  }

  # Lambda layers
  layers = [
    module.core_layer.layer_arn
  ]

  # IAM policies
  additional_policy_arns = [
    module.politician_trades_table.table_policy_arn,
    module.kms.kms_access_policy_arn,
    aws_iam_policy.lambda_politician_trades_s3_policy.arn
  ]

  tags = var.common_tags

  depends_on = [module.politician_trades_table]
}

# Step Functions State Machine for Politician Trades Aggregation
module "politician_trades_state_machine" {
  source = "./modules/step-functions"

  state_machine_name = "${var.project_name}-politician-trades-${var.environment}"
  environment        = var.environment

  # Step Functions definition with 4 steps (parallel downloads)
  definition = jsonencode({
    Comment = "Daily politician trades aggregation - fetch metadata, download forms in parallel, match trades, save to database. Pass {'date': 'YYYY-MM-DD'} to process a specific date, or omit for default (yesterday)."
    StartAt = "NormalizeInput"
    States = {
      # Step 0: Normalize input - handle 'backdate' or 'date' fields
      NormalizeInput = {
        Type    = "Pass"
        Comment = "Normalize input: if backdate provided, set date to null and pass backdate. Otherwise pass date and source."
        Parameters = {
          "backdate.$" = "$.backdate"
          "date.$"     = "$.date"
          "source.$"   = "$.source"
        }
        # Transform: if backdate exists, set date to null; otherwise keep date as is
        # This is handled by the Pass state - both fields will be present (one may be null)
        ResultPath = "$"
        Next       = "CongressionalPTRsPipeline"
      }

      # Congressional PTRs Pipeline: Fetcher → Nested Parallel (Senate/House)
      CongressionalPTRsPipeline = {
        Type    = "Parallel"
        Comment = "Congressional PTRs Pipeline: Fetcher → Nested Parallel (Senate/House)"
        Branches = [
          {
            StartAt = "FetchFormMetadata"
            States = {
              FetchFormMetadata = {
                Type       = "Task"
                Resource   = module.politician_trades_fetcher.function_arn
                Comment    = "Fetch Congressional PTR metadata (Senate/House). Returns URLs/metadata only - NO downloads. Lambda defaults to yesterday if date not provided."
                InputPath  = "$" # Pass through entire input - Lambda will extract 'date' or default
                ResultPath = "$.fetchResults"
                Next       = "NestedParallelPTRs"
                Retry = [
                  {
                    ErrorEquals     = ["States.ALL"]
                    IntervalSeconds = 30
                    MaxAttempts     = 3
                    BackoffRate     = 2.0
                  }
                ]
                Catch = [
                  {
                    ErrorEquals = ["States.ALL"]
                    ResultPath  = "$.error"
                    Next        = "FetchFormsFailed"
                  }
                ]
              }

              FetchFormsFailed = {
                Type    = "Pass"
                Comment = "Fetcher failed - continue with empty results for nested parallel"
                Parameters = {
                  "fetchResults" = {
                    "senatePTRs" = [],
                    "housePTRs"  = [],
                    "date.$"     = "$.date"
                  }
                }
                ResultPath = "$"
                Next       = "NestedParallelPTRs"
              }

              # Nested parallel for Senate and House PTRs
              NestedParallelPTRs = {
                Type    = "Parallel"
                Comment = "Nested parallel: Process Senate and House PTRs from fetcher output"
                Branches = [
                  {
                    # Senate Pipeline
                    StartAt = "TransformSenate"
                    States = {
                      TransformSenate = {
                        Type    = "Pass"
                        Comment = "Transform: Extract senatePTRs array from fetchResults. Preserve date for later use."
                        Parameters = {
                          "date.$" : "$.fetchResults.date", # Will be available at $.date in subsequent states
                          "items.$" : "$.fetchResults.senatePTRs",
                          "source" : "senate"
                        }
                        ResultPath = "$" # Replace entire state with transformed data
                        Next       = "DownloadSenate"
                      }
                      DownloadSenate = {
                        Type           = "Map"
                        Comment        = "Download Senate PTRs in parallel - saves to trades/YYYY-MM-DD/senate/*"
                        ItemsPath      = "$.items"
                        MaxConcurrency = 10
                        ResultPath     = "$.downloadResults"
                        Iterator = {
                          StartAt = "DownloadSenatePTR"
                          States = {
                            DownloadSenatePTR = {
                              Type     = "Task"
                              Resource = module.politician_trades_downloader.function_arn
                              Comment  = "Download a single Senate PTR"
                              Retry = [
                                {
                                  ErrorEquals     = ["Lambda.TooManyRequestsException", "Lambda.ServiceException"]
                                  IntervalSeconds = 60
                                  MaxAttempts     = 5
                                  BackoffRate     = 2.0
                                },
                                {
                                  ErrorEquals     = ["States.ALL"]
                                  IntervalSeconds = 10
                                  MaxAttempts     = 2
                                  BackoffRate     = 2.0
                                }
                              ]
                              Catch = [
                                {
                                  ErrorEquals = ["States.ALL"]
                                  ResultPath  = "$.error"
                                  Next        = "DownloadSenatePTRFailed"
                                }
                              ]
                              End = true
                            }
                            DownloadSenatePTRFailed = {
                              Type    = "Pass"
                              Comment = "Continue even if download fails"
                              Result  = { "success" : false, "error" : "Download failed" }
                              End     = true
                            }
                          }
                        }
                        Next = "MatchSenate"
                      }
                      MatchSenate = {
                        Type           = "Map"
                        Comment        = "Match trades from Senate PTRs to politicians - reads from trades/senate/{date}/*"
                        ItemsPath      = "$.downloadResults"
                        MaxConcurrency = 10
                        ResultPath     = "$.matchResults"
                        Iterator = {
                          StartAt = "MatchFileSenate"
                          States = {
                            MatchFileSenate = {
                              Type     = "Task"
                              Resource = module.politician_trades_senate_matcher.function_arn
                              Comment  = "Match trades from a single Senate PTR to politicians"
                              Retry = [
                                {
                                  ErrorEquals     = ["Lambda.TooManyRequestsException", "Lambda.ServiceException"]
                                  IntervalSeconds = 60
                                  MaxAttempts     = 5
                                  BackoffRate     = 2.0
                                },
                                {
                                  ErrorEquals     = ["States.ALL"]
                                  IntervalSeconds = 10
                                  MaxAttempts     = 2
                                  BackoffRate     = 2.0
                                }
                              ]
                              Catch = [
                                {
                                  ErrorEquals = ["States.ALL"]
                                  ResultPath  = "$.error"
                                  Next        = "MatchFailedSenate"
                                }
                              ]
                              End = true
                            }
                            MatchFailedSenate = {
                              Type    = "Pass"
                              Comment = "Continue even if matching fails"
                              Result  = { "matchedTrades" : [], "unmatchedCount" : 1, "error" : "Match failed" }
                              End     = true
                            }
                          }
                        }
                        End = true
                      }
                    }
                  },
                  {
                    # House Pipeline
                    StartAt = "TransformHouse"
                    States = {
                      TransformHouse = {
                        Type    = "Pass"
                        Comment = "Transform: Extract housePTRsMetadataS3Key from fetchResults. Preserve date for later use."
                        Parameters = {
                          "date.$" : "$.fetchResults.date", # Will be available at $.date in subsequent states
                          "metadataS3Key.$" : "$.fetchResults.housePTRsMetadataS3Key",
                          "source" : "house"
                        }
                        ResultPath = "$" # Replace entire state with transformed data
                        Next       = "DownloadHouse"
                      }
                      DownloadHouse = {
                        Type       = "Task"
                        Resource   = module.politician_trades_downloader.function_arn
                        Comment    = "Download House PTRs sequentially from S3 metadata - reads JSON from S3 and processes each file"
                        ResultPath = "$.downloadResults"
                        Next       = "MatchHouse"
                        Retry = [
                          {
                            ErrorEquals     = ["Lambda.TooManyRequestsException", "Lambda.ServiceException"]
                            IntervalSeconds = 60
                            MaxAttempts     = 5
                            BackoffRate     = 2.0
                          },
                          {
                            ErrorEquals     = ["States.ALL"]
                            IntervalSeconds = 10
                            MaxAttempts     = 2
                            BackoffRate     = 2.0
                          }
                        ]
                        Catch = [
                          {
                            ErrorEquals = ["States.ALL"]
                            ResultPath  = "$.error"
                            Next        = "MatchHouse"
                          }
                        ]
                      }
                      MatchHouse = {
                        Type       = "Task"
                        Resource   = module.politician_trades_house_matcher.function_arn
                        Comment    = "Match House PTR trades to politicians using Textract - handles skip cases internally"
                        ResultPath = "$.matchResults"
                        Retry = [
                          {
                            ErrorEquals     = ["Lambda.TooManyRequestsException", "Lambda.ServiceException"]
                            IntervalSeconds = 60
                            MaxAttempts     = 5
                            BackoffRate     = 2.0
                          },
                          {
                            ErrorEquals     = ["States.ALL"]
                            IntervalSeconds = 10
                            MaxAttempts     = 2
                            BackoffRate     = 2.0
                          }
                        ]
                        Catch = [
                          {
                            ErrorEquals = ["States.ALL"]
                            ResultPath  = "$.error"
                            Next        = "MatchHouseError"
                          }
                        ]
                        Next = "SaveHouseTrades"
                      }
                      MatchHouseError = {
                        Type    = "Pass"
                        Comment = "Handle matcher errors gracefully"
                        Result = {
                          "matchedTradesS3Key" : null,
                          "matchedTradesCount" : 0,
                          "unmatchedCount" : 0,
                          "error" : "House PTR matching failed",
                          "source" : "house"
                        }
                        ResultPath = "$.matchResults"
                        Next       = "SaveHouseTrades"
                      }
                      SaveHouseTrades = {
                        Type    = "Pass"
                        Comment = "Format matched House trades for saver. Trades are stored in S3 due to large size. Date preserved from TransformHouse."
                        Parameters = {
                          "date.$" : "$.date",
                          "matchedTradesS3Key.$" : "$.matchResults.matchedTradesS3Key",
                          "matchedTradesCount.$" : "$.matchResults.matchedTradesCount",
                          "unmatchedCount.$" : "$.matchResults.unmatchedCount",
                          "source.$" : "$.matchResults.source"
                        }
                        Next = "SaveHouseTradesTask"
                      }
                      SaveHouseTradesTask = {
                        Type       = "Task"
                        Resource   = module.politician_trades_saver.function_arn
                        Comment    = "Save matched House trades directly to DynamoDB"
                        ResultPath = "$.saveResults"
                        End        = true
                        Retry = [
                          {
                            ErrorEquals     = ["States.ALL"]
                            IntervalSeconds = 30
                            MaxAttempts     = 3
                            BackoffRate     = 2.0
                          }
                        ]
                        Catch = [
                          {
                            ErrorEquals = ["States.ALL"]
                            ResultPath  = "$.error"
                            Next        = "SaveHouseTradesFailed"
                          }
                        ]
                      }
                      SaveHouseTradesFailed = {
                        Type    = "Pass"
                        Comment = "Continue even if save fails"
                        Result  = { "success" : false, "error" : "Save failed" }
                        End     = true
                      }
                    }
                  }
                ]
                ResultPath = "$.nestedResults"
                End        = true
              }
            }
          }
        ]
        ResultPath = "$.pipelineResults"
        End        = true
        # No aggregation step - each pipeline saves directly to DynamoDB:
        # - Senate pipeline: Saves directly in MatchSenate Lambda (each matched trade saved immediately)
        # - House pipeline: Saves via SaveHouseTrades Lambda at end of chain
      }
    }
  })

  # Lambda ARNs for IAM permissions (all Lambdas that Step Functions will invoke)
  lambda_function_arns = [
    module.politician_trades_fetcher.function_arn,
    module.politician_trades_downloader.function_arn,
    module.politician_trades_senate_matcher.function_arn,
    module.politician_trades_house_matcher.function_arn,
    module.politician_trades_saver.function_arn
    # Note: 
    # - politician_trades_sec_matcher removed - SEC Glue job removed
  ]

  # Glue job names for IAM permissions (none - SEC Glue job removed)
  glue_job_names = []

  # Logging configuration
  log_level              = var.environment == "production" ? "ERROR" : "ALL"
  log_retention_days     = 7
  include_execution_data = true

  tags = var.common_tags

  depends_on = [
    module.politician_trades_fetcher,
    module.politician_trades_saver
  ]
}

# EventBridge Scheduler for Daily Politician Trades Aggregation (2:00 AM EST)

# IAM Role for EventBridge to invoke Politician Trades Step Function
resource "aws_iam_role" "politician_trades_scheduler_role" {
  name = "${var.project_name}-politician-trades-scheduler-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = var.common_tags
}

# IAM Policy for EventBridge to start Politician Trades Step Function
resource "aws_iam_role_policy" "politician_trades_scheduler_policy" {
  name = "${var.project_name}-politician-trades-scheduler-policy-${var.environment}"
  role = aws_iam_role.politician_trades_scheduler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "states:StartExecution"
        ]
        Resource = module.politician_trades_state_machine.state_machine_arn
      }
    ]
  })
}

module "politician_trades_scheduler" {
  source = "./modules/eventbridge-scheduler"

  rule_name           = "${var.project_name}-politician-trades-${var.environment}"
  rule_description    = "Trigger politician trades aggregation daily at 2:00 AM EST (after SEC filings are typically complete)"
  schedule_expression = "cron(0 6 ? * * *)" # 2:00 AM EST = 6:00 AM UTC (DST) or 7:00 AM UTC (Standard)
  enabled             = true

  # Target is Step Functions state machine
  target_arn = module.politician_trades_state_machine.state_machine_arn
  target_id  = "PoliticianTradesScheduler"

  # For Step Functions, we need to provide a role
  target_type     = "stepfunctions"
  target_role_arn = aws_iam_role.politician_trades_scheduler_role.arn

  target_input = jsonencode({
    backdate = null
    date     = null # EventBridge cannot generate dynamic dates - Glue will default to yesterday (previous day's filings)
    source   = "scheduler-daily"
  })

  purpose     = "PoliticianTradesAggregation"
  environment = var.environment
  tags        = var.common_tags

  depends_on = [
    module.politician_trades_state_machine,
    aws_iam_role.politician_trades_scheduler_role,
    aws_iam_role_policy.politician_trades_scheduler_policy
  ]
}


# OpenSearch Domain for SEC Filings Full-Text Search
# DISABLED FOR MVP - Agent will use DynamoDB queries + S3 file reads instead
# Can be re-enabled when funding is available (~$200/month)
# module "sec_filings_opensearch" {
#   source = "./modules/opensearch"
#
#   project_name = var.project_name
#   environment  = var.environment
#   domain_name  = "sec" # Shortened to meet 28-char limit: cosine-sec-staging = 18 chars, cosine-sec-production = 21 chars
#
#   # Engine version
#   engine_version = "OpenSearch_2.11"
#
#   # Cluster configuration - start small, scale as needed
#   instance_type  = var.environment == "production" ? "r6g.large.search" : "t3.small.search"
#   instance_count = var.environment == "production" ? 2 : 1
#
#   # Multi-AZ for production
#   zone_awareness_enabled  = var.environment == "production"
#   availability_zone_count = 2

#   # EBS configuration
#   ebs_enabled     = true
#   ebs_volume_type = "gp3"
#   ebs_volume_size = var.environment == "production" ? 100 : 20
#
#   # Security
#   kms_key_arn                     = module.kms.main_key_arn
#   node_to_node_encryption_enabled = true
#   enforce_https                   = true
#   tls_security_policy             = "Policy-Min-TLS-1-2-2019-07"
#
#   # Advanced security (optional - can enable later for fine-grained access control)
#   advanced_security_enabled      = false
#   internal_user_database_enabled = false
#
#   # Access policy - allow Glue job and other services
#   access_policy_json = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Principal = {
#           AWS = [
#             module.politician_trades_sec_glue_job.role_arn,
#             "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
#           ]
#         }
#         Action   = "es:*"
#         Resource = "arn:aws:es:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:domain/${var.project_name}-sec-${var.environment}/*"
#       }
#     ]
#   })
#
#   # Logging
#   log_publishing_options = []
#
#   # Advanced options
#   advanced_options = {}
#
#   common_tags = var.common_tags
#
#   depends_on = [
#     module.kms
#   ]
# }

# IAM Policy for Glue Job to access OpenSearch
# DISABLED FOR MVP - OpenSearch is not being used
# resource "aws_iam_role_policy" "glue_opensearch_access" {
#   name = "${var.project_name}-glue-opensearch-access-${var.environment}"
#   role = module.politician_trades_sec_glue_job.role_name
#
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Action = [
#           "es:ESHttpPost",
#           "es:ESHttpPut",
#           "es:DescribeElasticsearchDomain",
#           "es:DescribeDomain",
#           "es:ESHttpGet"
#         ]
#         Resource = [
#           "${module.sec_filings_opensearch.domain_arn}/*",
#           module.sec_filings_opensearch.domain_arn
#         ]
#       }
#     ]
#   })
# }

# Note: KMS key policies are managed by the KMS module
# No additional policy updates needed
