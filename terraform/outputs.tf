# Base Infrastructure Outputs
# outputs.tf




# KMS outputs
output "kms_key_id" {
  description = "ID of the main KMS key"
  value       = module.kms.main_key_id
}

output "kms_key_arn" {
  description = "ARN of the main KMS key"
  value       = module.kms.main_key_arn
}

output "kms_key_alias" {
  description = "Alias of the main KMS key"
  value       = module.kms.main_key_alias
}

output "dynamodb_key_id" {
  description = "ID of the DynamoDB KMS key"
  value       = module.kms.dynamodb_key_id
}

output "dynamodb_key_arn" {
  description = "ARN of the DynamoDB KMS key"
  value       = module.kms.dynamodb_key_arn
}

output "cloudwatch_key_id" {
  description = "ID of the CloudWatch KMS key"
  value       = module.kms.cloudwatch_key_id
}

output "cloudwatch_key_arn" {
  description = "ARN of the CloudWatch KMS key"
  value       = module.kms.cloudwatch_key_arn
}

# Cognito outputs
output "cognito_user_pool_id" {
  description = "ID of the Cognito User Pool"
  value       = module.cognito.user_pool_id
}

output "cognito_user_pool_arn" {
  description = "ARN of the Cognito User Pool"
  value       = module.cognito.user_pool_arn
}

output "cognito_user_pool_endpoint" {
  description = "Endpoint of the Cognito User Pool"
  value       = module.cognito.user_pool_endpoint
}

output "cognito_user_pool_client_id" {
  description = "ID of the Cognito User Pool Client"
  value       = module.cognito.user_pool_client_id
}

output "cognito_user_pool_client_secret" {
  description = "Secret of the Cognito User Pool Client"
  value       = module.cognito.user_pool_client_secret
  sensitive   = true
}

output "cognito_user_pool_domain" {
  description = "Domain of the Cognito User Pool"
  value       = module.cognito.user_pool_domain
}

output "cognito_user_pool_domain_cloudfront_distribution_arn" {
  description = "CloudFront distribution ARN of the Cognito User Pool domain"
  value       = module.cognito.user_pool_domain_cloudfront_distribution_arn
}

# DynamoDB outputs
output "user_profiles_table_name" {
  description = "Name of the user profiles DynamoDB table"
  value       = module.user_profiles_table.table_name
}

output "user_profiles_table_arn" {
  description = "ARN of the user profiles DynamoDB table"
  value       = module.user_profiles_table.table_arn
}

output "user_profiles_table_id" {
  description = "ID of the user profiles DynamoDB table"
  value       = module.user_profiles_table.table_id
}

output "user_profiles_table_stream_arn" {
  description = "Stream ARN of the user profiles DynamoDB table"
  value       = module.user_profiles_table.stream_arn
}

# DynamoDB KMS Key outputs
output "dynamodb_module_kms_key_arn" {
  description = "ARN of the KMS key used for DynamoDB encryption"
  value       = module.kms.dynamodb_key_arn
}

output "dynamodb_module_kms_key_id" {
  description = "ID of the KMS key used for DynamoDB encryption"
  value       = module.kms.dynamodb_key_id
}

# KMS Access Policy
output "kms_access_policy_arn" {
  description = "ARN of the IAM policy for accessing KMS keys"
  value       = module.kms.kms_access_policy_arn
}

# Security Events Table Outputs
output "security_events_table_name" {
  description = "Name of the security events DynamoDB table"
  value       = module.security_events_table.table_name
}

output "security_events_table_arn" {
  description = "ARN of the security events DynamoDB table"
  value       = module.security_events_table.table_arn
}

output "security_events_table_id" {
  description = "ID of the security events DynamoDB table"
  value       = module.security_events_table.table_id
}

output "security_events_table_stream_arn" {
  description = "Stream ARN of the security events DynamoDB table"
  value       = module.security_events_table.stream_arn
}

# REMOVED: User Sessions Table Outputs - consolidated into chat_sessions table

# Chat Connections Table Outputs
output "chat_connections_table_name" {
  description = "Name of the chat connections DynamoDB table"
  value       = module.chat_connections_table.table_name
}

output "chat_connections_table_arn" {
  description = "ARN of the chat connections DynamoDB table"
  value       = module.chat_connections_table.table_arn
}

output "chat_connections_table_id" {
  description = "ID of the chat connections DynamoDB table"
  value       = module.chat_connections_table.table_id
}

output "chat_connections_table_stream_arn" {
  description = "Stream ARN of the chat connections DynamoDB table"
  value       = module.chat_connections_table.stream_arn
}

# Chat Sessions Table Outputs
output "chat_sessions_table_name" {
  description = "Name of the chat sessions DynamoDB table"
  value       = module.chat_sessions_table.table_name
}

output "chat_sessions_table_arn" {
  description = "ARN of the chat sessions DynamoDB table"
  value       = module.chat_sessions_table.table_arn
}

output "chat_sessions_table_id" {
  description = "ID of the chat sessions DynamoDB table"
  value       = module.chat_sessions_table.table_id
}

output "chat_sessions_table_stream_arn" {
  description = "Stream ARN of the chat sessions DynamoDB table"
  value       = module.chat_sessions_table.stream_arn
}

# Alerts Table outputs
output "alerts_table_name" {
  description = "Name of the alerts DynamoDB table"
  value       = module.alerts_table.table_name
}

output "alerts_table_arn" {
  description = "ARN of the alerts DynamoDB table"
  value       = module.alerts_table.table_arn
}

output "alerts_table_id" {
  description = "ID of the alerts DynamoDB table"
  value       = module.alerts_table.table_id
}

output "alerts_table_stream_arn" {
  description = "Stream ARN of the alerts DynamoDB table"
  value       = module.alerts_table.stream_arn
}

# Stock Data Table outputs
output "stock_data_table_name" {
  description = "Name of the stock data DynamoDB table"
  value       = module.stock_data_table.table_name
}

output "stock_data_table_arn" {
  description = "ARN of the stock data DynamoDB table"
  value       = module.stock_data_table.table_arn
}

output "stock_data_table_id" {
  description = "ID of the stock data DynamoDB table"
  value       = module.stock_data_table.table_id
}

output "stock_data_stream_arn" {
  description = "Stream ARN of the stock data DynamoDB table"
  value       = module.stock_data_table.stream_arn
}

output "stock_data_table_policy_arn" {
  description = "ARN of the IAM policy for accessing stock_data table"
  value       = module.stock_data_table.table_policy_arn
}

# SEC Filings Cache Table outputs
output "sec_filings_table_name" {
  description = "Name of the SEC filings cache DynamoDB table"
  value       = module.sec_filings_table.table_name
}

output "sec_filings_table_arn" {
  description = "ARN of the SEC filings cache DynamoDB table"
  value       = module.sec_filings_table.table_arn
}

output "sec_filings_table_policy_arn" {
  description = "ARN of the IAM policy for accessing sec_filings_cache table"
  value       = module.sec_filings_table.table_policy_arn
}

# SEC Search Query Cache Table outputs
output "sec_search_query_cache_table_name" {
  description = "Name of the SEC search query cache table"
  value       = module.sec_search_query_cache_table.table_name
}

output "sec_search_query_cache_table_arn" {
  description = "ARN of the SEC search query cache table"
  value       = module.sec_search_query_cache_table.table_arn
}

output "sec_search_query_cache_table_policy_arn" {
  description = "ARN of the IAM policy for accessing the SEC search query cache table"
  value       = module.sec_search_query_cache_table.table_policy_arn
}

# CloudWatch outputs
output "security_log_group_name" {
  description = "Name of the security CloudWatch log group"
  value       = module.cloudwatch.security_log_group_name
}

output "security_log_group_arn" {
  description = "ARN of the security CloudWatch log group"
  value       = module.cloudwatch.security_log_group_arn
}

output "auth_log_group_name" {
  description = "Name of the authentication CloudWatch log group"
  value       = module.cloudwatch.auth_log_group_name
}

output "auth_log_group_arn" {
  description = "ARN of the authentication CloudWatch log group"
  value       = module.cloudwatch.auth_log_group_arn
}

output "application_log_group_name" {
  description = "Name of the application CloudWatch log group"
  value       = module.cloudwatch.application_log_group_name
}

output "application_log_group_arn" {
  description = "ARN of the application CloudWatch log group"
  value       = module.cloudwatch.application_log_group_arn
}

output "lambda_log_group_name" {
  description = "Name of the Lambda CloudWatch log group"
  value       = module.cloudwatch.lambda_log_group_name
}

output "lambda_log_group_arn" {
  description = "ARN of the Lambda CloudWatch log group"
  value       = module.cloudwatch.lambda_log_group_arn
}

output "api_gateway_log_group_name" {
  description = "Name of the API Gateway CloudWatch log group"
  value       = module.cloudwatch.api_gateway_log_group_name
}

output "api_gateway_log_group_arn" {
  description = "ARN of the API Gateway CloudWatch log group"
  value       = module.cloudwatch.api_gateway_log_group_arn
}

output "dashboard_url" {
  description = "URL of the CloudWatch dashboard"
  value       = module.cloudwatch.dashboard_url
}

output "failed_login_alarm_arn" {
  description = "ARN of the failed login CloudWatch alarm"
  value       = module.cloudwatch.failed_login_alarm_arn
}

output "suspicious_activity_alarm_arn" {
  description = "ARN of the suspicious activity CloudWatch alarm"
  value       = module.cloudwatch.suspicious_activity_alarm_arn
}

# Computed values for reference
output "environment_prefix" {
  description = "Prefix for resource naming"
  value       = "${var.project_name}-${var.environment}"
}

output "aws_region" {
  description = "AWS region being used"
  value       = var.aws_region
}

output "common_tags" {
  description = "Common tags applied to all resources"
  value       = var.common_tags
}

# Secrets Manager Outputs
output "oauth_secrets_enabled" {
  description = "Whether OAuth secrets are stored in Secrets Manager"
  value       = var.oauth_secrets_enabled
}

output "oauth_secret_arns" {
  description = "ARNs of OAuth secrets in Secrets Manager"
  value       = var.oauth_secrets_enabled ? module.secrets_manager.secret_arns : {}
}

output "oauth_secret_names" {
  description = "Names of OAuth secrets in Secrets Manager"
  value       = var.oauth_secrets_enabled ? module.secrets_manager.secret_names : {}
}

# NewsData Secrets Manager Outputs
output "newsdata_secret_arns" {
  description = "ARNs of NewsData secrets in Secrets Manager"
  value       = module.newsdata_secrets_manager.secret_arns
}

output "newsdata_secret_names" {
  description = "Names of NewsData secrets in Secrets Manager"
  value       = module.newsdata_secrets_manager.secret_names
}


# ============================================================================
# FRONTEND CONFIGURATION OUTPUT
# Consolidated configuration for frontend deployments across environments
# ============================================================================

output "frontend_auth_config" {
  description = "Complete authentication configuration for frontend applications"
  value = {
    user_pool_id    = module.cognito.user_pool_id
    user_pool_arn   = module.cognito.user_pool_arn
    client_id       = module.cognito.user_pool_client_id
    domain_name     = module.cognito.user_pool_domain
    full_domain_url = module.cognito.user_pool_domain != null ? "${module.cognito.user_pool_domain}.auth.${var.aws_region}.amazoncognito.com" : null
    region          = var.aws_region
    environment     = var.environment
  }
}

# ============================================================================
# SESSION MANAGEMENT OUTPUTS
# ============================================================================

# REMOVED: Session Management Config - consolidated into chat_sessions table

# ============================================================================
# LAMBDA LAYER OUTPUTS
# ============================================================================

output "core_layer_arn" {
  description = "ARN of the core dependencies Lambda layer"
  value       = module.core_layer.layer_arn
}

output "core_layer_version" {
  description = "Version of the core dependencies Lambda layer"
  value       = module.core_layer.layer_version
}

output "financial_layer_arn" {
  description = "ARN of the financial dependencies Lambda layer"
  value       = module.financial_layer.layer_arn
}

output "financial_layer_version" {
  description = "Version of the financial dependencies Lambda layer"
  value       = module.financial_layer.layer_version
}

output "crypto_layer_arn" {
  description = "ARN of the crypto dependencies Lambda layer"
  value       = module.crypto_layer.layer_arn
}

output "crypto_layer_version" {
  description = "Version of the crypto dependencies Lambda layer"
  value       = module.crypto_layer.layer_version
}

# News Table Outputs
output "news_table_name" {
  description = "Name of the news DynamoDB table"
  value       = module.news_table.table_name
}

output "news_table_arn" {
  description = "ARN of the news DynamoDB table"
  value       = module.news_table.table_arn
}

output "news_table_id" {
  description = "ID of the news DynamoDB table"
  value       = module.news_table.table_id
}

output "news_table_stream_arn" {
  description = "Stream ARN of the news DynamoDB table"
  value       = module.news_table.stream_arn
}

output "news_table_policy_arn" {
  description = "ARN of the IAM policy for accessing news table"
  value       = module.news_table.table_policy_arn
}

# Stock Data Infrastructure Outputs
output "stock_data_historical_loader_function_name" {
  description = "Name of the stock data historical loader Lambda function"
  value       = module.stock_data_historical_loader.function_name
}

output "stock_data_historical_loader_function_arn" {
  description = "ARN of the stock data historical loader Lambda function"
  value       = module.stock_data_historical_loader.function_arn
}

output "stock_data_historical_loader_state_machine_arn" {
  description = "ARN of the historical loader Step Functions state machine"
  value       = module.stock_data_historical_loader_state_machine.state_machine_arn
}

output "eod_aggregator_function_name" {
  description = "Name of the EOD aggregator Lambda function"
  value       = module.eod_aggregator.function_name
}

output "eod_aggregator_function_arn" {
  description = "ARN of the EOD aggregator Lambda function"
  value       = module.eod_aggregator.function_arn
}

output "eod_aggregator_state_machine_arn" {
  description = "ARN of the EOD aggregator Step Functions state machine"
  value       = module.eod_aggregator_state_machine.state_machine_arn
}

# Chat Files S3 Bucket Outputs
output "chat_files_bucket_name" {
  description = "Name of the chat files S3 bucket"
  value       = module.chat_files_s3.bucket_id
}

output "chat_files_bucket_arn" {
  description = "ARN of the chat files S3 bucket"
  value       = module.chat_files_s3.bucket_arn
}

output "chat_files_bucket_domain_name" {
  description = "Domain name of the chat files S3 bucket"
  value       = module.chat_files_s3.bucket_domain_name
}

# Stock Historical Data S3 Bucket Outputs
output "stock_historical_bucket_name" {
  description = "Name of the stock historical data S3 bucket"
  value       = module.stock_data_historical_s3.bucket_id
}

output "stock_historical_bucket_arn" {
  description = "ARN of the stock historical data S3 bucket"
  value       = module.stock_data_historical_s3.bucket_arn
}

output "stock_historical_bucket_domain_name" {
  description = "Domain name of the stock historical data S3 bucket"
  value       = module.stock_data_historical_s3.bucket_domain_name
}

# Agent File Upload Notifications
# Chat Files S3 Access Policy
output "lambda_s3_chat_files_policy_arn" {
  description = "ARN of the IAM policy for Lambda access to chat files S3 bucket"
  value       = aws_iam_policy.lambda_s3_chat_files_policy.arn
}

# USAspending Awards Index Table Outputs
output "usaspending_awards_table_name" {
  description = "Name of the USAspending awards index DynamoDB table"
  value       = module.usaspending_awards_index_table.table_name
}

output "usaspending_awards_table_arn" {
  description = "ARN of the USAspending awards index DynamoDB table"
  value       = module.usaspending_awards_index_table.table_arn
}

output "usaspending_awards_table_id" {
  description = "ID of the USAspending awards index DynamoDB table"
  value       = module.usaspending_awards_index_table.table_id
}

output "usaspending_awards_table_policy_arn" {
  description = "ARN of the IAM policy for accessing the USAspending awards index table"
  value       = module.usaspending_awards_index_table.table_policy_arn
}

# USAspending Data S3 Bucket Outputs
output "usaspending_data_s3_bucket_name" {
  description = "Name of the USAspending data S3 bucket"
  value       = module.usaspending_data_s3.bucket_id
}

output "usaspending_data_s3_bucket_arn" {
  description = "ARN of the USAspending data S3 bucket"
  value       = module.usaspending_data_s3.bucket_arn
}

output "lambda_usaspending_data_s3_policy_arn" {
  description = "ARN of the IAM policy for Lambda access to USAspending data S3 bucket"
  value       = aws_iam_policy.lambda_usaspending_data_s3_policy.arn
}

# Congress Bills Table Outputs
output "congress_bills_table_name" {
  description = "Name of the congress bills DynamoDB table"
  value       = module.congress_bills_table.table_name
}

output "congress_bills_table_arn" {
  description = "ARN of the congress bills DynamoDB table"
  value       = module.congress_bills_table.table_arn
}

output "congress_bills_table_id" {
  description = "ID of the congress bills DynamoDB table"
  value       = module.congress_bills_table.table_id
}

output "congress_bills_table_policy_arn" {
  description = "ARN of the IAM policy for accessing the congress bills table"
  value       = module.congress_bills_table.table_policy_arn
}

# Congress Bills Data S3 Bucket Outputs
output "congress_bills_data_s3_bucket_name" {
  description = "Name of the congress bills data S3 bucket"
  value       = module.congress_bills_data_s3.bucket_id
}

output "congress_bills_data_s3_bucket_arn" {
  description = "ARN of the congress bills data S3 bucket"
  value       = module.congress_bills_data_s3.bucket_arn
}

# LDA Filings Table Outputs
output "lda_filings_table_name" {
  description = "Name of the LDA filings DynamoDB table"
  value       = module.lda_filings_table.table_name
}

output "lda_filings_table_arn" {
  description = "ARN of the LDA filings DynamoDB table"
  value       = module.lda_filings_table.table_arn
}

output "lda_filings_table_id" {
  description = "ID of the LDA filings DynamoDB table"
  value       = module.lda_filings_table.table_id
}

output "lda_filings_table_policy_arn" {
  description = "ARN of the IAM policy for accessing the LDA filings table"
  value       = module.lda_filings_table.table_policy_arn
}

# LDA Disclosures S3 Bucket Outputs
output "lda_disclosures_s3_bucket_name" {
  description = "Name of the LDA disclosures S3 bucket"
  value       = module.lda_disclosures_s3.bucket_id
}

output "lda_disclosures_s3_bucket_arn" {
  description = "ARN of the LDA disclosures S3 bucket"
  value       = module.lda_disclosures_s3.bucket_arn
}

# Note: Parameter-filing mappings are stored in the same lda-filings table
# using PK = "PARAMETER_TYPE#VALUE" and SK = "FILING#{uuid}" or "CONTRIBUTION#{uuid}"

# OpenSearch Domain Outputs
# DISABLED FOR MVP - OpenSearch is not being used
# output "opensearch_domain_endpoint" {
#   description = "Endpoint of the OpenSearch domain for SEC filings"
#   value       = module.sec_filings_opensearch.domain_endpoint
# }
#
# output "opensearch_domain_arn" {
#   description = "ARN of the OpenSearch domain for SEC filings"
#   value       = module.sec_filings_opensearch.domain_arn
# }
#
# output "opensearch_domain_name" {
#   description = "Name of the OpenSearch domain for SEC filings"
#   value       = module.sec_filings_opensearch.domain_name
# }
#
# output "opensearch_dashboard_endpoint" {
#   description = "Dashboard endpoint of the OpenSearch domain for SEC filings"
#   value       = module.sec_filings_opensearch.dashboard_endpoint
# }

# OpenSearch Domain Outputs
# DISABLED FOR MVP - OpenSearch is not being used
# output "opensearch_domain_endpoint" {
#   description = "Endpoint of the OpenSearch domain for SEC filings"
#   value       = module.sec_filings_opensearch.domain_endpoint
# }
#
# output "opensearch_domain_arn" {
#   description = "ARN of the OpenSearch domain for SEC filings"
#   value       = module.sec_filings_opensearch.domain_arn
# }
#
# output "opensearch_domain_name" {
#   description = "Name of the OpenSearch domain for SEC filings"
#   value       = module.sec_filings_opensearch.domain_name
# }
#
# output "opensearch_dashboard_endpoint" {
#   description = "Dashboard endpoint of the OpenSearch domain for SEC filings"
#   value       = module.sec_filings_opensearch.dashboard_endpoint
# }
