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
  value       = module.dynamodb.user_profiles_table_name
}

output "user_profiles_table_arn" {
  description = "ARN of the user profiles DynamoDB table"
  value       = module.dynamodb.user_profiles_table_arn
}

output "user_profiles_table_id" {
  description = "ID of the user profiles DynamoDB table"
  value       = module.dynamodb.user_profiles_table_id
}

output "user_profiles_table_stream_arn" {
  description = "Stream ARN of the user profiles DynamoDB table"
  value       = module.dynamodb.user_profiles_table_stream_arn
}

# DynamoDB KMS Key outputs
output "dynamodb_module_kms_key_arn" {
  description = "ARN of the KMS key used for DynamoDB encryption"
  value       = module.dynamodb.dynamodb_kms_key_arn
}

output "dynamodb_module_kms_key_id" {
  description = "ID of the KMS key used for DynamoDB encryption"
  value       = module.dynamodb.dynamodb_kms_key_id
}

# Security Events Table Outputs
output "security_events_table_name" {
  description = "Name of the security events DynamoDB table"
  value       = module.dynamodb.security_events_table_name
}

output "security_events_table_arn" {
  description = "ARN of the security events DynamoDB table"
  value       = module.dynamodb.security_events_table_arn
}

output "security_events_table_id" {
  description = "ID of the security events DynamoDB table"
  value       = module.dynamodb.security_events_table_id
}

output "security_events_table_stream_arn" {
  description = "Stream ARN of the security events DynamoDB table"
  value       = module.dynamodb.security_events_table_stream_arn
}

# User Sessions Table Outputs
output "user_sessions_table_name" {
  description = "Name of the user sessions DynamoDB table"
  value       = module.dynamodb.user_sessions_table_name
}

output "user_sessions_table_arn" {
  description = "ARN of the user sessions DynamoDB table"
  value       = module.dynamodb.user_sessions_table_arn
}

output "user_sessions_table_id" {
  description = "ID of the user sessions DynamoDB table"
  value       = module.dynamodb.user_sessions_table_id
}

output "user_sessions_table_stream_arn" {
  description = "Stream ARN of the user sessions DynamoDB table"
  value       = module.dynamodb.user_sessions_table_stream_arn
}

# Chat Connections Table Outputs
output "chat_connections_table_name" {
  description = "Name of the chat connections DynamoDB table"
  value       = module.dynamodb.chat_connections_table_name
}

output "chat_connections_table_arn" {
  description = "ARN of the chat connections DynamoDB table"
  value       = module.dynamodb.chat_connections_table_arn
}

output "chat_connections_table_id" {
  description = "ID of the chat connections DynamoDB table"
  value       = module.dynamodb.chat_connections_table_id
}

output "chat_connections_table_stream_arn" {
  description = "Stream ARN of the chat connections DynamoDB table"
  value       = module.dynamodb.chat_connections_table_stream_arn
}

# Chat Sessions Table Outputs
output "chat_sessions_table_name" {
  description = "Name of the chat sessions DynamoDB table"
  value       = module.dynamodb.chat_sessions_table_name
}

output "chat_sessions_table_arn" {
  description = "ARN of the chat sessions DynamoDB table"
  value       = module.dynamodb.chat_sessions_table_arn
}

output "chat_sessions_table_id" {
  description = "ID of the chat sessions DynamoDB table"
  value       = module.dynamodb.chat_sessions_table_id
}

output "chat_sessions_table_stream_arn" {
  description = "Stream ARN of the chat sessions DynamoDB table"
  value       = module.dynamodb.chat_sessions_table_stream_arn
}

# Alerts Table outputs
output "alerts_table_name" {
  description = "Name of the alerts DynamoDB table"
  value       = module.dynamodb.alerts_table_name
}

output "alerts_table_arn" {
  description = "ARN of the alerts DynamoDB table"
  value       = module.dynamodb.alerts_table_arn
}

output "alerts_table_id" {
  description = "ID of the alerts DynamoDB table"
  value       = module.dynamodb.alerts_table_id
}

output "alerts_table_stream_arn" {
  description = "Stream ARN of the alerts DynamoDB table"
  value       = module.dynamodb.alerts_table_stream_arn
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

# Lambda Layer outputs - Multiple layers for better dependency management
output "lambda_layer_arns" {
  description = "ARNs of all Lambda layers (AI-Core consolidated into Core layer)"
  value = {
    core          = module.lambda_layer_core.layer_arn
    financial     = module.lambda_layer_financial.layer_arn
    strands       = module.lambda_layer_strands.layer_arn
    strands_tools = module.lambda_layer_strands_tools.layer_arn
    utility       = module.lambda_layer_utility.layer_arn
  }
}

output "lambda_layer_arn_list" {
  description = "List of essential Lambda layer ARNs for Lambda functions (4 layers - AI-Core consolidated into Core)"
  value = [
    module.lambda_layer_core.layer_arn,
    module.lambda_layer_financial.layer_arn,
    module.lambda_layer_strands.layer_arn,
    module.lambda_layer_strands_tools.layer_arn
  ]
}

output "lambda_layer_arn_list_with_utility" {
  description = "List of all Lambda layer ARNs including utility layer (5 layers - AI-Core consolidated into Core)"
  value = [
    module.lambda_layer_core.layer_arn,
    module.lambda_layer_financial.layer_arn,
    module.lambda_layer_strands.layer_arn,
    module.lambda_layer_strands_tools.layer_arn,
    module.lambda_layer_utility.layer_arn
  ]
}

# Individual layer outputs for specific use cases
output "lambda_layer_core_arn" {
  description = "ARN of the core dependencies Lambda layer"
  value       = module.lambda_layer_core.layer_arn
}

output "lambda_layer_financial_arn" {
  description = "ARN of the financial dependencies Lambda layer"
  value       = module.lambda_layer_financial.layer_arn
}

# AI Core layer consolidated into Core layer
# output "lambda_layer_ai_core_arn" {
#   description = "ARN of the AI core dependencies Lambda layer"
#   value       = module.lambda_layer_ai_core.layer_arn
# }

output "lambda_layer_strands_arn" {
  description = "ARN of the Strands core dependencies Lambda layer"
  value       = module.lambda_layer_strands.layer_arn
}

output "lambda_layer_strands_tools_arn" {
  description = "ARN of the Strands tools dependencies Lambda layer"
  value       = module.lambda_layer_strands_tools.layer_arn
}

output "lambda_layer_utility_arn" {
  description = "ARN of the utility dependencies Lambda layer"
  value       = module.lambda_layer_utility.layer_arn
}

# S3 bucket outputs
output "lambda_layers_bucket_name" {
  description = "Name of the S3 bucket for Lambda layers"
  value       = module.lambda_layers_bucket.bucket_id
}

output "lambda_layers_bucket_arn" {
  description = "ARN of the S3 bucket for Lambda layers"
  value       = module.lambda_layers_bucket.bucket_arn
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
