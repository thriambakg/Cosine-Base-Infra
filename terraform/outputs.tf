# Base Infrastructure Outputs
# outputs.tf

# Lambda Layer outputs
output "lambda_layer_arn" {
  description = "ARN of the shared Lambda layer"
  value       = module.shared_layer.layer_arn
}

output "lambda_layer_version" {
  description = "Version of the shared Lambda layer"
  value       = module.shared_layer.layer_version
}

output "layer_name" {
  description = "Name of the shared Lambda layer"
  value       = module.shared_layer.layer_name
}

output "compatible_runtimes" {
  description = "Compatible runtimes for the Lambda layer"
  value       = module.shared_layer.compatible_runtimes
}

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
