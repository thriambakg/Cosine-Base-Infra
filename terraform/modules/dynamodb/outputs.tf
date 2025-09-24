# DynamoDB Module Outputs
# modules/dynamodb/outputs.tf

# User Profiles Table
output "user_profiles_table_name" {
  description = "Name of the user profiles DynamoDB table"
  value       = aws_dynamodb_table.user_profiles.name
}

output "user_profiles_table_arn" {
  description = "ARN of the user profiles DynamoDB table"
  value       = aws_dynamodb_table.user_profiles.arn
}

output "user_profiles_table_id" {
  description = "ID of the user profiles DynamoDB table"
  value       = aws_dynamodb_table.user_profiles.id
}

output "user_profiles_stream_arn" {
  description = "ARN of the user profiles DynamoDB stream"
  value       = aws_dynamodb_table.user_profiles.stream_arn
}

output "user_profiles_table_stream_arn" {
  description = "ARN of the user profiles DynamoDB stream (alias)"
  value       = aws_dynamodb_table.user_profiles.stream_arn
}

# Security Events Table
output "security_events_table_name" {
  description = "Name of the security events DynamoDB table"
  value       = aws_dynamodb_table.security_events.name
}

output "security_events_table_arn" {
  description = "ARN of the security events DynamoDB table"
  value       = aws_dynamodb_table.security_events.arn
}

output "security_events_table_id" {
  description = "ID of the security events DynamoDB table"
  value       = aws_dynamodb_table.security_events.id
}

output "security_events_stream_arn" {
  description = "ARN of the security events DynamoDB stream"
  value       = aws_dynamodb_table.security_events.stream_arn
}

output "security_events_table_stream_arn" {
  description = "ARN of the security events DynamoDB stream (alias)"
  value       = aws_dynamodb_table.security_events.stream_arn
}

# REMOVED: User Sessions Table outputs - consolidated into chat_sessions table

# Alerts Table
output "alerts_table_name" {
  description = "Name of the alerts DynamoDB table"
  value       = aws_dynamodb_table.alerts.name
}

output "alerts_table_arn" {
  description = "ARN of the alerts DynamoDB table"
  value       = aws_dynamodb_table.alerts.arn
}

output "alerts_table_id" {
  description = "ID of the alerts DynamoDB table"
  value       = aws_dynamodb_table.alerts.id
}

output "alerts_stream_arn" {
  description = "ARN of the alerts DynamoDB stream"
  value       = aws_dynamodb_table.alerts.stream_arn
}

output "alerts_table_stream_arn" {
  description = "ARN of the alerts DynamoDB stream (alias)"
  value       = aws_dynamodb_table.alerts.stream_arn
}

# Combined outputs for easier access
output "all_table_names" {
  description = "List of all DynamoDB table names"
  value = [
    aws_dynamodb_table.user_profiles.name,
    aws_dynamodb_table.security_events.name,
    aws_dynamodb_table.chat_sessions.name,
    aws_dynamodb_table.chat_connections.name,
    aws_dynamodb_table.alerts.name,
    aws_dynamodb_table.stock_data.name
  ]
}

output "all_table_arns" {
  description = "List of all DynamoDB table ARNs"
  value = [
    aws_dynamodb_table.user_profiles.arn,
    aws_dynamodb_table.security_events.arn,
    aws_dynamodb_table.chat_sessions.arn,
    aws_dynamodb_table.chat_connections.arn,
    aws_dynamodb_table.alerts.arn,
    aws_dynamodb_table.stock_data.arn
  ]
}

output "user_profiles_table_policy_arn" {
  description = "ARN of the IAM policy for accessing user_profiles table"
  value       = aws_iam_policy.user_profiles_table_policy.arn
}

# DynamoDB KMS Key
output "dynamodb_kms_key_arn" {
  description = "ARN of the KMS key used for DynamoDB encryption"
  value       = aws_kms_key.dynamodb.arn
}

output "dynamodb_kms_key_id" {
  description = "ID of the KMS key used for DynamoDB encryption"
  value       = aws_kms_key.dynamodb.key_id
}

# Chat Connections Table Outputs
output "chat_connections_table_name" {
  description = "Name of the chat connections DynamoDB table"
  value       = aws_dynamodb_table.chat_connections.name
}

output "chat_connections_table_arn" {
  description = "ARN of the chat connections DynamoDB table"
  value       = aws_dynamodb_table.chat_connections.arn
}

output "chat_connections_table_id" {
  description = "ID of the chat connections DynamoDB table"
  value       = aws_dynamodb_table.chat_connections.id
}

output "chat_connections_table_stream_arn" {
  description = "Stream ARN of the chat connections DynamoDB table"
  value       = aws_dynamodb_table.chat_connections.stream_arn
}

# Chat Sessions Table Outputs
output "chat_sessions_table_name" {
  description = "Name of the chat sessions DynamoDB table"
  value       = aws_dynamodb_table.chat_sessions.name
}

output "chat_sessions_table_arn" {
  description = "ARN of the chat sessions DynamoDB table"
  value       = aws_dynamodb_table.chat_sessions.arn
}

output "chat_sessions_table_id" {
  description = "ID of the chat sessions DynamoDB table"
  value       = aws_dynamodb_table.chat_sessions.id
}

output "chat_sessions_table_stream_arn" {
  description = "Stream ARN of the chat sessions DynamoDB table"
  value       = aws_dynamodb_table.chat_sessions.stream_arn
}

# Stock Data Table Outputs
output "stock_data_table_name" {
  description = "Name of the stock data DynamoDB table"
  value       = aws_dynamodb_table.stock_data.name
}

output "stock_data_table_arn" {
  description = "ARN of the stock data DynamoDB table"
  value       = aws_dynamodb_table.stock_data.arn
}

output "stock_data_table_id" {
  description = "ID of the stock data DynamoDB table"
  value       = aws_dynamodb_table.stock_data.id
}

output "stock_data_stream_arn" {
  description = "Stream ARN of the stock data DynamoDB table"
  value       = aws_dynamodb_table.stock_data.stream_arn
}

output "stock_data_table_policy_arn" {
  description = "ARN of the IAM policy for accessing stock_data table"
  value       = aws_iam_policy.stock_data_table_policy.arn
}
