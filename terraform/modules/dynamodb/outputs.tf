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

# User Sessions Table
output "user_sessions_table_name" {
  description = "Name of the user sessions DynamoDB table"
  value       = aws_dynamodb_table.user_sessions.name
}

output "user_sessions_table_arn" {
  description = "ARN of the user sessions DynamoDB table"
  value       = aws_dynamodb_table.user_sessions.arn
}

output "user_sessions_table_id" {
  description = "ID of the user sessions DynamoDB table"
  value       = aws_dynamodb_table.user_sessions.id
}

output "user_sessions_stream_arn" {
  description = "ARN of the user sessions DynamoDB stream"
  value       = aws_dynamodb_table.user_sessions.stream_arn
}

# Combined outputs for easier access
output "all_table_names" {
  description = "List of all DynamoDB table names"
  value = [
    aws_dynamodb_table.user_profiles.name,
    aws_dynamodb_table.security_events.name,
    aws_dynamodb_table.user_sessions.name
  ]
}

output "all_table_arns" {
  description = "List of all DynamoDB table ARNs"
  value = [
    aws_dynamodb_table.user_profiles.arn,
    aws_dynamodb_table.security_events.arn,
    aws_dynamodb_table.user_sessions.arn
  ]
}
