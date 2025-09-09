# ============================================================================
# SESSION MANAGEMENT MODULE OUTPUTS
# ============================================================================

# DynamoDB Tables
output "sessions_table_name" {
  description = "Name of the sessions DynamoDB table"
  value       = aws_dynamodb_table.sessions.name
}

output "sessions_table_arn" {
  description = "ARN of the sessions DynamoDB table"
  value       = aws_dynamodb_table.sessions.arn
}

output "session_context_table_name" {
  description = "Name of the session context DynamoDB table"
  value       = aws_dynamodb_table.session_context.name
}

output "session_context_table_arn" {
  description = "ARN of the session context DynamoDB table"
  value       = aws_dynamodb_table.session_context.arn
}

# S3 Buckets
output "session_archives_bucket_name" {
  description = "Name of the session archives S3 bucket"
  value       = aws_s3_bucket.session_archives.bucket
}

output "session_archives_bucket_arn" {
  description = "ARN of the session archives S3 bucket"
  value       = aws_s3_bucket.session_archives.arn
}

output "session_archives_bucket_domain_name" {
  description = "Domain name of the session archives S3 bucket"
  value       = aws_s3_bucket.session_archives.bucket_domain_name
}

# IAM Policies
output "session_management_policy_arn" {
  description = "ARN of the session management IAM policy"
  value       = aws_iam_policy.session_management_policy.arn
}

# CloudWatch Log Groups
output "session_management_log_group_name" {
  description = "Name of the session management CloudWatch log group"
  value       = aws_cloudwatch_log_group.session_management.name
}

output "session_management_log_group_arn" {
  description = "ARN of the session management CloudWatch log group"
  value       = aws_cloudwatch_log_group.session_management.arn
}

# ============================================================================
# CONFIGURATION OUTPUTS
# ============================================================================

output "session_ttl_days" {
  description = "TTL in days for session data"
  value       = var.session_ttl_days
}

output "context_ttl_days" {
  description = "TTL in days for context data"
  value       = var.context_ttl_days
}
