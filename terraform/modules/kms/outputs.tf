# KMS Module Outputs
# modules/kms/outputs.tf

# Main KMS Key
output "main_key_id" {
  description = "ID of the main KMS key"
  value       = aws_kms_key.main.key_id
}

output "main_key_arn" {
  description = "ARN of the main KMS key"
  value       = aws_kms_key.main.arn
}

output "main_key_alias" {
  description = "Alias of the main KMS key"
  value       = aws_kms_alias.main.name
}

# DynamoDB KMS Key
output "dynamodb_key_id" {
  description = "ID of the DynamoDB KMS key"
  value       = aws_kms_key.dynamodb.key_id
}

output "dynamodb_key_arn" {
  description = "ARN of the DynamoDB KMS key"
  value       = aws_kms_key.dynamodb.arn
}

output "dynamodb_key_alias" {
  description = "Alias of the DynamoDB KMS key"
  value       = aws_kms_alias.dynamodb.name
}

# CloudWatch KMS Key
output "cloudwatch_key_id" {
  description = "ID of the CloudWatch KMS key"
  value       = aws_kms_key.cloudwatch.key_id
}

output "cloudwatch_key_arn" {
  description = "ARN of the CloudWatch KMS key"
  value       = aws_kms_key.cloudwatch.arn
}

output "cloudwatch_key_alias" {
  description = "Alias of the CloudWatch KMS key"
  value       = aws_kms_alias.cloudwatch.name
}

# All keys for convenience
output "all_key_ids" {
  description = "Map of all KMS key IDs"
  value = {
    main       = aws_kms_key.main.key_id
    dynamodb   = aws_kms_key.dynamodb.key_id
    cloudwatch = aws_kms_key.cloudwatch.key_id
  }
}

output "all_key_arns" {
  description = "Map of all KMS key ARNs"
  value = {
    main       = aws_kms_key.main.arn
    dynamodb   = aws_kms_key.dynamodb.arn
    cloudwatch = aws_kms_key.cloudwatch.arn
  }
}

# IAM Policy Output
output "kms_access_policy_arn" {
  description = "ARN of the IAM policy for accessing KMS keys"
  value       = aws_iam_policy.kms_access_policy.arn
}
