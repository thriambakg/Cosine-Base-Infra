# KMS Module Outputs
# modules/kms/outputs.tf
# All key outputs are null when var.create = false

# Main KMS Key
output "main_key_id" {
  description = "ID of the main KMS key"
  value       = var.create ? aws_kms_key.main[0].key_id : null
}

output "main_key_arn" {
  description = "ARN of the main KMS key"
  value       = var.create ? aws_kms_key.main[0].arn : null
}

output "main_key_alias" {
  description = "Alias of the main KMS key"
  value       = var.create ? aws_kms_alias.main[0].name : null
}

# DynamoDB KMS Key
output "dynamodb_key_id" {
  description = "ID of the DynamoDB KMS key"
  value       = var.create ? aws_kms_key.dynamodb[0].key_id : null
}

output "dynamodb_key_arn" {
  description = "ARN of the DynamoDB KMS key"
  value       = var.create ? aws_kms_key.dynamodb[0].arn : null
}

output "dynamodb_key_alias" {
  description = "Alias of the DynamoDB KMS key"
  value       = var.create ? aws_kms_alias.dynamodb[0].name : null
}

# CloudWatch KMS Key
output "cloudwatch_key_id" {
  description = "ID of the CloudWatch KMS key"
  value       = var.create ? aws_kms_key.cloudwatch[0].key_id : null
}

output "cloudwatch_key_arn" {
  description = "ARN of the CloudWatch KMS key"
  value       = var.create ? aws_kms_key.cloudwatch[0].arn : null
}

output "cloudwatch_key_alias" {
  description = "Alias of the CloudWatch KMS key"
  value       = var.create ? aws_kms_alias.cloudwatch[0].name : null
}

# All keys for convenience
output "all_key_ids" {
  description = "Map of all KMS key IDs"
  value = var.create ? {
    main       = aws_kms_key.main[0].key_id
    dynamodb   = aws_kms_key.dynamodb[0].key_id
    cloudwatch = aws_kms_key.cloudwatch[0].key_id
  } : {}
}

output "all_key_arns" {
  description = "Map of all KMS key ARNs"
  value = var.create ? {
    main       = aws_kms_key.main[0].arn
    dynamodb   = aws_kms_key.dynamodb[0].arn
    cloudwatch = aws_kms_key.cloudwatch[0].arn
  } : {}
}

# IAM Policy Output
output "kms_access_policy_arn" {
  description = "ARN of the IAM policy for accessing KMS keys"
  value       = aws_iam_policy.kms_access_policy.arn
}
