# DynamoDB Table Module Outputs
# modules/dynamodb-table/outputs.tf

output "table_name" {
  description = "Name of the DynamoDB table"
  value       = aws_dynamodb_table.this.name
}

output "table_arn" {
  description = "ARN of the DynamoDB table"
  value       = aws_dynamodb_table.this.arn
}

output "table_id" {
  description = "ID of the DynamoDB table"
  value       = aws_dynamodb_table.this.id
}

output "stream_arn" {
  description = "ARN of the DynamoDB stream"
  value       = aws_dynamodb_table.this.stream_arn
}

output "table_policy_arn" {
  description = "ARN of the IAM policy for accessing the table"
  value       = aws_iam_policy.table_policy.arn
}

