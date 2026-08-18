# DynamoDB Table Module Outputs
# modules/dynamodb-table/outputs.tf

output "table_name" {
  description = "Name of the DynamoDB table"
  value       = var.create ? aws_dynamodb_table.this[0].name : local.table_name
}

output "table_arn" {
  description = "ARN of the DynamoDB table"
  value       = var.create ? aws_dynamodb_table.this[0].arn : "arn:aws:dynamodb:${data.aws_region.current.id}:${data.aws_caller_identity.current.account_id}:table/${local.table_name}"
}

output "table_id" {
  description = "ID of the DynamoDB table"
  value       = var.create ? aws_dynamodb_table.this[0].id : local.table_name
}

output "stream_arn" {
  description = "ARN of the DynamoDB stream"
  value       = var.create ? aws_dynamodb_table.this[0].stream_arn : null
}
