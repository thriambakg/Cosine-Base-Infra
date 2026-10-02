# Preserve existing resources after introducing count = var.create ? 1 : 0
moved {
  from = aws_dynamodb_table.this
  to   = aws_dynamodb_table.this[0]
}
