moved {
  from = aws_kms_key.main
  to   = aws_kms_key.main[0]
}

moved {
  from = aws_kms_alias.main
  to   = aws_kms_alias.main[0]
}

moved {
  from = aws_kms_key.dynamodb
  to   = aws_kms_key.dynamodb[0]
}

moved {
  from = aws_kms_alias.dynamodb
  to   = aws_kms_alias.dynamodb[0]
}

moved {
  from = aws_kms_key.cloudwatch
  to   = aws_kms_key.cloudwatch[0]
}

moved {
  from = aws_kms_alias.cloudwatch
  to   = aws_kms_alias.cloudwatch[0]
}
