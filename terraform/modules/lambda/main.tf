# Basic Lambda Module
# modules/lambda/main.tf

# Data source for creating zip file from source directory
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = "${var.source_dir}/deployment.zip"
}

# Lookup existing role if provided
data "aws_iam_role" "existing" {
  count = var.existing_role_name != "" ? 1 : 0
  name  = var.existing_role_name
}

# IAM Role for Lambda execution (only when not using existing role)
resource "aws_iam_role" "lambda_execution_role" {
  count = var.existing_role_name == "" ? 1 : 0

  name = "${var.function_name}-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

# Basic execution policy attachment (only when creating role)
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  count      = var.existing_role_name == "" ? 1 : 0
  role       = aws_iam_role.lambda_execution_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Attach additional IAM policies (only when creating role)
resource "aws_iam_role_policy_attachment" "additional_policies" {
  count      = var.existing_role_name == "" ? length(var.additional_policy_arns) : 0
  role       = aws_iam_role.lambda_execution_role[0].name
  policy_arn = var.additional_policy_arns[count.index]
}

# Resolved role ARN
locals {
  lambda_role_arn = var.existing_role_name != "" ? data.aws_iam_role.existing[0].arn : aws_iam_role.lambda_execution_role[0].arn
}

# Lambda Function
resource "aws_lambda_function" "function" {
  function_name = var.function_name
  description   = var.description
  role          = local.lambda_role_arn
  handler       = var.handler
  runtime       = var.runtime
  timeout       = var.timeout
  memory_size   = var.memory_size

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = var.environment_variables
  }

  tags = var.tags
}


