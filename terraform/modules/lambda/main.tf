# Basic Lambda Module
# modules/lambda/main.tf

# Data source for creating zip file from source directory
# IMPROVED: Better change detection and path handling
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = "${var.source_dir}/deployment.zip"

  # Exclude files that shouldn't trigger rebuilds
  excludes = [
    "deployment.zip", # Exclude the output file itself
    "__pycache__/**", # Exclude Python cache files
    "*.pyc",          # Exclude compiled Python files
    ".git/**",        # Exclude git files if present
    ".DS_Store",      # Exclude macOS files
    "Thumbs.db"       # Exclude Windows files
  ]
}

# IAM Role for Lambda execution
# Use name_prefix to avoid 64-character limit (AWS appends ~26 char random suffix)
# Truncate to 38 chars max (AWS limit for name_prefix)
locals {
  # Truncate function name + "-role-" to fit within 38-char limit
  role_prefix      = "${var.function_name}-role-"
  role_name_prefix = length(local.role_prefix) > 38 ? "${substr(local.role_prefix, 0, 38)}" : local.role_prefix
}

resource "aws_iam_role" "lambda_execution_role" {
  name_prefix = local.role_name_prefix

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

# Basic execution policy attachment
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Attach additional IAM policies
resource "aws_iam_role_policy_attachment" "additional_policies" {
  count      = length(var.additional_policy_arns)
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = var.additional_policy_arns[count.index]
}

# Lambda Function
resource "aws_lambda_function" "function" {
  function_name = var.function_name
  description   = var.description
  role          = aws_iam_role.lambda_execution_role.arn
  handler       = var.handler
  runtime       = var.runtime
  timeout       = var.timeout
  memory_size   = var.memory_size

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  environment {
    variables = var.environment_variables
  }

  layers = var.layers

  # Only set reserved_concurrent_executions if provided (not null)
  reserved_concurrent_executions = var.reserved_concurrent_executions != null ? var.reserved_concurrent_executions : null

  tags = var.tags
}