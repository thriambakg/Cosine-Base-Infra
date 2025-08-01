# Lambda Module - Security Compliant by Default
# modules/lambda/main.tf

# Data sources
data "aws_caller_identity" "current" {}
data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# CloudWatch Log Group for Lambda function (with compliant retention)
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.cloudwatch_kms_key_arn

  tags = merge(var.tags, {
    Name    = "${var.function_name}-logs"
    Type    = "LogGroup"
    Purpose = "LambdaFunction"
  })
}

# IAM role for Lambda function
resource "aws_iam_role" "lambda" {
  name               = "${var.function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = merge(var.tags, {
    Name    = "${var.function_name}-role"
    Type    = "IAMRole"
    Purpose = "LambdaExecution"
  })
}

# Basic Lambda execution policy
resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# VPC execution policy (if VPC is configured)
resource "aws_iam_role_policy_attachment" "lambda_vpc" {
  count      = var.vpc_config != null ? 1 : 0
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# Custom policy for additional permissions
resource "aws_iam_role_policy" "lambda_custom" {
  count = var.custom_policy_json != "" ? 1 : 0
  name  = "${var.function_name}-custom-policy"
  role  = aws_iam_role.lambda.id

  policy = var.custom_policy_json
}

# Lambda function
resource "aws_lambda_function" "this" {
  function_name = var.function_name
  role          = aws_iam_role.lambda.arn
  handler       = var.handler
  runtime       = var.runtime
  timeout       = var.timeout
  memory_size   = var.memory_size
  publish       = var.publish

  # Source code
  filename         = var.filename
  source_code_hash = var.source_code_hash

  # Layers
  layers = var.layers

  # Environment variables (encrypted) - CKV_AWS_173
  dynamic "environment" {
    for_each = length(var.environment_variables) > 0 ? [1] : []
    content {
      variables = var.environment_variables
    }
  }

  # KMS key for environment variable encryption - CKV_AWS_173 (mandatory when env vars exist)
  kms_key_arn = var.lambda_kms_key_arn

  # Code signing configuration - CKV_AWS_272
  code_signing_config_arn = var.code_signing_config_arn

  # VPC configuration
  dynamic "vpc_config" {
    for_each = var.vpc_config != null ? [var.vpc_config] : []
    content {
      subnet_ids         = vpc_config.value.subnet_ids
      security_group_ids = vpc_config.value.security_group_ids
    }
  }

  # Dead letter queue configuration
  dynamic "dead_letter_config" {
    for_each = var.dead_letter_queue_arn != "" ? [1] : []
    content {
      target_arn = var.dead_letter_queue_arn
    }
  }

  # Tracing configuration
  tracing_config {
    mode = var.tracing_mode
  }

  # Reserved concurrent executions
  reserved_concurrent_executions = var.reserved_concurrent_executions

  tags = merge(var.tags, {
    Name    = var.function_name
    Type    = "LambdaFunction"
    Purpose = var.purpose
  })

  depends_on = [
    aws_iam_role_policy_attachment.lambda_basic,
    aws_cloudwatch_log_group.lambda
  ]

  # CKV_AWS_173: Lifecycle rule to enforce KMS encryption when environment variables exist
  lifecycle {
    precondition {
      condition = (
        length(var.environment_variables) == 0 ||
        var.lambda_kms_key_arn != null
      )
      error_message = "Lambda functions with environment variables must use KMS encryption (CKV_AWS_173). Provide lambda_kms_key_arn when environment_variables are defined."
    }
  }
} # Lambda function alias (for versioning)
resource "aws_lambda_alias" "this" {
  count            = var.create_alias ? 1 : 0
  name             = var.alias_name
  description      = "Alias for ${var.function_name} function"
  function_name    = aws_lambda_function.this.function_name
  function_version = var.publish ? aws_lambda_function.this.version : "$LATEST"
}

# CloudWatch metric filters for monitoring
resource "aws_cloudwatch_metric_filter" "lambda_errors" {
  count          = var.enable_error_monitoring ? 1 : 0
  name           = "${var.function_name}-errors"
  log_group_name = aws_cloudwatch_log_group.lambda.name
  pattern        = "ERROR"

  metric_transformation {
    name      = "${var.function_name}-ErrorCount"
    namespace = "Lambda/CustomMetrics"
    value     = "1"
  }
}

# CloudWatch alarm for Lambda errors
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  count               = var.enable_error_monitoring && var.alarm_sns_topic_arn != "" ? 1 : 0
  alarm_name          = "${var.function_name}-error-alarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "${var.function_name}-ErrorCount"
  namespace           = "Lambda/CustomMetrics"
  period              = "300"
  statistic           = "Sum"
  threshold           = var.error_threshold
  alarm_description   = "This metric monitors lambda errors for ${var.function_name}"
  alarm_actions       = [var.alarm_sns_topic_arn]

  tags = var.tags
}
