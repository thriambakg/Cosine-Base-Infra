# Generic Glue Job Module
# Creates an AWS Glue job with configurable settings

# Glue Job Script Location
resource "aws_glue_job" "this" {
  name     = var.job_name
  role_arn = aws_iam_role.glue_role.arn

  # Script location
  command {
    name            = "glueetl"
    script_location = var.script_location
    python_version  = var.python_version
  }

  # Job configuration
  glue_version = var.glue_version
  max_retries  = var.max_retries
  timeout      = var.timeout

  # Execution property for concurrent runs
  execution_property {
    max_concurrent_runs = var.concurrent_executions
  }

  # Worker configuration
  # Note: worker_type/number_of_workers and max_capacity are mutually exclusive
  # Use worker_type if provided, otherwise use max_capacity
  number_of_workers = var.worker_type != null ? var.number_of_workers : null
  worker_type       = var.worker_type != null ? var.worker_type : null
  max_capacity      = var.worker_type == null ? var.max_capacity : null

  # Default arguments (can be overridden at runtime)
  default_arguments = merge(
    {
      "--enable-spark-ui"                  = "true"
      "--spark-event-logs-path"            = "s3://${var.spark_logs_bucket}/glue-logs/"
      "--enable-continuous-cloudwatch-log" = "true"
      "--enable-metrics"                   = "true"
      "--job-bookmark-option"              = var.job_bookmark_option
      "--TempDir"                          = "s3://${var.temp_bucket}/glue-temp/"
      "--enable-glue-datacatalog"          = "false"
    },
    var.default_arguments
  )

  # Tags
  tags = var.tags
}

# IAM Role for Glue Job
# Use name instead of name_prefix to avoid length issues with long job names
# AWS role names max 64 chars, job names can be long, so we truncate if needed
resource "aws_iam_role" "glue_role" {
  name = length("${var.job_name}-role") > 64 ? substr("${var.job_name}-role", 0, 64) : "${var.job_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

# Basic Glue service role policy
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# CloudWatch Logs permissions
resource "aws_iam_role_policy" "cloudwatch_logs" {
  name = length("${var.job_name}-cloudwatch") > 128 ? substr("${var.job_name}-cloudwatch", 0, 128) : "${var.job_name}-cloudwatch"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

# S3 permissions (for reading/writing data)
# Supports multiple buckets via additional_s3_bucket_arns
resource "aws_iam_role_policy" "s3_access" {
  name = length("${var.job_name}-s3") > 128 ? substr("${var.job_name}-s3", 0, 128) : "${var.job_name}-s3"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = concat(
          [
            "${var.s3_bucket_arn}/*",
            var.s3_bucket_arn
          ],
          var.additional_s3_bucket_arns != null ? [
            for bucket_arn in var.additional_s3_bucket_arns : "${bucket_arn}/*"
          ] : [],
          var.additional_s3_bucket_arns != null ? var.additional_s3_bucket_arns : []
        )
      }
    ]
  })
}

# DynamoDB permissions (if needed)
resource "aws_iam_role_policy" "dynamodb_access" {
  count = var.dynamodb_table_arn != null ? 1 : 0

  name = length("${var.job_name}-dynamodb") > 128 ? substr("${var.job_name}-dynamodb", 0, 128) : "${var.job_name}-dynamodb"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = [
          var.dynamodb_table_arn,
          "${var.dynamodb_table_arn}/index/*"
        ]
      }
    ]
  })
}

# KMS permissions (if needed for encrypted S3/DynamoDB)
resource "aws_iam_role_policy" "kms_access" {
  count = var.kms_key_arn != null ? 1 : 0

  name = length("${var.job_name}-kms") > 128 ? substr("${var.job_name}-kms", 0, 128) : "${var.job_name}-kms"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey"
        ]
        Resource = var.kms_key_arn
      }
    ]
  })
}

# Additional custom policies (for specific use cases)
resource "aws_iam_role_policy" "custom" {
  for_each = var.additional_policies

  name = length("${var.job_name}-custom-${each.key}") > 128 ? substr("${var.job_name}-custom-${each.key}", 0, 128) : "${var.job_name}-custom-${each.key}"
  role = aws_iam_role.glue_role.id

  policy = each.value
}

# Support for attaching additional IAM policy ARNs (for shared policies)
resource "aws_iam_role_policy_attachment" "additional" {
  for_each = toset(var.additional_policy_arns)

  role       = aws_iam_role.glue_role.name
  policy_arn = each.value
}

