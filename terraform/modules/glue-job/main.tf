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

  # Worker configuration
  number_of_workers = var.number_of_workers
  worker_type       = var.worker_type
  max_capacity      = var.max_capacity

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
resource "aws_iam_role" "glue_role" {
  name_prefix = "${var.job_name}-glue-role-"

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
  name_prefix = "${var.job_name}-cloudwatch-"
  role        = aws_iam_role.glue_role.id

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
resource "aws_iam_role_policy" "s3_access" {
  name_prefix = "${var.job_name}-s3-"
  role        = aws_iam_role.glue_role.id

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
        Resource = [
          "${var.s3_bucket_arn}/*",
          var.s3_bucket_arn
        ]
      }
    ]
  })
}

# DynamoDB permissions (if needed)
resource "aws_iam_role_policy" "dynamodb_access" {
  count = var.dynamodb_table_arn != null ? 1 : 0

  name_prefix = "${var.job_name}-dynamodb-"
  role        = aws_iam_role.glue_role.id

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

  name_prefix = "${var.job_name}-kms-"
  role        = aws_iam_role.glue_role.id

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

  name_prefix = "${var.job_name}-custom-${each.key}-"
  role        = aws_iam_role.glue_role.id

  policy = each.value
}

