# Glue Job Module
# Generic, reusable module for AWS Glue ETL jobs

# ==============================================================================
# Data Sources
# ==============================================================================

# Get the S3 bucket for script storage
data "aws_s3_bucket" "script_bucket" {
  bucket = var.script_s3_bucket_id != null ? var.script_s3_bucket_id : (length(var.s3_bucket_ids) > 0 ? var.s3_bucket_ids[0] : null)
}

# Get S3 buckets for IAM policies
data "aws_s3_bucket" "job_buckets" {
  for_each = toset(var.s3_bucket_ids)
  bucket   = each.value
}

# ==============================================================================
# Source Code Upload (if source_code_dir is provided)
# ==============================================================================

# Archive the source code directory
data "archive_file" "glue_script_zip" {
  count       = var.source_code_dir != null ? 1 : 0
  type        = "zip"
  source_dir  = var.source_code_dir
  output_path = "${var.source_code_dir}/glue-deployment.zip"

  excludes = [
    "glue-deployment.zip",
    "__pycache__/**",
    "*.pyc",
    ".git/**",
    ".DS_Store",
    "Thumbs.db",
    "*.zip"
  ]
}

# Upload script to S3 (if source_code_dir is provided)
# Note: If script_location is already an S3 path, this won't run
resource "aws_s3_object" "glue_script" {
  count  = var.source_code_dir != null ? 1 : 0
  bucket = data.aws_s3_bucket.script_bucket.id
  key    = "${var.script_s3_prefix}/${var.job_name}/main.py"

  # Try main.py first, then script.py, then any .py file
  source = fileexists("${var.source_code_dir}/main.py") ? "${var.source_code_dir}/main.py" : (
    fileexists("${var.source_code_dir}/script.py") ? "${var.source_code_dir}/script.py" : (
      length(try(fileset(var.source_code_dir, "*.py"), [])) > 0 ? "${var.source_code_dir}/${sort(try(fileset(var.source_code_dir, "*.py"), []))[0]}" : null
    )
  )

  etag = var.source_code_dir != null && fileexists("${var.source_code_dir}/main.py") ? filemd5("${var.source_code_dir}/main.py") : (
    var.source_code_dir != null && fileexists("${var.source_code_dir}/script.py") ? filemd5("${var.source_code_dir}/script.py") : null
  )

  tags = var.tags
}

# Upload additional Python files from source directory (utilities, helpers, etc.)
resource "aws_s3_object" "glue_script_files" {
  for_each = var.source_code_dir != null ? toset([
    for f in try(fileset(var.source_code_dir, "*.py"), []) :
    f if f != "main.py" && f != "script.py"
  ]) : toset([])

  bucket = data.aws_s3_bucket.script_bucket.id
  key    = "${var.script_s3_prefix}/${var.job_name}/${each.value}"
  source = "${var.source_code_dir}/${each.value}"

  etag = filemd5("${var.source_code_dir}/${each.value}")

  tags = var.tags
}

# ==============================================================================
# IAM Role for Glue Job
# ==============================================================================

resource "aws_iam_role" "glue_job_role" {
  name_prefix = "${replace(var.job_name, "_", "-")}-role-"

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

# Attach AWS managed policy for Glue service role
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# S3 Access Policy
resource "aws_iam_role_policy" "s3_access" {
  name = "${var.job_name}-s3-access"
  role = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      # Read access to script bucket
      [
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:GetObjectVersion"
          ]
          Resource = [
            "${data.aws_s3_bucket.script_bucket.arn}/*"
          ]
        },
        {
          Effect = "Allow"
          Action = [
            "s3:ListBucket"
          ]
          Resource = [
            data.aws_s3_bucket.script_bucket.arn
          ]
        }
      ],
      # Read/Write access to job buckets
      length(var.s3_bucket_ids) > 0 ? [
        {
          Effect = "Allow"
          Action = [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:GetObjectVersion"
          ]
          Resource = [
            for bucket_id in var.s3_bucket_ids :
            "${data.aws_s3_bucket.job_buckets[bucket_id].arn}/*"
          ]
        },
        {
          Effect = "Allow"
          Action = [
            "s3:ListBucket"
          ]
          Resource = [
            for bucket_id in var.s3_bucket_ids :
            data.aws_s3_bucket.job_buckets[bucket_id].arn
          ]
        }
      ] : []
    )
  })
}

# DynamoDB Access Policy
resource "aws_iam_role_policy" "dynamodb_access" {
  count = length(var.dynamodb_table_names) > 0 ? 1 : 0
  name  = "${var.job_name}-dynamodb-access"
  role  = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem",
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = [
          for table_name in var.dynamodb_table_names :
          "arn:aws:dynamodb:*:*:table/${table_name}"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:DescribeTable"
        ]
        Resource = [
          for table_name in var.dynamodb_table_names :
          "arn:aws:dynamodb:*:*:table/${table_name}"
        ]
      }
    ]
  })
}

# KMS Access Policy (for encryption)
resource "aws_iam_role_policy" "kms_access" {
  count = length(var.kms_key_arns) > 0 ? 1 : 0
  name  = "${var.job_name}-kms-access"
  role  = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:Encrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = var.kms_key_arns
      }
    ]
  })
}

# Additional IAM Policies
resource "aws_iam_role_policy" "additional_policies" {
  count = length(var.additional_policy_statements) > 0 ? 1 : 0
  name  = "${var.job_name}-additional-policies"
  role  = aws_iam_role.glue_job_role.id

  policy = jsonencode({
    Version   = "2012-10-17"
    Statement = var.additional_policy_statements
  })
}

# CloudWatch Logs Policy
resource "aws_iam_role_policy" "cloudwatch_logs" {
  count = var.enable_cloudwatch_logs ? 1 : 0
  name  = "${var.job_name}-cloudwatch-logs"
  role  = aws_iam_role.glue_job_role.id

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
        Resource = [
          "arn:aws:logs:*:*:log-group:/aws-glue/jobs/*",
          "arn:aws:logs:*:*:log-group:/aws-glue/jobs/${var.job_name}*"
        ]
      }
    ]
  })
}

# ==============================================================================
# CloudWatch Log Group
# ==============================================================================

resource "aws_cloudwatch_log_group" "glue_job_logs" {
  count             = var.enable_cloudwatch_logs ? 1 : 0
  name              = var.log_group_name != null ? var.log_group_name : "/aws-glue/jobs/${var.job_name}"
  retention_in_days = var.log_retention_days

  tags = var.tags
}

# ==============================================================================
# Glue Job
# ==============================================================================

locals {
  # Determine script location - use provided script_location or construct from uploaded script
  resolved_script_location = var.script_location != null && startswith(var.script_location, "s3://") ? var.script_location : (
    var.source_code_dir != null ? "s3://${data.aws_s3_bucket.script_bucket.id}/${var.script_s3_prefix}/${var.job_name}/main.py" : (
      var.script_location != null ? var.script_location : "s3://${data.aws_s3_bucket.script_bucket.id}/${var.script_s3_prefix}/${var.job_name}/main.py"
    )
  )

  # Determine worker configuration
  worker_config = var.max_capacity != null ? {
    max_capacity = var.max_capacity
    } : (var.worker_type != null ? {
      worker_type       = var.worker_type
      number_of_workers = var.number_of_workers
      } : {
      max_capacity = 2 # Default fallback
  })

  # Merge default arguments with environment variables
  job_arguments = merge(
    var.default_arguments,
    {
      "--enable-glue-datacatalog" = "true"
      "--enable-metrics"          = "true"
      "--enable-spark-ui"         = "true"
      "--spark-event-logs-path"   = "s3://${data.aws_s3_bucket.script_bucket.id}/glue-logs/${var.job_name}/"
    },
    length(var.environment_variables) > 0 ? {
      for k, v in var.environment_variables : "--env-var-${k}" => v
    } : {}
  )
}

resource "aws_glue_job" "job" {
  name         = var.job_name
  description  = var.description
  role_arn     = aws_iam_role.glue_job_role.arn
  glue_version = var.glue_version

  command {
    name            = "glueetl"
    script_location = local.resolved_script_location
    python_version  = var.python_version
  }

  default_arguments = local.job_arguments

  max_retries = var.max_retries
  timeout     = var.timeout

  # Worker configuration
  max_capacity      = try(local.worker_config.max_capacity, null)
  worker_type       = try(local.worker_config.worker_type, null)
  number_of_workers = try(local.worker_config.number_of_workers, null)

  tags = var.tags

  depends_on = [
    aws_cloudwatch_log_group.glue_job_logs,
    aws_s3_object.glue_script
  ]
}

