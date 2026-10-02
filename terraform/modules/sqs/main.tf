# SQS Module
# modules/sqs/main.tf

locals {
  create_dlq = var.create && var.enable_dlq

  queue_name = var.fifo_queue ? "${var.project_name}-${var.queue_name}-${var.environment}.fifo" : "${var.project_name}-${var.queue_name}-${var.environment}"
  dlq_name   = var.fifo_queue ? "${var.project_name}-${var.queue_name}-dlq-${var.environment}.fifo" : "${var.project_name}-${var.queue_name}-dlq-${var.environment}"

  arn_prefix = "arn:aws:sqs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}"
  url_prefix = "https://sqs.${data.aws_region.current.name}.amazonaws.com/${data.aws_caller_identity.current.account_id}"

  queue_arn = var.create ? aws_sqs_queue.main[0].arn : "${local.arn_prefix}:${local.queue_name}"
  queue_url = var.create ? aws_sqs_queue.main[0].url : "${local.url_prefix}/${local.queue_name}"
  dlq_arn   = var.enable_dlq ? (var.create ? aws_sqs_queue.dlq[0].arn : "${local.arn_prefix}:${local.dlq_name}") : null
  dlq_url   = var.enable_dlq ? (var.create ? aws_sqs_queue.dlq[0].url : "${local.url_prefix}/${local.dlq_name}") : null
}

# Dead Letter Queue for failed message processing
resource "aws_sqs_queue" "dlq" {
  count = local.create_dlq ? 1 : 0

  name                        = local.dlq_name
  fifo_queue                  = var.fifo_queue
  content_based_deduplication = var.fifo_queue ? var.content_based_deduplication : false
  message_retention_seconds   = var.dlq_message_retention_seconds
  visibility_timeout_seconds  = var.dlq_visibility_timeout_seconds

  # Server-side encryption
  kms_master_key_id                 = var.kms_key_id
  kms_data_key_reuse_period_seconds = var.kms_data_key_reuse_period_seconds

  tags = merge(var.tags, {
    Name    = "${var.project_name}-${var.queue_name}-dlq-${var.environment}"
    Type    = "DeadLetterQueue"
    Purpose = var.purpose
  })
}

# Redrive allow policy for DLQ (separate resource to avoid cycle)
# This enables the "Start DLQ Redrive" feature in the AWS console
resource "aws_sqs_queue_redrive_allow_policy" "dlq_redrive_allow" {
  count = local.create_dlq ? 1 : 0

  queue_url = aws_sqs_queue.dlq[0].id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.main[0].arn]
  })

  depends_on = [
    aws_sqs_queue.dlq,
    aws_sqs_queue.main
  ]
}

# Main SQS Queue
resource "aws_sqs_queue" "main" {
  count = var.create ? 1 : 0

  name                       = local.queue_name
  message_retention_seconds  = var.message_retention_seconds
  visibility_timeout_seconds = var.visibility_timeout_seconds
  delay_seconds              = var.delay_seconds
  max_message_size           = var.max_message_size
  receive_wait_time_seconds  = var.receive_wait_time_seconds

  # Dead Letter Queue configuration
  redrive_policy = local.create_dlq ? jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq[0].arn
    maxReceiveCount     = var.max_receive_count
  }) : null

  # Server-side encryption
  kms_master_key_id                 = var.kms_key_id
  kms_data_key_reuse_period_seconds = var.kms_data_key_reuse_period_seconds

  # FIFO queue configuration
  fifo_queue                  = var.fifo_queue
  content_based_deduplication = var.content_based_deduplication


  tags = merge(var.tags, {
    Name    = "${var.project_name}-${var.queue_name}-${var.environment}"
    Type    = "SQSQueue"
    Purpose = var.purpose
  })
}

# IAM Policy for Lambda functions to access SQS
resource "aws_iam_policy" "sqs_access_policy" {
  name        = "${var.project_name}-${var.queue_name}-sqs-policy-${var.environment}"
  description = "Policy for Lambda functions to access ${var.queue_name} SQS queue"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [
        {
          Effect = "Allow"
          Action = [
            "sqs:SendMessage",
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes",
            "sqs:ChangeMessageVisibility"
          ]
          Resource = [
            local.queue_arn
          ]
        }
      ],
      var.enable_dlq ? [
        {
          Effect = "Allow"
          Action = [
            "sqs:SendMessage",
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes",
            "sqs:ChangeMessageVisibility"
          ]
          Resource = [
            local.dlq_arn
          ]
        }
      ] : [],
      var.kms_key_id != null ? [
        {
          Effect = "Allow"
          Action = [
            "kms:Decrypt",
            "kms:GenerateDataKey",
            "kms:DescribeKey"
          ]
          Resource = [
            "arn:aws:kms:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:key/${var.kms_key_id}"
          ]
        }
      ] : []
    )
  })

  tags = var.tags
}

# Get current AWS account info for KMS policy
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
