# SQS Module
# modules/sqs/main.tf

# Dead Letter Queue for failed message processing
resource "aws_sqs_queue" "dlq" {
  count = var.enable_dlq ? 1 : 0

  name                       = "${var.project_name}-${var.queue_name}-dlq-${var.environment}"
  message_retention_seconds  = var.dlq_message_retention_seconds
  visibility_timeout_seconds = var.dlq_visibility_timeout_seconds

  # Server-side encryption
  kms_master_key_id                 = var.kms_key_id
  kms_data_key_reuse_period_seconds = var.kms_data_key_reuse_period_seconds

  tags = merge(var.tags, {
    Name    = "${var.project_name}-${var.queue_name}-dlq-${var.environment}"
    Type    = "DeadLetterQueue"
    Purpose = var.purpose
  })
}

# Main SQS Queue
resource "aws_sqs_queue" "main" {
  name                       = "${var.project_name}-${var.queue_name}-${var.environment}"
  message_retention_seconds  = var.message_retention_seconds
  visibility_timeout_seconds = var.visibility_timeout_seconds
  delay_seconds              = var.delay_seconds
  max_message_size           = var.max_message_size
  receive_wait_time_seconds  = var.receive_wait_time_seconds

  # Dead Letter Queue configuration
  redrive_policy = var.enable_dlq ? jsonencode({
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
    Statement = [
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
          aws_sqs_queue.main.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:ChangeMessageVisibility"
        ]
        Resource = var.enable_dlq ? [
          aws_sqs_queue.dlq[0].arn
        ] : []
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = var.kms_key_id != null ? [
          "arn:aws:kms:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:key/${var.kms_key_id}"
        ] : []
      }
    ]
  })

  tags = var.tags
}

# Get current AWS account info for KMS policy
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}
