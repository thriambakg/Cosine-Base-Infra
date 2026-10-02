# SQS Module Outputs
# modules/sqs/outputs.tf
# When var.create = false, names/ARNs/URLs are synthesized (the queue does not exist).

# Main Queue Outputs
output "queue_name" {
  description = "Name of the SQS queue"
  value       = local.queue_name
}

output "queue_arn" {
  description = "ARN of the SQS queue"
  value       = local.queue_arn
}

output "queue_url" {
  description = "URL of the SQS queue"
  value       = local.queue_url
}

output "queue_id" {
  description = "ID of the SQS queue"
  value       = local.queue_url
}

# Dead Letter Queue Outputs
output "dlq_name" {
  description = "Name of the dead letter queue"
  value       = var.enable_dlq ? local.dlq_name : null
}

output "dlq_arn" {
  description = "ARN of the dead letter queue"
  value       = local.dlq_arn
}

output "dlq_url" {
  description = "URL of the dead letter queue"
  value       = local.dlq_url
}

output "dlq_id" {
  description = "ID of the dead letter queue"
  value       = local.dlq_url
}

# IAM Policy Output
output "sqs_access_policy_arn" {
  description = "ARN of the IAM policy for accessing the SQS queue"
  value       = aws_iam_policy.sqs_access_policy.arn
}

# Queue Attributes
output "queue_attributes" {
  description = "Map of queue attributes (null when the queue is not created)"
  value = var.create ? {
    name                        = aws_sqs_queue.main[0].name
    arn                         = aws_sqs_queue.main[0].arn
    url                         = aws_sqs_queue.main[0].url
    message_retention_seconds   = aws_sqs_queue.main[0].message_retention_seconds
    visibility_timeout_seconds  = aws_sqs_queue.main[0].visibility_timeout_seconds
    delay_seconds               = aws_sqs_queue.main[0].delay_seconds
    max_message_size            = aws_sqs_queue.main[0].max_message_size
    receive_wait_time_seconds   = aws_sqs_queue.main[0].receive_wait_time_seconds
    fifo_queue                  = aws_sqs_queue.main[0].fifo_queue
    content_based_deduplication = aws_sqs_queue.main[0].content_based_deduplication
  } : null
}
