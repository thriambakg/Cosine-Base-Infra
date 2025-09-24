# SQS Module Outputs
# modules/sqs/outputs.tf

# Main Queue Outputs
output "queue_name" {
  description = "Name of the SQS queue"
  value       = aws_sqs_queue.main.name
}

output "queue_arn" {
  description = "ARN of the SQS queue"
  value       = aws_sqs_queue.main.arn
}

output "queue_url" {
  description = "URL of the SQS queue"
  value       = aws_sqs_queue.main.url
}

output "queue_id" {
  description = "ID of the SQS queue"
  value       = aws_sqs_queue.main.id
}

# Dead Letter Queue Outputs
output "dlq_name" {
  description = "Name of the dead letter queue"
  value       = var.enable_dlq ? aws_sqs_queue.dlq[0].name : null
}

output "dlq_arn" {
  description = "ARN of the dead letter queue"
  value       = var.enable_dlq ? aws_sqs_queue.dlq[0].arn : null
}

output "dlq_url" {
  description = "URL of the dead letter queue"
  value       = var.enable_dlq ? aws_sqs_queue.dlq[0].url : null
}

output "dlq_id" {
  description = "ID of the dead letter queue"
  value       = var.enable_dlq ? aws_sqs_queue.dlq[0].id : null
}

# IAM Policy Output
output "sqs_access_policy_arn" {
  description = "ARN of the IAM policy for accessing the SQS queue"
  value       = aws_iam_policy.sqs_access_policy.arn
}

# Queue Attributes
output "queue_attributes" {
  description = "Map of queue attributes"
  value = {
    name                        = aws_sqs_queue.main.name
    arn                         = aws_sqs_queue.main.arn
    url                         = aws_sqs_queue.main.url
    message_retention_seconds   = aws_sqs_queue.main.message_retention_seconds
    visibility_timeout_seconds  = aws_sqs_queue.main.visibility_timeout_seconds
    delay_seconds               = aws_sqs_queue.main.delay_seconds
    max_message_size            = aws_sqs_queue.main.max_message_size
    receive_wait_time_seconds   = aws_sqs_queue.main.receive_wait_time_seconds
    fifo_queue                  = aws_sqs_queue.main.fifo_queue
    content_based_deduplication = aws_sqs_queue.main.content_based_deduplication
  }
}
