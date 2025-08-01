# SNS Module Outputs
# modules/sns/outputs.tf

output "topic_arn" {
  description = "ARN of the SNS topic"
  value       = aws_sns_topic.this.arn
}

output "topic_name" {
  description = "Name of the SNS topic"
  value       = aws_sns_topic.this.name
}

output "topic_id" {
  description = "ID of the SNS topic"
  value       = aws_sns_topic.this.id
}

output "topic_owner" {
  description = "AWS account ID of the SNS topic owner"
  value       = aws_sns_topic.this.owner
}

output "subscription_arns" {
  description = "Map of subscription ARNs by type"
  value = {
    email  = { for k, v in aws_sns_topic_subscription.email : k => v.arn }
    sms    = { for k, v in aws_sns_topic_subscription.sms : k => v.arn }
    sqs    = { for k, v in aws_sns_topic_subscription.sqs : k => v.arn }
    lambda = { for k, v in aws_sns_topic_subscription.lambda : k => v.arn }
    http   = { for k, v in aws_sns_topic_subscription.http : k => v.arn }
    https  = { for k, v in aws_sns_topic_subscription.https : k => v.arn }
  }
}

output "topic_policy" {
  description = "The JSON policy document of the SNS topic"
  value       = aws_sns_topic_policy.this.policy
}
