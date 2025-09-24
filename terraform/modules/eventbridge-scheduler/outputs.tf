# EventBridge Scheduler Module Outputs
# modules/eventbridge-scheduler/outputs.tf

output "rule_name" {
  description = "Name of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.scheduler_rule.name
}

output "rule_arn" {
  description = "ARN of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.scheduler_rule.arn
}

output "rule_id" {
  description = "ID of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.scheduler_rule.id
}

output "target_id" {
  description = "ID of the EventBridge target"
  value       = var.enabled ? aws_cloudwatch_event_target.scheduler_target[0].target_id : null
}

output "lambda_permission_id" {
  description = "ID of the Lambda permission (if applicable)"
  value       = var.enabled && var.target_type == "lambda" ? aws_lambda_permission.allow_eventbridge[0].id : null
}
