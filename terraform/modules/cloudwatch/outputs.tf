# CloudWatch Module Outputs
# modules/cloudwatch/outputs.tf

# Log Groups
output "security_events_log_group_name" {
  description = "Name of the security events log group"
  value       = aws_cloudwatch_log_group.security_events.name
}

output "security_events_log_group_arn" {
  description = "ARN of the security events log group"
  value       = aws_cloudwatch_log_group.security_events.arn
}

output "auth_events_log_group_name" {
  description = "Name of the authentication events log group"
  value       = aws_cloudwatch_log_group.auth_events.name
}

output "auth_events_log_group_arn" {
  description = "ARN of the authentication events log group"
  value       = aws_cloudwatch_log_group.auth_events.arn
}

output "application_log_group_name" {
  description = "Name of the application log group"
  value       = aws_cloudwatch_log_group.application.name
}

output "application_log_group_arn" {
  description = "ARN of the application log group"
  value       = aws_cloudwatch_log_group.application.arn
}

output "lambda_log_group_name" {
  description = "Name of the Lambda log group"
  value       = aws_cloudwatch_log_group.lambda.name
}

output "lambda_log_group_arn" {
  description = "ARN of the Lambda log group"
  value       = aws_cloudwatch_log_group.lambda.arn
}

output "api_gateway_log_group_name" {
  description = "Name of the API Gateway log group"
  value       = aws_cloudwatch_log_group.api_gateway.name
}

output "api_gateway_log_group_arn" {
  description = "ARN of the API Gateway log group"
  value       = aws_cloudwatch_log_group.api_gateway.arn
}

# Dashboard
output "dashboard_name" {
  description = "Name of the CloudWatch dashboard"
  value       = aws_cloudwatch_dashboard.main.dashboard_name
}

output "dashboard_url" {
  description = "URL of the CloudWatch dashboard"
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.main.dashboard_name}"
}

# Alarms
output "failed_login_alarm_name" {
  description = "Name of the failed login alarm"
  value       = aws_cloudwatch_metric_alarm.failed_login_alarm.alarm_name
}

output "failed_login_alarm_arn" {
  description = "ARN of the failed login alarm"
  value       = aws_cloudwatch_metric_alarm.failed_login_alarm.arn
}

output "suspicious_activity_alarm_name" {
  description = "Name of the suspicious activity alarm"
  value       = aws_cloudwatch_metric_alarm.suspicious_activity_alarm.alarm_name
}

output "suspicious_activity_alarm_arn" {
  description = "ARN of the suspicious activity alarm"
  value       = aws_cloudwatch_metric_alarm.suspicious_activity_alarm.arn
}

# All log groups for convenience
output "all_log_groups" {
  description = "Map of all log group names and ARNs"
  value = {
    security_events = {
      name = aws_cloudwatch_log_group.security_events.name
      arn  = aws_cloudwatch_log_group.security_events.arn
    }
    auth_events = {
      name = aws_cloudwatch_log_group.auth_events.name
      arn  = aws_cloudwatch_log_group.auth_events.arn
    }
    application = {
      name = aws_cloudwatch_log_group.application.name
      arn  = aws_cloudwatch_log_group.application.arn
    }
    lambda = {
      name = aws_cloudwatch_log_group.lambda.name
      arn  = aws_cloudwatch_log_group.lambda.arn
    }
    api_gateway = {
      name = aws_cloudwatch_log_group.api_gateway.name
      arn  = aws_cloudwatch_log_group.api_gateway.arn
    }
  }
}
