# CloudWatch Module
# modules/cloudwatch/main.tf

# Security Events Log Group
resource "aws_cloudwatch_log_group" "security_events" {
  name              = "/aws/${var.project_name}/${var.environment}/security-events"
  retention_in_days = var.security_log_retention_days
  kms_key_id        = var.kms_key_id

  tags = merge(var.tags, {
    Name    = "${var.project_name}-security-events-${var.environment}"
    Type    = "LogGroup"
    Purpose = "SecurityAudit"
  })
}

# Authentication Events Log Group
resource "aws_cloudwatch_log_group" "auth_events" {
  name              = "/aws/${var.project_name}/${var.environment}/auth-events"
  retention_in_days = var.auth_log_retention_days
  kms_key_id        = var.kms_key_id

  tags = merge(var.tags, {
    Name    = "${var.project_name}-auth-events-${var.environment}"
    Type    = "LogGroup"
    Purpose = "Authentication"
  })
}

# Application Logs
resource "aws_cloudwatch_log_group" "application" {
  name              = "/aws/${var.project_name}/${var.environment}/application"
  retention_in_days = var.application_log_retention_days
  kms_key_id        = var.kms_key_id

  tags = merge(var.tags, {
    Name    = "${var.project_name}-application-${var.environment}"
    Type    = "LogGroup"
    Purpose = "Application"
  })
}

# Lambda Functions Log Group
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.project_name}-${var.environment}"
  retention_in_days = var.lambda_log_retention_days
  kms_key_id        = var.kms_key_id

  tags = merge(var.tags, {
    Name    = "${var.project_name}-lambda-${var.environment}"
    Type    = "LogGroup"
    Purpose = "Lambda"
  })
}

# API Gateway Log Group
resource "aws_cloudwatch_log_group" "api_gateway" {
  name              = "/aws/apigateway/${var.project_name}-${var.environment}"
  retention_in_days = var.api_gateway_log_retention_days
  kms_key_id        = var.kms_key_id

  tags = merge(var.tags, {
    Name    = "${var.project_name}-api-gateway-${var.environment}"
    Type    = "LogGroup"
    Purpose = "APIGateway"
  })
}

# CloudWatch Dashboard
resource "aws_cloudwatch_dashboard" "main" {
  dashboard_name = "${var.project_name}-${var.environment}-dashboard"

  dashboard_body = jsonencode({
    widgets = [
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 12
        height = 6

        properties = {
          metrics = [
            ["AWS/Cognito", "SignInSuccesses", "UserPool", var.cognito_user_pool_id],
            [".", "SignInThrottles", ".", "."],
            [".", "CompromisedCredentialsRisk", ".", "."]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "Cognito Authentication Metrics"
          period  = 300
        }
      },
      {
        type   = "log"
        x      = 0
        y      = 6
        width  = 24
        height = 6

        properties = {
          query  = "SOURCE '${aws_cloudwatch_log_group.security_events.name}' | fields @timestamp, event_type, user_id, message | sort @timestamp desc | limit 100"
          region = var.aws_region
          title  = "Recent Security Events"
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 0
        width  = 12
        height = 6

        properties = {
          metrics = [
            ["AWS/DynamoDB", "ConsumedReadCapacityUnits", "TableName", var.user_profiles_table_name],
            [".", "ConsumedWriteCapacityUnits", ".", "."],
            [".", "ThrottledRequests", ".", "."]
          ]
          view    = "timeSeries"
          stacked = false
          region  = var.aws_region
          title   = "DynamoDB Metrics"
          period  = 300
        }
      }
    ]
  })

  # Note: CloudWatch dashboards do not support tags
}

# Security Event Metric Filter
resource "aws_cloudwatch_log_metric_filter" "failed_login_attempts" {
  name           = "${var.project_name}-failed-login-attempts-${var.environment}"
  log_group_name = aws_cloudwatch_log_group.security_events.name
  pattern        = "[time, request_id, event_type=\"login_failed\", ...]"

  metric_transformation {
    name      = "FailedLoginAttempts"
    namespace = "${var.project_name}/${var.environment}/Security"
    value     = "1"
  }
}

# Suspicious Activity Metric Filter
resource "aws_cloudwatch_log_metric_filter" "suspicious_activity" {
  name           = "${var.project_name}-suspicious-activity-${var.environment}"
  log_group_name = aws_cloudwatch_log_group.security_events.name
  pattern        = "[time, request_id, event_type=\"account_locked\" || event_type=\"multiple_failed_attempts\", ...]"

  metric_transformation {
    name      = "SuspiciousActivity"
    namespace = "${var.project_name}/${var.environment}/Security"
    value     = "1"
  }
}

# Failed Login Alarm
resource "aws_cloudwatch_metric_alarm" "failed_login_alarm" {
  alarm_name          = "${var.project_name}-failed-login-alarm-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "2"
  metric_name         = "FailedLoginAttempts"
  namespace           = "${var.project_name}/${var.environment}/Security"
  period              = "300"
  statistic           = "Sum"
  threshold           = var.failed_login_threshold
  alarm_description   = "This metric monitors failed login attempts"
  alarm_actions       = var.alarm_notification_topic_arn != "" ? [var.alarm_notification_topic_arn] : []

  tags = merge(var.tags, {
    Name    = "${var.project_name}-failed-login-alarm-${var.environment}"
    Type    = "Alarm"
    Purpose = "Security"
  })
}

# Suspicious Activity Alarm
resource "aws_cloudwatch_metric_alarm" "suspicious_activity_alarm" {
  alarm_name          = "${var.project_name}-suspicious-activity-alarm-${var.environment}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = "1"
  metric_name         = "SuspiciousActivity"
  namespace           = "${var.project_name}/${var.environment}/Security"
  period              = "300"
  statistic           = "Sum"
  threshold           = var.suspicious_activity_threshold
  alarm_description   = "This metric monitors suspicious authentication activity"
  alarm_actions       = var.alarm_notification_topic_arn != "" ? [var.alarm_notification_topic_arn] : []

  tags = merge(var.tags, {
    Name    = "${var.project_name}-suspicious-activity-alarm-${var.environment}"
    Type    = "Alarm"
    Purpose = "Security"
  })
}
