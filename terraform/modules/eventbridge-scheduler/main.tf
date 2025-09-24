# EventBridge Scheduler Module
# modules/eventbridge-scheduler/main.tf

# EventBridge Rule
resource "aws_cloudwatch_event_rule" "scheduler_rule" {
  name                = var.rule_name
  description         = var.rule_description
  schedule_expression = var.schedule_expression
  state               = var.enabled ? "ENABLED" : "DISABLED"

  tags = merge(var.tags, {
    Name        = var.rule_name
    Type        = "EventBridgeRule"
    Purpose     = var.purpose
    Environment = var.environment
  })
}

# EventBridge Target
resource "aws_cloudwatch_event_target" "scheduler_target" {
  count = var.enabled ? 1 : 0

  rule      = aws_cloudwatch_event_rule.scheduler_rule.name
  target_id = var.target_id
  arn       = var.target_arn

  # Optional input for the target
  input = var.target_input != null ? var.target_input : null

  # Optional role ARN for cross-account access
  role_arn = var.target_role_arn
}

# Lambda Permission (only if target is a Lambda function)
resource "aws_lambda_permission" "allow_eventbridge" {
  count = var.enabled && var.target_type == "lambda" ? 1 : 0

  statement_id  = "AllowExecutionFromEventBridge${title(replace(var.target_id, "-", ""))}"
  action        = "lambda:InvokeFunction"
  function_name = var.target_function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.scheduler_rule.arn
}
