/**
 * Generic Step Functions State Machine Module
 * 
 * This module creates an AWS Step Functions state machine with configurable
 * Lambda integrations, retry logic, and error handling.
 */

resource "aws_sfn_state_machine" "this" {
  name     = var.state_machine_name
  role_arn = aws_iam_role.step_functions.arn

  definition = var.definition

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
    include_execution_data = var.include_execution_data
    level                  = var.log_level
  }

  tags = merge(
    var.tags,
    {
      Name        = var.state_machine_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  )
}

# CloudWatch Log Group for Step Functions
resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/stepfunctions/${var.state_machine_name}"
  retention_in_days = var.log_retention_days

  tags = merge(
    var.tags,
    {
      Name        = "/aws/stepfunctions/${var.state_machine_name}"
      Environment = var.environment
    }
  )
}

# IAM Role for Step Functions
resource "aws_iam_role" "step_functions" {
  name = "${var.state_machine_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "states.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = merge(
    var.tags,
    {
      Name        = "${var.state_machine_name}-role"
      Environment = var.environment
    }
  )
}

# IAM Policy for Step Functions to invoke Lambda
resource "aws_iam_role_policy" "step_functions_lambda" {
  name = "${var.state_machine_name}-lambda-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = var.lambda_function_arns
      }
    ]
  })
}

# IAM Policy for Step Functions CloudWatch Logging
resource "aws_iam_role_policy" "step_functions_logging" {
  name = "${var.state_machine_name}-logging-policy"
  role = aws_iam_role.step_functions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogDelivery",
          "logs:GetLogDelivery",
          "logs:UpdateLogDelivery",
          "logs:DeleteLogDelivery",
          "logs:ListLogDeliveries",
          "logs:PutResourcePolicy",
          "logs:DescribeResourcePolicies",
          "logs:DescribeLogGroups"
        ]
        Resource = "*"
      }
    ]
  })
}

# Allow additional IAM policies to be attached
resource "aws_iam_role_policy_attachment" "additional" {
  count      = length(var.additional_policy_arns)
  role       = aws_iam_role.step_functions.name
  policy_arn = var.additional_policy_arns[count.index]
}

