# Example: Chat Agent Lambda Function using Multiple Layers
# This shows how to use the multi-layer approach in your application infrastructure

# Chat Agent Lambda Function
resource "aws_lambda_function" "chat_agent" {
  function_name = "${var.project_name}-chat-agent-${var.environment}"
  description   = "Financial chat agent with AI capabilities"
  handler       = "lambda_handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 300
  memory_size   = 1024

  # Source code
  filename         = "chat_agent.zip"
  source_code_hash = filebase64sha256("chat_agent.zip")

  # Use all layers for complete functionality
  layers = [
    # Get layer ARNs from base infrastructure
    data.terraform_remote_state.base_infra.outputs.lambda_layer_arn_list[0], # core
    data.terraform_remote_state.base_infra.outputs.lambda_layer_arn_list[1], # financial
    data.terraform_remote_state.base_infra.outputs.lambda_layer_arn_list[2], # ai
    data.terraform_remote_state.base_infra.outputs.lambda_layer_arn_list[3], # utility
  ]

  # Environment variables
  environment {
    variables = {
      # OpenTelemetry configuration
      OTEL_SDK_DISABLED                     = "true"
      OTEL_PYTHON_DISABLED_INSTRUMENTATIONS = "all"
      OTEL_PYTHON_CONTEXT                   = "contextvars_context"

      # Application configuration
      ENVIRONMENT = var.environment
      LOG_LEVEL   = "INFO"
    }
  }

  # IAM role
  role = aws_iam_role.chat_agent_role.arn

  tags = var.common_tags
}

# Alternative approach: Use specific layers only
resource "aws_lambda_function" "simple_agent" {
  function_name = "${var.project_name}-simple-agent-${var.environment}"
  description   = "Simple agent with only core and financial dependencies"
  handler       = "lambda_handler.lambda_handler"
  runtime       = "python3.11"
  timeout       = 60
  memory_size   = 512

  filename         = "simple_agent.zip"
  source_code_hash = filebase64sha256("simple_agent.zip")

  # Use only core and financial layers
  layers = [
    data.terraform_remote_state.base_infra.outputs.lambda_layer_core_arn,
    data.terraform_remote_state.base_infra.outputs.lambda_layer_financial_arn,
  ]

  role = aws_iam_role.simple_agent_role.arn

  tags = var.common_tags
}

# Data source to get base infrastructure outputs
data "terraform_remote_state" "base_infra" {
  backend = "s3"
  config = {
    bucket = "your-terraform-state-bucket"
    key    = "base-infrastructure/terraform.tfstate"
    region = var.aws_region
  }
}
