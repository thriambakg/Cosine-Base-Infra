output "function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.function.function_name
}

output "function_arn" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.function.arn
}

output "invoke_arn" {
  description = "Invoke ARN of the Lambda function"
  value       = aws_lambda_function.function.invoke_arn
}

output "execution_role_arn" {
  description = "ARN of the Lambda execution role"
  value       = local.lambda_role_arn
}

output "execution_role_name" {
  description = "Name of the Lambda execution role"
  value       = var.existing_role_name != "" ? var.existing_role_name : aws_iam_role.lambda_execution_role[0].name
}
