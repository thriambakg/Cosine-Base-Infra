# Lambda Layer Module Outputs
# modules/lambda-layer/outputs.tf

output "layer_arn" {
  description = "ARN of the Lambda layer"
  value       = aws_lambda_layer_version.shared_dependencies.arn
}

output "layer_version" {
  description = "Version of the Lambda layer"
  value       = aws_lambda_layer_version.shared_dependencies.version
}

output "layer_name" {
  description = "Name of the Lambda layer"
  value       = aws_lambda_layer_version.shared_dependencies.layer_name
}

output "compatible_runtimes" {
  description = "Compatible runtimes for the Lambda layer"
  value       = aws_lambda_layer_version.shared_dependencies.compatible_runtimes
}

output "layer_layer_arn" {
  description = "Layer ARN of the Lambda layer (without version)"
  value       = aws_lambda_layer_version.shared_dependencies.layer_arn
}
