# Base Infrastructure Outputs
# outputs.tf

output "lambda_layer_arn" {
  description = "ARN of the shared Lambda layer"
  value       = module.lambda_layer.layer_arn
}

output "lambda_layer_version" {
  description = "Version of the shared Lambda layer"
  value       = module.lambda_layer.layer_version
}

output "lambda_layer_version_arn" {
  description = "ARN of the shared Lambda layer with version"
  value       = module.lambda_layer.layer_version_arn
}

output "layer_name" {
  description = "Name of the shared Lambda layer"
  value       = module.lambda_layer.layer_name
}

output "compatible_runtimes" {
  description = "Compatible runtimes for the Lambda layer"
  value       = module.lambda_layer.compatible_runtimes
}
