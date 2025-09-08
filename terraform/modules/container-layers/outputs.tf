# Container Layer Management Module Outputs

output "ecr_repository_url" {
  description = "URL of the ECR repository for layer builder"
  value       = aws_ecr_repository.layer_builder.repository_url
}

output "ecr_artifacts_repository_url" {
  description = "URL of the ECR repository for layer artifacts"
  value       = aws_ecr_repository.layer_artifacts.repository_url
}

output "layer_builder_role_arn" {
  description = "ARN of the IAM role for layer builder"
  value       = aws_iam_role.layer_builder_role.arn
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket for layer artifacts"
  value       = aws_s3_bucket.layer_artifacts.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the S3 bucket for layer artifacts"
  value       = aws_s3_bucket.layer_artifacts.arn
}

output "lambda_layer_arns" {
  description = "Map of Lambda layer ARNs"
  value = {
    for k, v in aws_lambda_layer_version.layers : k => v.arn
  }
}

output "lambda_layer_arn_list" {
  description = "List of all Lambda layer ARNs for easy use in Lambda functions"
  value = [
    for k, v in aws_lambda_layer_version.layers : v.arn
  ]
}

# Individual layer outputs
output "lambda_layer_core_arn" {
  description = "ARN of the core dependencies Lambda layer"
  value       = try(aws_lambda_layer_version.layers["core"].arn, null)
}

output "lambda_layer_financial_arn" {
  description = "ARN of the financial dependencies Lambda layer"
  value       = try(aws_lambda_layer_version.layers["financial"].arn, null)
}

output "lambda_layer_strands_arn" {
  description = "ARN of the Strands core dependencies Lambda layer"
  value       = try(aws_lambda_layer_version.layers["strands"].arn, null)
}

output "lambda_layer_strands_tools_arn" {
  description = "ARN of the Strands tools dependencies Lambda layer"
  value       = try(aws_lambda_layer_version.layers["strands-tools"].arn, null)
}

output "lambda_layer_utility_arn" {
  description = "ARN of the utility dependencies Lambda layer"
  value       = try(aws_lambda_layer_version.layers["utility"].arn, null)
}
