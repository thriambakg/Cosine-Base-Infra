# Lambda Layer Module for shared dependencies
# modules/lambda-layer/main.tf

# Create a minimal empty layer structure first
data "archive_file" "empty_layer" {
  type        = "zip"
  output_path = "${path.module}/empty-layer.zip"

  source {
    content  = "# Empty layer - dependencies can be added via deployment process"
    filename = "python/placeholder.py"
  }
}

# Create an empty layer that can be used by other Lambda functions
# Dependencies should be managed externally or through deployment pipelines
resource "aws_lambda_layer_version" "shared_dependencies" {
  layer_name  = "${var.project_name}-shared-deps-${var.environment}"
  description = "Shared dependencies layer for ${var.project_name} Lambda functions"

  # Create a minimal layer with just a placeholder
  filename            = data.archive_file.empty_layer.output_path
  compatible_runtimes = ["python3.11", "python3.12"]
  source_code_hash    = data.archive_file.empty_layer.output_base64sha256

  depends_on = [data.archive_file.empty_layer]
}
