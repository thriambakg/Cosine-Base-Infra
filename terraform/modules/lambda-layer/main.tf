# Lambda Layer Module for shared dependencies
# modules/lambda-layer/main.tf

# Create a simple data archive for the empty layer
data "archive_file" "empty_layer" {
  type        = "zip"
  output_path = "${path.module}/empty-layer.zip"
  
  source {
    content  = "# Empty layer placeholder"
    filename = "python/__init__.py"
  }
}

# Create an empty layer that can be used by other Lambda functions
# Dependencies should be managed externally or through deployment pipelines
resource "aws_lambda_layer_version" "shared_dependencies" {
  layer_name  = "${var.project_name}-shared-deps-${var.environment}"
  description = "Shared dependencies layer for ${var.project_name} Lambda functions - populated externally"

  filename         = data.archive_file.empty_layer.output_path
  source_code_hash = data.archive_file.empty_layer.output_base64sha256
  compatible_runtimes = ["python3.11", "python3.12"]
}
