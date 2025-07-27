# Lambda Layer Module for shared dependencies
# modules/lambda-layer/main.tf

# Create the ZIP file using a null_resource to ensure it exists before being referenced
resource "null_resource" "create_empty_layer" {
  provisioner "local-exec" {
    command     = <<-EOT
      mkdir -p "${path.module}/temp/python"
      echo "# Empty layer placeholder" > "${path.module}/temp/python/__init__.py"
      cd "${path.module}/temp"
      zip -r "../empty-layer.zip" python/
      rm -rf "${path.module}/temp"
    EOT
    interpreter = ["/bin/bash", "-c"]
  }

  triggers = {
    always_run = timestamp()
  }
}

# Create an empty layer that can be used by other Lambda functions
# Dependencies should be managed externally or through deployment pipelines
resource "aws_lambda_layer_version" "shared_dependencies" {
  layer_name  = "${var.project_name}-shared-deps-${var.environment}"
  description = "Shared dependencies layer for ${var.project_name} Lambda functions - populated externally"

  filename            = "${path.module}/empty-layer.zip"
  compatible_runtimes = ["python3.11", "python3.12"]

  # Use a simple hash since we're creating a minimal layer
  source_code_hash = base64sha256("empty-layer-${var.environment}")

  depends_on = [null_resource.create_empty_layer]
}
