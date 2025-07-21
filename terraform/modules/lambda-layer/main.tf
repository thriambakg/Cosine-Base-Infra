# Lambda Layer Module for shared dependencies
# modules/lambda-layer/main.tf

resource "aws_lambda_layer_version" "shared_dependencies" {
  layer_name          = "${var.project_name}-shared-deps-${var.environment}"
  description         = "Shared dependencies for ${var.project_name} Lambda functions"
  filename            = data.archive_file.layer_zip.output_path
  compatible_runtimes = ["python3.11", "python3.12"]
  source_code_hash    = data.archive_file.layer_zip.output_base64sha256

  depends_on = [null_resource.pip_install]
}

# Install dependencies to layer directory
resource "null_resource" "pip_install" {
  triggers = {
    requirements = filemd5("${path.module}/requirements.txt")
    script      = filemd5("${path.module}/install-layer-deps.ps1")
  }

  provisioner "local-exec" {
    command     = "powershell -ExecutionPolicy Bypass -File \"${path.module}/install-layer-deps.ps1\" -ModulePath \"${path.module}\" -RequirementsFile \"${path.module}/requirements.txt\""
    interpreter = ["cmd", "/C"]
  }
}

# Create layer ZIP file
data "archive_file" "layer_zip" {
  type        = "zip"
  source_dir  = "${path.module}/layer"
  output_path = "${path.module}/../../temp/${var.project_name}-layer-${var.environment}.zip"
  depends_on  = [null_resource.pip_install]
}
