# Lambda Layer Module for shared dependencies
# modules/lambda-layer/main.tf

# Local variables for file paths
locals {
  requirements_path = "${path.module}/layer-definitions/${var.requirements_file}"
  build_script_path = "${path.module}/build-layer.sh"
  python_dir_path   = "${path.module}/python"
  layer_zip_path    = "${path.module}/layer.zip"
}

# Build the layer package
resource "null_resource" "build_layer" {
  triggers = {
    requirements_hash = filemd5(local.requirements_path)
    build_script_hash = filemd5(local.build_script_path)
    source_files_hash = var.source_files != [] ? join(",", [for f in var.source_files : filemd5("${path.module}/layer-definitions/${f}")]) : ""
  }

  provisioner "local-exec" {
    command     = "./build-layer.sh"
    interpreter = ["bash"]
    working_dir = path.module
    environment = {
      PYTHON_CMD        = var.python_command
      REQUIREMENTS_FILE = var.requirements_file
    }
  }
}

# Create the Lambda layer with all dependencies
resource "aws_lambda_layer_version" "shared_dependencies" {
  depends_on  = [null_resource.build_layer]
  layer_name  = "${var.project_name}-${var.layer_name_suffix}-${var.environment}"
  description = var.layer_description

  filename            = local.layer_zip_path
  source_code_hash    = filebase64sha256(local.layer_zip_path)
  compatible_runtimes = var.compatible_runtimes

  # Layer size limit is 250 MB unzipped
  # Dependencies are defined in the requirements file specified by var.requirements_file
}

