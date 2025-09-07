# Lambda Layer Module for shared dependencies
# modules/lambda-layer/main.tf

# Local variables for file paths
locals {
  requirements_path = "${path.module}/layer-definitions/${var.requirements_file}"
  build_script_path = "${path.module}/build-layer.sh"
  python_dir_path   = "${path.module}/python"
  layer_zip_path    = "${path.module}/layer-${var.layer_name_suffix}.zip"
}

# Build the layer package
resource "null_resource" "build_layer" {
  triggers = {
    requirements_hash = filemd5(local.requirements_path)
    build_script_hash = filemd5(local.build_script_path)
  }

  provisioner "local-exec" {
    command     = "./build-layer.sh ${var.layer_name_suffix}"
    interpreter = ["bash"]
    working_dir = path.module
    environment = {
      PYTHON_CMD = var.python_command
    }
  }
}

# Create the Lambda layer with all dependencies
resource "aws_lambda_layer_version" "shared_dependencies" {
  depends_on  = [null_resource.build_layer]
  layer_name  = "${var.project_name}-${var.layer_name_suffix}-${var.environment}"
  description = var.layer_description

  filename            = local.layer_zip_path
  source_code_hash    = fileexists(local.layer_zip_path) ? filebase64sha256(local.layer_zip_path) : null
  compatible_runtimes = var.compatible_runtimes

  # Layer size limit is 64 MB unzipped per layer
  # Dependencies are defined in the requirements file specified by var.requirements_file
}

