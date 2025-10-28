# Lambda Layer Module for shared dependencies
# modules/lambda-layer/main.tf

# Local variables for file paths
locals {
  requirements_path = "${path.module}/layer-definitions/${var.requirements_file}"
  build_script_path = "${path.module}/build-layer.sh"
  python_dir_path   = "${path.module}/python"
  layer_zip_path    = "${path.module}/layer-${var.layer_name_suffix}.zip"
  source_dir_path   = var.source_directory != "" ? var.source_directory : ""
}

# Build the layer package (only if zip file doesn't exist)
resource "null_resource" "build_layer" {
  count = fileexists(local.layer_zip_path) ? 0 : 1

  triggers = {
    requirements_hash = fileexists(local.requirements_path) ? filemd5(local.requirements_path) : "no-requirements"
    build_script_hash = filemd5(local.build_script_path)
    source_hash       = local.source_dir_path != "" && fileexists(local.source_dir_path) ? filemd5(local.source_dir_path) : "no-source"
  }

  provisioner "local-exec" {
    command     = "bash -c 'if [ -f build-layer.sh ]; then chmod +x build-layer.sh && ./build-layer.sh ${var.layer_name_suffix} ${local.source_dir_path}; else echo \"Build script not found, skipping local build\"; fi'"
    working_dir = path.module
    environment = {
      PYTHON_CMD = var.python_command
    }
  }
}

# Upload layer to S3 (for all layers to avoid API size limits)
resource "aws_s3_object" "layer_zip" {
  count = fileexists(local.layer_zip_path) ? 1 : 0

  bucket = var.s3_bucket_name
  key    = "lambda-layers/${var.project_name}-${var.layer_name_suffix}-deps-${var.environment}-${formatdate("YYYY-MM-DD-hhmm", timestamp())}.zip"
  source = local.layer_zip_path

  etag = fileexists(local.layer_zip_path) ? filemd5(local.layer_zip_path) : null

  depends_on = [null_resource.build_layer]
}

# Create the Lambda layer with all dependencies
resource "aws_lambda_layer_version" "shared_dependencies" {
  depends_on  = [null_resource.build_layer, aws_s3_object.layer_zip]
  layer_name  = "${var.project_name}-${var.layer_name_suffix}-deps-${var.environment}"
  description = var.layer_description

  # Use S3 for all layers to avoid API size limits
  filename          = null
  s3_bucket         = fileexists(local.layer_zip_path) ? var.s3_bucket_name : null
  s3_key            = fileexists(local.layer_zip_path) ? aws_s3_object.layer_zip[0].key : null
  s3_object_version = fileexists(local.layer_zip_path) ? aws_s3_object.layer_zip[0].version_id : null

  source_code_hash    = fileexists(local.layer_zip_path) ? filebase64sha256(local.layer_zip_path) : null
  compatible_runtimes = var.compatible_runtimes

  # Layer size limit is 250 MB unzipped per layer
  # Dependencies are defined in the requirements file specified by var.requirements_file
}

