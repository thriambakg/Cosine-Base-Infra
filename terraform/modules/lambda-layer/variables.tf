# Lambda Layer Module Variables
# modules/lambda-layer/variables.tf

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
}

# Note: aws_lambda_layer_version doesn't support tags

variable "compatible_runtimes" {
  description = "List of compatible Lambda runtimes"
  type        = list(string)
  default     = ["python3.11", "python3.12"]
}

variable "layer_description" {
  description = "Description for the Lambda layer"
  type        = string
  default     = "Shared dependencies for Lambda functions"
}

variable "requirements_file" {
  description = "Path to the requirements.txt file relative to layer-definitions folder"
  type        = string
  default     = "chat-agent-dependencies.txt"
}

variable "layer_name_suffix" {
  description = "Suffix to append to the layer name for identification"
  type        = string
  default     = "shared-deps"
}

variable "source_directory" {
  description = "Path to source code directory to include in the layer (optional)"
  type        = string
  default     = ""
}

variable "source_files" {
  description = "List of additional source files to include in the layer (optional)"
  type        = list(string)
  default     = []
}

variable "python_command" {
  description = "Python command to use for building the layer"
  type        = string
  default     = "python3.11"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for storing large layer files (>50MB)"
  type        = string
}
