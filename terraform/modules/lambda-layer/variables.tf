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

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default     = {}
}

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
