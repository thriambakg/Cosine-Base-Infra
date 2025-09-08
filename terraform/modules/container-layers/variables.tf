# Container Layer Management Module Variables

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment name (e.g., production, staging, development)"
  type        = string
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default     = {}
}

variable "s3_bucket_arn" {
  description = "ARN of the S3 bucket for layer artifacts"
  type        = string
}

variable "layer_definitions" {
  description = "Map of layer definitions with their configurations"
  type = map(object({
    description         = string
    compatible_runtimes = list(string)
    requirements_file   = string
    size_estimate       = string
  }))
  default = {}
}

variable "container_image_tag" {
  description = "Tag for the container image"
  type        = string
  default     = "latest"
}
