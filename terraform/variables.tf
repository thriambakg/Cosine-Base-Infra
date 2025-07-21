# Base Infrastructure Variables
# variables.tf

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "cosine"
}

variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
  default     = "staging"
  
  validation {
    condition = contains([
      "development",
      "staging", 
      "production"
    ], var.environment)
    error_message = "Environment must be development, staging, or production."
  }
}

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "common_tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "cosine"
    ManagedBy   = "terraform"
    Repository  = "Cosine-Base-Infra"
  }
}
