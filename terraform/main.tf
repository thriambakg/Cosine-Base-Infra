# Cosine Base Infrastructure
# Main Terraform configuration for shared resources

terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = var.common_tags
  }
}

# Lambda Layer for shared dependencies
module "shared_layer" {
  source = "./modules/lambda-layer"

  project_name = var.project_name
  environment  = var.environment

  tags = var.common_tags
}
