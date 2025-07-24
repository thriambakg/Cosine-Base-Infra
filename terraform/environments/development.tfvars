# Development Environment Variables
# environments/development.tfvars

project_name = "cosine"
environment  = "development"
aws_region   = "us-east-1"

common_tags = {
  Project     = "cosine"
  Environment = "development"
  ManagedBy   = "terraform"
  Repository  = "Cosine-Base-Infra"
  Owner       = "development-team"
  CostCenter  = "development"
}
