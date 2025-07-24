# Production Environment Variables
# environments/production.tfvars

project_name = "cosine"
environment  = "production"
aws_region   = "us-east-1"

common_tags = {
  Project     = "cosine"
  Environment = "production"
  ManagedBy   = "terraform"
  Repository  = "Cosine-Base-Infra"
  Owner       = "production-team"
  CostCenter  = "production"
}
