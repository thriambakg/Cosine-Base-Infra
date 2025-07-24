# Staging Environment Variables
# environments/staging.tfvars

project_name = "cosine"
environment  = "staging"
aws_region   = "us-east-1"

common_tags = {
  Project     = "cosine"
  Environment = "staging"
  ManagedBy   = "terraform"
  Repository  = "Cosine-Base-Infra"
  Owner       = "staging-team"
  CostCenter  = "staging"
}
