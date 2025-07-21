# Backend Configuration for Base Infrastructure
# backend.tf

terraform {
  backend "s3" {
    # Backend configuration will be provided via backend config files
    # See: backend-configs/ directory for environment-specific configurations
  }
}
