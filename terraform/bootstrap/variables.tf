# Bootstrap Infrastructure Variables

variable "state_bucket_name" {
  description = "Name of the S3 bucket for Terraform state storage"
  type        = string
  default     = "cosine-terraform-state-bucket"
}

variable "lock_table_name" {
  description = "Name of the DynamoDB table for Terraform state locking"
  type        = string
  default     = "cosine-terraform-locks"
}

variable "state_retention_days" {
  description = "Number of days to retain old versions of state files"
  type        = number
  default     = 90
}

variable "tags" {
  description = "Common tags to apply to all resources"
  type        = map(string)
  default = {
    Project   = "Cosine"
    ManagedBy = "Terraform"
    Purpose   = "BootstrapInfrastructure"
  }
}
