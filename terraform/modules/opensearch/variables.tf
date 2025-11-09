# OpenSearch Domain Module Variables

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
}

variable "domain_name" {
  description = "Name suffix for the OpenSearch domain (will be prefixed with project_name and suffixed with environment)"
  type        = string
}

variable "engine_version" {
  description = "OpenSearch engine version"
  type        = string
  default     = "OpenSearch_2.11"
}

# Cluster Configuration
variable "instance_type" {
  description = "Instance type for OpenSearch nodes"
  type        = string
  default     = "t3.medium.search"
}

variable "instance_count" {
  description = "Number of instances in the cluster"
  type        = number
  default     = 1
}

variable "dedicated_master_enabled" {
  description = "Enable dedicated master nodes"
  type        = bool
  default     = false
}

variable "dedicated_master_type" {
  description = "Instance type for dedicated master nodes"
  type        = string
  default     = "t3.small.search"
}

variable "dedicated_master_count" {
  description = "Number of dedicated master nodes"
  type        = number
  default     = 3
}

variable "zone_awareness_enabled" {
  description = "Enable zone awareness (multi-AZ)"
  type        = bool
  default     = false
}

variable "availability_zone_count" {
  description = "Number of availability zones (2 or 3)"
  type        = number
  default     = 2
}

# EBS Configuration
variable "ebs_enabled" {
  description = "Enable EBS volumes"
  type        = bool
  default     = true
}

variable "ebs_volume_type" {
  description = "EBS volume type (gp3, gp2, io1, standard)"
  type        = string
  default     = "gp3"
}

variable "ebs_volume_size" {
  description = "EBS volume size in GB"
  type        = number
  default     = 20
}

variable "ebs_iops" {
  description = "EBS IOPS (for io1 volume type)"
  type        = number
  default     = null
}

variable "ebs_throughput" {
  description = "EBS throughput in MB/s (for gp3 volume type)"
  type        = number
  default     = null
}

# VPC Configuration
variable "vpc_enabled" {
  description = "Enable VPC configuration"
  type        = bool
  default     = false
}

variable "subnet_ids" {
  description = "List of subnet IDs for VPC configuration"
  type        = list(string)
  default     = []
}

variable "security_group_ids" {
  description = "List of security group IDs for VPC configuration"
  type        = list(string)
  default     = []
}

# Security Configuration
variable "kms_key_arn" {
  description = "KMS key ARN for encryption at rest"
  type        = string
}

variable "node_to_node_encryption_enabled" {
  description = "Enable node-to-node encryption"
  type        = bool
  default     = true
}

variable "enforce_https" {
  description = "Enforce HTTPS for domain endpoint"
  type        = bool
  default     = true
}

variable "tls_security_policy" {
  description = "TLS security policy (Policy-Min-TLS-1-0-2019-07, Policy-Min-TLS-1-2-2019-07)"
  type        = string
  default     = "Policy-Min-TLS-1-2-2019-07"
}

# Advanced Security Options
variable "advanced_security_enabled" {
  description = "Enable advanced security options (fine-grained access control)"
  type        = bool
  default     = false
}

variable "internal_user_database_enabled" {
  description = "Enable internal user database (for advanced security)"
  type        = bool
  default     = false
}

variable "master_user_arn" {
  description = "Master user ARN (for advanced security)"
  type        = string
  default     = null
}

variable "master_user_name" {
  description = "Master user name (for advanced security with internal database)"
  type        = string
  default     = null
  sensitive   = true
}

variable "master_user_password" {
  description = "Master user password (for advanced security with internal database)"
  type        = string
  default     = null
  sensitive   = true
}

# Access Policy
variable "access_policy_json" {
  description = "IAM access policy JSON (if null, creates default policy)"
  type        = string
  default     = null
}

variable "domain_policy_json" {
  description = "OpenSearch domain policy JSON (for fine-grained access control)"
  type        = string
  default     = null
}

# Logging
variable "log_publishing_options" {
  description = "Log publishing options for CloudWatch"
  type = list(object({
    log_type                 = string
    cloudwatch_log_group_arn = string
    enabled                  = bool
  }))
  default = []
}

# Advanced Options
variable "advanced_options" {
  description = "Key-value string pairs to specify advanced configuration options"
  type        = map(string)
  default     = {}
}

# Tags
variable "common_tags" {
  description = "Common tags to apply to resources"
  type        = map(string)
  default     = {}
}

# Additional IAM Principals
variable "additional_iam_principals" {
  description = "Additional IAM principals (ARNs) to grant access to OpenSearch"
  type        = list(string)
  default     = []
}

