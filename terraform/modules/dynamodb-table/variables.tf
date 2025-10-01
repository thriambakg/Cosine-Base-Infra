# DynamoDB Table Module Variables
# modules/dynamodb-table/variables.tf

variable "project_name" {
  description = "Name of the project"
  type        = string
}

variable "environment" {
  description = "Environment (development, staging, production)"
  type        = string
}

variable "table_name" {
  description = "Name suffix for the DynamoDB table (will be prefixed with project_name and suffixed with environment)"
  type        = string
}

variable "hash_key" {
  description = "Hash key attribute name"
  type        = string
}

variable "range_key" {
  description = "Range key attribute name (optional)"
  type        = string
  default     = null
}

variable "attributes" {
  description = "List of attribute definitions"
  type = list(object({
    name = string
    type = string
  }))
}

variable "global_secondary_indexes" {
  description = "List of global secondary indexes"
  type = list(object({
    name            = string
    hash_key        = string
    range_key       = string
    projection_type = string
    read_capacity   = number
    write_capacity  = number
  }))
  default = []
}

variable "billing_mode" {
  description = "Billing mode for the table (PROVISIONED or PAY_PER_REQUEST)"
  type        = string
  default     = "PAY_PER_REQUEST"
}

variable "read_capacity" {
  description = "Read capacity units (only for PROVISIONED mode)"
  type        = number
  default     = null
}

variable "write_capacity" {
  description = "Write capacity units (only for PROVISIONED mode)"
  type        = number
  default     = null
}

variable "stream_enabled" {
  description = "Enable DynamoDB streams"
  type        = bool
  default     = false
}

variable "stream_view_type" {
  description = "Stream view type (KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES)"
  type        = string
  default     = "NEW_AND_OLD_IMAGES"
}

variable "point_in_time_recovery_enabled" {
  description = "Enable point-in-time recovery"
  type        = bool
  default     = true
}

variable "deletion_protection_enabled" {
  description = "Enable deletion protection"
  type        = bool
  default     = false
}

variable "ttl_enabled" {
  description = "Enable TTL"
  type        = bool
  default     = false
}

variable "ttl_attribute_name" {
  description = "TTL attribute name"
  type        = string
  default     = "ttl"
}

variable "kms_key_arn" {
  description = "KMS key ARN for encryption"
  type        = string
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = map(string)
  default     = {}
}

variable "table_type" {
  description = "Type of table (for tagging)"
  type        = string
  default     = "Data"
}

variable "table_purpose" {
  description = "Purpose of table (for tagging)"
  type        = string
  default     = "General"
}

variable "iam_policy_actions" {
  description = "List of IAM actions allowed for this table"
  type        = list(string)
  default = [
    "dynamodb:GetItem",
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:DeleteItem",
    "dynamodb:Query",
    "dynamodb:Scan"
  ]
}

variable "additional_iam_policy_statements" {
  description = "Additional IAM policy statements for the table policy"
  type        = list(any)
  default     = []
}

