# Generic DynamoDB Table Module
# modules/dynamodb-table/main.tf

data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

locals {
  table_name = "${var.project_name}-${var.table_name}-${var.environment}"
}

resource "aws_dynamodb_table" "this" {
  count = var.create ? 1 : 0

  name                        = local.table_name
  billing_mode                = var.billing_mode
  hash_key                    = var.hash_key
  range_key                   = var.range_key
  stream_enabled              = var.stream_enabled
  stream_view_type            = var.stream_enabled ? var.stream_view_type : null
  deletion_protection_enabled = var.deletion_protection_enabled

  # Capacity settings for provisioned mode
  read_capacity  = var.billing_mode == "PROVISIONED" ? var.read_capacity : null
  write_capacity = var.billing_mode == "PROVISIONED" ? var.write_capacity : null

  # Dynamic attributes
  dynamic "attribute" {
    for_each = var.attributes
    content {
      name = attribute.value.name
      type = attribute.value.type
    }
  }

  # Dynamic global secondary indexes
  dynamic "global_secondary_index" {
    for_each = var.global_secondary_indexes
    content {
      name            = global_secondary_index.value.name
      hash_key        = global_secondary_index.value.hash_key
      range_key       = global_secondary_index.value.range_key
      projection_type = global_secondary_index.value.projection_type
      read_capacity   = var.billing_mode == "PROVISIONED" ? global_secondary_index.value.read_capacity : null
      write_capacity  = var.billing_mode == "PROVISIONED" ? global_secondary_index.value.write_capacity : null
    }
  }

  # Server-side encryption
  server_side_encryption {
    enabled     = true
    kms_key_arn = var.kms_key_arn
  }

  # Point-in-time recovery
  point_in_time_recovery {
    enabled = var.point_in_time_recovery_enabled
  }

  # DynamoDB TTL: when disabled, items are not auto-deleted (ttl_* attributes on items are ignored by AWS)
  ttl {
    attribute_name = var.ttl_attribute_name
    enabled        = var.ttl_enabled
  }

  tags = merge(var.tags, {
    Name    = local.table_name
    Type    = var.table_type
    Purpose = var.table_purpose
  })

  lifecycle {
    prevent_destroy = false
  }

  # Deletion protection must be off before AWS will delete the table. This runs
  # on destroy so a single apply can hibernate protected tables.
  provisioner "local-exec" {
    when    = destroy
    command = "aws dynamodb update-table --table-name ${self.name} --no-deletion-protection-enabled || true"
  }
}
