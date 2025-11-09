# Generic OpenSearch Domain Module
# Creates an AWS OpenSearch Service domain with configurable settings

resource "aws_opensearch_domain" "this" {
  domain_name    = "${var.project_name}-${var.domain_name}-${var.environment}"
  engine_version = var.engine_version

  # Cluster configuration
  cluster_config {
    instance_type            = var.instance_type
    instance_count           = var.instance_count
    dedicated_master_enabled = var.dedicated_master_enabled
    dedicated_master_type    = var.dedicated_master_enabled ? var.dedicated_master_type : null
    dedicated_master_count   = var.dedicated_master_enabled ? var.dedicated_master_count : null
    zone_awareness_enabled   = var.zone_awareness_enabled

    dynamic "zone_awareness_config" {
      for_each = var.zone_awareness_enabled ? [1] : []
      content {
        availability_zone_count = var.availability_zone_count
      }
    }
  }

  # EBS volume configuration
  ebs_options {
    ebs_enabled = var.ebs_enabled
    volume_type = var.ebs_enabled ? var.ebs_volume_type : null
    volume_size = var.ebs_enabled ? var.ebs_volume_size : null
    iops        = var.ebs_enabled && var.ebs_volume_type == "io1" ? var.ebs_iops : null
    throughput  = var.ebs_enabled && var.ebs_volume_type == "gp3" ? var.ebs_throughput : null
  }

  # VPC configuration (optional)
  dynamic "vpc_options" {
    for_each = var.vpc_enabled ? [1] : []
    content {
      subnet_ids         = var.subnet_ids
      security_group_ids = var.security_group_ids
    }
  }

  # Encryption at rest
  encrypt_at_rest {
    enabled    = true
    kms_key_id = var.kms_key_arn
  }

  # Node-to-node encryption
  node_to_node_encryption {
    enabled = var.node_to_node_encryption_enabled
  }

  # Domain endpoint options
  domain_endpoint_options {
    enforce_https       = var.enforce_https
    tls_security_policy = var.tls_security_policy
  }

  # Advanced security options
  advanced_security_options {
    enabled                        = var.advanced_security_enabled
    internal_user_database_enabled = var.internal_user_database_enabled
    master_user_options {
      master_user_arn      = var.master_user_arn != null ? var.master_user_arn : null
      master_user_name     = var.master_user_name != null ? var.master_user_name : null
      master_user_password = var.master_user_password != null ? var.master_user_password : null
    }
  }

  # Access policy
  access_policies = var.access_policy_json != null ? var.access_policy_json : jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          AWS = "*"
        }
        Action   = "es:*"
        Resource = "arn:aws:es:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:domain/${var.project_name}-${var.domain_name}-${var.environment}/*"
      }
    ]
  })

  # Log publishing options
  dynamic "log_publishing_options" {
    for_each = var.log_publishing_options
    content {
      log_type                 = log_publishing_options.value.log_type
      cloudwatch_log_group_arn = log_publishing_options.value.cloudwatch_log_group_arn
      enabled                  = log_publishing_options.value.enabled
    }
  }

  # Advanced options
  advanced_options = var.advanced_options

  # Tags
  tags = merge(
    var.common_tags,
    {
      Name        = "${var.project_name}-${var.domain_name}-${var.environment}"
      Environment = var.environment
      Project     = var.project_name
    }
  )
}

# Data sources for current region and account
data "aws_region" "current" {}
data "aws_caller_identity" "current" {}

# OpenSearch Domain Policy (for fine-grained access control)
resource "aws_opensearch_domain_policy" "this" {
  count           = var.domain_policy_json != null ? 1 : 0
  domain_name     = aws_opensearch_domain.this.domain_name
  access_policies = var.domain_policy_json
}

