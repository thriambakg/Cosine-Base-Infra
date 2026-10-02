# KMS Module
# modules/kms/main.tf

# Data source for current AWS account and region
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Main KMS key for general encryption
# Note: This key policy includes CloudFront service principal to allow CloudFront to decrypt S3 objects
resource "aws_kms_key" "main" {
  count = var.create ? 1 : 0

  description              = "Main KMS key for ${var.project_name} ${var.environment}"
  key_usage                = "ENCRYPT_DECRYPT"
  customer_master_key_spec = "SYMMETRIC_DEFAULT"

  # Key rotation
  enable_key_rotation = var.enable_key_rotation

  # Deletion window
  deletion_window_in_days = var.deletion_window_in_days

  # Key policy
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid    = "EnableRootAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      }],
      length(var.key_administrators) > 0 ? [{
        Sid    = "AllowKeyAdministrators"
        Effect = "Allow"
        Principal = {
          AWS = var.key_administrators
        }
        Action = [
          "kms:Create*",
          "kms:Describe*",
          "kms:Enable*",
          "kms:List*",
          "kms:Put*",
          "kms:Update*",
          "kms:Revoke*",
          "kms:Disable*",
          "kms:Get*",
          "kms:Delete*",
          "kms:TagResource",
          "kms:UntagResource",
          "kms:ScheduleKeyDeletion",
          "kms:CancelKeyDeletion"
        ]
        Resource = "*"
      }] : [],
      length(var.additional_role_arns) > 0 ? [{
        Sid    = "AllowAdditionalRoles"
        Effect = "Allow"
        Principal = {
          AWS = var.additional_role_arns
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
        }] : [], [
        {
          Sid    = "AllowServiceUsage"
          Effect = "Allow"
          Principal = {
            Service = var.allowed_services
          }
          Action = [
            "kms:Encrypt",
            "kms:Decrypt",
            "kms:ReEncrypt*",
            "kms:GenerateDataKey*",
            "kms:DescribeKey"
          ]
          Resource = "*"
    }])
  })

  tags = merge(var.tags, {
    Name    = "${var.project_name}-main-key-${var.environment}"
    Type    = "KMSKey"
    Purpose = "GeneralEncryption"
  })
}

# KMS alias for the main key
resource "aws_kms_alias" "main" {
  count = var.create ? 1 : 0

  name          = "alias/${var.project_name}-main-${var.environment}"
  target_key_id = aws_kms_key.main[0].key_id
}

# DynamoDB-specific KMS key
resource "aws_kms_key" "dynamodb" {
  count = var.create ? 1 : 0

  description              = "DynamoDB encryption key for ${var.project_name} ${var.environment}"
  key_usage                = "ENCRYPT_DECRYPT"
  customer_master_key_spec = "SYMMETRIC_DEFAULT"

  # Key rotation
  enable_key_rotation = var.enable_key_rotation

  # Deletion window
  deletion_window_in_days = var.deletion_window_in_days

  # Key policy specific for DynamoDB
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid    = "EnableRootAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      }],
      length(var.key_administrators) > 0 ? [{
        Sid    = "AllowKeyAdministrators"
        Effect = "Allow"
        Principal = {
          AWS = var.key_administrators
        }
        Action = [
          "kms:Create*",
          "kms:Describe*",
          "kms:Enable*",
          "kms:List*",
          "kms:Put*",
          "kms:Update*",
          "kms:Revoke*",
          "kms:Disable*",
          "kms:Get*",
          "kms:Delete*",
          "kms:TagResource",
          "kms:UntagResource",
          "kms:ScheduleKeyDeletion",
          "kms:CancelKeyDeletion"
        ]
        Resource = "*"
      }] : [],
      length(var.additional_role_arns) > 0 ? [{
        Sid    = "AllowAdditionalRoles"
        Effect = "Allow"
        Principal = {
          AWS = var.additional_role_arns
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
        }] : [], [
        {
          Sid    = "AllowDynamoDBService"
          Effect = "Allow"
          Principal = {
            Service = "dynamodb.amazonaws.com"
          }
          Action = [
            "kms:Encrypt",
            "kms:Decrypt",
            "kms:ReEncrypt*",
            "kms:GenerateDataKey*",
            "kms:DescribeKey"
          ]
          Resource = "*"
    }])
  })

  tags = merge(var.tags, {
    Name    = "${var.project_name}-dynamodb-key-${var.environment}"
    Type    = "KMSKey"
    Purpose = "DynamoDBEncryption"
  })
}

# KMS alias for DynamoDB key
resource "aws_kms_alias" "dynamodb" {
  count = var.create ? 1 : 0

  name          = "alias/${var.project_name}-dynamodb-${var.environment}"
  target_key_id = aws_kms_key.dynamodb[0].key_id
}

# CloudWatch Logs KMS key
resource "aws_kms_key" "cloudwatch" {
  count = var.create ? 1 : 0

  description              = "CloudWatch Logs encryption key for ${var.project_name} ${var.environment}"
  key_usage                = "ENCRYPT_DECRYPT"
  customer_master_key_spec = "SYMMETRIC_DEFAULT"

  # Key rotation
  enable_key_rotation = var.enable_key_rotation

  # Deletion window
  deletion_window_in_days = var.deletion_window_in_days

  # Key policy for CloudWatch Logs
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid    = "EnableRootAccess"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      }],
      length(var.key_administrators) > 0 ? [{
        Sid    = "AllowKeyAdministrators"
        Effect = "Allow"
        Principal = {
          AWS = var.key_administrators
        }
        Action = [
          "kms:Create*",
          "kms:Describe*",
          "kms:Enable*",
          "kms:List*",
          "kms:Put*",
          "kms:Update*",
          "kms:Revoke*",
          "kms:Disable*",
          "kms:Get*",
          "kms:Delete*",
          "kms:TagResource",
          "kms:UntagResource",
          "kms:ScheduleKeyDeletion",
          "kms:CancelKeyDeletion"
        ]
        Resource = "*"
        }] : [], [
        {
          Sid    = "AllowCloudWatchLogs"
          Effect = "Allow"
          Principal = {
            Service = "logs.${data.aws_region.current.name}.amazonaws.com"
          }
          Action = [
            "kms:Encrypt",
            "kms:Decrypt",
            "kms:ReEncrypt*",
            "kms:GenerateDataKey*",
            "kms:DescribeKey"
          ]
          Resource = "*"
    }])
  })

  tags = merge(var.tags, {
    Name    = "${var.project_name}-cloudwatch-key-${var.environment}"
    Type    = "KMSKey"
    Purpose = "CloudWatchLogsEncryption"
  })
}

# KMS alias for CloudWatch key
resource "aws_kms_alias" "cloudwatch" {
  count = var.create ? 1 : 0

  name          = "alias/${var.project_name}-cloudwatch-${var.environment}"
  target_key_id = aws_kms_key.cloudwatch[0].key_id
}

# IAM policy for KMS access
# Kept when keys are disabled (consumers attach it by ARN); it then only grants DescribeKey, which is metadata-only.
resource "aws_iam_policy" "kms_access_policy" {
  name        = "${var.project_name}-kms-access-policy-${var.environment}"
  description = "IAM policy for accessing KMS keys"
  path        = "/"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = var.create ? tolist([
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]) : tolist(["kms:DescribeKey"])
        Resource = var.create ? tolist([
          aws_kms_key.main[0].arn,
          aws_kms_key.dynamodb[0].arn,
          aws_kms_key.cloudwatch[0].arn
        ]) : tolist(["arn:aws:kms:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:key/*"])
      }
    ]
  })

  tags = merge(var.tags, {
    Name    = "${var.project_name}-kms-access-policy-${var.environment}"
    Type    = "IAMPolicy"
    Purpose = "KMSAccess"
  })
}
