# DynamoDB Module
# modules/dynamodb/main.tf

# KMS key for DynamoDB encryption
resource "aws_kms_key" "dynamodb" {
  description             = "KMS key for DynamoDB table encryption"
  enable_key_rotation     = true
  deletion_window_in_days = 10

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow use of the key for DynamoDB"
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
      }
    ]
  })
}

# Get current AWS account info for KMS policy
data "aws_caller_identity" "current" {}

# User Profiles Table
resource "aws_dynamodb_table" "user_profiles" {
  name                        = "${var.project_name}-user-profiles-${var.environment}"
  billing_mode                = var.billing_mode
  hash_key                    = "user_id"
  stream_enabled              = var.stream_enabled
  stream_view_type            = var.stream_enabled ? var.stream_view_type : null
  deletion_protection_enabled = var.deletion_protection_enabled

  # Capacity settings for provisioned mode
  read_capacity  = var.billing_mode == "PROVISIONED" ? var.read_capacity : null
  write_capacity = var.billing_mode == "PROVISIONED" ? var.write_capacity : null

  attribute {
    name = "user_id"
    type = "S"
  }

  attribute {
    name = "email"
    type = "S"
  }

  attribute {
    name = "created_at"
    type = "S"
  }

  # Global Secondary Index for email lookups
  global_secondary_index {
    name            = "EmailIndex"
    hash_key        = "email"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Global Secondary Index for timestamp queries
  global_secondary_index {
    name            = "CreatedAtIndex"
    hash_key        = "created_at"
    projection_type = "KEYS_ONLY"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Server-side encryption
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.dynamodb.arn
  }

  # Point-in-time recovery
  point_in_time_recovery {
    enabled = var.point_in_time_recovery_enabled
  }

  # TTL configuration
  ttl {
    attribute_name = var.ttl_attribute_name
    enabled        = var.ttl_enabled
  }

  tags = merge(var.tags, {
    Name    = "${var.project_name}-user-profiles-${var.environment}"
    Type    = "UserData"
    Purpose = "UserProfiles"
  })

  lifecycle {
    prevent_destroy = false
  }
}

# IAM Policy for Lambda functions to access user_profiles table
resource "aws_iam_policy" "user_profiles_table_policy" {
  name        = "${var.project_name}-user-profiles-table-policy-${var.environment}"
  description = "Policy for Lambda functions to access user_profiles DynamoDB table"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = [
          aws_dynamodb_table.user_profiles.arn,
          "${aws_dynamodb_table.user_profiles.arn}/index/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.dynamodb.arn
      }
    ]
  })

  tags = var.tags
}

# Security Events Table
resource "aws_dynamodb_table" "security_events" {
  name                        = "${var.project_name}-security-events-${var.environment}"
  billing_mode                = var.billing_mode
  hash_key                    = "event_id"
  range_key                   = "timestamp"
  stream_enabled              = var.stream_enabled
  stream_view_type            = var.stream_enabled ? var.stream_view_type : null
  deletion_protection_enabled = var.deletion_protection_enabled

  # Capacity settings for provisioned mode
  read_capacity  = var.billing_mode == "PROVISIONED" ? var.read_capacity : null
  write_capacity = var.billing_mode == "PROVISIONED" ? var.write_capacity : null

  attribute {
    name = "event_id"
    type = "S"
  }

  attribute {
    name = "timestamp"
    type = "S"
  }

  attribute {
    name = "user_id"
    type = "S"
  }

  attribute {
    name = "event_type"
    type = "S"
  }

  # Global Secondary Index for user-based queries
  global_secondary_index {
    name            = "UserIndex"
    hash_key        = "user_id"
    range_key       = "timestamp"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Global Secondary Index for event type queries
  global_secondary_index {
    name            = "EventTypeIndex"
    hash_key        = "event_type"
    range_key       = "timestamp"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Server-side encryption
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.dynamodb.arn
  }

  # Point-in-time recovery
  point_in_time_recovery {
    enabled = var.point_in_time_recovery_enabled
  }

  # TTL for automatic cleanup (security events older than 1 year)
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  tags = merge(var.tags, {
    Name    = "${var.project_name}-security-events-${var.environment}"
    Type    = "SecurityData"
    Purpose = "AuditLogs"
  })

  lifecycle {
    prevent_destroy = false
  }
}

# REMOVED: User Sessions Table - consolidated into chat_sessions table

# Alerts Table
resource "aws_dynamodb_table" "alerts" {
  name                        = "${var.project_name}-alerts-${var.environment}"
  billing_mode                = var.billing_mode
  hash_key                    = "alert_status"
  range_key                   = "created_at"
  stream_enabled              = var.stream_enabled
  stream_view_type            = var.stream_enabled ? var.stream_view_type : null
  deletion_protection_enabled = var.deletion_protection_enabled

  # Capacity settings for provisioned mode
  read_capacity  = var.billing_mode == "PROVISIONED" ? var.read_capacity : null
  write_capacity = var.billing_mode == "PROVISIONED" ? var.write_capacity : null

  attribute {
    name = "alert_status"
    type = "S"
  }

  attribute {
    name = "created_at"
    type = "S"
  }

  attribute {
    name = "alert_id"
    type = "S"
  }

  attribute {
    name = "user_email"
    type = "S"
  }

  # Global Secondary Index for alert ID lookups
  global_secondary_index {
    name            = "AlertIdIndex"
    hash_key        = "alert_id"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Global Secondary Index for user-based alert queries (using user_email as hash key)
  global_secondary_index {
    name            = "UserAlertsIndex"
    hash_key        = "user_email"
    range_key       = "created_at"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Server-side encryption
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.dynamodb.arn
  }

  # Point-in-time recovery
  point_in_time_recovery {
    enabled = var.point_in_time_recovery_enabled
  }

  # TTL for automatic cleanup of triggered alerts (30 days after triggered)
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  tags = merge(var.tags, {
    Name    = "${var.project_name}-alerts-${var.environment}"
    Type    = "AlertData"
    Purpose = "StockAlerts"
  })

  lifecycle {
    prevent_destroy = false
  }
}

# Chat Connections Table - for managing active WebSocket connections
resource "aws_dynamodb_table" "chat_connections" {
  name                        = "${var.project_name}-chat-connections-${var.environment}"
  billing_mode                = var.billing_mode
  hash_key                    = "connection_id"
  stream_enabled              = var.stream_enabled
  stream_view_type            = var.stream_enabled ? var.stream_view_type : null
  deletion_protection_enabled = var.deletion_protection_enabled

  # Capacity settings for provisioned mode
  read_capacity  = var.billing_mode == "PROVISIONED" ? var.read_capacity : null
  write_capacity = var.billing_mode == "PROVISIONED" ? var.write_capacity : null

  attribute {
    name = "connection_id"
    type = "S"
  }

  attribute {
    name = "user_id"
    type = "S"
  }

  attribute {
    name = "session_id"
    type = "S"
  }

  # Global Secondary Index for user-based connection queries
  global_secondary_index {
    name            = "UserConnectionsIndex"
    hash_key        = "user_id"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Global Secondary Index for session-based connection queries
  global_secondary_index {
    name            = "SessionConnectionsIndex"
    hash_key        = "session_id"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Server-side encryption
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.dynamodb.arn
  }

  # Point-in-time recovery
  point_in_time_recovery {
    enabled = var.point_in_time_recovery_enabled
  }

  # TTL for automatic cleanup of stale connections (24 hours)
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  tags = merge(var.tags, {
    Name    = "${var.project_name}-chat-connections-${var.environment}"
    Type    = "ConnectionData"
    Purpose = "WebSocketConnections"
  })

  lifecycle {
    prevent_destroy = false
  }
}

# Chat Sessions Table - for storing complete chat sessions
resource "aws_dynamodb_table" "chat_sessions" {
  name                        = "${var.project_name}-chat-sessions-${var.environment}"
  billing_mode                = var.billing_mode
  hash_key                    = "user_id"
  range_key                   = "session_id"
  stream_enabled              = var.stream_enabled
  stream_view_type            = var.stream_enabled ? var.stream_view_type : null
  deletion_protection_enabled = false

  # Capacity settings for provisioned mode
  read_capacity  = var.billing_mode == "PROVISIONED" ? var.read_capacity : null
  write_capacity = var.billing_mode == "PROVISIONED" ? var.write_capacity : null

  attribute {
    name = "user_id"
    type = "S"
  }

  attribute {
    name = "session_id"
    type = "S"
  }

  attribute {
    name = "created_at"
    type = "N"
  }

  # Global Secondary Index for querying sessions by creation date
  global_secondary_index {
    name            = "CreatedAtIndex"
    hash_key        = "user_id"
    range_key       = "created_at"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Server-side encryption
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.dynamodb.arn
  }

  # Point-in-time recovery
  point_in_time_recovery {
    enabled = var.point_in_time_recovery_enabled
  }

  # TTL for automatic cleanup of old sessions (30 days)
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  tags = merge(var.tags, {
    Name    = "${var.project_name}-chat-sessions-${var.environment}"
    Type    = "ChatData"
    Purpose = "CompleteChatSessions"
  })

  lifecycle {
    prevent_destroy = false
  }
}

# Stock Data Table - for storing real-time and historical stock data
resource "aws_dynamodb_table" "stock_data" {
  name                        = "${var.project_name}-stock-data-${var.environment}"
  billing_mode                = var.billing_mode
  hash_key                    = "PK"
  range_key                   = "SK"
  stream_enabled              = var.stream_enabled
  stream_view_type            = var.stream_enabled ? var.stream_view_type : null
  deletion_protection_enabled = var.deletion_protection_enabled

  # Capacity settings for provisioned mode
  read_capacity  = var.billing_mode == "PROVISIONED" ? var.read_capacity : null
  write_capacity = var.billing_mode == "PROVISIONED" ? var.write_capacity : null

  attribute {
    name = "PK"
    type = "S"
  }

  attribute {
    name = "SK"
    type = "S"
  }

  attribute {
    name = "GSI1PK"
    type = "S"
  }

  attribute {
    name = "GSI1SK"
    type = "S"
  }

  attribute {
    name = "GSI2PK"
    type = "S"
  }

  attribute {
    name = "GSI2SK"
    type = "S"
  }

  attribute {
    name = "GSI3PK"
    type = "S"
  }

  attribute {
    name = "GSI3SK"
    type = "S"
  }

  attribute {
    name = "GSI4PK"
    type = "S"
  }

  attribute {
    name = "GSI4SK"
    type = "S"
  }

  attribute {
    name = "GSI5PK"
    type = "S"
  }

  attribute {
    name = "GSI5SK"
    type = "S"
  }

  # Global Secondary Index 1: Industry-Based Screening
  global_secondary_index {
    name            = "IndustryIndex"
    hash_key        = "GSI1PK"
    range_key       = "GSI1SK"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Global Secondary Index 2: Volatility-Based Screening
  global_secondary_index {
    name            = "VolatilityIndex"
    hash_key        = "GSI2PK"
    range_key       = "GSI2SK"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Global Secondary Index 3: Price Change-Based Screening
  global_secondary_index {
    name            = "PriceChangeIndex"
    hash_key        = "GSI3PK"
    range_key       = "GSI3SK"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Global Secondary Index 4: Market Cap-Based Screening
  global_secondary_index {
    name            = "MarketCapIndex"
    hash_key        = "GSI4PK"
    range_key       = "GSI4SK"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Global Secondary Index 5: Price-Based Screening
  global_secondary_index {
    name            = "PriceIndex"
    hash_key        = "GSI5PK"
    range_key       = "GSI5SK"
    projection_type = "ALL"

    read_capacity  = var.billing_mode == "PROVISIONED" ? var.gsi_read_capacity : null
    write_capacity = var.billing_mode == "PROVISIONED" ? var.gsi_write_capacity : null
  }

  # Server-side encryption
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.dynamodb.arn
  }

  # Point-in-time recovery
  point_in_time_recovery {
    enabled = var.point_in_time_recovery_enabled
  }

  # TTL for automatic cleanup of historical data (optional)
  ttl {
    attribute_name = "expires_at"
    enabled        = var.ttl_enabled
  }

  tags = merge(var.tags, {
    Name    = "${var.project_name}-stock-data-${var.environment}"
    Type    = "StockData"
    Purpose = "RealTimeStockData"
  })

  lifecycle {
    prevent_destroy = false
  }
}

# IAM Policy for Lambda functions to access stock_data table
resource "aws_iam_policy" "stock_data_table_policy" {
  name        = "${var.project_name}-stock-data-table-policy-${var.environment}"
  description = "Policy for Lambda functions to access stock_data DynamoDB table"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:BatchGetItem",
          "dynamodb:BatchWriteItem"
        ]
        Resource = [
          aws_dynamodb_table.stock_data.arn,
          "${aws_dynamodb_table.stock_data.arn}/index/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = aws_kms_key.dynamodb.arn
      }
    ]
  })

  tags = var.tags
}
