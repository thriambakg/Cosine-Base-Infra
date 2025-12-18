# DynamoDB Module Refactor Guide

## Overview
This guide documents the refactor from a monolithic DynamoDB module to individual table module calls using a generic `dynamodb-table` module.

## Architecture Change

### Before (Monolithic):
```hcl
module "dynamodb" {
  source = "./modules/dynamodb"
  # All tables defined inside the module
}
```

### After (Modular):
```hcl
module "user_profiles_table" {
  source = "./modules/dynamodb-table"
  # User profiles specific config
}

module "news_table" {
  source = "./modules/dynamodb-table"
  # News specific config
}
# ... one module call per table
```

## Table Configurations

### 1. News Table (NEW - with title-based search GSI5)

```hcl
# News Table - for storing financial news articles
module "news_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "news"
  
  hash_key  = "PK"
  range_key = "SK"
  
  attributes = [
    { name = "PK", type = "S" },
    { name = "SK", type = "S" },
    { name = "GSI1PK", type = "S" },
    { name = "GSI1SK", type = "S" },
    { name = "GSI2PK", type = "S" },
    { name = "GSI2SK", type = "S" },
    { name = "GSI3PK", type = "S" },
    { name = "GSI3SK", type = "S" },
    { name = "GSI4PK", type = "S" },
    { name = "GSI4SK", type = "S" },
    { name = "GSI5PK", type = "S" },
    { name = "GSI5SK", type = "S" }
  ]
  
  global_secondary_indexes = [
    {
      name            = "GSI1"
      hash_key        = "GSI1PK"
      range_key       = "GSI1SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "GSI2"
      hash_key        = "GSI2PK"
      range_key       = "GSI2SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "GSI3"
      hash_key        = "GSI3PK"
      range_key       = "GSI3SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "GSI4"
      hash_key        = "GSI4PK"
      range_key       = "GSI4SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "GSI5"  # Title-based search
      hash_key        = "GSI5PK"
      range_key       = "GSI5SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    }
  ]
  
  billing_mode                   = var.dynamodb_billing_mode
  read_capacity                  = var.dynamodb_read_capacity
  write_capacity                 = var.dynamodb_write_capacity
  stream_enabled                 = var.dynamodb_stream_enabled
  stream_view_type               = var.dynamodb_stream_view_type
  point_in_time_recovery_enabled = var.dynamodb_point_in_time_recovery_enabled
  deletion_protection_enabled    = var.dynamodb_deletion_protection_enabled
  ttl_enabled                    = true
  ttl_attribute_name             = "ttl"
  
  kms_key_arn                    = module.kms.dynamodb_key_arn
  
  table_type    = "NewsData"
  table_purpose = "FinancialNews"
  
  additional_iam_policy_statements = [
    {
      Effect = "Allow"
      Action = [
        "comprehend:DetectKeyPhrases",
        "comprehend:DetectEntities",
        "comprehend:DetectSentiment"
      ]
      Resource = "*"
    }
  ]
  
  tags = var.common_tags
  
  depends_on = [module.kms]
}
```

### 2. User Profiles Table

```hcl
module "user_profiles_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "user-profiles"
  
  hash_key  = "user_id"
  range_key = null
  
  attributes = [
    { name = "user_id", type = "S" },
    { name = "email", type = "S" },
    { name = "created_at", type = "S" }
  ]
  
  global_secondary_indexes = [
    {
      name            = "EmailIndex"
      hash_key        = "email"
      range_key       = null
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "CreatedAtIndex"
      hash_key        = "created_at"
      range_key       = null
      projection_type = "KEYS_ONLY"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    }
  ]
  
  billing_mode                   = var.dynamodb_billing_mode
  read_capacity                  = var.dynamodb_read_capacity
  write_capacity                 = var.dynamodb_write_capacity
  stream_enabled                 = var.dynamodb_stream_enabled
  stream_view_type               = var.dynamodb_stream_view_type
  point_in_time_recovery_enabled = var.dynamodb_point_in_time_recovery_enabled
  deletion_protection_enabled    = var.dynamodb_deletion_protection_enabled
  ttl_enabled                    = var.dynamodb_ttl_enabled
  ttl_attribute_name             = var.dynamodb_ttl_attribute_name
  
  kms_key_arn = module.kms.dynamodb_key_arn
  
  table_type    = "UserData"
  table_purpose = "UserProfiles"
  
  tags = var.common_tags
  
  depends_on = [module.kms]
}
```

### 3. Stock Data Table

```hcl
module "stock_data_table" {
  source = "./modules/dynamodb-table"

  project_name = var.project_name
  environment  = var.environment
  table_name   = "stock-data"
  
  hash_key  = "PK"
  range_key = "SK"
  
  attributes = [
    { name = "PK", type = "S" },
    { name = "SK", type = "S" },
    { name = "GSI1PK", type = "S" },
    { name = "GSI1SK", type = "S" },
    { name = "GSI2PK", type = "S" },
    { name = "GSI2SK", type = "S" },
    { name = "GSI3PK", type = "S" },
    { name = "GSI3SK", type = "S" },
    { name = "GSI4PK", type = "S" },
    { name = "GSI4SK", type = "S" },
    { name = "GSI5PK", type = "S" },
    { name = "GSI5SK", type = "S" }
  ]
  
  global_secondary_indexes = [
    {
      name            = "IndustryIndex"
      hash_key        = "GSI1PK"
      range_key       = "GSI1SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "VolatilityIndex"
      hash_key        = "GSI2PK"
      range_key       = "GSI2SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "PriceChangeIndex"
      hash_key        = "GSI3PK"
      range_key       = "GSI3SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "MarketCapIndex"
      hash_key        = "GSI4PK"
      range_key       = "GSI4SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    },
    {
      name            = "PriceIndex"
      hash_key        = "GSI5PK"
      range_key       = "GSI5SK"
      projection_type = "ALL"
      read_capacity   = var.dynamodb_gsi_read_capacity
      write_capacity  = var.dynamodb_gsi_write_capacity
    }
  ]
  
  billing_mode                   = var.dynamodb_billing_mode
  read_capacity                  = var.dynamodb_read_capacity
  write_capacity                 = var.dynamodb_write_capacity
  stream_enabled                 = var.dynamodb_stream_enabled
  stream_view_type               = var.dynamodb_stream_view_type
  point_in_time_recovery_enabled = var.dynamodb_point_in_time_recovery_enabled
  deletion_protection_enabled    = var.dynamodb_deletion_protection_enabled
  ttl_enabled                    = var.dynamodb_ttl_enabled
  ttl_attribute_name             = "expires_at"
  
  kms_key_arn = module.kms.dynamodb_key_arn
  
  table_type    = "StockData"
  table_purpose = "RealTimeStockData"
  
  iam_policy_actions = [
    "dynamodb:GetItem",
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:DeleteItem",
    "dynamodb:Query",
    "dynamodb:Scan",
    "dynamodb:BatchGetItem",
    "dynamodb:BatchWriteItem"
  ]
  
  tags = var.common_tags
  
  depends_on = [module.kms]
}
```

## GSI Purpose Summary

### News Table GSIs:
- **GSI1:** Category-based search (e.g., `CATEGORY#technology`)
- **GSI2:** Sentiment-based search (e.g., `SENTIMENT#positive`)
- **GSI3:** AI Tag-based search (e.g., `TAG#earnings`)
- **GSI4:** Source-based search (e.g., `SOURCE#Reuters`)
- **GSI5:** Title-based search using `contains` filter (✨ NEW)
  - `GSI5PK`: `"TITLE_SEARCH"` (constant for all articles)
  - `GSI5SK`: Full article title for `contains()` filtering

## Migration Steps

1. ✅ Create generic `dynamodb-table` module
2. ⏳ Update `main.tf` with separate module calls for each table
3. ⏳ Update outputs to reference new module structure
4. ⏳ Test with `terraform plan`
5. ⏳ Deploy with `terraform apply`
6. ⏳ Remove old monolithic `dynamodb` module

## Benefits

- ✅ **Modularity:** Each table is independently configurable
- ✅ **Flexibility:** Easy to add/remove/modify individual tables
- ✅ **Maintainability:** Clear separation of concerns
- ✅ **Reusability:** Generic module can be used across projects
- ✅ **Title-based search:** New GSI5 enables efficient title searches for news articles

