# SNS Module

## Overview
This module creates a security-compliant AWS SNS topic with optional KMS encryption and various subscription types. The module follows AWS security best practices and is designed to pass compliance scans.

## Security Features
- **KMS Encryption**: Optional server-side encryption with customer-managed KMS keys
- **Access Control**: Restrictive topic policy with account-level isolation
- **Service Integration**: Controlled permissions for S3, CloudWatch, and Lambda services
- **Input Validation**: Comprehensive validation for email addresses, phone numbers, and URLs

## Usage

### Basic SNS Topic
```hcl
module "notifications" {
  source = "./modules/sns"
  
  topic_name   = "app-notifications"
  display_name = "Application Notifications"
  purpose      = "General application notifications"
  
  tags = {
    Environment = "production"
    Application = "myapp"
  }
}
```

### SNS Topic with KMS Encryption
```hcl
module "secure_notifications" {
  source = "./modules/sns"
  
  topic_name    = "secure-notifications"
  display_name  = "Secure Notifications"
  kms_key_arn   = module.kms.key_arn
  
  # Allow S3 to publish events
  allow_s3_publish = true
  s3_bucket_arns   = [module.s3_bucket.bucket_arn]
  
  # Email subscriptions
  email_addresses = [
    "admin@company.com",
    "alerts@company.com"
  ]
  
  tags = {
    Environment = "production"
    Encryption  = "kms"
  }
}
```

### SNS Topic with Multiple Subscription Types
```hcl
module "multi_notifications" {
  source = "./modules/sns"
  
  topic_name   = "multi-channel-alerts"
  display_name = "Multi-Channel Alerts"
  
  # Email notifications
  email_addresses = ["ops@company.com"]
  
  # SMS notifications (E.164 format)
  phone_numbers = ["+1234567890"]
  
  # SQS subscriptions with filtering
  sqs_subscriptions = {
    "error-queue" = {
      queue_arn = module.error_queue.queue_arn
      filter_policy = {
        event_type = ["error", "critical"]
      }
    }
    "warning-queue" = {
      queue_arn = module.warning_queue.queue_arn
      filter_policy = {
        event_type = ["warning"]
      }
    }
  }
  
  # Lambda function subscriptions
  lambda_function_arns = [
    module.alert_processor.function_arn
  ]
  
  # HTTP/HTTPS endpoints
  https_endpoints = [
    "https://webhook.company.com/alerts"
  ]
  
  tags = {
    Environment = "production"
    Purpose     = "multi-channel-alerting"
  }
}
```

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|----------|
| topic_name | Name of the SNS topic | `string` | n/a | yes |
| display_name | Display name for the SNS topic | `string` | `null` | no |
| kms_key_arn | ARN of KMS key for encryption | `string` | `null` | no |
| purpose | Purpose/description of the SNS topic | `string` | `"General notifications"` | no |
| tags | Tags to apply to SNS topic | `map(string)` | `{}` | no |
| allow_s3_publish | Allow S3 buckets to publish to this topic | `bool` | `false` | no |
| s3_bucket_arns | List of S3 bucket ARNs allowed to publish | `list(string)` | `null` | no |
| allow_cloudwatch_publish | Allow CloudWatch to publish to this topic | `bool` | `false` | no |
| allow_lambda_publish | Allow Lambda functions to publish to this topic | `bool` | `false` | no |
| email_addresses | List of email addresses to subscribe | `list(string)` | `[]` | no |
| phone_numbers | List of phone numbers (E.164 format) to subscribe | `list(string)` | `[]` | no |
| sqs_subscriptions | Map of SQS subscriptions to create | `map(object)` | `{}` | no |
| lambda_function_arns | List of Lambda function ARNs to subscribe | `list(string)` | `[]` | no |
| http_endpoints | List of HTTP endpoints to subscribe | `list(string)` | `[]` | no |
| https_endpoints | List of HTTPS endpoints to subscribe | `list(string)` | `[]` | no |

## Outputs

| Name | Description |
|------|-------------|
| topic_arn | ARN of the SNS topic |
| topic_name | Name of the SNS topic |
| topic_id | ID of the SNS topic |
| topic_owner | AWS account ID of the SNS topic owner |
| subscription_arns | Map of subscription ARNs by type |
| topic_policy | The JSON policy document of the SNS topic |

## Compliance Features

### Security Compliance
- **CKV_AWS_26**: SNS topic encrypted with KMS (when kms_key_arn is provided)
- **CKV_AWS_169**: SNS topic policy restricts access appropriately
- **CKV2_AWS_43**: SNS topic policy prevents cross-account access (uses AWS:SourceOwner condition)

### Operational Excellence
- **Input Validation**: Email format, phone number E.164 format, URL format validation
- **Resource Tagging**: Comprehensive tagging strategy
- **Flexible Subscriptions**: Support for multiple subscription types with filtering

## Dependencies
- AWS Provider >= 5.0
- KMS module (if using encryption)
- SQS module (if using SQS subscriptions)
- Lambda module (if using Lambda subscriptions)

## Notes
- Phone numbers must be in E.164 format (e.g., +1234567890)
- Email addresses are validated for proper format
- HTTP/HTTPS endpoints are validated for proper protocol
- Topic policy automatically includes account-level isolation
- KMS encryption is optional but recommended for sensitive notifications
