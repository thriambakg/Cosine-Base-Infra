# S3 Module - Security Compliant by Default

This S3 module creates secure S3 buckets that are compliant with security best practices out of the box.

## Security Features

### Compliance Checks Addressed:
- ✅ **CKV_AWS_300**: Lifecycle configuration with abort incomplete multipart uploads
- ✅ **CKV2_AWS_61**: Lifecycle configuration enabled by default
- ✅ **CKV_AWS_144**: Optional cross-region replication support
- ✅ **CKV_AWS_21**: Versioning enabled by default
- ✅ **CKV2_AWS_62**: Optional S3 event notifications
- ✅ **CKV2_AWS_6**: Public access block enforced
- ✅ **CKV_AWS_18**: Optional access logging

### Security Features:
- **Encryption**: Server-side encryption with customer-managed KMS keys
- **Versioning**: Always enabled for data protection
- **Public Access**: Completely blocked by default
- **Lifecycle Management**: Automated cleanup of incomplete uploads and old versions
- **Access Logging**: Optional centralized access logging
- **Cross-Region Replication**: Optional disaster recovery
- **Event Notifications**: Optional SNS notifications for bucket events

## Usage

```hcl
module "secure_bucket" {
  source = "./modules/s3"

  bucket_name   = "my-secure-bucket"
  environment   = "production"
  purpose       = "application-data"
  kms_key_arn   = module.kms.main_key_arn

  # Optional: Enable advanced features
  enable_cross_region_replication = true
  notification_topic_arn         = aws_sns_topic.bucket_notifications.arn
  access_log_bucket             = aws_s3_bucket.access_logs.id

  tags = {
    Project = "MyProject"
    Owner   = "DevOps"
  }
}
```

## Variables

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| bucket_name | Name of the S3 bucket | `string` | n/a | yes |
| environment | Environment name | `string` | n/a | yes |
| kms_key_arn | ARN of KMS key for encryption | `string` | n/a | yes |
| purpose | Purpose of the bucket | `string` | `"general"` | no |
| force_destroy | Allow bucket destruction with objects | `bool` | `false` | no |

## Outputs

| Name | Description |
|------|-------------|
| bucket_id | ID of the S3 bucket |
| bucket_arn | ARN of the S3 bucket |
| bucket_domain_name | Domain name of the S3 bucket |
| replica_bucket_id | ID of replica bucket (if enabled) |

## Security Defaults

- **Versioning**: Always enabled
- **Encryption**: KMS encryption required
- **Public Access**: Completely blocked
- **Multipart Upload Cleanup**: 7 days
- **Old Version Retention**: 30 days
- **Replication**: Optional, disabled by default
- **Lifecycle Transitions**: Optional, disabled by default
