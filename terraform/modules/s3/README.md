# S3 Module

A security-compliant S3 bucket module with encryption, versioning, lifecycle management, and optional static file uploads.

## Features

- **Always Enabled:**
  - Versioning (CKV_AWS_21)
  - Server-side encryption with KMS (CKV_AWS_19)
  - Access logging (CKV_AWS_18) - configurable
  - Lifecycle configuration (CKV_AWS_300)
  - Public access block (CKV_AWS_54)

- **Optional:**
  - Cross-region replication (CKV_AWS_144)
  - Lifecycle transitions (IA, Glacier)
  - Object expiration
  - Static file uploads
  - CloudFront OAC compatibility

## Static Files Upload

The module supports uploading static files during bucket creation via the `static_files` parameter:

```hcl
module "my_bucket" {
  source = "./modules/s3"
  
  bucket_name = "my-bucket"
  environment = "production"
  kms_key_arn = module.kms.main_key_arn
  
  # Upload multiple static files
  static_files = [
    {
      source_path  = "${path.module}/../static-files/data/reference.csv"
      s3_key       = "reference.csv"
      content_type = "text/csv"
    },
    {
      source_path  = "${path.module}/../static-files/config/settings.json"
      s3_key       = "config/settings.json"
      content_type = "application/json"
    }
  ]
}
```

### Static Files Parameters

- `source_path` (required): Path to the source file, relative to where Terraform is executed (typically the Terraform root directory)
- `s3_key` (required): Destination S3 key/path where the file will be stored
- `content_type` (optional): MIME type of the file. If not specified, Terraform will attempt to detect it

### Features

- **Automatic Change Detection:** Uses file hash (`filemd5`) to detect changes and trigger updates
- **Encryption:** Inherits bucket-level encryption settings
- **Tagging:** Automatically tags files with source path and purpose
- **Idempotent:** Safe to run multiple times - only updates if file content changes

## Usage Example

See `main.tf` for the politician trades bucket example:

```hcl
module "politician_trades_s3" {
  source = "./modules/s3"
  
  bucket_name = "${var.project_name}-politician-trades-${var.environment}"
  environment = var.environment
  purpose     = "PoliticianTradesData"
  kms_key_arn = module.kms.main_key_arn
  
  static_files = [
    {
      source_path  = "${path.module}/../static-files/lists/politicians.csv"
      s3_key       = "politicians.csv"
      content_type = "text/csv"
    }
  ]
}
```
