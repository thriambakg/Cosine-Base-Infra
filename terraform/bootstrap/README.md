# Bootstrap Infrastructure

This directory contains the bootstrap infrastructure needed to set up Terraform remote state management for the Cosine project.

## Purpose

The bootstrap creates:
- **S3 Bucket**: Stores Terraform state files with versioning and encryption
- **DynamoDB Table**: Provides state locking to prevent concurrent modifications
- **Security**: Proper access controls, encryption, and lifecycle policies

## Usage

### First Time Setup

1. **Deploy the bootstrap infrastructure**:
   ```bash
   cd terraform/bootstrap
   terraform init
   terraform plan
   terraform apply
   ```

2. **Note the outputs** - you'll need these for backend configuration:
   ```bash
   terraform output
   ```

### Backend Configuration

After bootstrap is deployed, use these backend configurations:

#### Development
```hcl
# backend-configs/development.tfbackend
bucket         = "cosine-terraform-state-bucket"
key            = "base-infrastructure/development/terraform.tfstate"
region         = "us-east-1"
dynamodb_table = "cosine-terraform-locks"
encrypt        = true
```

#### Staging
```hcl
# backend-configs/staging.tfbackend
bucket         = "cosine-terraform-state-bucket"
key            = "base-infrastructure/staging/terraform.tfstate"
region         = "us-east-1"
dynamodb_table = "cosine-terraform-locks"
encrypt        = true
```

#### Production
```hcl
# backend-configs/production.tfbackend
bucket         = "cosine-terraform-state-bucket"
key            = "base-infrastructure/production/terraform.tfstate"
region         = "us-east-1"
dynamodb_table = "cosine-terraform-locks"
encrypt        = true
```

## Security Features

- **Encryption**: State files encrypted with AES256 or KMS
- **Versioning**: State file versions retained for rollback capability
- **Access Control**: Bucket policy prevents insecure connections
- **Lifecycle**: Automatic cleanup of old state file versions
- **Locking**: DynamoDB table prevents concurrent state modifications

## Important Notes

- **Run Once**: This bootstrap should only be run once per AWS account
- **State Storage**: The bootstrap itself uses local state (no remote backend)
- **Permissions**: Ensure your AWS credentials have sufficient permissions
- **Backup**: Consider backing up the bootstrap state file manually

## Cleanup

To destroy the bootstrap infrastructure (use with extreme caution):
```bash
terraform destroy
```

⚠️ **Warning**: This will delete all Terraform state files and lock table. Ensure all other infrastructure is destroyed first.
