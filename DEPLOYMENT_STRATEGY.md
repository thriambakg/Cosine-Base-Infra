# Deployment Strategy Guide

## Overview

The Cosine Base Infrastructure has a two-tier deployment approach:

1. **Bootstrap** (Manual, One-time)
2. **Application Infrastructure** (Automated via GitHub Actions)

## 1. Bootstrap Deployment (Manual)

### Prerequisites
- AWS CLI configured with admin credentials
- Terraform >= 1.0 installed locally
- Sufficient AWS permissions to create S3 buckets and DynamoDB tables

### Steps

1. **Deploy Bootstrap Infrastructure**:
   ```bash
   cd terraform/bootstrap
   terraform init
   terraform plan
   terraform apply
   ```

2. **Verify Bootstrap**:
   ```bash
   # Check S3 bucket exists
   aws s3 ls s3://cosine-terraform-state-bucket/
   
   # Check DynamoDB table exists
   aws dynamodb describe-table --table-name cosine-terraform-locks
   ```

3. **Note Outputs**:
   ```bash
   terraform output
   ```

### Important Notes
- ⚠️ **Run only once per AWS account**
- ⚠️ **Keep bootstrap state file safe** (consider backing up locally)
- ⚠️ **Don't run this in CI/CD** - manual deployment only

## 2. Application Infrastructure (Automated)

Once bootstrap is complete, your GitHub Actions pipeline can deploy the main infrastructure.

### Pipeline Flow
1. **Trigger**: Push to main/staging/develop branches
2. **Initialize**: Uses S3 backend created by bootstrap
3. **Plan**: Creates deployment plan
4. **Apply**: Deploys infrastructure (with approval for production)

### Environment Mapping
- `develop` branch → `development` environment
- `staging` branch → `staging` environment  
- `main` branch → `production` environment

### Manual Override
Use `workflow_dispatch` to deploy any environment manually:
1. Go to GitHub Actions
2. Select "Deploy Base Infrastructure"
3. Click "Run workflow"
4. Choose environment and options

## 3. Required AWS Permissions

### Bootstrap Deployment (Admin)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:*",
        "dynamodb:*",
        "iam:GetRole",
        "iam:PassRole"
      ],
      "Resource": "*"
    }
  ]
}
```

### Pipeline Permissions (CI/CD)
Your GitHub Actions need these AWS permissions:
- S3 access to state bucket
- DynamoDB access to lock table
- Full permissions for resources being managed (Cognito, DynamoDB, KMS, CloudWatch)

## 4. State File Organization

After bootstrap, state files are organized as:
```
s3://cosine-terraform-state-bucket/
├── base-infrastructure/
│   ├── development/terraform.tfstate
│   ├── staging/terraform.tfstate
│   └── production/terraform.tfstate
└── application/                    # For main project
    ├── development/terraform.tfstate
    ├── staging/terraform.tfstate
    └── production/terraform.tfstate
```

## 5. Troubleshooting

### Bootstrap Issues
- **Bucket already exists**: Check if someone already deployed bootstrap
- **Permission denied**: Ensure AWS credentials have admin permissions
- **State corruption**: Bootstrap uses local state, keep the `.tfstate` file safe

### Pipeline Issues
- **Backend not found**: Ensure bootstrap was deployed successfully
- **State lock errors**: Check DynamoDB table exists and is accessible
- **Permission errors**: Verify GitHub Actions AWS credentials

## 6. Security Best Practices

### Bootstrap
- Use MFA for admin account
- Enable CloudTrail for audit logging
- Restrict access to bootstrap directory

### Pipeline
- Use OIDC for GitHub Actions (no long-lived credentials)
- Rotate AWS keys regularly
- Monitor CloudTrail for unexpected changes

## 7. Backup and Recovery

### State Backup
- S3 versioning is enabled automatically
- Consider cross-region replication for production
- Regular backups of bootstrap state file

### Disaster Recovery
- Document bootstrap deployment process
- Keep terraform code in version control
- Test restore procedures regularly
