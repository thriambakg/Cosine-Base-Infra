# 🚀 Standardized CI/CD Pipeline - Base Infrastructure

## Overview

This repository now uses a standardized, reusable CI/CD pipeline that is shared with the main application repository (Cosine2.0). This ensures consistent deployment practices across all infrastructure components.

## 📋 Pipeline Structure

```
.github/workflows/
├── deploy-base-infrastructure.yml  # Main workflow (calls reusable pipeline)
└── reusable/
    └── terraform-pipeline.yml     # Shared reusable pipeline
```

## 🎯 Base Infrastructure Responsibilities

This repository manages the foundational AWS infrastructure:
- **🏗️ VPC & Networking**: Virtual Private Cloud, subnets, route tables
- **🔐 Security Groups**: Network access controls and firewall rules
- **🗃️ Shared Storage**: S3 buckets for application data and assets
- **⚡ Lambda Layers**: Shared code libraries for serverless functions
- **🔑 IAM Roles**: Service roles and policies for applications
- **📊 Monitoring**: CloudWatch log groups and basic monitoring

## 🚀 Deployment Process

### Automatic Deployments
- **main branch** → Production environment
- **develop branch** → Staging environment
- **staging branch** → Staging environment

### Manual Deployments
Use GitHub Actions workflow dispatch:
1. Go to **Actions** tab
2. Select **Deploy Base Infrastructure**
3. Click **Run workflow**
4. Choose environment: development, staging, or production
5. Optionally enable destroy mode

## 🔧 Environment Configuration

### Backend State Storage
```yaml
Development:   base-infra/development/terraform.tfstate
Staging:       base-infra/staging/terraform.tfstate
Production:    base-infra/production/terraform.tfstate
```

### Required GitHub Secrets
- `AWS_ACCESS_KEY_ID`: AWS access key for deployments
- `AWS_SECRET_ACCESS_KEY`: AWS secret key for deployments

## 📊 Pipeline Features

✅ **Security Scanning**: TFLint, tfsec, Checkov
✅ **Multi-Environment**: Development, Staging, Production
✅ **State Management**: S3 backend with DynamoDB locking
✅ **Plan Artifacts**: Upload/download between plan and apply
✅ **Comprehensive Logging**: Detailed deployment summaries
✅ **Destroy Support**: Safe infrastructure teardown

## 🔗 Application Dependencies

The main application repository (Cosine2.0) **depends** on this base infrastructure:
- Application deployment will **fail** if base infrastructure is not deployed first
- Pipeline automatically validates base infrastructure state
- Shared resources (VPC, IAM roles) are referenced by application infrastructure

## ⚠️ Important Notes

### Deployment Order
1. **First**: Deploy base infrastructure (this repository)
2. **Second**: Deploy application infrastructure (Cosine2.0)

### Shared Resources
This infrastructure creates shared resources used by applications:
- Destroying base infrastructure will **impact all applications**
- Use destroy functionality **very carefully** in production
- Consider dependencies before making breaking changes

### State Management
- State files are automatically managed in S3
- DynamoDB provides state locking to prevent conflicts
- Environment isolation ensures safe parallel development

## 📚 Quick Reference

### Common Commands
```bash
# View current state
terraform state list

# Check for drift
terraform plan

# Manual apply (if needed)
terraform apply

# Emergency destroy (use carefully!)
terraform destroy
```

### Useful Links
- [Main Application Repository](https://github.com/your-org/Cosine2.0)
- [Standardized Pipeline Guide](../Cosine2.0/STANDARDIZED_PIPELINE_GUIDE.md)
- [Terraform Documentation](https://terraform.io/docs)

---

**Pipeline Version**: v2.0 (Standardized)
**Infrastructure Type**: Base/Foundational
**Supported Environments**: Development, Staging, Production
