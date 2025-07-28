# Cosine Base Infrastructure

This repository contains the shared infrastructure components for the Cosine project, including Lambda layers and other reusable AWS resources.

## 🏗️ Architecture Overview

This base infrastructure repository provides:
- **Lambda Layers**: Shared Python dependencies (yfinance, numpy, pandas)
- **Centralized Management**: Single source of truth for shared resources
- **Multi-Environment Support**: Development, staging, and production configurations
- **Automated CI/CD**: GitHub Actions for infrastructure deployment

## 📁 Repository Structure

```
Cosine-Base-Infra/
├── .github/workflows/           # CI/CD pipeline configurations
│   └── deploy-base-infrastructure.yml
├── terraform/                   # Infrastructure as Code
│   ├── main.tf                 # Main Terraform configuration
│   ├── variables.tf            # Variable definitions
│   ├── outputs.tf              # Output definitions
│   ├── backend.tf              # State backend configuration
│   ├── backend-configs/        # Environment-specific backend configs
│   │   ├── development.tfbackend
│   │   ├── staging.tfbackend
│   │   └── production.tfbackend
│   ├── environments/           # Environment-specific variables
│   │   ├── development.tfvars
│   │   ├── staging.tfvars
│   │   └── production.tfvars
│   └── modules/
│       └── lambda-layer/       # Lambda layer module
│           ├── main.tf
│           ├── variables.tf
│           ├── outputs.tf
│           ├── requirements.txt
│           ├── install-layer-deps.ps1
│           └── README.md
```

## 🚀 Quick Start

### Prerequisites
- AWS CLI configured with appropriate permissions
- Terraform v1.4.0 or later
- PowerShell (for dependency installation)

### Initial Setup

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd Cosine-Base-Infra
   ```

2. **Initialize Terraform**:
   ```bash
   cd terraform
   terraform init -backend-config="backend-configs/staging.tfbackend"
   ```

3. **Plan and apply**:
   ```bash
   terraform plan -var-file="environments/staging.tfvars"
   terraform apply -var-file="environments/staging.tfvars"
   ```

## 🔧 Lambda Layer Module

The Lambda layer module creates a shared layer containing Python dependencies for data analysis and financial data retrieval.

### Included Packages
- **yfinance** (v0.2.28): Yahoo Finance data retrieval
- **numpy** (≥1.24.0): Numerical computing
- **pandas** (≥2.0.0): Data manipulation and analysis
- **requests** (≥2.31.0): HTTP library

### Usage in Other Projects

After deployment, reference the layer in your Lambda functions:

```hcl
# In your application's Terraform configuration
data "terraform_remote_state" "base_infrastructure" {
  backend = "s3"
  config = {
    bucket = "cosine-terraform-state-staging"
    key    = "base-infrastructure/staging/terraform.tfstate"
    region = "us-east-1"
  }
}

resource "aws_lambda_function" "example" {
  # ... other configuration ...
  
  layers = [
    data.terraform_remote_state.base_infrastructure.outputs.lambda_layer_arn
  ]
}
```

## 🌍 Environment Management

### Environments
- **Development**: For testing and development work
- **Staging**: Pre-production testing environment
- **Production**: Live production environment

### Backend State Management
Each environment uses separate S3 buckets for Terraform state:
- Development: `cosine-terraform-state-dev`
- Staging: `cosine-terraform-state-staging`
- Production: `cosine-terraform-state-prod`

### Deploying to Different Environments

```bash
# Development
terraform init -backend-config="backend-configs/development.tfbackend"
terraform apply -var-file="environments/development.tfvars"

# Staging
terraform init -backend-config="backend-configs/staging.tfbackend"
terraform apply -var-file="environments/staging.tfvars"

# Production
terraform init -backend-config="backend-configs/production.tfbackend"
terraform apply -var-file="environments/production.tfvars"
```

## 🤖 CI/CD Pipeline

The GitHub Actions pipeline automatically:
- **Validates** Terraform configuration
- **Plans** infrastructure changes
- **Applies** changes on merge to main/develop branches
- **Comments** on PRs with plan details
- **Scans** for security issues with Checkov

### Pipeline Triggers
- **Push** to main/develop branches
- **Pull requests** to main/develop branches
- **Manual dispatch** with environment selection

### Required Secrets
Configure these secrets in your GitHub repository:
- `AWS_ACCESS_KEY_ID`: AWS access key for deployment
- `AWS_SECRET_ACCESS_KEY`: AWS secret key for deployment

## 🔒 Security

### Best Practices Implemented
- **State encryption**: Terraform state is encrypted in S3
- **IAM least privilege**: Resources use minimal required permissions
- **Security scanning**: Automated Checkov security analysis
- **Environment isolation**: Separate state buckets per environment

### Security Scanning
The pipeline includes automated security scanning with Checkov, which:
- Scans Terraform code for security issues
- Uploads results to GitHub Security tab
- Provides detailed reports on potential vulnerabilities

## 📊 Outputs

After deployment, the following outputs are available:

| Output | Description |
|--------|-------------|
| `lambda_layer_arn` | ARN of the shared Lambda layer |
| `lambda_layer_version` | Version number of the layer |
| `lambda_layer_version_arn` | ARN including version |
| `layer_name` | Name of the layer |
| `compatible_runtimes` | Supported Python runtimes |

## 🔧 Local Development

### Testing Layer Dependencies Locally

```powershell
# Navigate to the lambda-layer module
cd terraform/modules/lambda-layer

# Run the dependency installation script
./install-layer-deps.ps1
```

This creates a virtual environment and installs dependencies for local testing.

## 📝 Contributing

1. Create a feature branch from `develop`
2. Make your changes
3. Test locally with `terraform plan`
4. Create a pull request to `develop`
5. After review, merge to `main` for production deployment

## 🏷️ Tagging Strategy

Resources are automatically tagged with:
- **Project**: cosine
- **Environment**: development/staging/production
- **ManagedBy**: terraform
- **Repository**: Cosine-Base-Infra

## 🆘 Troubleshooting

### Common Issues

1. **PowerShell Execution Policy**:
   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   ```

2. **AWS Permissions**: Ensure your AWS credentials have permissions for:
   - Lambda layer creation and management
   - S3 bucket access for state storage
   - DynamoDB table access for state locking

3. **State Lock Issues**:
   ```bash
   terraform force-unlock <lock-id>
   ```

### Getting Help

For issues with this infrastructure:
1. Check the GitHub Actions logs
2. Review Terraform plan output
3. Validate AWS permissions
4. Check the security scan results

## 📄 License

This project is part of the Cosine financial analysis platform.
