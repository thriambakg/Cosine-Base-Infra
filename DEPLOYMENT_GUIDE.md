# Cosine Base Infrastructure Deployment Guide

This guide provides step-by-step instructions for deploying the complete base infrastructure for the Cosine project.

## 📋 Overview

The base infrastructure now includes:
- ✅ **Cognito Module**: Authentication with user pools, MFA, and security features
- ✅ **DynamoDB Module**: User profiles, security events, and session tables
- ✅ **KMS Module**: Encryption keys for all services
- ✅ **CloudWatch Module**: Comprehensive logging and monitoring
- ✅ **Lambda Layer Module**: Shared dependencies

## 🚀 Deployment Steps

### 1. Pre-Deployment Validation

Before deploying, validate the configuration:

```powershell
# Navigate to terraform directory
cd "c:\Users\Thriambak\Documents\Code\Cosine-Base-Infra\terraform"

# Validate configuration
terraform validate

# Format code
terraform fmt -recursive

# Check for security issues (optional)
tfsec .
```

### 2. Environment-Specific Deployment

#### Development Environment

```powershell
# Initialize with development backend
terraform init -backend-config="backend-configs/development.tfbackend"

# Plan deployment
terraform plan -var-file="environments/development.auto.tfvars" -out="dev.tfplan"

# Review plan and apply
terraform apply "dev.tfplan"
```

#### Staging Environment

```powershell
# Initialize with staging backend
terraform init -backend-config="backend-configs/staging.tfbackend"

# Plan deployment
terraform plan -var-file="environments/staging.auto.tfvars" -out="staging.tfplan"

# Review plan and apply
terraform apply "staging.tfplan"
```

#### Production Environment

```powershell
# Initialize with production backend
terraform init -backend-config="backend-configs/production.tfbackend"

# Plan deployment
terraform plan -var-file="environments/production.auto.tfvars" -out="prod.tfplan"

# Review plan carefully and apply
terraform apply "prod.tfplan"
```

### 3. Post-Deployment Verification

After deployment, verify the infrastructure:

```powershell
# Check outputs
terraform output

# Test Cognito User Pool
aws cognito-idp describe-user-pool --user-pool-id $(terraform output -raw cognito_user_pool_id)

# Test DynamoDB tables
aws dynamodb describe-table --table-name $(terraform output -raw user_profiles_table_name)

# Test KMS keys
aws kms describe-key --key-id $(terraform output -raw kms_key_id)

# Check CloudWatch log groups
aws logs describe-log-groups --log-group-name-prefix "/aws/cosine"
```

## 📊 Expected Outputs

After successful deployment, you should see outputs similar to:

```hcl
cognito_user_pool_id = "us-east-1_ABC123DEF"
cognito_user_pool_client_id = "1a2b3c4d5e6f7g8h9i0j"
user_profiles_table_name = "cosine-staging-user-profiles"
security_events_table_name = "cosine-staging-security-events"
user_sessions_table_name = "cosine-staging-user-sessions"
kms_key_id = "12345678-1234-1234-1234-123456789012"
security_log_group_name = "/aws/cosine/staging/security"
dashboard_url = "https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=cosine-staging-dashboard"
```

## 🔧 Configuration Notes

### Environment Differences

| Component | Development | Staging | Production |
|-----------|-------------|---------|------------|
| **Cognito MFA** | OFF | OPTIONAL | ON |
| **KMS Rotation** | Disabled | Enabled | Enabled |
| **DynamoDB Backup** | Disabled | Enabled | Enabled |
| **Log Retention** | 7-30 days | 14-180 days | 30-365 days |
| **Deletion Protection** | Disabled | Enabled | Enabled |

### Security Settings

#### Production Security Features:
- ✅ MFA required for all users
- ✅ Advanced security mode enforced
- ✅ Point-in-time recovery enabled
- ✅ Deletion protection enabled
- ✅ Maximum log retention
- ✅ Strict alert thresholds

#### Development Features:
- ⚠️ MFA disabled for ease of testing
- ⚠️ Shorter log retention for cost savings
- ⚠️ Deletion protection disabled for cleanup
- ⚠️ Relaxed alert thresholds

## 🔄 Integration with Main Project

### Step 1: Reference Base Infrastructure

In your main Cosine project's Terraform configuration:

```hcl
# main.tf in Cosine2.0/terraform/
data "terraform_remote_state" "base_infra" {
  backend = "s3"
  config = {
    bucket = "cosine-terraform-state-${var.environment}"
    key    = "base-infrastructure/${var.environment}/terraform.tfstate"
    region = var.aws_region
  }
}
```

### Step 2: Use Base Infrastructure Outputs

```hcl
# Lambda function example
resource "aws_lambda_function" "chat_api" {
  # ... other configuration

  environment {
    variables = {
      # Authentication
      USER_POOL_ID     = data.terraform_remote_state.base_infra.outputs.cognito_user_pool_id
      USER_POOL_CLIENT = data.terraform_remote_state.base_infra.outputs.cognito_user_pool_client_id
      
      # Data storage
      USER_PROFILES_TABLE = data.terraform_remote_state.base_infra.outputs.user_profiles_table_name
      SECURITY_EVENTS_TABLE = data.terraform_remote_state.base_infra.outputs.security_events_table_name
      USER_SESSIONS_TABLE = data.terraform_remote_state.base_infra.outputs.user_sessions_table_name
      
      # Encryption
      KMS_KEY_ID = data.terraform_remote_state.base_infra.outputs.kms_key_id
    }
  }

  # Use shared layer
  layers = [data.terraform_remote_state.base_infra.outputs.lambda_layer_arn]

  # KMS encryption
  kms_key_arn = data.terraform_remote_state.base_infra.outputs.kms_key_arn
}
```

### Step 3: Configure IAM Permissions

```hcl
# IAM role for Lambda to access base infrastructure resources
resource "aws_iam_role_policy" "lambda_base_infra_access" {
  name = "lambda-base-infra-access"
  role = aws_iam_role.lambda_role.id

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
          data.terraform_remote_state.base_infra.outputs.user_profiles_table_arn,
          data.terraform_remote_state.base_infra.outputs.security_events_table_arn,
          data.terraform_remote_state.base_infra.outputs.user_sessions_table_arn,
          "${data.terraform_remote_state.base_infra.outputs.user_profiles_table_arn}/index/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = [
          data.terraform_remote_state.base_infra.outputs.kms_key_arn,
          data.terraform_remote_state.base_infra.outputs.dynamodb_key_arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = [
          "${data.terraform_remote_state.base_infra.outputs.lambda_log_group_arn}:*"
        ]
      }
    ]
  })
}
```

## 🛠️ Maintenance

### Regular Tasks

1. **Monitor Costs**: Check CloudWatch dashboard monthly
2. **Review Security Events**: Check security logs weekly
3. **Update Dependencies**: Update Lambda layer dependencies quarterly
4. **Backup Verification**: Test backup and restore procedures monthly

### Updates and Changes

1. **Test in Development**: Always test changes in dev environment first
2. **Plan Carefully**: Review Terraform plans before applying
3. **Monitor After Changes**: Watch CloudWatch metrics after updates
4. **Document Changes**: Update this guide and README as needed

## 🚨 Troubleshooting

### Common Issues

#### 1. State Backend Issues
```powershell
# If state backend is not accessible
terraform init -reconfigure -backend-config="backend-configs/staging.tfbackend"
```

#### 2. Resource Conflicts
```powershell
# If resources already exist
terraform import aws_dynamodb_table.user_profiles cosine-staging-user-profiles
```

#### 3. Permission Errors
```powershell
# Check AWS credentials
aws sts get-caller-identity

# Verify permissions
aws iam simulate-principal-policy --policy-source-arn $(aws sts get-caller-identity --query Arn --output text) --action-names dynamodb:CreateTable
```

### Debugging Steps

1. **Enable Debug Logging**:
   ```powershell
   $env:TF_LOG="DEBUG"
   terraform plan
   ```

2. **Check AWS CloudTrail**: Review API calls for errors

3. **Validate Terraform**: Ensure configuration is valid
   ```powershell
   terraform validate
   terraform fmt -check
   ```

## ✅ Deployment Checklist

Before deploying to production:

- [ ] All modules tested in development
- [ ] Security review completed
- [ ] Backup and recovery procedures documented
- [ ] Monitoring and alerting configured
- [ ] Cost estimates reviewed
- [ ] Compliance requirements met
- [ ] Documentation updated
- [ ] Team training completed

## 📞 Support

For deployment issues:
1. Check this deployment guide
2. Review the main README.md
3. Check Terraform documentation
4. Create an issue in the repository
5. Contact the infrastructure team
