# Cosine Base Infrastructure

This repository contains the shared infrastructure components for the Cosine project using Terraform. It provides reusable modules for authentication, data storage, encryption, and monitoring that can be used across different environments and projects.

## Product design case study

FinGov / Cosine product design write-up (problem, rejected directions, constraints, final UI):

**→ [FinGov Product Design Case Study](./FinGov-Product-Design-Case-Study.md)**

## 🏗️ Architecture Overview

The base infrastructure includes:

- **Authentication**: AWS Cognito User Pool with configurable MFA and security policies
- **Data Storage**: DynamoDB tables for user profiles, security events, and session management
- **Encryption**: KMS keys for service-specific encryption (main, DynamoDB, CloudWatch)
- **Monitoring**: CloudWatch log groups, dashboards, and security alarms
- **Shared Resources**: Lambda layers for common dependencies

## 📁 Repository Structure

```
terraform/
├── main.tf                    # Main Terraform configuration
├── variables.tf               # Variable definitions
├── outputs.tf                 # Output definitions
├── backend.tf                 # Remote state configuration
├── modules/                   # Reusable Terraform modules
│   ├── cognito/              # Authentication module
│   ├── dynamodb/             # Data storage module
│   ├── kms/                  # Encryption keys module
│   ├── cloudwatch/           # Monitoring module
│   └── lambda-layer/         # Shared dependencies module
├── environments/             # Environment-specific configurations
│   ├── development.auto.tfvars
│   ├── staging.auto.tfvars
│   └── production.auto.tfvars
└── backend-configs/          # Backend configurations per environment
    ├── development.tfbackend
    ├── staging.tfbackend
    └── production.tfbackend
```

## 🚀 Quick Start

### Prerequisites

1. **AWS CLI** configured with appropriate credentials
2. **Terraform** >= 1.0 installed
3. **Access** to AWS account with necessary permissions

### Deployment Steps

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd Cosine-Base-Infra/terraform
   ```

2. **Initialize Terraform** (for each environment)
   ```bash
   # Development
   terraform init -backend-config=backend-configs/development.tfbackend
   
   # Staging
   terraform init -backend-config=backend-configs/staging.tfbackend
   
   # Production
   terraform init -backend-config=backend-configs/production.tfbackend
   ```

3. **Plan the deployment**
   ```bash
   # Development
   terraform plan -var-file=environments/development.auto.tfvars
   
   # Staging
   terraform plan -var-file=environments/staging.auto.tfvars
   
   # Production
   terraform plan -var-file=environments/production.auto.tfvars
   ```

4. **Apply the configuration**
   ```bash
   # Development
   terraform apply -var-file=environments/development.auto.tfvars
   
   # Staging
   terraform apply -var-file=environments/staging.auto.tfvars
   
   # Production
   terraform apply -var-file=environments/production.auto.tfvars
   ```

## 📦 Modules

### Cognito Module
Provides AWS Cognito User Pool with:
- Configurable MFA (OFF/OPTIONAL/ON)
- Advanced security features
- Custom domain support
- OAuth 2.0/OpenID Connect flows
- Rate limiting and bot protection

**Outputs**: User Pool ID, Client ID, Domain, etc.

### DynamoDB Module
Creates three tables:
- **User Profiles**: Stores user account information with email GSI
- **Security Events**: Audit trail for security-related events
- **User Sessions**: Manages active user sessions with TTL

**Features**: Encryption, streams, point-in-time recovery, deletion protection

### KMS Module
Provides encryption keys:
- **Main Key**: General purpose encryption
- **DynamoDB Key**: Specifically for DynamoDB encryption
- **CloudWatch Key**: For log group encryption

**Features**: Automatic rotation, service-specific policies, proper IAM controls

### CloudWatch Module
Sets up monitoring infrastructure:
- **Log Groups**: Security, Auth, Application, Lambda, API Gateway
- **Dashboard**: Centralized monitoring view
- **Alarms**: Failed login and suspicious activity detection
- **Metric Filters**: Security event parsing

### Lambda Layer Module
Creates shared Lambda layer with common dependencies for Python runtime.

## 🔧 Configuration

### Environment Variables

Each environment (development, staging, production) has its own configuration file in the `environments/` directory. Key differences:

| Setting | Development | Staging | Production |
|---------|-------------|---------|------------|
| MFA | OFF | OPTIONAL | ON |
| Key Rotation | Disabled | Enabled | Enabled |
| Log Retention | 7-30 days | 14-180 days | 30-365 days |
| Deletion Protection | Disabled | Enabled | Enabled |
| Alert Thresholds | Relaxed | Moderate | Strict |

### Customization

To customize for your environment:

1. **Update variables** in the appropriate `.auto.tfvars` file
2. **Modify tags** to match your organization's standards
3. **Adjust retention policies** based on compliance requirements
4. **Configure callback URLs** for your application domains

### Required Variables

At minimum, you must set:
- `environment`: Target environment name
- `cognito_callback_urls`: Your application's callback URLs
- `cognito_logout_urls`: Your application's logout URLs

## 📊 Outputs

The infrastructure exposes comprehensive outputs for use in other Terraform configurations:

### Authentication
- `cognito_user_pool_id`: For Lambda/API Gateway integration
- `cognito_user_pool_client_id`: For frontend configuration
- `cognito_user_pool_client_secret`: For backend authentication

### Data Storage
- `user_profiles_table_name`: For application database operations
- `security_events_table_name`: For audit logging
- `user_sessions_table_name`: For session management

### Encryption
- `kms_key_arn`: For encrypting application data
- `dynamodb_key_arn`: For DynamoDB encryption
- `cloudwatch_key_arn`: For log encryption

### Monitoring
- `security_log_group_name`: For security event logging
- `application_log_group_name`: For application logging
- `dashboard_url`: For monitoring access

## 🔐 Security Features

### Encryption
- **At Rest**: All DynamoDB tables encrypted with customer-managed KMS keys
- **In Transit**: HTTPS/TLS for all API communications
- **Logs**: CloudWatch logs encrypted with dedicated KMS key

### Authentication
- **MFA Support**: SMS, TOTP, or hardware tokens
- **Advanced Security**: Bot detection, compromised credential detection
- **Rate Limiting**: Protection against brute force attacks
- **Password Policies**: Configurable complexity requirements

### Monitoring
- **Security Events**: Comprehensive logging of authentication events
- **Failed Login Detection**: Automated alerting on suspicious activity
- **Audit Trail**: Complete history of user actions in DynamoDB
- **Real-time Dashboards**: CloudWatch dashboard for security monitoring

### Access Control
- **Least Privilege**: IAM policies follow principle of least privilege
- **Service-specific Keys**: Separate KMS keys for different services
- **Resource-based Policies**: Granular access control on all resources

## 🌍 Multi-Environment Support

The infrastructure supports three environments out of the box:

### Development
- **Purpose**: Local development and testing
- **Security**: Relaxed for ease of development
- **Cost**: Optimized for minimal charges
- **Retention**: Short log retention periods

### Staging
- **Purpose**: Pre-production testing
- **Security**: Production-like settings
- **Cost**: Balanced between cost and functionality
- **Retention**: Moderate log retention

### Production
- **Purpose**: Live user environment
- **Security**: Maximum security enforcement
- **Cost**: Optimized for reliability over cost
- **Retention**: Long-term log retention for compliance

## 🔄 Integration with Main Project

To use this base infrastructure in your main Cosine project:

1. **Reference as Terraform Data Source**:
   ```hcl
   data "terraform_remote_state" "base_infra" {
     backend = "s3"
     config = {
       bucket = "cosine-terraform-state"
       key    = "base-infra/${var.environment}.tfstate"
       region = "us-east-1"
     }
   }
   ```

2. **Use Outputs in Resources**:
   ```hcl
   resource "aws_lambda_function" "api" {
     # ... other configuration
     
     environment {
       variables = {
         USER_POOL_ID    = data.terraform_remote_state.base_infra.outputs.cognito_user_pool_id
         USER_TABLE_NAME = data.terraform_remote_state.base_infra.outputs.user_profiles_table_name
         KMS_KEY_ID      = data.terraform_remote_state.base_infra.outputs.kms_key_id
       }
     }
   }
   ```

## 📈 Cost Optimization

### Development Environment
- Pay-per-request DynamoDB billing
- Shorter log retention periods
- Disabled point-in-time recovery
- No key rotation (for cost savings)

### Production Environment
- Can switch to provisioned capacity for predictable workloads
- Longer retention for compliance
- Full backup and recovery features
- All security features enabled

### Monitoring Costs
Use the CloudWatch dashboard to monitor:
- DynamoDB consumed capacity
- Lambda invocation counts
- Log ingestion volume
- KMS key usage

## 🔍 Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure your AWS credentials have sufficient permissions for all resources
2. **State Lock**: If Terraform state is locked, check for running operations or manually unlock
3. **Resource Conflicts**: Ensure resource names are unique across environments
4. **Backend Access**: Verify S3 bucket and DynamoDB table exist for state management

### Debugging

1. **Enable Terraform Debug Logging**:
   ```bash
   export TF_LOG=DEBUG
   terraform plan
   ```

2. **Check AWS CloudTrail**: For API call debugging
3. **Review CloudWatch Logs**: For runtime issues
4. **Validate Configuration**:
   ```bash
   terraform validate
   terraform fmt -check
   ```

## 🤝 Contributing

1. **Follow Terraform Best Practices**: Use consistent formatting and naming
2. **Update Documentation**: Keep README and module docs current
3. **Test Changes**: Validate in development environment first
4. **Security Review**: Ensure no sensitive data in code or state

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For questions or support:
- Create an issue in this repository
- Review the troubleshooting section
- Check AWS documentation for service-specific guidance
