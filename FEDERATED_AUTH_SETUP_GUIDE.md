# Setting Up Federated Authentication with Cognito

This guide walks you through enabling third-party authentication (Google, Apple, Microsoft) for your Cosine application.

## Prerequisites ✅

- Base infrastructure deployed and working
- Access to Google, Apple, and Microsoft developer consoles
- Domain name for production (optional but recommended)

## Step 1: Configure OAuth Applications

### 🔍 Google OAuth Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable Google+ API
4. Go to "Credentials" > "Create Credentials" > "OAuth 2.0 Client ID"
5. Configure OAuth consent screen:
   - Application name: "Cosine"
   - Authorized domains: `yourdomain.com` (for production)
6. Create OAuth client:
   - Application type: Web application
   - Authorized redirect URIs:
     ```
     https://cosine-staging.auth.us-east-1.amazoncognito.com/oauth2/idpresponse
     ```
7. Copy the Client ID and Client Secret

### 🍎 Apple Sign-In Setup

1. Go to [Apple Developer Console](https://developer.apple.com/)
2. Sign in with your Apple Developer account
3. Go to "Certificates, Identifiers & Profiles"
4. Create a new App ID:
   - Description: "Cosine App"
   - Enable "Sign In with Apple"
5. Create a Services ID:
   - Description: "Cosine Web Service"
   - Identifier: `com.yourdomain.cosine.web`
   - Configure "Sign In with Apple":
     - Primary App ID: Select the App ID created above
     - Return URLs: `https://cosine-staging.auth.us-east-1.amazoncognito.com/oauth2/idpresponse`
6. Create a Key for Sign In with Apple:
   - Key Name: "Cosine Apple Auth Key"
   - Enable "Sign In with Apple"
   - Download the key file (.p8)
7. Note down:
   - Services ID (Client ID)
   - Team ID
   - Key ID
   - Private key content

### 🪟 Microsoft OAuth Setup

1. Go to [Azure Portal](https://portal.azure.com/)
2. Navigate to "Azure Active Directory" > "App registrations"
3. Click "New registration":
   - Name: "Cosine"
   - Supported account types: "Accounts in any organizational directory and personal Microsoft accounts"
   - Redirect URI: `https://cosine-staging.auth.us-east-1.amazoncognito.com/oauth2/idpresponse`
4. After creation, go to "Certificates & secrets"
5. Create a new client secret
6. Copy the Application (client) ID and client secret value

## Step 2: Update Terraform Configuration

### Enable Google Provider

1. Create a `.tfvars` file for sensitive variables:
   ```bash
   # terraform/secrets.tfvars
   cognito_enable_google_provider = true
   cognito_google_client_id = "your-google-client-id.apps.googleusercontent.com"
   cognito_google_client_secret = "your-google-client-secret"
   ```

2. Apply the changes:
   ```bash
   cd terraform
   terraform plan -var-file="secrets.tfvars"
   terraform apply -var-file="secrets.tfvars"
   ```

### Enable Apple Provider (Optional)

```bash
# Add to secrets.tfvars
cognito_enable_apple_provider = true
cognito_apple_client_id = "com.yourdomain.cosine.web"
cognito_apple_team_id = "YOUR_TEAM_ID"
cognito_apple_key_id = "YOUR_KEY_ID"
cognito_apple_private_key = "-----BEGIN PRIVATE KEY-----\nYOUR_PRIVATE_KEY_CONTENT\n-----END PRIVATE KEY-----"
```

### Enable Microsoft Provider (Optional)

```bash
# Add to secrets.tfvars
cognito_enable_microsoft_provider = true
cognito_microsoft_client_id = "your-microsoft-app-id"
cognito_microsoft_client_secret = "your-microsoft-client-secret"
```

## Step 3: Update Frontend Configuration

1. Install required dependencies:
   ```bash
   cd frontend
   npm install aws-amplify @aws-amplify/ui-react
   ```

2. Update your Amplify configuration with the new Cognito outputs:
   ```typescript
   // src/config/amplify-config.ts
   const amplifyConfig = {
     Auth: {
       userPoolId: 'us-east-1_YOUR_POOL_ID',
       userPoolWebClientId: 'YOUR_CLIENT_ID',
       oauth: {
         domain: 'cosine-staging.auth.us-east-1.amazoncognito.com',
         socialProviders: ['GOOGLE', 'APPLE', 'MICROSOFT'],
         redirectSignIn: ['http://localhost:3000/auth/callback'],
         redirectSignOut: ['http://localhost:3000/'],
       }
     }
   };
   ```

3. Use the enhanced login component in your app

## Step 4: Test the Integration

1. **Local Development:**
   ```bash
   cd frontend
   npm start
   ```

2. **Test Each Provider:**
   - Click "Continue with Google" → Should redirect to Google OAuth
   - Click "Continue with Apple" → Should redirect to Apple Sign-In
   - Click "Continue with Microsoft" → Should redirect to Microsoft OAuth

3. **Verify User Creation:**
   - Check Cognito User Pool in AWS Console
   - Users should appear with federated identity attributes

## Step 5: Production Deployment

### 🏢 **Enterprise & B2B Considerations**

For production applications with many users or B2B enterprise customers, additional configurations are required:

#### **Multi-Tenant Architecture**
```hcl
# terraform/environments/production.auto.tfvars
# Separate user pools per tenant for enterprise isolation
cognito_enable_multi_tenant = true
cognito_tenant_isolation_mode = "user_pool_per_tenant"  # or "user_groups"
```

#### **Enterprise Identity Providers**
Add support for enterprise SSO providers:
```hcl
# SAML providers for enterprise customers
cognito_enable_saml_providers = true
cognito_saml_providers = {
  "customer-corp" = {
    metadata_url = "https://customer-corp.com/saml/metadata"
    display_name = "Customer Corp SSO"
  }
  "enterprise-client" = {
    metadata_file = "path/to/metadata.xml"
    display_name = "Enterprise Client"
  }
}

# OIDC providers for additional flexibility
cognito_oidc_providers = {
  "okta" = {
    issuer_url = "https://dev-12345.okta.com"
    client_id = "your-okta-client-id"
    client_secret = "your-okta-secret"
  }
}
```

#### **Advanced Security Settings**
```hcl
# Enhanced security for production
cognito_advanced_security_mode = "ENFORCED"
cognito_password_policy = {
  minimum_length = 12
  require_lowercase = true
  require_uppercase = true
  require_numbers = true
  require_symbols = true
  temporary_password_validity_days = 1
}

# Account takeover protection
cognito_account_takeover_protection = {
  notify_configuration = {
    block_email = {
      subject = "Account Security Alert"
      html_body = "We blocked a suspicious sign-in attempt"
    }
  }
  actions = {
    low_action = "NO_ACTION"
    medium_action = "MFA_IF_CONFIGURED"
    high_action = "BLOCK"
  }
}
```

### 🎯 **Scalability Configurations**

#### **Rate Limiting & Quotas**
```hcl
# API Gateway throttling
api_gateway_throttle_settings = {
  rate_limit = 10000    # requests per second
  burst_limit = 20000   # burst capacity
}

# Cognito quotas (consider AWS limits)
# - 120 RPS for authentication operations
# - 25 RPS for user management operations
# Monitor and request quota increases as needed
```

#### **Multi-Region Setup**
```hcl
# For global applications
primary_region = "us-east-1"
backup_regions = ["us-west-2", "eu-west-1"]

# Cross-region replication for user data
enable_cross_region_backup = true
```

1. **Update Callback URLs:**
   - Add production domain to OAuth app configurations
   - Update Terraform variables with production URLs

2. **Security Considerations:**
   - Use AWS Secrets Manager for OAuth credentials ✅
   - Enable HTTPS only with proper SSL/TLS certificates
   - Configure proper CORS settings for your domains
   - Set up comprehensive monitoring and alerts
   - Implement Web Application Firewall (WAF)
   - Enable AWS Config for compliance monitoring

## Troubleshooting 🔧

### Common Issues:

1. **Redirect URI Mismatch:**
   - Ensure OAuth app redirect URIs match Cognito domain exactly
   - Include the `/oauth2/idpresponse` path

2. **CORS Errors:**
   - Check that your domain is whitelisted in OAuth apps
   - Verify Amplify configuration domains

3. **Apple Sign-In Issues:**
   - Ensure Services ID is properly configured
   - Verify the private key format and permissions

4. **User Attribute Mapping:**
   - Check Cognito identity provider attribute mapping
   - Ensure required attributes are mapped correctly

### Useful Commands:

```bash
# Check Cognito configuration
aws cognito-idp describe-user-pool --user-pool-id us-east-1_YOUR_POOL_ID

# Test OAuth endpoints
curl -X GET "https://cosine-staging.auth.us-east-1.amazoncognito.com/.well-known/openid_configuration"

# View Terraform outputs
terraform output
```

## Next Steps 🚀

### **Phase 1: Basic Implementation** (Current Guide)
1. **Standard OAuth Providers:** Google, Apple, Microsoft ✅
2. **Basic Security:** Secrets Manager, KMS encryption ✅
3. **Development Testing:** Local and staging environments ✅

### **Phase 2: Production Readiness**
1. **Enhanced User Experience:**
   - Add user profile management with custom attributes
   - Implement account linking between providers
   - Add progressive MFA (SMS → TOTP → Hardware tokens)
   - Custom login UI with your branding

2. **Operational Excellence:**
   - Automated credential rotation
   - Health checks and monitoring dashboards
   - Performance optimization (response times < 500ms)
   - Load testing and capacity planning

### **Phase 3: Enterprise & B2B Scale**
1. **Multi-Tenant Architecture:**
   - Tenant isolation strategies
   - Per-tenant customization (branding, policies)
   - Tenant-specific identity providers
   - Usage analytics and billing integration

2. **Enterprise SSO Integration:**
   ```bash
   # Add enterprise providers to your infrastructure
   terraform apply -var-file="enterprise.tfvars" \
     -var="enable_saml_providers=true" \
     -var="enable_oidc_providers=true"
   ```

3. **Advanced Analytics:**
   - Track login methods usage by tenant
   - Monitor authentication success rates
   - User behavior analytics and fraud detection
   - Custom reporting for enterprise customers

4. **Compliance & Governance:**
   - SOC 2 Type II certification
   - ISO 27001 compliance
   - GDPR/CCPA data handling
   - Regular security assessments

### **Phase 4: Advanced Features**
1. **AI/ML Integration:**
   - Risk-based authentication
   - Behavioral biometrics
   - Adaptive MFA based on context
   - Automated fraud detection

2. **Global Scale:**
   - Multi-region deployment
   - Edge authentication with CloudFront
   - Global load balancing
   - Data residency compliance

3. **Custom Solutions:**
   - White-label authentication for enterprise customers
   - Custom OAuth scopes and claims
   - Advanced token management
   - Session federation across applications

### **Migration Path for Scale:**

```mermaid
graph TD
    A[Current: Basic OAuth] --> B[Add Enterprise SSO]
    B --> C[Multi-Tenant Setup]
    C --> D[Advanced Security]
    D --> E[Global Deployment]
    
    B1[SAML/OIDC Providers] --> B
    C1[Tenant Isolation] --> C
    C2[Custom Branding] --> C
    D1[Advanced MFA] --> D
    D2[Fraud Detection] --> D
    E1[Multi-Region] --> E
    E2[Edge Auth] --> E
```

## Security Best Practices 🔒

### **Development/Small Scale**
1. **Credentials Management:**
   - Never commit OAuth secrets to version control ✅
   - Use environment variables or AWS Secrets Manager ✅
   - Rotate credentials regularly

### **Enterprise/B2B Production**
1. **Advanced Credential Management:**
   - Implement automated credential rotation (30-90 days)
   - Use AWS Secrets Manager with cross-region replication
   - Set up AWS CloudTrail for audit logging
   - Implement least-privilege IAM policies

2. **Enterprise Security Requirements:**
   ```hcl
   # SOC 2 / ISO 27001 compliance settings
   cognito_compliance_settings = {
     enable_detailed_logging = true
     password_history_size = 24
     account_recovery_setting = "admin_only"  # For enterprise
     temporary_password_validity_days = 1
   }
   
   # Data residency controls
   data_residency_region = "us-east-1"  # or required region
   enable_data_encryption_at_rest = true
   enable_data_encryption_in_transit = true
   ```

3. **Multi-Tenant Isolation:**
   - Separate encryption keys per tenant
   - Tenant-specific IAM roles and policies
   - Isolated CloudWatch log groups
   - Per-tenant rate limiting

4. **Compliance & Auditing:**
   - Enable AWS Config for resource compliance
   - Set up AWS Security Hub for security posture
   - Implement AWS GuardDuty for threat detection
   - Configure AWS Macie for data classification

5. **User Data Protection:**
   - Implement proper data encryption (AES-256)
   - Follow GDPR/CCPA/SOX compliance requirements
   - Regular security audits and penetration testing
   - Data retention and deletion policies
   - User consent management

6. **Advanced Monitoring:**
   - Set up CloudWatch alarms for authentication failures
   - Monitor for suspicious login patterns and velocity
   - Implement real-time fraud detection
   - User behavior analytics and anomaly detection
   - Integration with SIEM systems

### **Enterprise SSO Integration**
```yaml
# Support for enterprise identity providers
Enterprise SAML Providers:
  - Okta
  - Azure AD
  - Google Workspace
  - Ping Identity
  - OneLogin
  - Custom SAML 2.0 providers

Enterprise OIDC Providers:
  - Auth0
  - Keycloak
  - Custom OpenID Connect providers
```

### **High Availability & Disaster Recovery**
```hcl
# Multi-AZ deployment
availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]

# Backup and recovery
backup_retention_days = 35  # SOC 2 requirement
cross_region_backup_enabled = true
point_in_time_recovery = true

# Disaster recovery RTO/RPO targets
recovery_time_objective_minutes = 60   # 1 hour
recovery_point_objective_minutes = 15  # 15 minutes
```

## Support 📞

### **Development & Small Scale Issues:**
If you encounter issues:
1. Check AWS CloudWatch logs for detailed error messages
2. Verify OAuth app configurations in respective developer consoles
3. Test with different browsers and incognito mode
4. Review Cognito user pool logs in AWS Console

### **Enterprise & Production Support:**

#### **Escalation Path:**
1. **Level 1:** Internal DevOps team monitoring dashboards
2. **Level 2:** AWS Enterprise Support (if applicable)
3. **Level 3:** Security team for authentication-related incidents
4. **Level 4:** Executive escalation for business-critical issues

#### **SLA Targets for Production:**
```yaml
Authentication Service Availability: 99.9%
Response Time: < 500ms for 95th percentile
Recovery Time Objective (RTO): 1 hour
Recovery Point Objective (RPO): 15 minutes
```

#### **Enterprise Monitoring Stack:**
```hcl
# Recommended monitoring tools
monitoring_stack = {
  metrics     = "CloudWatch + Datadog/New Relic"
  logging     = "CloudWatch Logs + ELK Stack"
  alerting    = "PagerDuty + Slack integrations"
  dashboards  = "Grafana + CloudWatch Dashboards"
  uptime      = "Pingdom + StatusPage.io"
}
```

#### **Common Enterprise Issues & Solutions:**

1. **SAML Metadata Updates:**
   ```bash
   # Automated SAML metadata refresh
   aws cognito-idp update-identity-provider \
     --user-pool-id us-east-1_XXXXX \
     --provider-name "Enterprise-SAML" \
     --provider-details file://new-metadata.json
   ```

2. **Bulk User Management:**
   ```bash
   # CSV import for enterprise onboarding
   aws cognito-idp create-user-import-job \
     --user-pool-id us-east-1_XXXXX \
     --job-name "Q1-2025-Onboarding" \
     --cloud-watch-logs-role-arn arn:aws:iam::ACCOUNT:role/CognitoImportRole
   ```

3. **Tenant Isolation Verification:**
   ```bash
   # Audit tenant data isolation
   aws cognito-idp admin-list-groups-for-user \
     --user-pool-id us-east-1_XXXXX \
     --username user@enterprise.com
   ```

## 🏢 Enterprise Deployment Checklist

### **Pre-Production Checklist:**
- [ ] Multi-region disaster recovery tested
- [ ] Security audit completed (penetration testing)
- [ ] Performance testing with expected load
- [ ] Compliance documentation (SOC 2, ISO 27001)
- [ ] Data retention and deletion policies implemented
- [ ] Incident response procedures documented
- [ ] Staff training on enterprise features completed
- [ ] Customer onboarding documentation created

### **Go-Live Checklist:**
- [ ] DNS failover configured
- [ ] Monitoring and alerting active
- [ ] Support escalation procedures in place
- [ ] Backup and recovery tested
- [ ] Rate limiting and DDoS protection enabled
- [ ] Security headers and CORS properly configured
- [ ] SSL/TLS certificates valid and monitored
- [ ] Customer communication plan ready

### **Post-Launch Monitoring:**
- [ ] Daily authentication metrics review
- [ ] Weekly security posture assessment
- [ ] Monthly compliance reporting
- [ ] Quarterly disaster recovery testing
- [ ] Annual security audit and penetration testing
