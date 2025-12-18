# Staging Federated Authentication Deployment Guide

## 🎯 Overview

This guide will help you deploy the same federated authentication system to staging that you'll later use for production. Perfect for testing your OAuth integration before going live!

## Prerequisites ✅

You already have:
- ✅ Google Cloud Platform OAuth application
- ✅ Azure OAuth application  
- ✅ Client IDs and secrets for both providers

## Step 1: Update OAuth Application Redirect URIs

### Google Cloud Platform:
1. Go to [Google Cloud Console Credentials](https://console.cloud.google.com/apis/credentials)
2. Select your existing OAuth 2.0 Client ID
3. In **Authorized redirect URIs**, add:
   ```
   https://cosine-staging.auth.us-east-1.amazoncognito.com/oauth2/idpresponse
   ```
4. Click **Save**

### Microsoft Azure:
1. Go to [Azure App Registrations](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade)
2. Select your existing app registration
3. Go to **Authentication** → **Web** → **Redirect URIs**
4. Add:
   ```
   https://cosine-staging.auth.us-east-1.amazoncognito.com/oauth2/idpresponse
   ```
5. Click **Save**

## Step 2: Store OAuth Secrets in AWS Secrets Manager (Recommended)

### Option A: Using the Setup Script (Easiest)
```bash
cd terraform
chmod +x setup-oauth-secrets-staging.sh
./setup-oauth-secrets-staging.sh
```

### Option B: Manual AWS CLI Commands
```bash
# Create the OAuth secrets (replace with your actual values)
aws secretsmanager create-secret \
  --name "cosine/oauth-credentials/staging" \
  --description "OAuth credentials for Cosine staging environment" \
  --secret-string '{
    "google_client_id": "YOUR_ACTUAL_GOOGLE_CLIENT_ID.apps.googleusercontent.com",
    "google_client_secret": "YOUR_ACTUAL_GOOGLE_CLIENT_SECRET",
    "microsoft_client_id": "YOUR_ACTUAL_MICROSOFT_APPLICATION_ID", 
    "microsoft_client_secret": "YOUR_ACTUAL_MICROSOFT_CLIENT_SECRET"
  }' \
  --region us-east-1 \
  --tags '[
    {"Key": "Environment", "Value": "staging"},
    {"Key": "Project", "Value": "cosine"},
    {"Key": "ManagedBy", "Value": "terraform"}
  ]'
```

## Step 3: Deploy the Staging Infrastructure

```bash
# Navigate to terraform directory
cd terraform

# Initialize Terraform (if not already done)
terraform init

# Plan the staging deployment
terraform plan \
  -var-file="environments/staging.auto.tfvars"

# Apply the changes
terraform apply \
  -var-file="environments/staging.auto.tfvars"
```

## Step 4: Get Staging Cognito Configuration

After successful deployment:

```bash
# Get all outputs
terraform output

# Get specific Cognito details for staging
terraform output cognito_user_pool_id
terraform output cognito_user_pool_client_id
terraform output cognito_user_pool_domain
```

## Step 5: Test the Staging Environment

### Access the Hosted UI:
```
https://cosine-staging.auth.us-east-1.amazoncognito.com/login?client_id=YOUR_CLIENT_ID&response_type=code&scope=email+openid+profile&redirect_uri=https://staging.cosine.app/auth/callback
```

### Frontend Configuration for Staging:
```typescript
// Staging configuration
const stagingAmplifyConfig = {
  Auth: {
    userPoolId: 'us-east-1_XXXXXXXXX',        // From terraform output
    userPoolWebClientId: 'XXXXXXXXXXXXXXXXXX', // From terraform output
    oauth: {
      domain: 'cosine-staging.auth.us-east-1.amazoncognito.com',
      socialProviders: ['GOOGLE', 'MICROSOFT'],
      redirectSignIn: [
        'https://staging.cosine.app/auth/callback'
      ],
      redirectSignOut: [
        'https://staging.cosine.app'
      ],
      responseType: 'code'
    }
  }
};
```

## Step 6: Alternative Deployment (Direct Secrets)

If you prefer not to use Secrets Manager for staging:

```bash
# Create secrets file from template
cp secrets-staging.tfvars.example secrets-staging.tfvars

# Edit with your actual credentials
# vim secrets-staging.tfvars

# Deploy with secrets file
terraform apply \
  -var-file="environments/staging.auto.tfvars" \
  -var-file="secrets-staging.tfvars"
```

## 🔒 Staging Security Features

Your staging environment includes:

✅ **Optional MFA** - Less restrictive for testing  
✅ **Advanced Security Enforced** - Risk-based authentication  
✅ **Secrets Manager** - OAuth credentials securely stored  
✅ **Production-like Configuration** - Validates your production setup  
✅ **KMS Encryption** - Customer-managed encryption keys  

## 🧪 Testing Checklist

- [ ] Google OAuth login works
- [ ] Microsoft OAuth login works  
- [ ] Users are created in Cognito User Pool
- [ ] JWT tokens are properly issued
- [ ] Logout functionality works
- [ ] User attributes are populated correctly

## 🚀 Ready for Production

Once staging is working perfectly:

1. **Copy staging configuration to production.auto.tfvars** (already done)
2. **Update production OAuth redirect URIs** 
3. **Create production OAuth secrets**
4. **Deploy to production**

## Troubleshooting 🔧

### Common Staging Issues:

1. **"Invalid redirect_uri" error:**
   - Verify redirect URI is exactly: `https://cosine-staging.auth.us-east-1.amazoncognito.com/oauth2/idpresponse`
   - Check staging URLs in OAuth applications

2. **Secrets not found:**
   ```bash
   # Check if secrets exist
   aws secretsmanager list-secrets --region us-east-1 --query 'SecretList[?contains(Name, `staging`)].Name'
   ```

3. **Test OAuth endpoints:**
   ```bash
   curl -s "https://cosine-staging.auth.us-east-1.amazoncognito.com/.well-known/openid_configuration" | jq .
   ```

### Verification Commands:

```bash
# Check Cognito configuration
aws cognito-idp describe-user-pool --user-pool-id $(terraform output -raw cognito_user_pool_id)

# List identity providers
aws cognito-idp list-identity-providers --user-pool-id $(terraform output -raw cognito_user_pool_id)

# Check CloudWatch logs
aws logs describe-log-groups --log-group-name-prefix "/aws/cognito"
```

## Next Steps After Staging Success 🎯

1. **Validate all OAuth flows work correctly**
2. **Test with your frontend application**  
3. **Verify user management APIs**
4. **Move identical configuration to production**
5. **Set up monitoring and alerts**

## Support

Staging logs location: `/aws/cognito/userpool/errors`  
Terraform state: Check outputs for all configuration details  
OAuth testing: Use staging hosted UI for validation
