# Production Federated Authentication Deployment Guide

## Prerequisites ✅

You mentioned you already have:
- ✅ Google Cloud Platform OAuth application set up
- ✅ Azure OAuth application set up  
- ✅ Client IDs and secrets for both providers

## Step 1: Update OAuth Application Redirect URIs

### Google Cloud Platform:
1. Go to [Google Cloud Console Credentials](https://console.cloud.google.com/apis/credentials)
2. Select your existing OAuth 2.0 Client ID
3. In **Authorized redirect URIs**, add:
   ```
   https://cosine-production.auth.us-east-1.amazoncognito.com/oauth2/idpresponse
   ```
4. Click **Save**

### Microsoft Azure:
1. Go to [Azure App Registrations](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade)
2. Select your existing app registration
3. Go to **Authentication** → **Web** → **Redirect URIs**
4. Add:
   ```
   https://cosine-production.auth.us-east-1.amazoncognito.com/oauth2/idpresponse
   ```
5. Click **Save**

## Step 2: Store OAuth Secrets in AWS Secrets Manager (Recommended)

Replace the placeholders with your actual values and run:

```bash
# Navigate to terraform directory
cd terraform

# Create the OAuth secrets (replace with your actual values)
aws secretsmanager create-secret \
  --name "cosine/oauth-credentials/production" \
  --description "OAuth credentials for Cosine production environment" \
  --secret-string '{
    "google_client_id": "YOUR_ACTUAL_GOOGLE_CLIENT_ID.apps.googleusercontent.com",
    "google_client_secret": "YOUR_ACTUAL_GOOGLE_CLIENT_SECRET",
    "microsoft_client_id": "YOUR_ACTUAL_MICROSOFT_APPLICATION_ID", 
    "microsoft_client_secret": "YOUR_ACTUAL_MICROSOFT_CLIENT_SECRET"
  }' \
  --region us-east-1

# Verify the secret was created
aws secretsmanager describe-secret \
  --secret-id "cosine/oauth-credentials/production" \
  --region us-east-1
```

## Step 3: Deploy the Infrastructure

```bash
# Initialize Terraform (if not already done)
terraform init

# Plan the deployment with production configuration
terraform plan \
  -var-file="environments/production.auto.tfvars"

# Apply the changes
terraform apply \
  -var-file="environments/production.auto.tfvars"
```

## Step 4: Get Your Cognito Configuration Details

After successful deployment, get the Cognito details for frontend configuration:

```bash
# Get Cognito outputs
terraform output

# Specifically get Cognito details
terraform output cognito_user_pool_id
terraform output cognito_user_pool_client_id
terraform output cognito_user_pool_domain
```

## Step 5: Frontend Configuration

Your frontend will need these values (from terraform output):

```typescript
// Frontend configuration (e.g., in Amplify config)
const amplifyConfig = {
  Auth: {
    userPoolId: 'us-east-1_XXXXXXXXX',        // From terraform output
    userPoolWebClientId: 'XXXXXXXXXXXXXXXXXX', // From terraform output
    oauth: {
      domain: 'cosine-production.auth.us-east-1.amazoncognito.com',
      socialProviders: ['GOOGLE', 'MICROSOFT'],
      redirectSignIn: [
        'https://cosine.app/auth/callback',
        'https://app.cosine.io/auth/callback'
      ],
      redirectSignOut: [
        'https://cosine.app',
        'https://app.cosine.io'
      ],
      responseType: 'code'
    }
  }
};
```

## Step 6: Test the Integration

1. **Access the Hosted UI:**
   ```
   https://cosine-production.auth.us-east-1.amazoncognito.com/login?client_id=YOUR_CLIENT_ID&response_type=code&scope=email+openid+profile&redirect_uri=https://cosine.app/auth/callback
   ```

2. **Verify Providers:**
   - You should see "Continue with Google" and "Continue with Microsoft" buttons
   - Test each provider login flow
   - Verify users are created in Cognito User Pool

## Security Features Enabled 🔒

Your production deployment includes:

✅ **MFA Required** - Users must set up multi-factor authentication  
✅ **Advanced Security Enforced** - Risk-based authentication and device tracking  
✅ **Secrets Manager** - OAuth credentials securely stored and encrypted  
✅ **Short Token Validity** - 30-minute access/ID tokens for enhanced security  
✅ **KMS Encryption** - All user data encrypted with customer-managed keys  

## Alternative: Direct Variable Approach (Less Secure)

If you prefer not to use Secrets Manager, create a secrets file:

```bash
# Create secrets-production.tfvars (NEVER commit this file)
cat > secrets-production.tfvars << EOF
cognito_google_client_id     = "YOUR_ACTUAL_GOOGLE_CLIENT_ID.apps.googleusercontent.com"
cognito_google_client_secret = "YOUR_ACTUAL_GOOGLE_CLIENT_SECRET"
cognito_microsoft_client_id     = "YOUR_ACTUAL_MICROSOFT_APPLICATION_ID"
cognito_microsoft_client_secret = "YOUR_ACTUAL_MICROSOFT_CLIENT_SECRET"
EOF

# Deploy with secrets file
terraform apply \
  -var-file="environments/production.auto.tfvars" \
  -var-file="secrets-production.tfvars"
```

## Troubleshooting 🔧

### Common Issues:

1. **"Invalid redirect_uri" error:**
   - Verify redirect URIs are exactly: `https://cosine-production.auth.us-east-1.amazoncognito.com/oauth2/idpresponse`
   - Check for typos in OAuth app configurations

2. **"Client authentication failed" error:**
   - Verify OAuth secrets are correct in Secrets Manager
   - Check that client IDs match between Cognito and OAuth providers

3. **CORS errors:**
   - Ensure your production domains are added to Cognito callback/logout URLs
   - Verify OAuth apps allow your production domains

4. **Users can't complete sign-up:**
   - Check MFA configuration (phone number required for SMS)
   - Verify email domain is not blocked

### Verification Commands:

```bash
# Check Cognito configuration
aws cognito-idp describe-user-pool --user-pool-id $(terraform output -raw cognito_user_pool_id)

# Test OAuth endpoints
curl -s "https://cosine-production.auth.us-east-1.amazoncognito.com/.well-known/openid_configuration" | jq .

# List identity providers
aws cognito-idp list-identity-providers --user-pool-id $(terraform output -raw cognito_user_pool_id)
```

## Next Steps 🚀

1. **Monitor Authentication:** Set up CloudWatch dashboards for login metrics
2. **User Management:** Implement admin APIs for user management  
3. **Custom Branding:** Customize the Cognito hosted UI with your branding
4. **Advanced Features:** Add custom user attributes, groups, and roles

## Support

If you encounter issues:
1. Check CloudWatch logs: `/aws/cognito/userpool/errors`
2. Review Terraform outputs for configuration details
3. Test OAuth providers individually in their respective consoles
