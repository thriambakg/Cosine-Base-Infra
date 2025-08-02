# Console-Managed OAuth Secrets Guide

## 🎯 Overview

Your Terraform will create empty AWS Secrets Manager resources that you'll populate manually in the AWS Console. This approach gives you maximum security while still allowing infrastructure automation.

## Step 1: Deploy Infrastructure

Run your Terraform deployment as normal:

```bash
cd terraform
terraform plan -var-file="environments/staging.auto.tfvars"
terraform apply -var-file="environments/staging.auto.tfvars"
```

This will create:
- ✅ Empty Secrets Manager secret: `cosine-oauth-credentials-staging`
- ✅ All other infrastructure (Cognito, DynamoDB, etc.)
- ✅ Cognito configured to read from Secrets Manager

## Step 2: Update OAuth App Redirect URIs

### Google Cloud Platform:
1. Go to [Google Cloud Console Credentials](https://console.cloud.google.com/apis/credentials)
2. Select your OAuth 2.0 Client ID
3. Add redirect URI: `https://cosine-staging.auth.us-east-1.amazoncognito.com/oauth2/idpresponse`

### Microsoft Azure:
1. Go to [Azure App Registrations](https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationsListBlade)
2. Select your app registration  
3. Go to **Authentication** → **Web** → **Redirect URIs**
4. Add redirect URI: `https://cosine-staging.auth.us-east-1.amazoncognito.com/oauth2/idpresponse`

## Step 3: Populate Secrets in AWS Console

### Access AWS Secrets Manager:
1. Go to [AWS Secrets Manager Console](https://console.aws.amazon.com/secretsmanager/)
2. Find secret: `cosine-oauth-credentials-staging`
3. Click **Retrieve secret value**
4. Click **Edit**

### Update the Secret JSON:
Replace the placeholder values with your actual OAuth credentials:

```json
{
  "google_client_id": "YOUR_ACTUAL_GOOGLE_CLIENT_ID.apps.googleusercontent.com",
  "google_client_secret": "YOUR_ACTUAL_GOOGLE_CLIENT_SECRET",
  "microsoft_client_id": "YOUR_ACTUAL_MICROSOFT_APPLICATION_ID",
  "microsoft_client_secret": "YOUR_ACTUAL_MICROSOFT_CLIENT_SECRET"
}
```

### Example with Real Values:
```json
{
  "google_client_id": "123456789-abcdefghijklmnop.apps.googleusercontent.com",
  "google_client_secret": "GOCSPX-abcdefghijklmnopqrstuvwx",
  "microsoft_client_id": "12345678-1234-1234-1234-123456789abc",
  "microsoft_client_secret": "abc~defghijklmnopqrstuvwxyz123456"
}
```

## Step 4: Test the Integration

After updating the secrets, test your authentication:

1. **Get Cognito hosted UI URL:**
   ```bash
   terraform output cognito_user_pool_domain
   ```

2. **Access the login page:**
   ```
   https://cosine-staging.auth.us-east-1.amazoncognito.com/login?client_id=YOUR_CLIENT_ID&response_type=code&scope=email+openid+profile&redirect_uri=https://staging.cosine.app/auth/callback
   ```

3. **Verify OAuth providers appear:**
   - ✅ "Continue with Google" button
   - ✅ "Continue with Microsoft" button

## 🔒 Security Benefits

This approach provides:

✅ **Infrastructure as Code** - Terraform manages the infrastructure  
✅ **Secure Credential Management** - Secrets never in version control  
✅ **Access Control** - AWS IAM controls who can view/edit secrets  
✅ **Encryption** - Secrets encrypted with your KMS key  
✅ **Audit Trail** - CloudTrail logs all secret access  

## 🔧 Troubleshooting

### Cognito Can't Read Secrets:
```bash
# Check if secret exists
aws secretsmanager describe-secret --secret-id "cosine-oauth-credentials-staging" --region us-east-1

# Verify secret format
aws secretsmanager get-secret-value --secret-id "cosine-oauth-credentials-staging" --region us-east-1
```

### OAuth Providers Not Working:
1. Verify redirect URIs are exactly: `https://cosine-staging.auth.us-east-1.amazoncognito.com/oauth2/idpresponse`
2. Check secret values are correct (no extra spaces/characters)
3. Ensure Google Client ID ends with `.apps.googleusercontent.com`
4. Verify Microsoft Client ID is in GUID format

### Force Cognito to Re-read Secrets:
If you update secrets but Cognito still shows old values:
```bash
# Trigger a Cognito update by running terraform apply again
terraform apply -var-file="environments/staging.auto.tfvars"
```

## Next Steps for Production

1. **Test thoroughly in staging**
2. **Deploy production infrastructure** (same process)
3. **Update production OAuth redirect URIs**
4. **Populate production secrets in console**
5. **Test production authentication**

## Important Notes

- ⚠️ **Never commit actual OAuth credentials to git**
- ✅ **Terraform ignores secret changes** (you can update them safely)
- 🔄 **Terraform won't overwrite your manual updates**
- 🔐 **Use different secrets for staging vs production**

The secret names will be:
- Staging: `cosine-oauth-credentials-staging`
- Production: `cosine-oauth-credentials-production`
