# Using AWS Secrets Manager for OAuth Credentials

This guide explains how to use AWS Secrets Manager to securely store and manage OAuth provider credentials.

## Benefits of Using Secrets Manager

✅ **Enhanced Security**: Credentials encrypted with KMS keys  
✅ **Automatic Rotation**: Support for credential rotation  
✅ **Audit Trail**: CloudTrail logging of all access  
✅ **Fine-grained Access**: IAM-based access control  
✅ **Version Control**: Multiple versions with rollback capability  

## Quick Setup

### 1. Enable Secrets Manager Integration

```hcl
# In your secrets.tfvars file
oauth_secrets_enabled = true
secrets_recovery_window_days = 7

# Your OAuth credentials (will be stored in Secrets Manager)
cognito_enable_google_provider = true
cognito_google_client_id = "your-google-client-id"
cognito_google_client_secret = "your-google-client-secret"
```

### 2. Deploy Infrastructure

```bash
terraform apply -var-file="secrets.tfvars"
```

After deployment, your OAuth credentials will be:
- Encrypted with your project's KMS key
- Stored in Secrets Manager as `cosine-oauth-credentials-staging`
- Automatically retrieved by Cognito identity providers

### 3. Update Credentials (Post-Deployment)

You can update OAuth credentials without touching Terraform:

**Via AWS Console:**
1. Go to AWS Secrets Manager
2. Find `cosine-oauth-credentials-staging`
3. Click "Retrieve secret value" → "Edit"
4. Update the JSON values
5. Save changes

**Via AWS CLI:**
```bash
# Get current secret
aws secretsmanager get-secret-value \
  --secret-id cosine-oauth-credentials-staging \
  --query SecretString --output text

# Update secret (example)
aws secretsmanager update-secret \
  --secret-id cosine-oauth-credentials-staging \
  --secret-string '{
    "google_client_id": "new-google-client-id",
    "google_client_secret": "new-google-client-secret",
    "apple_client_id": "your-apple-client-id",
    "apple_team_id": "your-apple-team-id",
    "apple_key_id": "your-apple-key-id",
    "apple_private_key": "your-apple-private-key",
    "microsoft_client_id": "your-microsoft-client-id",
    "microsoft_client_secret": "your-microsoft-client-secret"
  }'
```

## Secret Structure

The OAuth credentials secret contains a JSON object:

```json
{
  "google_client_id": "your-google-client-id.apps.googleusercontent.com",
  "google_client_secret": "your-google-client-secret",
  "apple_client_id": "com.yourdomain.cosine.web",
  "apple_team_id": "YOUR_TEAM_ID",
  "apple_key_id": "YOUR_KEY_ID",
  "apple_private_key": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
  "microsoft_client_id": "your-microsoft-app-id",
  "microsoft_client_secret": "your-microsoft-client-secret"
}
```

## Security Best Practices

### 1. IAM Access Policies

Create restrictive IAM policies for accessing OAuth secrets:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue"
      ],
      "Resource": "arn:aws:secretsmanager:*:*:secret:cosine-oauth-credentials-*"
    }
  ]
}
```

### 2. Monitor Access

Set up CloudWatch alarms for secret access:

```bash
# Create metric filter for secret access
aws logs put-metric-filter \
  --log-group-name /aws/secretsmanager \
  --filter-name oauth-secret-access \
  --filter-pattern '[timestamp, request_id, event_name="GetSecretValue", ..., secret_name="cosine-oauth-credentials*"]' \
  --metric-transformations \
    metricName=OAuthSecretAccess \
    metricNamespace=Cosine/Security \
    metricValue=1
```

### 3. Rotate Credentials Regularly

```bash
# Schedule credential rotation (example for Google)
aws events put-rule \
  --name rotate-oauth-credentials \
  --schedule-expression "rate(90 days)" \
  --description "Rotate OAuth credentials quarterly"
```

## Development Workflow

### Local Development
For local development, you can still use direct variables:

```hcl
# In terraform/environments/development.auto.tfvars
oauth_secrets_enabled = false

# Direct variables for local testing
cognito_google_client_id = "dev-google-client-id"
cognito_google_client_secret = "dev-google-client-secret"
```

### Production Deployment
For production, always use Secrets Manager:

```hcl
# In terraform/environments/production.auto.tfvars
oauth_secrets_enabled = true
secrets_recovery_window_days = 30

# Initial credentials (will be moved to Secrets Manager)
cognito_google_client_id = "prod-google-client-id"
cognito_google_client_secret = "prod-google-client-secret"
```

## Troubleshooting

### 1. Secret Not Found Error
```
Error: reading Secrets Manager Secret Version
```
**Solution**: Ensure `oauth_secrets_enabled = true` and secret exists

### 2. Permission Denied
```
Error: operation error SecretsManager: GetSecretValue, https response error
```
**Solution**: Check IAM permissions for the Terraform execution role

### 3. Invalid JSON in Secret
```
Error: invalid character in JSON
```
**Solution**: Validate JSON format in Secrets Manager console

## Migration from Direct Variables

To migrate existing OAuth setup to Secrets Manager:

1. **First deployment with Secrets Manager enabled:**
   ```bash
   terraform apply -var-file="secrets.tfvars" -var="oauth_secrets_enabled=true"
   ```

2. **Remove sensitive variables from tfvars:**
   ```bash
   # Keep only the enable flags, remove actual credentials
   cognito_enable_google_provider = true
   # Remove: cognito_google_client_id = "..."
   # Remove: cognito_google_client_secret = "..."
   ```

3. **Update credentials in Secrets Manager only**

This approach ensures zero-downtime migration to secure credential storage.
