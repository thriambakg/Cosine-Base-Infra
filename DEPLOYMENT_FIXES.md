# Deployment Fixes and Requirements

## KMS Policy Fixes Applied

### Problem
The KMS module was generating invalid IAM policies when the `key_administrators` variable was empty (default: `[]`). AWS IAM policies cannot have statements with empty principals, causing deployment failures with the error:
```
Policy contains a statement with no principal
```

### Solution
Modified all three KMS key policies in `terraform/modules/kms/main.tf` to use conditional logic:

```hcl
policy = jsonencode({
  Version = "2012-10-17"
  Statement = concat([
    {
      Sid = "EnableRootAccess"
      # ... root access statement
    }], 
    length(var.key_administrators) > 0 ? [{
      Sid = "AllowKeyAdministrators"
      # ... administrator statement - only included if administrators are specified
    }] : [], [
    {
      Sid = "AllowServiceUsage"
      # ... service-specific statement
    }])
})
```

This ensures the `AllowKeyAdministrators` statement is only included when administrators are actually specified.

## IAM Permission Requirements

### Current Error
```
User: arn:aws:iam::339712742264:user/BaseInfraGA is not authorized to perform: cognito-idp:CreateUserPool
```

### Required IAM Permissions
The deployment user (`BaseInfraGA`) needs the following additional permissions:

#### Cognito Identity Provider Permissions
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "cognito-idp:CreateUserPool",
                "cognito-idp:CreateUserPoolClient",
                "cognito-idp:CreateUserPoolDomain",
                "cognito-idp:DescribeUserPool",
                "cognito-idp:DescribeUserPoolClient",
                "cognito-idp:DescribeUserPoolDomain",
                "cognito-idp:UpdateUserPool",
                "cognito-idp:UpdateUserPoolClient",
                "cognito-idp:DeleteUserPool",
                "cognito-idp:DeleteUserPoolClient",
                "cognito-idp:DeleteUserPoolDomain",
                "cognito-idp:ListUserPools",
                "cognito-idp:ListUserPoolClients",
                "cognito-idp:TagResource",
                "cognito-idp:UntagResource",
                "cognito-idp:ListTagsForResource"
            ],
            "Resource": "*"
        }
    ]
}
```

#### Additional Permissions for Complete Deployment
The user also needs:
- Full KMS permissions for key creation and management
- DynamoDB permissions for table creation
- CloudWatch Logs permissions for log group creation
- IAM permissions for role creation and policy attachment

### Recommendation
Consider attaching the following AWS managed policies to the `BaseInfraGA` user:
- `PowerUserAccess` (provides broad permissions excluding IAM user management)
- Or create a custom policy with specific permissions for the resources being deployed

## Next Steps

1. **Update IAM Permissions**: Add the required Cognito and other service permissions to the `BaseInfraGA` user
2. **Test Deployment**: Run `terraform apply` again after fixing permissions
3. **Monitor Deployment**: Watch for any additional permission errors during deployment

## Validation Status
- ✅ KMS policy syntax fixed
- ✅ Terraform validation passes
- ⏳ IAM permissions need to be updated
- ⏳ Full deployment test pending

## Files Modified
- `terraform/modules/kms/main.tf` - Fixed conditional KMS policies for all three keys (Cognito, DynamoDB, CloudWatch)
