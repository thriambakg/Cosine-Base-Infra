# AWS Secrets Manager Module

This module provides a simplified interface for managing AWS Secrets Manager secrets with proper encryption and access controls.

## Features

- **Secure Storage**: Encrypted secrets with optional KMS key
- **Lifecycle Management**: Configurable recovery windows
- **Automatic Rotation**: Support for Lambda-based rotation (optional)
- **Tagging**: Consistent resource tagging
- **Multiple Secrets**: Manage multiple secrets through a single module

## Usage

### Basic Usage

```hcl
module "secrets" {
  source = "./modules/secrets-manager"

  project_name = "cosine"
  environment  = "staging"
  
  secrets = {
    oauth-credentials = {
      description = "OAuth provider credentials"
      secret_data = {
        google_client_id     = "your-google-client-id"
        google_client_secret = "your-google-client-secret"
        apple_client_id      = "your-apple-client-id"
        apple_team_id        = "your-apple-team-id"
      }
    }
  }
  
  tags = {
    Project = "cosine"
    Purpose = "OAuth"
  }
}
```

### With KMS Encryption

```hcl
module "secrets" {
  source = "./modules/secrets-manager"

  project_name = "cosine"
  environment  = "staging"
  kms_key_id   = module.kms.main_key_id
  
  secrets = {
    database-credentials = {
      description = "Database connection credentials"
      secret_data = {
        username = "admin"
        password = "secure-password"
        host     = "db.example.com"
      }
    }
  }
}
```

### With Automatic Rotation

```hcl
module "secrets" {
  source = "./modules/secrets-manager"

  project_name = "cosine"
  environment  = "staging"
  
  secrets = {
    api-keys = {
      description = "External API keys"
      secret_data = {
        api_key    = "secret-key"
        api_secret = "secret-value"
      }
    }
  }
  
  automatic_rotation = {
    api-keys = {
      rotation_lambda_arn = aws_lambda_function.rotate_api_keys.arn
      rotation_rules = {
        automatically_after_days = 30
      }
    }
  }
}
```

## Variables

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|----------|
| project_name | Name of the project | string | - | yes |
| environment | Environment name | string | - | yes |
| secrets | Map of secrets to create | map(object) | {} | no |
| kms_key_id | KMS key ID for encryption | string | null | no |
| recovery_window_days | Recovery window in days | number | 7 | no |
| automatic_rotation | Rotation configuration | map(object) | {} | no |
| tags | Additional tags | map(string) | {} | no |

## Outputs

| Name | Description |
|------|-------------|
| secret_arns | ARNs of created secrets |
| secret_ids | IDs of created secrets |
| secret_names | Names of created secrets |
| secret_versions | Version IDs of secret versions |
| secret_values | Retrieved secret values (sensitive) |

## Security Considerations

1. **KMS Encryption**: Always use KMS encryption for sensitive secrets
2. **Access Control**: Implement proper IAM policies for secret access
3. **Rotation**: Enable automatic rotation for long-lived credentials
4. **Monitoring**: Set up CloudWatch alarms for secret access
5. **Recovery**: Configure appropriate recovery windows

## Best Practices

1. Group related secrets logically
2. Use descriptive names and descriptions
3. Tag secrets consistently for cost tracking
4. Implement least-privilege access policies
5. Monitor secret usage and access patterns
