# Secrets Manager Module Outputs
# modules/secrets-manager/outputs.tf

output "secret_arns" {
  description = "ARNs of created secrets"
  value = {
    for k, v in aws_secretsmanager_secret.secrets : k => v.arn
  }
}

output "secret_ids" {
  description = "IDs of created secrets"
  value = {
    for k, v in aws_secretsmanager_secret.secrets : k => v.id
  }
}

output "secret_names" {
  description = "Names of created secrets"
  value = {
    for k, v in aws_secretsmanager_secret.secrets : k => v.name
  }
}

output "secret_versions" {
  description = "Version IDs of secret versions"
  value = {
    for k, v in aws_secretsmanager_secret_version.secret_versions : k => v.version_id
  }
}

# Output for retrieving secret values (marked as sensitive)
output "secret_values" {
  description = "Retrieved secret values (use with caution)"
  value = {
    for k, v in data.aws_secretsmanager_secret_version.current : k => jsondecode(v.secret_string)
  }
  sensitive = true
}

# IAM Policy Output
output "secret_access_policy_arn" {
  description = "ARN of the IAM policy for accessing secrets"
  value       = aws_iam_policy.secret_access_policy.arn
}