# Secrets Manager Module Main Resources
# modules/secrets-manager/main.tf

# Create secrets
resource "aws_secretsmanager_secret" "secrets" {
  for_each = var.secrets

  name        = "${var.project_name}-${each.key}-${var.environment}"
  description = each.value.description
  kms_key_id  = var.kms_key_id

  recovery_window_in_days = var.recovery_window_days

  tags = merge(var.tags, {
    Name        = "${var.project_name}-${each.key}-${var.environment}"
    Type        = "Secret"
    Purpose     = "SecureStorage"
    SecretType  = each.key
  })
}

# Store secret values
resource "aws_secretsmanager_secret_version" "secret_versions" {
  for_each = var.secrets

  secret_id = aws_secretsmanager_secret.secrets[each.key].id
  secret_string = jsonencode(each.value.secret_data)

  lifecycle {
    ignore_changes = [secret_string]
  }
}

# Optional: Configure automatic rotation
resource "aws_secretsmanager_secret_rotation" "secret_rotation" {
  for_each = var.automatic_rotation

  secret_id           = aws_secretsmanager_secret.secrets[each.key].id
  rotation_lambda_arn = each.value.rotation_lambda_arn

  rotation_rules {
    automatically_after_days = each.value.rotation_rules.automatically_after_days
  }

  depends_on = [aws_secretsmanager_secret_version.secret_versions]
}

# Data source for retrieving secret values (for reference)
data "aws_secretsmanager_secret_version" "current" {
  for_each = var.secrets

  secret_id = aws_secretsmanager_secret.secrets[each.key].id

  depends_on = [aws_secretsmanager_secret_version.secret_versions]
}
