# S3 Module Outputs
# modules/s3/outputs.tf

output "bucket_id" {
  description = "ID of the S3 bucket"
  value       = var.create ? aws_s3_bucket.this[0].id : var.bucket_name
}

output "bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = var.create ? aws_s3_bucket.this[0].arn : "arn:aws:s3:::${var.bucket_name}"
}

output "bucket_domain_name" {
  description = "Domain name of the S3 bucket"
  value       = var.create ? aws_s3_bucket.this[0].bucket_domain_name : "${var.bucket_name}.s3.amazonaws.com"
}

output "bucket_regional_domain_name" {
  description = "Regional domain name of the S3 bucket"
  value       = var.create ? aws_s3_bucket.this[0].bucket_regional_domain_name : "${var.bucket_name}.s3.${data.aws_region.current.id}.amazonaws.com"
}

output "replica_bucket_id" {
  description = "ID of the replica S3 bucket (if created)"
  value       = var.create && var.enable_cross_region_replication ? aws_s3_bucket.replica[0].id : null
}

output "replica_bucket_arn" {
  description = "ARN of the replica S3 bucket (if created)"
  value       = var.create && var.enable_cross_region_replication ? aws_s3_bucket.replica[0].arn : null
}
