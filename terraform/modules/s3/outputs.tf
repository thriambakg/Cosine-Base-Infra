# S3 Module Outputs
# modules/s3/outputs.tf

output "bucket_id" {
  description = "ID of the S3 bucket"
  value       = aws_s3_bucket.this.id
}

output "bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.this.arn
}

output "bucket_domain_name" {
  description = "Domain name of the S3 bucket"
  value       = aws_s3_bucket.this.bucket_domain_name
}

output "bucket_regional_domain_name" {
  description = "Regional domain name of the S3 bucket"
  value       = aws_s3_bucket.this.bucket_regional_domain_name
}

output "replica_bucket_id" {
  description = "ID of the replica S3 bucket (if created)"
  value       = var.enable_cross_region_replication ? aws_s3_bucket.replica[0].id : null
}

output "replica_bucket_arn" {
  description = "ARN of the replica S3 bucket (if created)"
  value       = var.enable_cross_region_replication ? aws_s3_bucket.replica[0].arn : null
}
