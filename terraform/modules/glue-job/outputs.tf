output "job_name" {
  description = "Name of the Glue job"
  value       = aws_glue_job.this.name
}

output "job_arn" {
  description = "ARN of the Glue job"
  value       = aws_glue_job.this.arn
}

output "job_id" {
  description = "ID of the Glue job"
  value       = aws_glue_job.this.id
}

output "role_arn" {
  description = "ARN of the IAM role for the Glue job"
  value       = aws_iam_role.glue_role.arn
}

output "role_name" {
  description = "Name of the IAM role for the Glue job"
  value       = aws_iam_role.glue_role.name
}

