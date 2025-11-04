output "job_name" {
  description = "Name of the Glue job"
  value       = aws_glue_job.job.name
}

output "job_arn" {
  description = "ARN of the Glue job"
  value       = aws_glue_job.job.arn
}

output "job_id" {
  description = "ID of the Glue job"
  value       = aws_glue_job.job.id
}

output "execution_role_arn" {
  description = "ARN of the Glue job execution role"
  value       = aws_iam_role.glue_job_role.arn
}

output "execution_role_name" {
  description = "Name of the Glue job execution role"
  value       = aws_iam_role.glue_job_role.name
}

output "script_location" {
  description = "S3 location of the Glue script"
  value       = aws_glue_job.job.command[0].script_location
}

output "cloudwatch_log_group_name" {
  description = "CloudWatch log group name for the Glue job"
  value       = var.enable_cloudwatch_logs ? aws_cloudwatch_log_group.glue_job_logs[0].name : null
}

output "glue_job_name" {
  description = "Name of the Glue job (alias for job_name)"
  value       = aws_glue_job.job.name
}

