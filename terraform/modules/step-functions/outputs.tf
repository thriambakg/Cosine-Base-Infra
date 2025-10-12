output "state_machine_arn" {
  description = "ARN of the Step Functions state machine"
  value       = aws_sfn_state_machine.this.arn
}

output "state_machine_id" {
  description = "ID of the Step Functions state machine"
  value       = aws_sfn_state_machine.this.id
}

output "state_machine_name" {
  description = "Name of the Step Functions state machine"
  value       = aws_sfn_state_machine.this.name
}

output "role_arn" {
  description = "ARN of the IAM role used by the state machine"
  value       = aws_iam_role.step_functions.arn
}

output "role_name" {
  description = "Name of the IAM role used by the state machine"
  value       = aws_iam_role.step_functions.name
}

output "log_group_name" {
  description = "Name of the CloudWatch log group for Step Functions"
  value       = aws_cloudwatch_log_group.step_functions.name
}

