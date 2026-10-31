output "execution_role_arn" {
  description = "Shared execution identity ARN for processor tasks."
  value       = aws_iam_role.execution.arn
}

output "execution_role_name" {
  description = "Shared execution identity name for processor tasks."
  value       = aws_iam_role.execution.name
}

output "task_role_arns" {
  description = "Initially policy-free task identity ARNs by processor mode."
  value       = { for mode, role in aws_iam_role.task : mode => role.arn }
}

output "task_role_names" {
  description = "Initially policy-free task identity names by processor mode."
  value       = { for mode, role in aws_iam_role.task : mode => role.name }
}

output "step_functions_role_id" {
  description = "Step Functions role ID, or null for the base module."
  value       = one(aws_iam_role.step_functions[*].id)
}

output "step_functions_role_arn" {
  description = "Step Functions role ARN, or null for the base module."
  value       = one(aws_iam_role.step_functions[*].arn)
}
