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
