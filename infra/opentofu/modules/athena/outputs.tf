output "workgroup_name" {
  description = "Operator Athena workgroup name."
  value       = aws_athena_workgroup.operator.name
}

output "workgroup_arn" {
  description = "Operator Athena workgroup ARN."
  value       = local.workgroup_arn
}

output "cutoff_topic_arn" {
  description = "SNS topic that triggers the cutoff Lambda: the freeze hook for Athena."
  value       = local.cutoff_topic_arn
}

output "cutoff_topic_name" {
  description = "Cutoff SNS topic name."
  value       = aws_sns_topic.cutoff.name
}

output "cutoff_function_name" {
  description = "Cutoff Lambda function name."
  value       = aws_lambda_function.cutoff.function_name
}

output "cutoff_role_name" {
  description = "Cutoff Lambda role name, excluded from the cost freeze."
  value       = aws_iam_role.cutoff.name
}
