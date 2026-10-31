output "task_definition_arns" {
  description = "Exact registered task definition revision ARNs keyed by processor mode."
  value       = { for mode, task in aws_ecs_task_definition.processor : mode => task.arn }
}

output "processor_container_name" {
  description = "Container name shared by all registered processor task definitions."
  value       = "processor"
}

output "cluster_arn" {
  description = "Own ECS processor cluster ARN."
  value       = aws_ecs_cluster.processor.arn
}

output "state_machine_arn" {
  description = "Active Standard state-machine ARN, or null during bootstrap."
  value       = one(aws_sfn_state_machine.processor[*].arn)
}
