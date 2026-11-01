output "cloudfront_domain_name" {
  description = "CloudFront distribution hostname for the static web product."
  value       = module.static_web.distribution_domain_name
}

output "cloudfront_distribution_id" {
  description = "CloudFront distribution ID for the static web product."
  value       = module.static_web.distribution_id
}

output "acm_dns_validation_records" {
  description = "DNS validation records returned by the apex ACM certificate."
  value = {
    for option in aws_acm_certificate.static_web.domain_validation_options : option.domain_name => {
      name  = option.resource_record_name
      type  = option.resource_record_type
      value = option.resource_record_value
    }
  }
}

output "api_loopback_contract" {
  description = "API loopback placeholder until the serving modules exist."
  value       = null
}

output "dashboard_runtime_env" {
  description = "Runtime environment consumed by the dashboard."
  value = {
    VITE_API_BASE_URL = module.static_web.api_base_url
  }
}

output "processor_repository_uri" {
  description = "Own private repository URI for processor images."
  value       = module.processor_registry.repository_uri
}

output "processor_task_definition_arns" {
  description = "Exact registered processor revision ARNs by mode; empty before an image digest."
  value       = module.processing.task_definition_arns
}

output "processor_container_name" {
  description = "Container name used by registered processor tasks."
  value       = module.processing.processor_container_name
}

output "processor_task_role_arns" {
  description = "Processor task identity ARNs by mode."
  value       = module.runtime_iam.task_role_arns
}

output "processor_task_role_names" {
  description = "Processor task identity names by mode."
  value       = module.runtime_iam.task_role_names
}

output "processor_execution_role_arn" {
  description = "Shared execution identity ARN for processor tasks."
  value       = module.runtime_iam.execution_role_arn
}

output "processor_execution_role_name" {
  description = "Shared execution identity name for processor tasks."
  value       = module.runtime_iam.execution_role_name
}

output "processor_cluster_arn" {
  description = "Own ECS processor cluster ARN."
  value       = module.processing.cluster_arn
}

output "processor_state_machine_arn" {
  description = "Active processor state-machine ARN, or null during bootstrap."
  value       = module.processing.state_machine_arn
}

output "processor_step_functions_role_id" {
  description = "Processor Step Functions execution role ID."
  value       = module.runtime_iam.step_functions_role_id
}

output "processor_step_functions_role_arn" {
  description = "Processor Step Functions execution role ARN."
  value       = module.runtime_iam.step_functions_role_arn
}
