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
