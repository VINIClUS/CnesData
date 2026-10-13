output "public_subnet_ids" {
  description = "Sorted unique public subnet IDs: AWS_PUBLIC_SUBNET_IDS (CSV) and the ASL Subnets."
  value       = sort(distinct([for subnet in aws_subnet.public : subnet.id]))
  # Consumers get the tuple only once a task launched into it can route to AWS.
  depends_on = [aws_route.internet, aws_route_table_association.public]
}

output "security_group_id" {
  description = "Zero-ingress task group: AWS_SECURITY_GROUP_ID and the only ASL SecurityGroups ID."
  value       = aws_security_group.tasks.id
  depends_on = [
    aws_vpc_security_group_egress_rule.dns,
    aws_vpc_security_group_egress_rule.gateway_tls,
    aws_vpc_security_group_egress_rule.aws_api_tls,
  ]
}

output "public_route_table_id" {
  description = "Route table of the public subnets, with the default IPv4 route to the IGW."
  value       = aws_route_table.public.id
}

output "internet_gateway_id" {
  description = "Internet gateway carrying the task egress over its ephemeral public IPv4."
  value       = aws_internet_gateway.this.id
}

output "gateway_endpoint_ids" {
  description = "Gateway endpoint ID per service (s3, dynamodb); null when that one is disabled."
  value = {
    for service, enabled in var.gateway_endpoints :
    service => enabled ? aws_vpc_endpoint.gateway[service].id : null
  }
}
