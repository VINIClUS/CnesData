output "issuer" {
  description = "OIDC issuer of the pool: OIDC_ISSUER and the dashboard VITE_OIDC_AUTHORITY."
  value       = "https://${aws_cognito_user_pool.this.endpoint}"
}

output "audience" {
  description = "API resource server identifier, the bound access token aud: OIDC_AUDIENCE."
  value       = aws_cognito_resource_server.api.identifier
}

output "user_pool_id" {
  description = "User pool ID."
  value       = aws_cognito_user_pool.this.id
}

output "client_id" {
  description = "Public SPA client ID: the dashboard VITE_OIDC_CLIENT_ID."
  value       = aws_cognito_user_pool_client.dashboard.id
}

output "domain" {
  description = "Managed prefix domain origin: OIDC_DOMAIN, VITE_OIDC_DOMAIN and the web CSP."
  value       = local.domain_origin
}

output "authorize_url" {
  description = "Absolute authorization endpoint of the hosted UI."
  value       = "${local.domain_origin}/oauth2/authorize"
}

output "token_url" {
  description = "Absolute token endpoint of the hosted UI."
  value       = "${local.domain_origin}/oauth2/token"
}

output "logout_url" {
  description = "Absolute logout endpoint of the hosted UI."
  value       = "${local.domain_origin}/logout"
}
