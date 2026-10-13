terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

locals {
  app_origin  = "https://cnesdata.com.br"
  api_origin  = "https://api.cnesdata.com.br"
  api_scope   = "api.access"
  oidc_scopes = ["openid", "email", "profile"]
  domain_origin = format(
    "https://%s.auth.%s.amazoncognito.com",
    aws_cognito_user_pool_domain.this.domain,
    data.aws_region.current.region,
  )
}

data "aws_region" "current" {}

# An omitted tier creates ESSENTIALS, so LITE is explicit. Resource binding, which puts the API
# origin in the access token aud, is documented as exclusive to managed login: whether the classic
# hosted UI of a LITE pool honours it is the capability check recorded for PROD-INFRA-012, and its
# failure is an owner decision (ESSENTIALS or a verifier change), never a silent tier change.
resource "aws_cognito_user_pool" "this" {
  name                = "${var.name_prefix}-users"
  user_pool_tier      = "LITE"
  deletion_protection = "ACTIVE"
  mfa_configuration   = "OFF"
  username_attributes = ["email"]

  admin_create_user_config {
    allow_admin_create_user_only = true
  }

  # Without this setting Cognito keeps the legacy recovery order, which prefers SMS over email.
  account_recovery_setting {
    recovery_mechanism {
      name     = "verified_email"
      priority = 1
    }
  }

  password_policy {
    minimum_length    = 14
    require_lowercase = true
    require_numbers   = true
    require_symbols   = true
    require_uppercase = true
  }

  # The API default is case-sensitive and the setting is immutable after creation.
  username_configuration {
    case_sensitive = false
  }

  # Tenant memberships are keyed by the token subject: a replacement pool issues new subjects to
  # every account, so destroy is blocked both in AWS and in the plan.
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_cognito_resource_server" "api" {
  user_pool_id = aws_cognito_user_pool.this.id
  identifier   = local.api_origin
  name         = "${var.name_prefix}-api"

  scope {
    scope_name        = local.api_scope
    scope_description = "Access to the CnesData API"
  }
}

# Cognito has no setting that requires PKCE: a public client without a secret, restricted to the
# code flow, is what can be enforced here, and the dashboard always sends an S256 challenge. Every
# sign-in goes through the hosted UI, so the only SDK flow left is the refresh grant.
resource "aws_cognito_user_pool_client" "dashboard" {
  name                                 = "${var.name_prefix}-dashboard"
  user_pool_id                         = aws_cognito_user_pool.this.id
  generate_secret                      = false
  allowed_oauth_flows_user_pool_client = true
  allowed_oauth_flows                  = ["code"]
  allowed_oauth_scopes = concat(
    local.oidc_scopes,
    ["${aws_cognito_resource_server.api.identifier}/${local.api_scope}"],
  )
  callback_urls                 = ["${local.app_origin}/auth/callback"]
  logout_urls                   = [local.app_origin]
  supported_identity_providers  = ["COGNITO"]
  explicit_auth_flows           = ["ALLOW_REFRESH_TOKEN_AUTH"]
  prevent_user_existence_errors = "ENABLED"
  enable_token_revocation       = true
}

# Version 1 is the classic hosted UI, the only login UI of a LITE pool.
resource "aws_cognito_user_pool_domain" "this" {
  domain                = "cnesdata-${var.domain_suffix}"
  user_pool_id          = aws_cognito_user_pool.this.id
  managed_login_version = 1
}
