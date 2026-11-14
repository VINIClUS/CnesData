mock_provider "aws" {
  mock_resource "aws_acm_certificate" {
    defaults = {
      arn = "arn:aws:acm:us-east-1:123456789012:certificate/11111111-1111-4111-8111-111111111111"
    }
  }
  mock_resource "aws_cloudfront_function" {
    defaults = {
      arn = "arn:aws:cloudfront::123456789012:function/fixture-spa"
    }
  }
  mock_resource "aws_cognito_user_pool" {
    defaults = {
      endpoint = "cognito-idp.us-east-2.amazonaws.com/us-east-2_fixture"
    }
  }
  mock_resource "aws_s3_bucket" {
    defaults = {
      bucket_regional_domain_name = "fixture-data.s3.us-east-2.amazonaws.com"
    }
  }
  mock_data "aws_region" {
    defaults = {
      region = "us-east-2"
    }
  }
}

mock_provider "aws" {
  alias = "global_edge"
  mock_resource "aws_acm_certificate" {
    defaults = {
      arn = "arn:aws:acm:us-east-1:123456789012:certificate/11111111-1111-4111-8111-111111111111"
    }
  }
  mock_resource "aws_cloudfront_function" {
    defaults = {
      arn = "arn:aws:cloudfront::123456789012:function/fixture-spa"
    }
  }
  mock_resource "aws_cognito_user_pool" {
    defaults = {
      endpoint = "cognito-idp.us-east-2.amazonaws.com/us-east-2_fixture"
    }
  }
  mock_resource "aws_s3_bucket" {
    defaults = {
      bucket_regional_domain_name = "fixture-data.s3.us-east-2.amazonaws.com"
    }
  }
  mock_data "aws_region" {
    defaults = {
      region = "us-east-1"
    }
  }
}

override_resource {
  target = module.control_plane.aws_dynamodb_table.control_plane
  values = {
    arn = "arn:aws:dynamodb:us-east-2:123456789012:table/cnesdata-prod-control-plane"
  }
}

override_resource {
  target = module.data_buckets.aws_s3_bucket.data
  values = {
    arn                         = "arn:aws:s3:::cnesdata-prod-data"
    id                          = "cnesdata-prod-data"
    bucket_regional_domain_name = "fixture-data.s3.us-east-2.amazonaws.com"
  }
}

override_resource {
  target = module.data_buckets.aws_s3_bucket.audit
  values = {
    arn                         = "arn:aws:s3:::cnesdata-prod-audit"
    id                          = "cnesdata-prod-audit"
    bucket_regional_domain_name = "fixture-data.s3.us-east-2.amazonaws.com"
  }
}

override_resource {
  target = module.runtime_iam.aws_iam_role.execution
  values = {
    arn = "arn:aws:iam::123456789012:role/cnesdata-prod-processor-execution"
    id  = "cnesdata-prod-processor-execution"
  }
}

override_resource {
  target = module.runtime_iam.aws_iam_role.task["unit"]
  values = {
    arn = "arn:aws:iam::123456789012:role/cnesdata-prod-processor-unit"
    id  = "cnesdata-prod-processor-unit"
  }
}

override_resource {
  target = module.runtime_iam.aws_iam_role.task["recovery"]
  values = {
    arn = "arn:aws:iam::123456789012:role/cnesdata-prod-processor-recovery"
    id  = "cnesdata-prod-processor-recovery"
  }
}

override_resource {
  target = module.runtime_iam.aws_iam_role.task["audit"]
  values = {
    arn = "arn:aws:iam::123456789012:role/cnesdata-prod-processor-audit"
    id  = "cnesdata-prod-processor-audit"
  }
}

override_resource {
  target = module.runtime_iam.aws_iam_role.step_functions[0]
  values = {
    arn = "arn:aws:iam::123456789012:role/cnesdata-prod-step-functions"
    id  = "cnesdata-prod-step-functions"
  }
}

variables {
  shared_outputs = {
    schema_version = 2
    account_id     = "123456789012"
    kms_key_arn    = "arn:aws:kms:us-east-2:123456789012:key/example"
    roles_anywhere = {
      api_role_arn     = "arn:aws:iam::123456789012:role/cnesdata-api"
      profile_arn      = "arn:aws:rolesanywhere:us-east-2:123456789012:profile/0123456789abcdef0"
      trust_anchor_arn = "arn:aws:rolesanywhere:us-east-2:123456789012:trust-anchor/0123456789abcdef0"
      crl = {
        crl_id      = "crl-0123456789abcdef0"
        enabled     = true
        next_update = "2026-12-31T23:59:59Z"
        sha256      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
      }
    }
    github = {
      infrastructure_plan_role_arn   = "arn:aws:iam::123456789012:role/github-infrastructure-plan"
      infrastructure_deploy_role_arn = "arn:aws:iam::123456789012:role/github-infrastructure-deploy"
      host_deploy_role_arn           = "arn:aws:iam::123456789012:role/github-host-deploy"
    }
    budget_freeze = {
      budget_arn = "arn:aws:budgets::123456789012:budget/cnesdata-prod"
      action_arn = "arn:aws:budgets::123456789012:budget/cnesdata-prod/action/freeze"
      policy_arn = "arn:aws:iam::123456789012:policy/cnesdata-budget-freeze"
    }
    storage = {
      state_bucket_name    = "shared-opentofu-state-example"
      state_bucket_arn     = "arn:aws:s3:::shared-opentofu-state-example"
      evidence_bucket_name = "shared-infra-evidence-example"
      evidence_bucket_arn  = "arn:aws:s3:::shared-infra-evidence-example"
    }
    account_baseline = {
      s3_public_access_block = {
        block_public_acls       = true
        block_public_policy     = true
        ignore_public_acls      = true
        restrict_public_buckets = true
      }
    }
  }
}

run "demo_seed_desligado_por_padrao" {
  command = plan

  assert {
    condition     = output.demo_seed_iam_contract == null
    error_message = "demo_seed_default=expected_null_contract"
  }
}

run "demo_seed_contrato_da_raiz" {
  command = plan

  variables {
    demo_seed = {
      tenant_partition_key  = "TENANT#64656d6f"
      trusted_principal_arn = "arn:aws:iam::123456789012:role/synthetic/demo-operator"
    }
  }

  override_resource {
    target = module.runtime_iam.aws_iam_role.demo_seed[0]
    values = {
      arn = "arn:aws:iam::123456789012:role/cnesdata-prod-demo-seed"
    }
  }

  assert {
    condition     = output.demo_seed_iam_contract == module.runtime_iam.demo_seed_iam_contract
    error_message = "demo_seed_contract=expected_unmodified_module_output"
  }

  assert {
    condition = (
      length(keys(output.demo_seed_iam_contract)) == 9 &&
      output.demo_seed_iam_contract.contract_version == "cnesdata.demo-seed-iam/v1" &&
      output.demo_seed_iam_contract.table_arn == module.control_plane.table_arn &&
      output.demo_seed_iam_contract.tenant_partition_key == var.demo_seed.tenant_partition_key &&
      output.demo_seed_iam_contract.max_session_duration_seconds == 3600
    )
    error_message = "demo_seed_contract=expected_dedicated_tenant_table_and_3600_iam_cap"
  }
}

run "saidas_cognito_da_raiz" {
  command = plan

  override_resource {
    target = module.cognito.aws_cognito_user_pool_client.dashboard
    values = {
      id = "fixture-dashboard-client"
    }
  }

  assert {
    condition     = output.cognito_client_id == "fixture-dashboard-client"
    error_message = "cognito_client_id must be the dashboard client id"
  }

  assert {
    condition     = output.cognito_issuer == module.cognito.issuer
    error_message = "cognito_issuer must forward module.cognito.issuer"
  }

  assert {
    condition     = output.cognito_client_id == module.cognito.client_id
    error_message = "cognito_client_id must forward module.cognito.client_id"
  }

  assert {
    condition     = output.cognito_domain == module.cognito.domain
    error_message = "cognito_domain must forward module.cognito.domain"
  }

  assert {
    condition     = output.cognito_authorize_url == module.cognito.authorize_url
    error_message = "cognito_authorize_url must forward module.cognito.authorize_url"
  }

  assert {
    condition     = output.cognito_token_url == module.cognito.token_url
    error_message = "cognito_token_url must forward module.cognito.token_url"
  }

  assert {
    condition     = output.cognito_logout_url == module.cognito.logout_url
    error_message = "cognito_logout_url must forward module.cognito.logout_url"
  }

  assert {
    condition = alltrue([
      startswith(output.cognito_issuer, "https://"),
      startswith(output.cognito_domain, "https://"),
      output.cognito_authorize_url == "${output.cognito_domain}/oauth2/authorize",
      output.cognito_token_url == "${output.cognito_domain}/oauth2/token",
      output.cognito_logout_url == "${output.cognito_domain}/logout",
    ])
    error_message = "hosted UI endpoints must derive from the managed domain origin"
  }
}

run "contrato_de_ativacao" {
  command = plan

  assert {
    condition     = output.activation_blocking_preconditions == []
    error_message = "CloudFront Free plan is ACTIVE per #476; no precondition may block activation"
  }

  assert {
    condition = (
      output.athena_cutoff_contract.window_bytes * output.athena_cutoff_contract.windows_per_month
      <= output.athena_cutoff_contract.month_limit_bytes
    )
    error_message = "daily Athena window must fit the monthly scan limit"
  }

  assert {
    condition = (
      output.athena_cutoff_contract.per_query_cutoff_bytes
      <= output.athena_cutoff_contract.month_limit_bytes
    )
    error_message = "per-query cutoff must not exceed the monthly scan limit"
  }
}

run "orcamento_de_task_horas" {
  command = plan

  assert {
    condition = (
      output.scheduler_task_hour_budget.scheduled_hours
      + output.scheduler_task_hour_budget.unit_reserve_hours
      <= output.scheduler_task_hour_budget.monthly_limit_hours
    )
    error_message = "scheduled task-hours plus the unit reserve must fit the monthly limit"
  }

  assert {
    condition     = output.scheduler_task_hour_budget.monthly_limit_hours <= 100
    error_message = "monthly task-hour limit must not exceed 100"
  }
}

run "particao_do_freeze" {
  command = plan

  assert {
    condition = length(setintersection(
      toset(output.freeze_target_role_arns),
      toset(output.freeze_excluded_role_arns),
    )) == 0
    error_message = "freeze target and excluded roles must be disjoint"
  }

  assert {
    condition = (
      length(output.freeze_target_role_arns) > 0 && length(output.freeze_excluded_role_arns) > 0
    )
    error_message = "freeze partition must name both target and excluded roles"
  }

  assert {
    condition     = contains(output.freeze_target_role_arns, var.shared_outputs.roles_anywhere.api_role_arn)
    error_message = "the shared API role must be a freeze target"
  }
}

run "shared_outputs_com_conta_invalida" {
  command = plan

  variables {
    shared_outputs = {
      schema_version = 2
      account_id     = "123"
      kms_key_arn    = "arn:aws:kms:us-east-2:123456789012:key/example"
      roles_anywhere = {
        api_role_arn     = "arn:aws:iam::123456789012:role/cnesdata-api"
        profile_arn      = "arn:aws:rolesanywhere:us-east-2:123456789012:profile/0123456789abcdef0"
        trust_anchor_arn = "arn:aws:rolesanywhere:us-east-2:123456789012:trust-anchor/0123456789abcdef0"
        crl = {
          crl_id      = "crl-0123456789abcdef0"
          enabled     = true
          next_update = "2026-12-31T23:59:59Z"
          sha256      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }
      }
      github = {
        infrastructure_plan_role_arn   = "arn:aws:iam::123456789012:role/github-infrastructure-plan"
        infrastructure_deploy_role_arn = "arn:aws:iam::123456789012:role/github-infrastructure-deploy"
        host_deploy_role_arn           = "arn:aws:iam::123456789012:role/github-host-deploy"
      }
      budget_freeze = {
        budget_arn = "arn:aws:budgets::123456789012:budget/cnesdata-prod"
        action_arn = "arn:aws:budgets::123456789012:budget/cnesdata-prod/action/freeze"
        policy_arn = "arn:aws:iam::123456789012:policy/cnesdata-budget-freeze"
      }
      storage = {
        state_bucket_name    = "shared-opentofu-state-example"
        state_bucket_arn     = "arn:aws:s3:::shared-opentofu-state-example"
        evidence_bucket_name = "shared-infra-evidence-example"
        evidence_bucket_arn  = "arn:aws:s3:::shared-infra-evidence-example"
      }
      account_baseline = {
        s3_public_access_block = {
          block_public_acls       = true
          block_public_policy     = true
          ignore_public_acls      = true
          restrict_public_buckets = true
        }
      }
    }
  }

  expect_failures = [var.shared_outputs]
}

run "shared_outputs_com_schema_antigo" {
  command = plan

  variables {
    shared_outputs = {
      schema_version = 1
      account_id     = "123456789012"
      kms_key_arn    = "arn:aws:kms:us-east-2:123456789012:key/example"
      roles_anywhere = {
        api_role_arn     = "arn:aws:iam::123456789012:role/cnesdata-api"
        profile_arn      = "arn:aws:rolesanywhere:us-east-2:123456789012:profile/0123456789abcdef0"
        trust_anchor_arn = "arn:aws:rolesanywhere:us-east-2:123456789012:trust-anchor/0123456789abcdef0"
        crl = {
          crl_id      = "crl-0123456789abcdef0"
          enabled     = true
          next_update = "2026-12-31T23:59:59Z"
          sha256      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }
      }
      github = {
        infrastructure_plan_role_arn   = "arn:aws:iam::123456789012:role/github-infrastructure-plan"
        infrastructure_deploy_role_arn = "arn:aws:iam::123456789012:role/github-infrastructure-deploy"
        host_deploy_role_arn           = "arn:aws:iam::123456789012:role/github-host-deploy"
      }
      budget_freeze = {
        budget_arn = "arn:aws:budgets::123456789012:budget/cnesdata-prod"
        action_arn = "arn:aws:budgets::123456789012:budget/cnesdata-prod/action/freeze"
        policy_arn = "arn:aws:iam::123456789012:policy/cnesdata-budget-freeze"
      }
      storage = {
        state_bucket_name    = "shared-opentofu-state-example"
        state_bucket_arn     = "arn:aws:s3:::shared-opentofu-state-example"
        evidence_bucket_name = "shared-infra-evidence-example"
        evidence_bucket_arn  = "arn:aws:s3:::shared-infra-evidence-example"
      }
      account_baseline = {
        s3_public_access_block = {
          block_public_acls       = true
          block_public_policy     = true
          ignore_public_acls      = true
          restrict_public_buckets = true
        }
      }
    }
  }

  expect_failures = [var.shared_outputs]
}

run "shared_outputs_com_bloqueio_publico_desligado" {
  command = plan

  variables {
    shared_outputs = {
      schema_version = 2
      account_id     = "123456789012"
      kms_key_arn    = "arn:aws:kms:us-east-2:123456789012:key/example"
      roles_anywhere = {
        api_role_arn     = "arn:aws:iam::123456789012:role/cnesdata-api"
        profile_arn      = "arn:aws:rolesanywhere:us-east-2:123456789012:profile/0123456789abcdef0"
        trust_anchor_arn = "arn:aws:rolesanywhere:us-east-2:123456789012:trust-anchor/0123456789abcdef0"
        crl = {
          crl_id      = "crl-0123456789abcdef0"
          enabled     = true
          next_update = "2026-12-31T23:59:59Z"
          sha256      = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }
      }
      github = {
        infrastructure_plan_role_arn   = "arn:aws:iam::123456789012:role/github-infrastructure-plan"
        infrastructure_deploy_role_arn = "arn:aws:iam::123456789012:role/github-infrastructure-deploy"
        host_deploy_role_arn           = "arn:aws:iam::123456789012:role/github-host-deploy"
      }
      budget_freeze = {
        budget_arn = "arn:aws:budgets::123456789012:budget/cnesdata-prod"
        action_arn = "arn:aws:budgets::123456789012:budget/cnesdata-prod/action/freeze"
        policy_arn = "arn:aws:iam::123456789012:policy/cnesdata-budget-freeze"
      }
      storage = {
        state_bucket_name    = "shared-opentofu-state-example"
        state_bucket_arn     = "arn:aws:s3:::shared-opentofu-state-example"
        evidence_bucket_name = "shared-infra-evidence-example"
        evidence_bucket_arn  = "arn:aws:s3:::shared-infra-evidence-example"
      }
      account_baseline = {
        s3_public_access_block = {
          block_public_acls       = false
          block_public_policy     = true
          ignore_public_acls      = true
          restrict_public_buckets = true
        }
      }
    }
  }

  expect_failures = [var.shared_outputs]
}
