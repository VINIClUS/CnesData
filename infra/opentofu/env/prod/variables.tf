variable "shared_outputs" {
  description = "Versioned outputs consumed from the shared personal-infra state."
  type = object({
    schema_version = number
    account_id     = string
    kms_key_arn    = string
    roles_anywhere = object({
      api_role_arn     = string
      profile_arn      = string
      trust_anchor_arn = string
      crl = object({
        crl_id      = string
        enabled     = bool
        next_update = string
        sha256      = string
      })
    })
    github = object({
      infrastructure_plan_role_arn   = string
      infrastructure_deploy_role_arn = string
      host_deploy_role_arn           = string
    })
    budget_freeze = object({
      budget_arn = string
      action_arn = string
      policy_arn = string
    })
    storage = object({
      state_bucket_name    = string
      state_bucket_arn     = string
      evidence_bucket_name = string
      evidence_bucket_arn  = string
    })
    account_baseline = object({
      s3_public_access_block = object({
        block_public_acls       = bool
        block_public_policy     = bool
        ignore_public_acls      = bool
        restrict_public_buckets = bool
      })
    })
  })
  nullable = false

  validation {
    condition     = var.shared_outputs.schema_version == 2
    error_message = "shared_outputs.schema_version must be 2."
  }

  validation {
    condition     = can(regex("^[0-9]{12}$", var.shared_outputs.account_id))
    error_message = "shared_outputs.account_id must contain exactly 12 digits."
  }

  validation {
    condition = alltrue([
      for arn in [
        var.shared_outputs.kms_key_arn,
        var.shared_outputs.roles_anywhere.api_role_arn,
        var.shared_outputs.roles_anywhere.profile_arn,
        var.shared_outputs.roles_anywhere.trust_anchor_arn,
        var.shared_outputs.github.infrastructure_plan_role_arn,
        var.shared_outputs.github.infrastructure_deploy_role_arn,
        var.shared_outputs.github.host_deploy_role_arn,
        var.shared_outputs.budget_freeze.budget_arn,
        var.shared_outputs.budget_freeze.action_arn,
        var.shared_outputs.budget_freeze.policy_arn,
        var.shared_outputs.storage.state_bucket_arn,
        var.shared_outputs.storage.evidence_bucket_arn,
        ] : can(regex(
          "^arn:(aws|aws-us-gov|aws-cn):[a-z0-9-]+:[a-z0-9-]*:([0-9]{12})?:.+$",
          arn,
      ))
    ])
    error_message = "Every shared_outputs ARN must use a valid AWS ARN shape."
  }

  validation {
    condition = (
      length(trimspace(var.shared_outputs.roles_anywhere.crl.crl_id)) > 0 &&
      can(regex("^[0-9a-f]{64}$", var.shared_outputs.roles_anywhere.crl.sha256))
    )
    error_message = "Roles Anywhere CRL evidence requires an ID and lowercase SHA-256."
  }

  validation {
    condition     = var.shared_outputs.roles_anywhere.crl.enabled != null
    error_message = "Roles Anywhere CRL enabled status must be a boolean."
  }

  validation {
    condition = can(
      formatdate("YYYY-MM-DD'T'hh:mm:ssZ", var.shared_outputs.roles_anywhere.crl.next_update)
    )
    error_message = "Roles Anywhere CRL next_update must be an RFC 3339 timestamp."
  }

  validation {
    condition = alltrue([
      for bucket in [
        var.shared_outputs.storage.state_bucket_name,
        var.shared_outputs.storage.evidence_bucket_name,
      ] : can(regex("^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$", bucket))
    ])
    error_message = "Shared storage bucket names must use the approved S3 name shape."
  }

  validation {
    condition = try(alltrue([
      for enabled in [
        var.shared_outputs.account_baseline.s3_public_access_block.block_public_acls,
        var.shared_outputs.account_baseline.s3_public_access_block.block_public_policy,
        var.shared_outputs.account_baseline.s3_public_access_block.ignore_public_acls,
        var.shared_outputs.account_baseline.s3_public_access_block.restrict_public_buckets,
      ] : enabled == true
    ]), false)
    error_message = "Account S3 Block Public Access evidence must enable all four flags."
  }
}
