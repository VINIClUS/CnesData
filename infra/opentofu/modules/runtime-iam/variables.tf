variable "name_prefix" {
  description = "Product namespace for processor identities."
  type        = string
  nullable    = false
}

variable "account_id" {
  description = "Own account boundary for ECS task assumption."
  type        = string
  nullable    = false
}

variable "repository_arn" {
  description = "Own private processor repository ARN."
  type        = string
  nullable    = false
}

variable "log_group_arn" {
  description = "Own precreated processor log group ARN."
  type        = string
  nullable    = false
}

variable "state_machine_arn" {
  description = "Exact unqualified own state-machine ARN, or null for the base module."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.state_machine_arn == null ? true : (
      var.state_machine_arn == format(
        "arn:aws:states:us-east-2:%s:stateMachine:%s", var.account_id, var.name_prefix,
      )
    )
    error_message = "state_machine_arn_identity=expected_own_unqualified_machine"
  }
}

variable "runtime_grants" {
  description = "Producer identities for optional runtime application grants."
  type = object({
    table_arn        = string
    index_arns       = object({ gsi1 = string, gsi4 = string, gsi6 = string })
    data_bucket_arn  = string
    audit_bucket_arn = string
    api_role_arn     = string
  })
  default  = null
  nullable = true

  validation {
    condition = (
      (var.runtime_grants == null || var.state_machine_arn != null) &&
      (!var.api_step_functions_enabled ||
      (var.runtime_grants != null && var.state_machine_arn != null))
    )
    error_message = var.runtime_grants == null ? (
      "api_step_functions_enabled=requires_runtime_grants"
      ) : (var.api_step_functions_enabled ? format(
        "%s\n%s",
        "runtime_grants_machine=expected_own_machine",
        "api_step_functions_enabled=requires_runtime_grants",
    ) : "runtime_grants_machine=expected_own_machine")
  }

  validation {
    condition = var.runtime_grants == null ? true : (
      var.runtime_grants.table_arn == format(
        "arn:aws:dynamodb:us-east-2:%s:table/%s-control-plane", var.account_id, var.name_prefix,
      )
    )
    error_message = "runtime_grants_table=expected_own_table"
  }

  validation {
    condition = var.runtime_grants == null ? true : try(alltrue([
      for name in ["gsi1", "gsi4", "gsi6"] :
      var.runtime_grants.index_arns[name] == format(
        "arn:aws:dynamodb:us-east-2:%s:table/%s-control-plane/index/%s",
        var.account_id, var.name_prefix, name,
      )
    ]), false)
    error_message = "runtime_grants_indexes=expected_exact_gsi1_gsi4_gsi6"
  }

  validation {
    condition = var.runtime_grants == null ? true : (
      var.runtime_grants.data_bucket_arn == "arn:aws:s3:::${var.name_prefix}-data" &&
      var.runtime_grants.audit_bucket_arn == "arn:aws:s3:::${var.name_prefix}-audit"
    )
    error_message = "runtime_grants_buckets=expected_own_data_and_audit"
  }

  validation {
    condition = var.runtime_grants == null ? true : try(
      can(regex(
        "^arn:aws:iam::[0-9]{12}:role/([^/*?]+/)*[A-Za-z0-9_+=,.@-]{1,64}$",
        var.runtime_grants.api_role_arn,
      )) &&
      can(regex("^[!-~]+$", var.runtime_grants.api_role_arn)) &&
      split(":", var.runtime_grants.api_role_arn)[4] == var.account_id,
      false,
    )
    error_message = "api_role_arn_identity=expected_own_iam_role"
  }

}

variable "api_step_functions_enabled" {
  description = "Enable API Step Functions grants after processor activation."
  type        = bool
  default     = false
  nullable    = false
}
