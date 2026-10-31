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
