variable "image_digest" {
  description = "Processor image digest supplied by the production root."
  type        = string
  nullable    = true

  validation {
    condition = var.image_digest == null ? true : can(
      regex("^sha256:[0-9a-f]{64}$", var.image_digest)
    )
    error_message = "image_digest_shape=expected_null_or_sha256_lower_hex64"
  }
}

variable "name_prefix" {
  description = "Product namespace for processor task families."
  type        = string
  nullable    = false
}

variable "repository_uri" {
  description = "Own private processor repository URI."
  type        = string
  nullable    = false
}

variable "task_role_arns" {
  description = "Existing task role ARNs keyed by processor mode."
  type        = map(string)
  nullable    = false
}

variable "execution_role_arn" {
  description = "Shared execution role ARN for processor tasks."
  type        = string
  nullable    = false
}

variable "log_group_name" {
  description = "Precreated processor CloudWatch log group name."
  type        = string
  nullable    = false
}

variable "control_plane_table" {
  description = "Control-plane table name shared by processor modes."
  type        = string
  nullable    = false
}

variable "data_bucket" {
  description = "Data bucket name for unit and recovery modes."
  type        = string
  nullable    = false
}

variable "audit_bucket" {
  description = "Audit bucket name for outbox dispatch."
  type        = string
  nullable    = false
}

variable "audit_retention_days" {
  description = "Audit bucket COMPLIANCE retention in days."
  type        = number
  nullable    = false
}

variable "state_machine_arn" {
  description = "Reserved unqualified production state-machine ARN."
  type        = string
  nullable    = false
}

variable "public_subnet_ids" {
  description = "Actual public subnet IDs for unit and recovery RunTask inputs."
  type        = list(string)
  nullable    = false
}

variable "security_group_id" {
  description = "Actual zero-ingress task security group ID."
  type        = string
  nullable    = false
}
