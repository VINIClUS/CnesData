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
