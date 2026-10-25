variable "name_prefix" {
  description = "Prefix of the user pool, resource server and app client names."
  type        = string
  nullable    = false
}

variable "domain_suffix" {
  description = <<-EOT
    Reviewed, non-secret suffix of the managed prefix domain cnesdata-<suffix>. Prefix domains are
    unique per Region across accounts: a taken prefix fails the apply as a reported blocker, with
    no fallback suffix.
  EOT
  type        = string
  nullable    = false

  validation {
    condition     = can(regex("^[a-z0-9]{6,16}$", var.domain_suffix))
    error_message = "domain_suffix_invalid expected=lowercase_alphanumeric length=6..16"
  }

  validation {
    condition     = !can(regex("aws|amazon|cognito", var.domain_suffix))
    error_message = "domain_suffix_reserved forbidden=aws,amazon,cognito"
  }
}
