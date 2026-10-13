variable "name_prefix" {
  description = "Prefix of the Name tag of every network resource."
  type        = string
  nullable    = false
}

variable "vpc_cidr" {
  description = "Primary IPv4 CIDR of the VPC, from /16 to /28."
  type        = string
  nullable    = false

  validation {
    condition = (
      can(regex("^[0-9.]+/(1[6-9]|2[0-8])$", var.vpc_cidr)) &&
      try(cidrhost(var.vpc_cidr, 0) == split("/", var.vpc_cidr)[0], false)
    )
    error_message = "vpc_cidr must be a canonical IPv4 CIDR from /16 to /28."
  }
}

variable "public_subnets" {
  description = <<-EOT
    IPv4 CIDR of each public subnet, keyed by availability zone ID. One AZ by design: neither the
    production network validator nor Fargate RunTask needs a second, which is added only when a
    service validation requires it (a subnet has no fixed cost).
  EOT
  type        = map(string)
  nullable    = false

  validation {
    condition     = length(var.public_subnets) >= 1 && length(var.public_subnets) <= 2
    error_message = "public_subnets must declare one subnet, or two when a service requires it."
  }

  validation {
    condition = alltrue([
      for zone_id, cidr in var.public_subnets :
      can(regex("^[a-z]+[0-9]+-az[0-9]+$", zone_id)) &&
      can(regex("^[0-9.]+/[0-9]{1,2}$", cidr)) &&
      try(cidrhost(cidr, 0) == split("/", cidr)[0], false)
    ])
    error_message = "public_subnets keys must be AZ IDs and values canonical IPv4 CIDRs."
  }
}

variable "gateway_endpoints" {
  description = <<-EOT
    Free S3 and DynamoDB gateway endpoints on the public route table. Both default to on: they have
    no hourly or data processing charge, keep that traffic (ECR image layers included) off the
    internet gateway and let the task group reach it through the managed prefix lists.
  EOT
  type = object({
    s3       = bool
    dynamodb = bool
  })
  default = {
    s3       = true
    dynamodb = true
  }
  nullable = false
}
