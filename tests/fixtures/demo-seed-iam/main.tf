terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.54.0"
    }
  }
}
provider "aws" {
  region = "us-east-2"
}
variable "name_prefix" {
  type = any
}
variable "account_id" {
  type = any
}
variable "repository_arn" {
  type = any
}
variable "log_group_arn" {
  type = any
}
variable "state_machine_arn" {
  type = any
}
variable "runtime_grants" {
  type = any
}
variable "canary_release" {
  type = any
}
variable "demo_seed" {
  type    = any
  default = null
}
module "subject" {
  source            = "../../../infra/opentofu/modules/runtime-iam"
  name_prefix       = var.name_prefix
  account_id        = var.account_id
  repository_arn    = var.repository_arn
  log_group_arn     = var.log_group_arn
  state_machine_arn = var.state_machine_arn
  runtime_grants    = var.runtime_grants
  canary_release    = var.canary_release
  demo_seed         = var.demo_seed
}
output "demo_seed_iam_contract" {
  value = module.subject.demo_seed_iam_contract
}
output "canary_contract" {
  value = {
    canary_task_role_arn              = module.subject.canary_task_role_arn
    canary_execution_role_arn         = module.subject.canary_execution_role_arn
    canary_seeder_role_arn            = module.subject.canary_seeder_role_arn
    canary_verifier_role_arn          = module.subject.canary_verifier_role_arn
    canary_verifier_role_id           = module.subject.canary_verifier_role_id
    promotion_fence_operator_role_arn = module.subject.promotion_fence_operator_role_arn
    canary_session_names              = module.subject.canary_session_names
    canary_event_id                   = module.subject.canary_event_id
    canary_object_arns                = module.subject.canary_object_arns
    canary_max_session_duration       = module.subject.canary_max_session_duration
  }
}
