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

variable "static_promotion" {
  type    = any
  default = null
}

variable "static_web" {
  type = any
}

module "subject" {
  source            = "../../../infra/opentofu/modules/runtime-iam"
  name_prefix       = "cnesdata-prod"
  account_id        = "123456789012"
  repository_arn    = "arn:aws:ecr:us-east-2:123456789012:repository/cnesdata-prod"
  log_group_arn     = "arn:aws:logs:us-east-2:123456789012:log-group:/ecs/cnesdata-prod:*"
  state_machine_arn = "arn:aws:states:us-east-2:123456789012:stateMachine:cnesdata-prod"
  static_promotion  = var.static_promotion
  static_web        = var.static_web
  tags = {
    Project     = "cnesdata"
    Environment = "prod"
    ManagedBy   = "opentofu"
    Owner       = "vinisantana"
  }
}

output "static_promotion_iam_contract" {
  value     = module.subject.static_promotion_iam_contract
  sensitive = true
}
