terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.33"
    }
  }

  backend "s3" {
    key          = "cnesdata/prod/opentofu.tfstate"
    encrypt      = true
    use_lockfile = true
  }
}
