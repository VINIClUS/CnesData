module "control_plane" {
  source = "../../modules/control-plane"

  table_name = "${local.name_prefix}-control-plane"
}

module "data_buckets" {
  source = "../../modules/data-buckets"

  bucket_names = {
    data  = "${local.name_prefix}-data"
    audit = "${local.name_prefix}-audit"
  }
}

module "cognito" {
  source = "../../modules/cognito"

  name_prefix = local.name_prefix
  # Reviewed, non-secret suffix of cnesdata-<suffix>.auth.us-east-2.amazoncognito.com: a taken
  # prefix blocks the apply until a new suffix is reviewed here.
  domain_suffix = "prod54qgag"
}

module "network" {
  source = "../../modules/network"

  name_prefix = local.name_prefix
  vpc_cidr    = "10.42.0.0/24"
  public_subnets = {
    "use2-az1" = "10.42.0.0/26"
  }
}
