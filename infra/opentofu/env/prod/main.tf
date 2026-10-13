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
