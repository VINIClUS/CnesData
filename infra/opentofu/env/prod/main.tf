module "control_plane" {
  source = "../../modules/control-plane"

  table_name = "${local.name_prefix}-control-plane"
}
