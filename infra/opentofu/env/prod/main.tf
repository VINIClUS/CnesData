module "control_plane" {
  source = "../../modules/control-plane"

  table_name = "${local.name_prefix}-control-plane"
}

module "processor_registry" {
  source = "../../modules/processor-registry"

  name_prefix = local.name_prefix
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

resource "aws_acm_certificate" "static_web" {
  provider          = aws.global_edge
  domain_name       = "cnesdata.com.br"
  validation_method = "DNS"

  lifecycle {
    create_before_destroy = true
  }
}

module "static_web" {
  source = "../../modules/static-web"

  providers = {
    aws             = aws
    aws.global_edge = aws.global_edge
  }

  bucket_name = "${local.name_prefix}-static-web"

  certificate_arn = aws_acm_certificate.static_web.arn

  cognito = {
    issuer_url          = module.cognito.issuer
    hosted_login_origin = module.cognito.domain
    client_id           = module.cognito.client_id
  }

  serving_origin               = module.data_buckets.serving_origin
  csp_mode                     = "report_only"
  certificate_domain_validated = false
  waf_ruleset                  = "baseline_v1"
}

resource "aws_cloudwatch_log_group" "processor" {
  name              = "/ecs/${local.name_prefix}-processor"
  retention_in_days = 7
}

module "runtime_iam" {
  source = "../../modules/runtime-iam"

  name_prefix    = local.name_prefix
  account_id     = var.shared_outputs.account_id
  repository_arn = module.processor_registry.repository_arn
  log_group_arn  = aws_cloudwatch_log_group.processor.arn
}

module "processing" {
  source = "../../modules/processing"

  image_digest         = var.image_digest
  name_prefix          = local.name_prefix
  repository_uri       = module.processor_registry.repository_uri
  task_role_arns       = module.runtime_iam.task_role_arns
  execution_role_arn   = module.runtime_iam.execution_role_arn
  log_group_name       = aws_cloudwatch_log_group.processor.name
  control_plane_table  = module.control_plane.table_name
  data_bucket          = module.data_buckets.data_bucket_name
  audit_bucket         = module.data_buckets.audit_bucket_name
  audit_retention_days = module.data_buckets.audit_retention_days
  public_subnet_ids    = module.network.public_subnet_ids
  security_group_id    = module.network.security_group_id
  state_machine_arn = format(
    "arn:aws:states:us-east-2:%s:stateMachine:%s",
    var.shared_outputs.account_id,
    local.name_prefix,
  )
}
