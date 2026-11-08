"""Inventário de outputs e tipos por módulo da raiz de produção."""

from tests.production.infra._athena_contracts import (
    ATHENA_MODULE_TYPES,
    OBSERVABILITY_MODULE_TYPES,
    ROOT_OUTPUTS,
)

OUTPUTS = {
    "cloudfront_domain_name", "cloudfront_distribution_id", "acm_dns_validation_records",
    "api_loopback_contract", "dashboard_runtime_env", "processor_repository_uri",
    "processor_task_definition_arns", "processor_container_name", "processor_task_role_arns",
    "processor_task_role_names", "processor_execution_role_arn", "processor_execution_role_name",
    "processor_cluster_arn", "processor_state_machine_arn", "processor_step_functions_role_id",
    "processor_step_functions_role_arn", "scheduler_schedule_arns", "scheduler_role_arns",
    "scheduler_freeze_target_role_names", "scheduler_task_hour_budget", "scheduler_alarm_names",
} | ROOT_OUTPUTS
STATIC_TYPES = {
    "aws_s3_bucket", "aws_s3_bucket_public_access_block", "aws_s3_bucket_ownership_controls",
    "aws_s3_bucket_versioning", "aws_s3_bucket_server_side_encryption_configuration",
    "aws_s3_bucket_policy", "aws_s3_bucket_lifecycle_configuration",
    "aws_cloudfront_origin_access_control", "aws_cloudfront_function",
    "aws_cloudfront_cache_policy",
    "aws_cloudfront_response_headers_policy", "aws_wafv2_web_acl", "aws_cloudfront_distribution",
}
MODULE_TYPES = {
    "static_web": STATIC_TYPES,
    "control_plane": {"aws_dynamodb_table", "terraform_data"},
    "data_buckets": {
        "aws_s3_bucket", "aws_s3_bucket_public_access_block", "aws_s3_bucket_ownership_controls",
        "aws_s3_bucket_versioning", "aws_s3_bucket_server_side_encryption_configuration",
        "aws_s3_bucket_policy", "aws_s3_bucket_lifecycle_configuration",
        "aws_s3_bucket_cors_configuration", "aws_s3_bucket_object_lock_configuration",
    },
    "cognito": {
        "aws_cognito_user_pool", "aws_cognito_resource_server", "aws_cognito_user_pool_client",
        "aws_cognito_user_pool_domain", "aws_cognito_managed_login_branding",
    },
    "network": {
        "aws_vpc", "aws_default_security_group", "aws_internet_gateway", "aws_subnet",
        "aws_route_table", "aws_route", "aws_route_table_association", "aws_vpc_endpoint",
        "aws_security_group", "aws_vpc_security_group_egress_rule",
    },
    "processor_registry": {"aws_ecr_repository", "aws_ecr_lifecycle_policy"},
    "runtime_iam": {"aws_iam_role", "aws_iam_role_policy"},
    "schedulers": {
        "aws_iam_role", "aws_iam_role_policy", "aws_scheduler_schedule_group",
        "aws_scheduler_schedule", "aws_cloudwatch_event_rule", "aws_cloudwatch_metric_alarm",
    },
    "processing": {
        "aws_ecs_task_definition", "aws_ecs_cluster", "aws_sfn_state_machine",
        "aws_iam_role_policy",
    },
    "athena": ATHENA_MODULE_TYPES,
    "observability": OBSERVABILITY_MODULE_TYPES,
}
