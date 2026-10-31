"""Contratos estáticos de composição, preservados da suíte existente."""

ACCOUNT_BPA = "aws_s3_account_public_access_block"
BPA = ("account_baseline", "s3_public_access_block")

ARN_PATHS = (
    ("kms_key_arn",),
    ("roles_anywhere", "api_role_arn"),
    ("roles_anywhere", "profile_arn"),
    ("roles_anywhere", "trust_anchor_arn"),
    ("github", "infrastructure_plan_role_arn"),
    ("github", "host_deploy_role_arn"),
    ("github", "infrastructure_deploy_role_arn"),
    ("budget_freeze", "budget_arn"),
    ("budget_freeze", "action_arn"),
    ("budget_freeze", "policy_arn"),
    ("storage", "state_bucket_arn"),
    ("storage", "evidence_bucket_arn"),
)

INVALID_SHARED_VALUES = (
    (("schema_version",), 1),
    (("account_id",), "123"),
    (("kms_key_arn",), "arn:aws:kms:us-east-2:1:key/example"),
    (("roles_anywhere", "crl", "crl_id"), "   "),
    (("roles_anywhere", "crl", "enabled"), None),
    (("roles_anywhere", "crl", "next_update"), "not-a-timestamp"),
    (("roles_anywhere", "crl", "sha256"), "ABC123"),
    (("storage", "state_bucket_name"), "INVALID_BUCKET"),
    (("storage", "evidence_bucket_name"), "ab"),
    *((path, "not-an-arn") for path in ARN_PATHS),
    ((*BPA, "block_public_acls"), False),
    ((*BPA, "block_public_policy"), False),
    ((*BPA, "ignore_public_acls"), False),
    ((*BPA, "restrict_public_buckets"), False),
)

PROHIBITED_RESOURCE_TYPES = (
    "aws_kms_key",
    "aws_kms_alias",
    "aws_ssm_parameter",
    "aws_budgets_budget",
    ACCOUNT_BPA,
    "aws_rolesanywhere_profile",
    "aws_iam_openid_connect_provider",
    "cloudflare_record",
    "hostinger_dns_record",
    "aws_iam_user",
    "aws_iam_user_policy",
    "aws_iam_access_key",
)

SHARED_RESOURCE_PREFIXES = (
    "aws_kms_",
    "aws_ssm_",
    "aws_rolesanywhere_",
    "aws_budgets_",
    "cloudflare_",
    "hostinger_",
    "aws_iam_user",
)

SHARED_RESOURCE_EXACT = {"aws_iam_access_key", "aws_iam_openid_connect_provider", ACCOUNT_BPA}
