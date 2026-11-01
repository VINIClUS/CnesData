"""Nomes, limites e leitores compartilhados dos contratos Athena, alarmes e custo."""

import json
from pathlib import Path
from typing import Any

from tests.production.infra._athena_root_plan import ACCOUNT, PREFIX, Node

REPO_ROOT = Path(__file__).resolve().parents[3]
MODULES = REPO_ROOT / "infra" / "opentofu" / "modules"
MANIFEST_PATH = MODULES / "observability" / "cost-manifest.json"
SCHEMA_PATH = REPO_ROOT / "schemas" / "cost-manifest.schema.json"
POLICIES = REPO_ROOT / "policies"
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "plans"

WORKGROUP = f"{PREFIX}-operator"
WORKGROUP_ARN = f"arn:aws:athena:us-east-2:{ACCOUNT}:workgroup/{WORKGROUP}"
RESULTS_BUCKET = f"{PREFIX}-athena-results"
CUTOFF_NAME = f"{PREFIX}-athena-cutoff"
CUTOFF_ALARM_ARN = f"arn:aws:cloudwatch:us-east-2:{ACCOUNT}:alarm:{CUTOFF_NAME}"
OPERATOR_TOPIC = f"{PREFIX}-operator-alarms"
OPERATOR_TOPIC_ARN = f"arn:aws:sns:us-east-2:{ACCOUNT}:{OPERATOR_TOPIC}"
CUTOFF_TOPIC_ARN = f"arn:aws:sns:us-east-2:{ACCOUNT}:{CUTOFF_NAME}"
CUTOFF_ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/{CUTOFF_NAME}"
CUTOFF_FUNCTION_ARN = f"arn:aws:lambda:us-east-2:{ACCOUNT}:function:{CUTOFF_NAME}"
BUDGET_ARN = f"arn:aws:budgets::{ACCOUNT}:budget/cnesdata-prod"
SHARED_API_ROLE = f"arn:aws:iam::{ACCOUNT}:role/cnesdata-api"

PER_QUERY_CUTOFF_BYTES = 5_000_000_000
WINDOW_BYTES = 2_000_000_000
WINDOW_SECONDS = 86_400
MONTH_LIMIT_BYTES = 100_000_000_000
WINDOWS_PER_MONTH = 31
DETECTION_LATENCY_SECONDS = 300
BLOCKING_PRECONDITIONS = ["cloudfront_free_plan_active"]
OVERSHOOT_QUERIES = 4
CROSSING_QUERIES = 1

ROOT_OUTPUTS = frozenset({
    "athena_workgroup_name", "athena_cutoff_topic_arn", "operator_alarm_topic_arn",
    "freeze_target_role_arns", "freeze_excluded_role_arns", "athena_cutoff_contract",
    "activation_blocking_preconditions",
})
ATHENA_MODULE_TYPES = frozenset({
    "aws_athena_workgroup", "aws_cloudwatch_metric_alarm", "aws_iam_role", "aws_iam_role_policy",
    "aws_lambda_function", "aws_lambda_permission", "aws_s3_bucket",
    "aws_s3_bucket_lifecycle_configuration", "aws_s3_bucket_ownership_controls",
    "aws_s3_bucket_policy", "aws_s3_bucket_public_access_block",
    "aws_s3_bucket_server_side_encryption_configuration", "aws_sns_topic",
    "aws_sns_topic_policy", "aws_sns_topic_subscription",
})
OBSERVABILITY_MODULE_TYPES = frozenset({
    "aws_cloudwatch_event_rule", "aws_cloudwatch_log_metric_filter",
    "aws_cloudwatch_metric_alarm", "aws_sns_topic", "aws_sns_topic_policy",
})

MODULE_ADDRESSES = frozenset({
    "module.athena.aws_s3_bucket.results",
    "module.athena.aws_s3_bucket_public_access_block.results",
    "module.athena.aws_s3_bucket_ownership_controls.results",
    "module.athena.aws_s3_bucket_server_side_encryption_configuration.results",
    "module.athena.aws_s3_bucket_policy.results",
    "module.athena.aws_s3_bucket_lifecycle_configuration.results",
    "module.athena.aws_athena_workgroup.operator",
    "module.athena.aws_sns_topic.cutoff",
    "module.athena.aws_sns_topic_policy.cutoff",
    "module.athena.aws_cloudwatch_metric_alarm.cutoff",
    "module.athena.aws_iam_role.cutoff",
    "module.athena.aws_iam_role_policy.cutoff",
    "module.athena.aws_lambda_function.cutoff",
    "module.athena.aws_lambda_permission.cutoff",
    "module.athena.aws_sns_topic_subscription.cutoff",
})
CUTOFF_POLICY_ADDRESS = "module.athena.aws_iam_role_policy.cutoff"
ALARM_SUFFIXES = frozenset({
    "athena-cutoff",
    "states-execution-failures",
    "ddb-throttles",
    "ddb-system-errors",
    "ddb-sustained-write",
    "unit-task-failures",
    "unit-task-hours-proxy",
    "cutoff-lambda-errors",
    "cutoff-sns-delivery-failures",
    "processor-degraded",
    "execution-attempts-exhausted",
})
ALARM_NAMES = frozenset(f"{PREFIX}-{suffix}" for suffix in ALARM_SUFFIXES)


def at(plan: Node, address: str) -> Node:
    matches = [item for item in plan["resource_changes"] if item["address"] == address]
    assert len(matches) == 1, f"resource_missing address={address}"
    return matches[0]["change"]["after"]


def of_type(plan: Node, kind: str) -> list[Node]:
    return [
        item for item in plan["resource_changes"]
        if item["mode"] == "managed" and item["type"] == kind
    ]


def by_name(plan: Node, kind: str, field: str, value: str) -> Node:
    found = [item for item in of_type(plan, kind) if item["change"]["after"].get(field) == value]
    assert len(found) == 1, f"resource_missing type={kind} {field}={value} count={len(found)}"
    return found[0]["change"]["after"]


def alarms(plan: Node) -> dict[str, Node]:
    return {
        item["change"]["after"]["alarm_name"]: item["change"]["after"]
        for item in of_type(plan, "aws_cloudwatch_metric_alarm")
    }


def alarm_metric_count(alarm: Node) -> int:
    queries = alarm.get("metric_query") or []
    if not queries:
        return 1
    return sum(1 for query in queries if query.get("metric"))


def document(text: str) -> Any:
    return json.loads(text)
