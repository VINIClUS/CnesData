import json

from tests.production.infra._athena_contracts import (
    ACCOUNT,
    ALARM_NAMES,
    BLOCKING_PRECONDITIONS,
    CUTOFF_FUNCTION_ARN,
    CUTOFF_NAME,
    CUTOFF_POLICY_ADDRESS,
    CUTOFF_TOPIC_ARN,
    MANIFEST_PATH,
    MODULE_ADDRESSES,
    OPERATOR_TOPIC_ARN,
    RESULTS_BUCKET,
    SCHEMA_PATH,
    SHARED_API_ROLE,
    WORKGROUP,
    alarms,
    at,
    by_name,
    of_type,
)
from tests.production.infra._athena_root_plan import (  # noqa: F401
    Node,
    fixture_all_root_plans,
    fixture_bootstrap_plan,
    fixture_scheduled_plan,
)
from tests.production.infra._cost_manifest_model import (
    AGGREGATE_LIMIT,
    PRODUCT_LIMIT,
    SCENARIOS,
    totals,
    validate,
)
from tests.production.infra._step_functions_iam_contracts import output_value, policy_grants

ATHENA_ROLES = {
    "target": {
        SHARED_API_ROLE,
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-processor-recovery",
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-scheduler-recovery",
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-step-functions",
    },
    "excluded": {
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-processor-audit",
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-scheduler-audit",
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-processor-execution",
        f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-processor-unit",
        f"arn:aws:iam::{ACCOUNT}:role/{CUTOFF_NAME}",
    },
}
API_POLICY_ADDRESS = "module.runtime_iam.aws_iam_role_policy.api[0]"
FORBIDDEN_SERVICES = ("athena:", "glue:")


def _policy_actions(plan: Node) -> dict[str, set[str]]:
    policies = {
        item["address"]: item["change"]["after"]["policy"]
        for kind in ("aws_iam_role_policy", "aws_iam_policy")
        for item in of_type(plan, kind)
    }
    return {
        address: {action for action, _, _ in policy_grants(policy)}
        for address, policy in policies.items()
    }


def _reaches_athena_or_glue(action: str) -> bool:
    return action == "*" or action.lower().startswith(FORBIDDEN_SERVICES)


def test_athena_fora_das_roles_do_api(all_root_plans: dict[str, Node]) -> None:
    for label, plan in all_root_plans.items():
        actions = _policy_actions(plan)
        assert API_POLICY_ADDRESS in actions, f"api_policy_missing plan={label}"
        offenders = {
            address: sorted(found)
            for address, grants in actions.items()
            if (found := {item for item in grants if _reaches_athena_or_glue(item)})
            and address != CUTOFF_POLICY_ADDRESS
        }
        assert offenders == {}, f"athena_outside_cutoff plan={label} offenders={offenders!r}"
        assert actions[CUTOFF_POLICY_ADDRESS] == {"athena:UpdateWorkGroup"}, f"plan={label}"


def test_workgroup_com_cutoff_por_query_e_configuracao_forcada(bootstrap_plan: Node) -> None:
    workgroup = at(bootstrap_plan, "module.athena.aws_athena_workgroup.operator")
    assert workgroup["name"] == WORKGROUP
    assert workgroup["force_destroy"] is False
    assert workgroup["state"] == "ENABLED"
    (configuration,) = workgroup["configuration"]
    assert configuration["bytes_scanned_cutoff_per_query"] == 5_000_000_000
    assert configuration["enforce_workgroup_configuration"] is True
    assert configuration["publish_cloudwatch_metrics_enabled"] is True
    assert configuration["requester_pays_enabled"] is False
    (result,) = configuration["result_configuration"]
    assert result["output_location"] == f"s3://{RESULTS_BUCKET}/results/"
    assert result["expected_bucket_owner"] == ACCOUNT
    (encryption,) = result["encryption_configuration"]
    assert encryption["encryption_option"] == "SSE_S3"
    bucket = at(bootstrap_plan, "module.athena.aws_s3_bucket.results")
    assert bucket["bucket"] == RESULTS_BUCKET
    assert bucket["force_destroy"] is False


def test_alarme_de_cutoff_aciona_sns_da_lambda(scheduled_plan: Node) -> None:
    alarm = alarms(scheduled_plan)[f"{CUTOFF_NAME}"]
    function = at(scheduled_plan, "module.athena.aws_lambda_function.cutoff")
    subscription = at(scheduled_plan, "module.athena.aws_sns_topic_subscription.cutoff")
    assert at(scheduled_plan, "module.athena.aws_sns_topic.cutoff")["name"] == CUTOFF_NAME
    assert alarm["alarm_actions"] == [CUTOFF_TOPIC_ARN]
    assert not alarm["ok_actions"]
    assert not alarm["insufficient_data_actions"]
    assert subscription["topic_arn"] == CUTOFF_TOPIC_ARN
    assert subscription["protocol"] == "lambda"
    assert subscription["endpoint"] == CUTOFF_FUNCTION_ARN
    assert function["handler"] == "cutoff_handler.handler"
    assert function["runtime"] == "python3.13"
    others = {name: item for name, item in alarms(scheduled_plan).items() if name != CUTOFF_NAME}
    for name, other in others.items():
        used = [
            item
            for key in ("alarm_actions", "ok_actions", "insufficient_data_actions")
            for item in (other.get(key) or [])
        ]
        assert CUTOFF_TOPIC_ARN not in used, f"cutoff_topic_reused alarm={name}"


def test_manifest_do_produto_ate_8_e_agregado_ate_15() -> None:
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    assert validate(manifest, schema) == []
    computed = totals(manifest)
    for scenario in SCENARIOS:
        assert computed["product"][scenario] <= PRODUCT_LIMIT, f"scenario={scenario}"
        assert computed["aggregate"][scenario] <= AGGREGATE_LIMIT, f"scenario={scenario}"
    assert computed["product"]["min"] <= computed["product"]["base"] <= computed["product"]["max"]


def test_freeze_preserva_audit_e_backup(bootstrap_plan: Node) -> None:
    target = set(output_value(bootstrap_plan, "freeze_target_role_arns"))
    excluded = set(output_value(bootstrap_plan, "freeze_excluded_role_arns"))
    roles = {
        f"arn:aws:iam::{ACCOUNT}:role/{item['change']['after']['name']}"
        for item in of_type(bootstrap_plan, "aws_iam_role")
    }
    assert target == ATHENA_ROLES["target"]
    assert excluded == ATHENA_ROLES["excluded"]
    assert target & excluded == set()
    assert target | excluded == roles | {SHARED_API_ROLE}
    scheduler_targets = output_value(bootstrap_plan, "scheduler_freeze_target_role_names")
    assert {f"arn:aws:iam::{ACCOUNT}:role/{name}" for name in scheduler_targets} <= target
    assert output_value(bootstrap_plan, "freeze_target_role_arns") == sorted(target)
    assert output_value(bootstrap_plan, "freeze_excluded_role_arns") == sorted(excluded)


def test_plan_agenda_workgroup_e_alarmes(
    bootstrap_plan: Node, scheduled_plan: Node
) -> None:
    for plan in (bootstrap_plan, scheduled_plan):
        changes = plan["resource_changes"]
        scheduled = [item for item in changes if item["address"].startswith("module.athena.")]
        assert {item["address"] for item in scheduled} == MODULE_ADDRESSES
        assert all(item["change"]["actions"] == ["create"] for item in scheduled)
        assert set(alarms(plan)) >= ALARM_NAMES
        by_name(plan, "aws_sns_topic", "name", CUTOFF_NAME)
        assert output_value(plan, "athena_workgroup_name") == WORKGROUP
        assert output_value(plan, "operator_alarm_topic_arn") == OPERATOR_TOPIC_ARN
        assert output_value(plan, "athena_cutoff_topic_arn") == CUTOFF_TOPIC_ARN
        assert output_value(plan, "activation_blocking_preconditions") == BLOCKING_PRECONDITIONS
    assert set(alarms(bootstrap_plan)) == ALARM_NAMES
    scheduler_alarms = output_value(scheduled_plan, "scheduler_alarm_names")
    names = {name for group in scheduler_alarms.values() for name in _flatten(group)}
    assert set(alarms(scheduled_plan)) == ALARM_NAMES | names


def _flatten(group: object) -> list[str]:
    if isinstance(group, str):
        return [group]
    if isinstance(group, dict):
        return [name for value in group.values() for name in _flatten(value)]
    return []
