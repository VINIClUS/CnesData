import json
import re

from tests.production.infra._athena_contracts import (
    ACCOUNT,
    BUDGET_ARN,
    CUTOFF_ALARM_ARN,
    CUTOFF_NAME,
    CUTOFF_POLICY_ADDRESS,
    CUTOFF_ROLE_ARN,
    CUTOFF_TOPIC_ARN,
    DETECTION_LATENCY_SECONDS,
    MODULES,
    MONTH_LIMIT_BYTES,
    OVERSHOOT_QUERIES,
    PER_QUERY_CUTOFF_BYTES,
    REPO_ROOT,
    WINDOW_BYTES,
    WINDOW_SECONDS,
    WINDOWS_PER_MONTH,
    WORKGROUP,
    WORKGROUP_ARN,
    alarms,
    at,
    document,
    of_type,
)
from tests.production.infra._athena_root_plan import (  # noqa: F401
    Node,
    fixture_bootstrap_plan,
    fixture_scheduled_plan,
)
from tests.production.infra._step_functions_iam_contracts import output_value, policy_grants

ROLE_TRUST = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:SourceAccount": ACCOUNT}},
        }
    ],
}
FORBIDDEN_PREFIXES = ("iam:", "s3:", "dynamodb:", "logs:", "kms:", "sts:", "lambda:", "sns:", "*")
DIMENSIONS = {"QueryType": "DML", "WorkGroup": WORKGROUP}


def test_role_de_cutoff_permite_somente_update_work_group_no_workgroup_exato(
    bootstrap_plan: Node,
) -> None:
    policy = at(bootstrap_plan, CUTOFF_POLICY_ADDRESS)
    assert policy_grants(policy["policy"]) == frozenset(
        {("athena:UpdateWorkGroup", WORKGROUP_ARN, "{}")}
    )
    role = at(bootstrap_plan, "module.athena.aws_iam_role.cutoff")
    assert role["name"] == CUTOFF_NAME
    assert (policy["name"], policy["role"]) == (f"{CUTOFF_NAME}-workgroup", role["name"])


def test_matriz_negativa_da_role_de_cutoff(bootstrap_plan: Node) -> None:
    grants = policy_grants(at(bootstrap_plan, CUTOFF_POLICY_ADDRESS)["policy"])
    actions = {action for action, _, _ in grants}
    resources = {resource for _, resource, _ in grants}
    assert resources == {WORKGROUP_ARN}
    assert "athena:*" not in actions
    assert "athena:StartQueryExecution" not in actions
    assert not [item for item in actions if item.startswith(FORBIDDEN_PREFIXES)]
    assert {item for item in actions if item.startswith("athena:")} == {"athena:UpdateWorkGroup"}


def test_role_de_cutoff_confia_so_na_lambda_da_conta_e_nao_tem_anexos(
    bootstrap_plan: Node,
) -> None:
    role = at(bootstrap_plan, "module.athena.aws_iam_role.cutoff")
    assert document(role["assume_role_policy"]) == ROLE_TRUST
    assert not role.get("inline_policy")
    assert not role.get("managed_policy_arns")
    attachments = [
        item["address"]
        for item in bootstrap_plan["resource_changes"]
        if item["address"].startswith("module.athena.")
        and item["type"] in {"aws_iam_role_policy_attachment", "aws_iam_policy_attachment"}
    ]
    assert attachments == []
    inline = [
        item["address"] for item in of_type(bootstrap_plan, "aws_iam_role_policy")
        if item["address"].startswith("module.athena.")
    ]
    assert inline == [CUTOFF_POLICY_ADDRESS]


def test_lambda_de_cutoff_tem_ambiente_exato_sem_concorrencia_reservada(
    scheduled_plan: Node,
) -> None:
    function = at(scheduled_plan, "module.athena.aws_lambda_function.cutoff")
    assert function["function_name"] == CUTOFF_NAME
    assert function["environment"] == [{"variables": {"ATHENA_WORKGROUP": WORKGROUP}}]
    assert function.get("reserved_concurrent_executions") in (None, -1)
    assert not function.get("vpc_config")
    assert not function.get("layers")
    assert not function.get("tracing_config") or function["tracing_config"][0]["mode"] != "Active"
    assert function["source_code_hash"]
    assert ".terraform" in str(function["filename"]).split("/"), "zip_inside_versioned_tree"
    assert function["role"] == CUTOFF_ROLE_ARN


def test_assinatura_do_topico_de_cutoff_invoca_a_lambda(scheduled_plan: Node) -> None:
    permission = at(scheduled_plan, "module.athena.aws_lambda_permission.cutoff")
    function = at(scheduled_plan, "module.athena.aws_lambda_function.cutoff")
    assert permission["action"] == "lambda:InvokeFunction"
    assert permission["principal"] == "sns.amazonaws.com"
    assert permission["source_arn"] == CUTOFF_TOPIC_ARN
    assert permission["function_name"] == function["function_name"]


def test_politica_do_topico_de_cutoff_aceita_so_o_alarme_e_o_budget(scheduled_plan: Node) -> None:
    policy = at(scheduled_plan, "module.athena.aws_sns_topic_policy.cutoff")
    assert policy["arn"] == CUTOFF_TOPIC_ARN
    statements = document(policy["policy"])["Statement"]
    assert len(statements) == 2

    def publish(service: str, source: str) -> dict[str, object]:
        return {
            "Effect": "Allow",
            "Principal": {"Service": service},
            "Action": "sns:Publish",
            "Resource": CUTOFF_TOPIC_ARN,
            "Condition": {
                "StringEquals": {"aws:SourceAccount": ACCOUNT},
                "ArnEquals": {"aws:SourceArn": source},
            },
        }

    expected = [
        publish("cloudwatch.amazonaws.com", CUTOFF_ALARM_ARN),
        publish("budgets.amazonaws.com", BUDGET_ARN),
    ]
    assert sorted(_without_sid(item) for item in statements) == sorted(
        json.dumps(item, sort_keys=True) for item in expected
    )


def _without_sid(statement: dict[str, object]) -> str:
    kept = {key: value for key, value in statement.items() if key != "Sid"}
    return json.dumps(kept, sort_keys=True)


def test_alarme_de_cutoff_soma_tres_estados_dml_em_janela_deslizante_de_1_dia(
    scheduled_plan: Node,
) -> None:
    alarm = alarms(scheduled_plan)[CUTOFF_NAME]
    assert alarm["threshold"] == WINDOW_BYTES
    assert alarm["comparison_operator"] == "GreaterThanOrEqualToThreshold"
    assert alarm["evaluation_periods"] == 1
    assert alarm["treat_missing_data"] == "notBreaching"
    assert not alarm.get("period")
    queries = {query["id"]: query for query in alarm["metric_query"]}
    assert set(queries) == {"total", "succeeded", "failed", "canceled"}
    assert queries["total"]["expression"] == (
        "FILL(succeeded, 0) + FILL(failed, 0) + FILL(canceled, 0)"
    )
    assert queries["total"]["return_data"] is True
    for identifier, state in (
        ("succeeded", "SUCCEEDED"), ("failed", "FAILED"), ("canceled", "CANCELED")
    ):
        (metric,) = queries[identifier]["metric"]
        assert metric["namespace"] == "AWS/Athena"
        assert metric["metric_name"] == "ProcessedBytes"
        assert metric["stat"] == "Sum"
        assert metric["period"] == WINDOW_SECONDS
        assert metric["dimensions"] == {**DIMENSIONS, "QueryState": state}
        assert not queries[identifier]["return_data"]


def test_janela_e_corte_por_query_cabem_nos_100_gb_do_mes(scheduled_plan: Node) -> None:
    alarm = alarms(scheduled_plan)[CUTOFF_NAME]
    workgroup = at(scheduled_plan, "module.athena.aws_athena_workgroup.operator")
    (configuration,) = workgroup["configuration"]
    contract = output_value(scheduled_plan, "athena_cutoff_contract")
    periods = {
        query["metric"][0]["period"] for query in alarm["metric_query"] if query.get("metric")
    }
    (period,) = periods
    cutoff = configuration["bytes_scanned_cutoff_per_query"]
    assert contract == {
        "window_bytes": WINDOW_BYTES,
        "window_seconds": WINDOW_SECONDS,
        "per_query_cutoff_bytes": PER_QUERY_CUTOFF_BYTES,
        "windows_per_month": WINDOWS_PER_MONTH,
        "overshoot_queries": OVERSHOOT_QUERIES,
        "detection_latency_s": DETECTION_LATENCY_SECONDS,
        "month_limit_bytes": MONTH_LIMIT_BYTES,
    }
    assert alarm["threshold"] == contract["window_bytes"]
    assert period == contract["window_seconds"]
    assert cutoff == contract["per_query_cutoff_bytes"]
    assert configuration["enforce_workgroup_configuration"] is True
    assert period * alarm["evaluation_periods"] <= 86_400, "evaluation_not_per_minute"
    assert contract["windows_per_month"] * period >= 31 * 86_400
    worst_case = (
        contract["windows_per_month"] * alarm["threshold"]
        + contract["overshoot_queries"] * cutoff
    )
    assert worst_case <= contract["month_limit_bytes"], f"month_bound_bytes={worst_case}"


def test_unico_ignore_changes_e_o_estado_do_workgroup() -> None:
    found = []
    for path in sorted((REPO_ROOT / "infra" / "opentofu").rglob("*.tf")):
        if ".terraform" in path.parts:
            continue
        relative = path.relative_to(MODULES.parent).as_posix()
        lines = path.read_text(encoding="utf-8").splitlines()
        found.extend((relative, line.strip()) for line in lines if "ignore_changes" in line)
    assert [item[1] for item in found] == ["ignore_changes = [state]"], f"found={found!r}"
    assert found[0][0].startswith("modules/athena/")
    text = (MODULES / found[0][0].removeprefix("modules/")).read_text(encoding="utf-8")
    block = re.search(r'resource "aws_athena_workgroup" "operator" \{(.*?)\n\}\n', text, re.S)
    assert block is not None
    assert "ignore_changes = [state]" in block.group(1)
