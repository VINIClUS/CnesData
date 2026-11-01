import re
from pathlib import Path

import pytest

from tests.production.infra._athena_contracts import (
    ACCOUNT,
    ALARM_NAMES,
    CUTOFF_NAME,
    OPERATOR_TOPIC,
    PREFIX,
    REPO_ROOT,
    alarm_metric_count,
    alarms,
    at,
    by_name,
    document,
    of_type,
)
from tests.production.infra._athena_root_plan import (  # noqa: F401
    Node,
    fixture_bootstrap_plan,
    fixture_scheduled_plan,
)
from tests.production.infra._processing_native import NativePlans, Tofu
from tests.production.infra._schedulers_plan import planned, schedulers_run

TABLE = f"{PREFIX}-control-plane"
MACHINE = f"arn:aws:states:us-east-2:{ACCOUNT}:stateMachine:{PREFIX}"
LOG_GROUP = f"/ecs/{PREFIX}-processor"
ALARM_ARN_PATTERN = f"arn:aws:cloudwatch:us-east-2:{ACCOUNT}:alarm:{PREFIX}-*"
EVENTS = "AWS/Events"
PROCESSOR_NAMESPACE = "CnesData/Processor"
SCHEDULER_ALARM_COUNT = 5
TOTAL_ALARM_METRICS = 25
EXPECTED_METRICS: dict[str, set[tuple[str, str, tuple[tuple[str, str], ...]]]] = {
    "states-execution-failures": {
        ("AWS/States", name, (("StateMachineArn", MACHINE),))
        for name in ("ExecutionsFailed", "ExecutionsTimedOut", "ExecutionsAborted")
    },
    "ddb-throttles": {
        ("AWS/DynamoDB", name, (("TableName", TABLE),))
        for name in (
            "ReadMaxOnDemandThroughputThrottleEvents",
            "WriteMaxOnDemandThroughputThrottleEvents",
        )
    },
    "ddb-system-errors": {
        ("AWS/DynamoDB", "SystemErrors", (("Operation", operation), ("TableName", TABLE)))
        for operation in ("GetItem", "Query", "UpdateItem")
    },
    "ddb-sustained-write": {
        ("AWS/DynamoDB", "ConsumedWriteCapacityUnits", (("TableName", TABLE),))
    },
    "unit-task-failures": {
        (EVENTS, "TriggeredRules", (("RuleName", f"{PREFIX}-unit-task-failed"),))
    },
    "unit-task-hours-proxy": {
        (EVENTS, "TriggeredRules", (("RuleName", f"{PREFIX}-unit-task-stopped"),))
    },
    "cutoff-lambda-errors": {("AWS/Lambda", "Errors", (("FunctionName", CUTOFF_NAME),))},
    "cutoff-sns-delivery-failures": {
        ("AWS/SNS", "NumberOfNotificationsFailed", (("TopicName", CUTOFF_NAME),))
    },
    "processor-degraded": {(PROCESSOR_NAMESPACE, "ProcessorDegraded", ())},
    "execution-attempts-exhausted": {(PROCESSOR_NAMESPACE, "ExecutionAttemptsExhausted", ())},
}
FILTERS = {
    f"{PREFIX}-processor-degraded": (
        '{ ($.event = "processor_execution_probe_failed") || '
        '($.event = "processor_entrypoint_failed") || '
        '($.event = "processor_recovery_completed" && $.failed > 0) || '
        '($.event = "processor_recovery_completed" && $.reconcile_failed IS TRUE) }',
        "ProcessorDegraded",
    ),
    f"{PREFIX}-execution-attempts-exhausted": (
        '{ $.event = "dispatch_deferred *reason=monthly_attempts_exhausted" }',
        "ExecutionAttemptsExhausted",
    ),
}
SOURCES = {
    "processor_execution_probe_failed": "apps/data_processor/src/data_processor/recovery.py",
    "processor_recovery_completed": "apps/data_processor/src/data_processor/recovery.py",
    "processor_entrypoint_failed": "apps/data_processor/src/data_processor/main.py",
    "dispatch_deferred": "packages/cnes_domain/src/cnes_domain/orchestration/dispatch_admission.py",
    "monthly_attempts_exhausted": "packages/cnes_domain/src/cnes_domain/ports/environment_gate.py",
    "reconcile_failed": "apps/data_processor/src/data_processor/recovery.py",
}


def _metrics(alarm: Node) -> set[tuple[str, str, tuple[tuple[str, str], ...]]]:
    queries = [item["metric"][0] for item in alarm.get("metric_query") or [] if item.get("metric")]
    if not queries:
        queries = [alarm]
    return {
        (
            item["namespace"],
            item["metric_name"],
            tuple(sorted((item.get("dimensions") or {}).items())),
        )
        for item in queries
    }


def _operator_arn(plan: Node) -> str:
    return str(by_name(plan, "aws_sns_topic", "name", OPERATOR_TOPIC)["arn"])


def test_alarmes_de_observabilidade_leem_as_metricas_exatas(scheduled_plan: Node) -> None:
    observed = alarms(scheduled_plan)
    for suffix, expected in EXPECTED_METRICS.items():
        assert _metrics(observed[f"{PREFIX}-{suffix}"]) == expected, f"alarm={suffix}"


def test_alarmes_de_observabilidade_acionam_somente_o_topico_do_operador(
    scheduled_plan: Node,
) -> None:
    operator = _operator_arn(scheduled_plan)
    for name, alarm in alarms(scheduled_plan).items():
        if name == CUTOFF_NAME:
            continue
        assert alarm["alarm_actions"] == [operator], f"alarm={name}"
        assert not alarm["ok_actions"], f"alarm={name}"
        assert not alarm["insufficient_data_actions"], f"alarm={name}"


def test_alarmes_da_386_na_raiz_tem_exatamente_o_topico_do_operador(scheduled_plan: Node) -> None:
    operator = _operator_arn(scheduled_plan)
    scheduler = {n: a for n, a in alarms(scheduled_plan).items() if "-scheduler-" in n}
    assert len(scheduler) == SCHEDULER_ALARM_COUNT
    assert set(alarms(scheduled_plan)) == ALARM_NAMES | set(scheduler)
    for name, alarm in scheduler.items():
        assert alarm["alarm_actions"] == [operator], f"alarm={name}"
        assert not alarm["ok_actions"]
        assert not alarm["insufficient_data_actions"]


def test_variavel_alarm_actions_dos_schedulers_tem_default_vazio(
    processing_native: NativePlans,
) -> None:
    module = processing_native.configuration["root_module"]["module_calls"]["schedulers"]["module"]
    assert module["variables"]["alarm_actions"]["default"] == []


def test_limites_do_cloudwatch_e_contagem_de_metricas_de_alarme(scheduled_plan: Node) -> None:
    total = 0
    for name, alarm in alarms(scheduled_plan).items():
        periods = [
            query["metric"][0]["period"]
            for query in alarm.get("metric_query") or []
            if query.get("metric")
        ] or [alarm["period"]]
        assert len(alarm["alarm_actions"] or []) <= 5, f"alarm={name}"
        for period in periods:
            span = period * alarm["evaluation_periods"]
            assert span <= 604_800, f"alarm={name} span={span}"
            assert period >= 3600 or span <= 86_400, f"alarm={name} period={period}"
        total += alarm_metric_count(alarm)
    assert total == TOTAL_ALARM_METRICS, f"alarm_metrics={total}"


def test_alarme_de_consumo_sustentado_usa_oitenta_por_cento_do_teto_de_escrita(
    scheduled_plan: Node,
) -> None:
    alarm = alarms(scheduled_plan)[f"{PREFIX}-ddb-sustained-write"]
    assert alarm["period"] == 300
    assert alarm["threshold"] == 0.8 * 25 * 300
    assert alarm["evaluation_periods"] == 12
    assert alarm["datapoints_to_alarm"] == 12
    assert alarm["statistic"] == "Sum"


def test_alarme_de_horas_de_task_e_proxy_semanal_de_unit_tasks(scheduled_plan: Node) -> None:
    alarm = alarms(scheduled_plan)[f"{PREFIX}-unit-task-hours-proxy"]
    assert (alarm["period"], alarm["evaluation_periods"]) == (604_800, 1)
    assert alarm["threshold"] == 10
    assert alarm["treat_missing_data"] == "notBreaching"


def test_metric_filters_do_processor_usam_eventos_reais(scheduled_plan: Node) -> None:
    filters = {
        item["change"]["after"]["name"]: item["change"]["after"]
        for item in of_type(scheduled_plan, "aws_cloudwatch_log_metric_filter")
    }
    assert set(filters) == set(FILTERS)
    for name, (pattern, metric) in FILTERS.items():
        assert filters[name]["pattern"] == pattern
        assert filters[name]["log_group_name"] == LOG_GROUP
        (transformation,) = filters[name]["metric_transformation"]
        assert (transformation["namespace"], transformation["name"]) == (
            PROCESSOR_NAMESPACE, metric,
        )
        assert transformation["value"] == "1"
    for token in re.findall(r'\$\.event = "([a-z_]+)', "".join(p for p, _ in FILTERS.values())):
        source = (REPO_ROOT / SOURCES[token]).read_text(encoding="utf-8")
        assert token in source, f"event_missing token={token}"
    for token in ("monthly_attempts_exhausted", "reconcile_failed"):
        assert token in (REPO_ROOT / SOURCES[token]).read_text(encoding="utf-8")


@pytest.mark.parametrize("name", ["unit-task-failed", "unit-task-stopped"])
def test_regras_de_evento_da_task_unit_filtram_o_cluster_e_a_familia(
    scheduled_plan: Node, name: str
) -> None:
    rule = by_name(scheduled_plan, "aws_cloudwatch_event_rule", "name", f"{PREFIX}-{name}")
    pattern = document(rule["event_pattern"])
    assert pattern["source"] == ["aws.ecs"]
    assert pattern["detail-type"] == ["ECS Task State Change"]
    assert pattern["detail"]["group"] == [f"family:{PREFIX}-processor-unit"]
    assert pattern["detail"]["lastStatus"] == ["STOPPED"]
    assert ("containers" in str(pattern["detail"])) == (name == "unit-task-failed")


def test_topico_do_operador_aceita_publicacao_so_do_cloudwatch_da_conta(
    scheduled_plan: Node,
) -> None:
    topic = by_name(scheduled_plan, "aws_sns_topic", "name", OPERATOR_TOPIC)
    policy = at(scheduled_plan, "module.observability.aws_sns_topic_policy.operator")
    assert policy["arn"] == topic["arn"]
    (statement,) = document(policy["policy"])["Statement"]
    statement.pop("Sid", None)
    assert statement == {
        "Effect": "Allow",
        "Principal": {"Service": "cloudwatch.amazonaws.com"},
        "Action": "sns:Publish",
        "Resource": topic["arn"],
        "Condition": {
            "StringEquals": {"aws:SourceAccount": ACCOUNT},
            "ArnLike": {"aws:SourceArn": ALARM_ARN_PATTERN},
        },
    }


def test_plan_isolado_dos_schedulers_mantem_alarmes_sem_acoes(
    tofu_context: Tofu, prod_root: Path, tmp_path: Path
) -> None:
    run = schedulers_run(tofu_context, prod_root, (tmp_path, "isolado", {}))
    plan = planned(run, "isolado")
    isolated = alarms(plan)
    assert len(isolated) == SCHEDULER_ALARM_COUNT
    for name, alarm in isolated.items():
        assert not alarm["alarm_actions"], f"alarm={name}"
        assert not alarm["ok_actions"], f"alarm={name}"
        assert not alarm["insufficient_data_actions"], f"alarm={name}"
