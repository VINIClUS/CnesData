import json
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

from tests.production.infra._schedulers_plan import (
    ACCOUNT,
    AUDIT_REVISION,
    BASE_INPUTS,
    CLUSTER,
    EXECUTION_ROLE,
    PREFIX,
    RECOVERY_REVISION,
    SECURITY_GROUP,
    SUBNETS,
    Node,
    SchedulersRun,
    planned,
    schedulers_run,
)
from tests.production.infra._step_functions_iam_contracts import (
    literal_grants,
    managed_resources,
    output_value,
    policy_grants,
    resource_at,
)

type Tofu = tuple[str, dict[str, str]]

SUBJECT = "module.subject"
MODES = {"recovery": RECOVERY_REVISION, "audit": AUDIT_REVISION}
PASS_ROLE_SERVICE = {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}}


@pytest.fixture(scope="module")
def run_case(
    tofu_context: Tofu, prod_root: Path, tmp_path_factory: pytest.TempPathFactory
) -> Callable[[str, Node], SchedulersRun]:
    tmp_path = tmp_path_factory.mktemp("schedulers")

    def run(label: str, overrides: Node) -> SchedulersRun:
        return schedulers_run(tofu_context, prod_root, (tmp_path, label, overrides))

    return run


@pytest.fixture(scope="module")
def plan(run_case: Callable[[str, Node], SchedulersRun]) -> Node:
    return planned(run_case("scheduled", {}), "scheduled")


@pytest.fixture(scope="module")
def bootstrap_plan(run_case: Callable[[str, Node], SchedulersRun]) -> Node:
    overrides: Node = {"recovery_task_definition_arn": None, "audit_task_definition_arn": None}
    return planned(run_case("bootstrap", overrides), "bootstrap")


def _after(plan: Node, address: str) -> Node:
    return cast("Node", resource_at(plan, address)["change"]["after"])


def _schedule(plan: Node, mode: str) -> Node:
    return _after(plan, f'{SUBJECT}.aws_scheduler_schedule.mode["{mode}"]')


def _block(value: Node, name: str) -> Node:
    blocks = value[name]
    assert len(blocks) == 1, f"block_count name={name} count={len(blocks)}"
    return cast("Node", blocks[0])


def test_schedules_a_cada_30_min_sem_retry(plan: Node) -> None:
    schedules = managed_resources(plan, "aws_scheduler_schedule")
    assert len(schedules) == 2, f"schedule_count={len(schedules)}"
    for mode in MODES:
        schedule = _schedule(plan, mode)
        assert schedule["schedule_expression"] == "rate(30 minutes)", f"cadence mode={mode}"
        assert _block(schedule, "flexible_time_window")["mode"] == "OFF", f"window mode={mode}"
        assert schedule["state"] == "ENABLED"
        assert schedule["name"] == f"{PREFIX}-{mode}"
        target = _block(schedule, "target")
        assert target["arn"] == CLUSTER, f"target_cluster mode={mode}"
        assert _block(target, "retry_policy")["maximum_retry_attempts"] == 0, f"retry mode={mode}"
        ecs = _block(target, "ecs_parameters")
        assert ecs["launch_type"] == "FARGATE"
        assert ecs["task_count"] == 1
        assert ecs["enable_execute_command"] is False
        network = _block(ecs, "network_configuration")
        assert network["assign_public_ip"] is True
        assert network["security_groups"] == [SECURITY_GROUP]
        assert network["subnets"] == SUBNETS


def test_modos_com_revisao_e_role_proprias(plan: Node) -> None:
    roles = {}
    for mode, revision in MODES.items():
        target = _block(_schedule(plan, mode), "target")
        assert _block(target, "ecs_parameters")["task_definition_arn"] == revision
        assert revision.endswith((":7", ":3")), f"revision_not_explicit mode={mode}"
        roles[mode] = target["role_arn"]
        assert roles[mode] == f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-scheduler-{mode}"
        role = _after(plan, f'{SUBJECT}.aws_iam_role.scheduler["{mode}"]')
        assert role["name"] == f"{PREFIX}-scheduler-{mode}"
    assert len(set(roles.values())) == 2, f"shared_scheduler_role={roles!r}"
    groups = {
        _after(plan, f'{SUBJECT}.aws_scheduler_schedule_group.mode["{mode}"]')["name"]
        for mode in MODES
    }
    assert groups == {f"{PREFIX}-recovery", f"{PREFIX}-audit"}


def _schedule_arn(mode: str) -> str:
    return f"arn:aws:scheduler:us-east-2:{ACCOUNT}:schedule/{PREFIX}-{mode}/{PREFIX}-{mode}"


def test_scheduler_trust_exato_e_pass_role_restrito(plan: Node) -> None:
    for mode, revision in MODES.items():
        role = _after(plan, f'{SUBJECT}.aws_iam_role.scheduler["{mode}"]')
        assert json.loads(role["assume_role_policy"]) == {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "scheduler.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                    "Condition": {
                        "StringEquals": {
                            "aws:SourceAccount": ACCOUNT,
                            "aws:SourceArn": _schedule_arn(mode),
                        }
                    },
                }
            ],
        }, f"scheduler_trust mode={mode}"
        policy = _after(plan, f'{SUBJECT}.aws_iam_role_policy.run["{mode}"]')
        assert policy["role"] == role["name"]
        expected = literal_grants(
            ("ecs:RunTask",), (revision,), {"ArnEquals": {"ecs:cluster": CLUSTER}}
        ) | literal_grants(
            ("iam:PassRole",),
            (BASE_INPUTS["task_role_arns"][mode], EXECUTION_ROLE),
            PASS_ROLE_SERVICE,
        )
        assert policy_grants(policy["policy"]) == expected, f"scheduler_grants mode={mode}"


def test_alarme_por_falha_repetida(plan: Node) -> None:
    for mode in MODES:
        rule = _after(plan, f'{SUBJECT}.aws_cloudwatch_event_rule.task_outcome["{mode}_failed"]')
        pattern = json.loads(rule["event_pattern"])
        assert pattern["detail"]["group"] == [f"family:{PREFIX}-processor-{mode}"]
        assert pattern["detail"]["lastStatus"] == ["STOPPED"]
        alarm = _after(plan, f'{SUBJECT}.aws_cloudwatch_metric_alarm.repeated_failure["{mode}"]')
        assert alarm["dimensions"] == {"RuleName": rule["name"]}
        assert alarm["metric_name"] == "TriggeredRules"
        assert (alarm["period"], alarm["evaluation_periods"], alarm["datapoints_to_alarm"]) == (
            3600,
            2,
            2,
        ), f"single_failure_alarms mode={mode}"
        assert alarm["threshold"] == 1
        assert alarm["treat_missing_data"] == "notBreaching"


def test_alarme_de_invocacao_perdida_e_backlog_do_outbox(plan: Node) -> None:
    for mode in MODES:
        alarm = _after(plan, f'{SUBJECT}.aws_cloudwatch_metric_alarm.lost_invocation["{mode}"]')
        queries = {query["id"]: query for query in alarm["metric_query"]}
        assert queries["lost"]["expression"] == "target_errors + dropped"
        metrics = {
            _block(queries[name], "metric")["metric_name"]: _block(queries[name], "metric")[
                "dimensions"
            ]
            for name in ("target_errors", "dropped")
        }
        group = {"ScheduleGroup": f"{PREFIX}-{mode}"}
        assert metrics == {"TargetErrorCount": group, "InvocationDroppedCount": group}
    backlog = _after(plan, f"{SUBJECT}.aws_cloudwatch_metric_alarm.outbox_backlog[0]")
    success = _after(plan, f'{SUBJECT}.aws_cloudwatch_event_rule.task_outcome["audit_succeeded"]')
    assert backlog["dimensions"] == {"RuleName": success["name"]}
    assert backlog["comparison_operator"] == "LessThanThreshold"
    assert backlog["treat_missing_data"] == "breaching"
    assert json.loads(success["event_pattern"])["detail"]["containers"] == {"exitCode": [0]}


def test_audit_fora_do_alvo_de_freeze(plan: Node) -> None:
    assert output_value(plan, "freeze_target_role_names") == [f"{PREFIX}-scheduler-recovery"]
    assert output_value(plan, "freeze_excluded_role_names") == [f"{PREFIX}-scheduler-audit"]


def test_orcamento_de_96_task_horas_em_30_dias(plan: Node) -> None:
    assert output_value(plan, "task_hour_budget") == {
        "monthly_limit_hours": 100,
        "recovery_hours": 48,
        "audit_hours": 48,
        "scheduled_hours": 96,
        "unit_reserve_hours": 4,
        "billed_minutes_per_run": 2,
        "cadence_minutes": 30,
    }


def test_sem_revisoes_cria_roles_sem_run_task_e_nenhum_schedule(bootstrap_plan: Node) -> None:
    for kind in ("aws_scheduler_schedule", "aws_cloudwatch_metric_alarm"):
        assert not managed_resources(bootstrap_plan, kind), f"bootstrap_resources kind={kind}"
    for mode in MODES:
        policy = _after(bootstrap_plan, f'{SUBJECT}.aws_iam_role_policy.run["{mode}"]')
        assert {action for action, _, _ in policy_grants(policy["policy"])} == {"iam:PassRole"}


@pytest.mark.parametrize(
    ("variable", "revision"),
    [
        ("recovery_task_definition_arn", RECOVERY_REVISION.rsplit(":", 1)[0]),
        ("recovery_task_definition_arn", AUDIT_REVISION),
        ("audit_task_definition_arn", RECOVERY_REVISION),
        ("audit_task_definition_arn", AUDIT_REVISION.replace(ACCOUNT, "999999999999")),
        ("audit_task_definition_arn", AUDIT_REVISION.rsplit(":", 1)[0] + ":0"),
    ],
)
def test_rejeita_revisao_de_outro_modo_conta_ou_sem_numero(
    run_case: Callable[[str, Node], SchedulersRun], variable: str, revision: str
) -> None:
    label = f"invalid-{variable[:5]}-{abs(hash(revision)) % 10**6}"
    run = run_case(label, {variable: revision})
    assert run.diagnostics, f"revision_accepted variable={variable} revision={revision}"
    assert any("_revision=expected_own_" in str(item) for item in run.diagnostics)


def test_rejeita_role_de_task_compartilhada_entre_modos(
    run_case: Callable[[str, Node], SchedulersRun],
) -> None:
    shared = BASE_INPUTS["task_role_arns"]["recovery"]
    roles = BASE_INPUTS["task_role_arns"] | {"audit": shared}
    run = run_case("shared-task-role", {"task_role_arns": roles})
    assert any("distinct_role_per_mode" in str(item) for item in run.diagnostics)
