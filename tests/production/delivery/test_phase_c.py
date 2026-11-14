"""Phase C: oráculos por modo, canaries de recovery e audit, reabertura e watchdog."""

from __future__ import annotations

import copy
import hashlib
import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from scripts.promotion.accept_routing import PhaseCWatchdog, WindowCutoff, accept_and_reopen
from scripts.promotion.phase_c_audit import accept_audit_canary, review_audit_plan
from scripts.promotion.phase_c_recovery import (
    INITIAL,
    RUN_POLICY,
    SCHEDULE,
    VARIABLES,
    accept_recovery_canary,
    execute_reviewed_phase_c,
    phase_c_context,
    review_recovery_plan,
)

from cnes_domain.control_plane.environment_gate import PermitHolder, ReleaseUnitPermit
from cnes_infra.aws.mode_canary import canonical_text
from scripts.promotion.environment_gate import acquire_initial_or_recovery
from scripts.promotion.mode_canary import ModeCanaryDenied, execute_mode_canary
from scripts.promotion.phase_b import MACHINE, execute_reviewed_phase_b
from scripts.promotion.rollback_api_static import RECOVERY_SECONDS
from tests.production.delivery.test_mode_canary import _Context, _launched, _record, _stopped
from tests.production.delivery.test_phase_b_execution import _establish
from tests.production.delivery.test_phase_b_execution import activation as activation_fixture
from tests.production.delivery.test_phase_b_execution import case as case_fixture
from tests.production.delivery.test_phase_b_execution import ddb as ddb_fixture
from tests.production.delivery.test_phase_b_execution import github as github_fixture
from tests.production.delivery.test_phase_b_execution import operation as operation_fixture

activation = activation_fixture
case = case_fixture
ddb = ddb_fixture
github = github_fixture
operation = operation_fixture
ROOT = Path(__file__).resolve().parents[3]
COMMANDS = {"recovery": ["recover-once"], "audit": ["dispatch-outbox-once"]}
TOPIC = "arn:aws:sns:us-east-2:123456789012:cnesdata-prod-operator"


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@pytest.fixture(params=["initial", "established"])
def phase_c(request, operation, tmp_path, activation):
    if request.param == "established":
        _establish(operation, tmp_path, activation)
    evidence = execute_reviewed_phase_b(
        operation.context, operation.close, operation.native, operation.approval
    )
    operation.phase_b = evidence
    operation.ctx = phase_c_context(operation.context, evidence)
    operation.dirty = False
    return operation


def _schedule(ctx, mode, revision):
    canary = ctx.phase_b.canary
    network = canary["network"]
    return {
        "name": f"cnesdata-prod-{mode}",
        "group_name": f"cnesdata-prod-{mode}",
        "state": "ENABLED",
        "schedule_expression": "rate(30 minutes)",
        "flexible_time_window": [{"mode": "OFF", "maximum_window_in_minutes": None}],
        "target": [{
            "arn": canary["cluster"],
            "role_arn": f"arn:aws:iam::{canary['account_id']}:role/cnesdata-prod-scheduler-{mode}",
            "retry_policy": [{"maximum_retry_attempts": 0, "maximum_event_age_in_seconds": 900}],
            "ecs_parameters": [{
                "task_definition_arn": revision,
                "launch_type": "FARGATE",
                "task_count": 1,
                "enable_ecs_managed_tags": False,
                "enable_execute_command": False,
                "network_configuration": [{
                    "assign_public_ip": True,
                    "security_groups": [network["security_group"]],
                    "subnets": sorted(network["subnets"]),
                }],
            }],
        }],
    }


def _policy(ctx, mode, revision):
    canary = ctx.phase_b.canary
    statements = [] if revision is None else [{
        "Effect": "Allow",
        "Action": "ecs:RunTask",
        "Resource": revision,
        "Condition": {"ArnEquals": {"ecs:cluster": canary["cluster"]}},
    }]
    statements.append({
        "Effect": "Allow",
        "Action": "iam:PassRole",
        "Resource": [f"arn:aws:iam::{canary['account_id']}:role/cnesdata-prod-processor-{mode}"],
        "Condition": {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}},
    })
    return {"policy": json.dumps({"Version": "2012-10-17", "Statement": statements})}


def _change(address, actions, before, after):
    return {"mode": "managed", "address": address,
            "change": {"actions": actions, "before": before, "after": after, "after_unknown": {}}}


def _plan(ctx, mode):
    revision, prior = ctx.revisions[mode], ctx.prior[mode]
    routes = ctx.routes_for(mode)
    changes = [_change(MACHINE, ["no-op"], ctx.phase_b.machine, ctx.phase_b.machine)]
    if prior is None:
        for address in INITIAL[mode]:
            after = _schedule(ctx, mode, revision) if address == SCHEDULE.format(mode) else {}
            changes.append(_change(address, ["create"], None, after))
    else:
        changes.append(_change(SCHEDULE.format(mode), ["update"],
                               _schedule(ctx, mode, prior), _schedule(ctx, mode, revision)))
    changes.append(_change(RUN_POLICY.format(mode), ["update"],
                           _policy(ctx, mode, prior), _policy(ctx, mode, revision)))
    other = "audit" if mode == "recovery" else "recovery"
    if routes[other] is not None:
        current = _schedule(ctx, other, routes[other])
        changes.append(_change(SCHEDULE.format(other), ["no-op"], current, current))
    variables = {VARIABLES[name]: {"value": value} for name, value in routes.items()}
    return {"variables": variables, "resource_changes": changes}


class _Tofu:
    def __init__(self, op, mode):
        self.op, self.mode = op, mode

    def __call__(self, args, **_):
        boundary = self.op.boundary
        if args[1] == "show":
            raw = json.dumps(_plan(self.op.ctx, self.mode)).encode()
            return subprocess.CompletedProcess(args, 0, raw, b"")
        if args[1] == "apply":
            boundary.serial += 1
            boundary.revisions[self.mode] = self.op.ctx.revisions[self.mode]
            return subprocess.CompletedProcess(args, 0, b"phase c apply", b"")
        return subprocess.CompletedProcess(args, 2 if self.op.dirty else 0, b"plan", b"")


def _approval(op, mode, phase=None):
    binary, audit = op.tmp / f"phase-c-{mode}.tfplan", op.tmp / f"phase-c-{mode}-audit.json"
    binary.write_bytes(f"reviewed phase c {mode}".encode())
    report = {
        "decision": "ACCEPT",
        "phase": phase or f"phase-c-{mode}",
        "plan_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "checkpoint": op.native.checkpoint()[0],
        "source_sha": op.close["common"]["source_sha"],
    }
    audit.write_text(json.dumps(report))
    return {"plan_path": str(binary), "audit_path": str(audit),
            "audit_sha256": hashlib.sha256(audit.read_bytes()).hexdigest()}


def _apply(op, monkeypatch, mode):
    monkeypatch.setattr("scripts.promotion.verify_routes.subprocess.run", _Tofu(op, mode))
    op.tmp = op.native.directory
    evidence = execute_reviewed_phase_c(op.ctx, op.close, op.native, _approval(op, mode))
    op.ctx = op.ctx.accept(evidence)
    return evidence


def _readbacks(ctx):
    canary = ctx.phase_b.canary
    account, network = canary["account_id"], canary["network"]
    vpc = {"Subnets": sorted(network["subnets"]), "SecurityGroups": [network["security_group"]],
           "AssignPublicIp": "ENABLED"}

    def schedule(mode):
        return {"Target": {
            "Arn": canary["cluster"],
            "RoleArn": f"arn:aws:iam::{account}:role/cnesdata-prod-scheduler-{mode}",
            "RetryPolicy": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 900},
            "EcsParameters": {"TaskDefinitionArn": ctx.revisions[mode], "TaskCount": 1,
                              "LaunchType": "FARGATE", "PlatformVersion": "1.4.0",
                              "NetworkConfiguration": {"awsvpcConfiguration": vpc}},
        }}

    def task(mode):
        return {"taskDefinition": {
            "taskDefinitionArn": ctx.revisions[mode],
            "taskRoleArn": f"arn:aws:iam::{account}:role/cnesdata-prod-processor-{mode}",
            "containerDefinitions": [{"name": "processor", "command": COMMANDS.get(mode, [])}],
        }}

    def alarm(mode):
        return {"AlarmName": f"cnesdata-prod-scheduler-{mode}-repeated-failure",
                "Namespace": "AWS/Events", "MetricName": "TriggeredRules",
                "Dimensions": [{"Name": "RuleName",
                                "Value": f"cnesdata-prod-scheduler-{mode}-failed"}],
                "Period": 3600, "EvaluationPeriods": 2, "DatapointsToAlarm": 2, "Threshold": 1.0,
                "ComparisonOperator": "GreaterThanOrEqualToThreshold", "ActionsEnabled": True,
                "AlarmActions": [TOPIC]}

    machine = ctx.phase_b.machine
    return {
        "state_machine": {"stateMachineArn": machine["arn"], "definition": machine["definition"]},
        "schedules": {mode: schedule(mode) for mode in ("recovery", "audit")},
        "task_definitions": {mode: task(mode) for mode in ("unit", "recovery", "audit")},
        "alarms": {mode: alarm(mode) for mode in ("recovery", "audit")},
    }


def _mode_run(ctx, mode, frozen=False, readbacks=None):
    canary = ctx.phase_b.canary
    common = {key: canary[key] for key in ("candidate_sha256", "source_sha", "release_id")}
    record = _record(mode, activation_sha256=ctx.activation_sha256, canary_id=canary["canary_id"],
                     machine_arn=ctx.phase_b.machine["arn"], revisions=dict(ctx.revisions),
                     **common)
    observed = readbacks or _readbacks(ctx)
    calls = _Context({"ecs:RunTask": _launched(mode), "ecs:DescribeTasks": _stopped(mode)})
    for name in ("ecs:RunTask", "ecs:DescribeTasks"):
        for task in calls.responses[name]["tasks"]:
            task["taskDefinitionArn"] = ctx.revisions[mode]
    originals = {"record": canonical_text(record).encode(), "readbacks": observed,
                 "readbacks_after": observed, "previous": None, "frozen": frozen,
                 "audit_event_ids": ["run.publishing.recovery-a"], "seed_sha256": "5" * 64,
                 "started_at": "2026-11-13T12:01:00Z"}
    return execute_mode_canary(mode, originals, calls)


def _mutate_schedule(plan, mode, **ecs):
    for item in plan["resource_changes"]:
        if item["address"] == SCHEDULE.format(mode):
            item["change"]["after"]["target"][0]["ecs_parameters"][0].update(ecs)


def _grant(plan, mode, statement):
    for item in plan["resource_changes"]:
        if item["address"] == RUN_POLICY.format(mode):
            policy = json.loads(item["change"]["after"]["policy"])
            policy["Statement"].append(statement)
            item["change"]["after"]["policy"] = json.dumps(policy)


def _foreign(plan, address, action="update"):
    plan["resource_changes"].append(_change(address, [action], {"a": 1}, {"a": 2}))


def _expect(review, plan, ctx, reason):
    with pytest.raises(ValueError, match=reason):
        review(plan, ctx)


def test_plan_de_recovery_nao_muda_unit_nem_audit(phase_c):
    ctx = phase_c.ctx
    plan = _plan(ctx, "recovery")
    assert review_recovery_plan(plan, ctx) == ("initial" if ctx.initial else "established")
    mutants = {
        "delta=unexpected": [
            lambda bad: _foreign(bad, MACHINE),
            lambda bad: _foreign(bad, SCHEDULE.format("audit")),
            lambda bad: _foreign(bad, RUN_POLICY.format("audit")),
        ],
        "delta=task_revision": [lambda bad: _foreign(
            bad, 'module.processing.aws_ecs_task_definition.processor["recovery"]', "create")],
        "variables=audit": [lambda bad: bad["variables"][VARIABLES["audit"]].update(
            value=ctx.revisions["audit"])],
        "variables=unit": [lambda bad: bad["variables"][VARIABLES["unit"]].update(value="x")],
        "schedule=": [lambda bad: _mutate_schedule(bad, "recovery", task_count=2),
                      lambda bad: _mutate_schedule(bad, "recovery", task_definition_arn="x")],
        "run_policy=": [lambda bad: _grant(bad, "recovery", {"Effect": "Allow",
                                                             "Action": "s3:*", "Resource": "*"})],
    }
    for reason, edits in mutants.items():
        for edit in edits:
            bad = copy.deepcopy(plan)
            edit(bad)
            _expect(review_recovery_plan, bad, ctx, reason)
    _expect(review_audit_plan, _plan(ctx, "audit"), ctx, "phase_c=order")


def test_plan_de_audit_nao_muda_unit_nem_recovery(phase_c, monkeypatch):
    _apply(phase_c, monkeypatch, "recovery")
    ctx = phase_c.ctx
    plan = _plan(ctx, "audit")
    assert review_audit_plan(plan, ctx) == ("initial" if ctx.initial else "established")
    mutants = {
        "delta=unexpected": [
            lambda bad: _foreign(bad, MACHINE),
            lambda bad: _foreign(bad, SCHEDULE.format("recovery")),
            lambda bad: _foreign(bad, RUN_POLICY.format("recovery")),
        ],
        "variables=recovery": [lambda bad: bad["variables"][VARIABLES["recovery"]].update(
            value=ctx.prior["recovery"])],
        "variables=unit": [lambda bad: bad["variables"][VARIABLES["unit"]].update(value="x")],
        "schedule=": [lambda bad: _mutate_schedule(bad, "audit", task_count=2)],
    }
    for reason, edits in mutants.items():
        for edit in edits:
            bad = copy.deepcopy(plan)
            edit(bad)
            _expect(review_audit_plan, bad, ctx, reason)
    _expect(review_recovery_plan, _plan(ctx, "recovery"), ctx, "phase_c=order")


def test_applies_de_c_encadeiam_activation_checkpoint_e_rotas(phase_c, monkeypatch):
    forged = copy.deepcopy(phase_c.phase_b)
    forged["common"]["activation_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="phase_c=activation_binding"):
        phase_c_context(phase_c.context, forged)
    tofu = _Tofu(phase_c, "recovery")
    monkeypatch.setattr("scripts.promotion.verify_routes.subprocess.run", tofu)
    phase_c.tmp = phase_c.native.directory
    with pytest.raises(ValueError, match="exact_independent_audit_required"):
        execute_reviewed_phase_c(phase_c.ctx, phase_c.close, phase_c.native,
                                 _approval(phase_c, "recovery", "phase-c-audit"))
    recovery = _apply(phase_c, monkeypatch, "recovery")
    audit = _apply(phase_c, monkeypatch, "audit")
    activation_sha256 = phase_c.phase_b["common"]["activation_sha256"]
    assert recovery["before"] == phase_c.phase_b["after"]
    assert audit["before"] == recovery["after"]
    assert [recovery["kind"], audit["kind"]] == ["phase-c-recovery", "phase-c-audit"]
    for evidence in (recovery, audit):
        assert evidence["common"]["activation_sha256"] == activation_sha256
    revisions = phase_c.ctx.revisions
    expected_audit = None if phase_c.ctx.initial else phase_c.ctx.prior["audit"]
    assert recovery["routes"] == {**revisions, "audit": expected_audit}
    assert audit["routes"] == dict(revisions)
    assert phase_c.adapter.observe_environment_gate("prod").fence.state.value == "CLOSED"
    tampered = copy.deepcopy(audit)
    tampered["routes"]["audit"] = "x"
    with pytest.raises(ValueError, match="phase_c=evidence"):
        phase_c.ctx.accept(tampered)


def test_recovery_sobreposta_nao_passa_de_uma_task(phase_c, monkeypatch):
    _apply(phase_c, monkeypatch, "recovery")
    ctx, gate = phase_c.ctx, phase_c.adapter
    release = phase_c.close["fence"]["promotion_id"]
    tenant = f"prod-canary-{ctx.phase_b.canary['canary_id']}"
    first = PermitHolder(tenant, "recovery-a", "0123456789abcdef")
    second = PermitHolder(tenant, "recovery-b", "fedcba9876543210")
    now = datetime.now(UTC)
    assert acquire_initial_or_recovery(gate, first, now, release)
    assert not acquire_initial_or_recovery(gate, second, now, release)
    assert acquire_initial_or_recovery(gate, first, now, release)
    assert not acquire_initial_or_recovery(gate, second, now, None)
    held = gate.observe_environment_gate("prod")
    assert getattr(held.permit, "holder", None) == first
    readbacks = _readbacks(ctx)
    raw = _mode_run(ctx, "recovery", readbacks=readbacks)
    with pytest.raises(ValueError, match="canary=unit_in_flight"):
        accept_recovery_canary(raw, ctx, readbacks, held)
    assert gate.release_unit_permit(ReleaseUnitPermit("prod", held.permit.token, now))
    idle = gate.observe_environment_gate("prod")
    accepted = accept_recovery_canary(raw, ctx, readbacks, idle)
    assert (accepted["mode"], accepted["task_receipts"]) == ("recovery", 1)
    duplicated = json.loads(raw)
    duplicated["task_receipts"] = duplicated["task_receipts"] * 2
    with pytest.raises(ValueError, match="canary=single_task"):
        accept_recovery_canary(canonical_text(duplicated).encode(), ctx, readbacks, idle)


@pytest.mark.parametrize(
    ("path", "value", "reason"),
    [
        (("task_definitions", "recovery", "taskDefinition", "taskRoleArn"), "x", "route=role"),
        (("schedules", "recovery", "Target", "RetryPolicy", "MaximumEventAgeInSeconds"), 3600,
         "route=deadline"),
        (("schedules", "recovery", "Target", "EcsParameters", "NetworkConfiguration",
          "awsvpcConfiguration", "SecurityGroups"), ["sg-other"], "route=network"),
        (("alarms", "recovery", "Dimensions"), [], "route=alarm"),
        (("alarms", "recovery", "AlarmActions"), [], "route=alarm"),
    ],
)
def test_canary_de_recovery_confere_rede_role_prazo_e_alarme(phase_c, monkeypatch, path, value,
                                                              reason):
    _apply(phase_c, monkeypatch, "recovery")
    ctx = phase_c.ctx
    readbacks = _readbacks(ctx)
    node = readbacks
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    raw = _mode_run(ctx, "recovery", readbacks=readbacks)
    idle = phase_c.adapter.observe_environment_gate("prod")
    with pytest.raises(ValueError, match=reason):
        accept_recovery_canary(raw, ctx, readbacks, idle)
    with pytest.raises(ValueError, match="canary=routing"):
        accept_recovery_canary(raw, ctx, _readbacks(ctx), idle)


def test_audit_funciona_sob_freeze_simulado(phase_c, monkeypatch):
    _apply(phase_c, monkeypatch, "recovery")
    _apply(phase_c, monkeypatch, "audit")
    ctx, readbacks = phase_c.ctx, _readbacks(phase_c.ctx)
    with pytest.raises(ModeCanaryDenied, match="operation=recovery-start"):
        _mode_run(ctx, "recovery", frozen=True)
    raw = _mode_run(ctx, "audit", frozen=True)
    accepted = accept_audit_canary(raw, ctx, readbacks)
    assert accepted["freeze_proof_sha256"] == json.loads(raw)["freeze_proof_sha256"]
    with pytest.raises(ValueError, match="canary=freeze_proof"):
        accept_audit_canary(_mode_run(ctx, "audit"), ctx, readbacks)
    pending = json.loads(raw)
    pending["pending"] = ["audit_task_failed"]
    with pytest.raises(ValueError, match="canary=pending"):
        accept_audit_canary(canonical_text(pending).encode(), ctx, readbacks)


def _canaries(op):
    ctx, readbacks = op.ctx, _readbacks(op.ctx)
    idle = op.adapter.observe_environment_gate("prod")
    return {
        "recovery": accept_recovery_canary(_mode_run(ctx, "recovery"), ctx, readbacks, idle),
        "audit": accept_audit_canary(_mode_run(ctx, "audit", frozen=True), ctx, readbacks),
    }


def _machine_elsewhere(op):
    unit = op.ctx.revisions["unit"]
    text = op.ctx.phase_b.machine["definition"].replace(unit, unit.rsplit(":", 1)[0] + ":4242")
    op.boundary.describe_state_machine = lambda **_: {
        "stateMachineArn": op.ctx.phase_b.machine["arn"], "definition": text}


MIXED = {
    "incompleto": None,
    "scheduler_fora_de_banda": lambda op: setattr(op.boundary, "drift", True),
    "maquina_mista": _machine_elsewhere,
    "state_sujo": lambda op: setattr(op, "dirty", True),
    "checkpoint_mudou": lambda op: setattr(op.boundary, "serial", op.boundary.serial + 1),
}


@pytest.mark.parametrize("case_name", sorted(MIXED))
def test_estado_misto_nao_reabre_fence(phase_c, monkeypatch, case_name):
    _apply(phase_c, monkeypatch, "recovery")
    if case_name == "incompleto":
        ctx = phase_c.ctx
        idle = phase_c.adapter.observe_environment_gate("prod")
        recovery = accept_recovery_canary(_mode_run(ctx, "recovery"), ctx, _readbacks(ctx), idle)
        canaries, reason = {"recovery": recovery}, "reopen=phase_c_incomplete"
    else:
        _apply(phase_c, monkeypatch, "audit")
        canaries = _canaries(phase_c)
        MIXED[case_name](phase_c)
        reason = "reopen=|phase_b=|phase_c="
    with pytest.raises(ValueError, match=reason):
        accept_and_reopen(phase_c.ctx, phase_c.close, phase_c.native, canaries)
    state = phase_c.adapter.observe_environment_gate("prod")
    assert state.fence.state.value == "CLOSED"
    assert state.fence.promotion_id == phase_c.close["fence"]["promotion_id"]


def test_rotas_aceitas_e_state_limpo_reabrem_fence(phase_c, monkeypatch):
    _apply(phase_c, monkeypatch, "recovery")
    _apply(phase_c, monkeypatch, "audit")
    canaries = _canaries(phase_c)
    evidence = accept_and_reopen(phase_c.ctx, phase_c.close, phase_c.native, canaries)
    assert evidence["status"] == "routes-open-operational-pending"
    assert evidence["routes"] == dict(phase_c.ctx.revisions)
    assert _instant(evidence["reopened_at"]) <= _instant(phase_c.close["fixed_deadline"])
    state = phase_c.adapter.observe_environment_gate("prod")
    assert state.fence.state.value == "OPEN"
    assert state.permit is None


def test_watchdog_converge_antes_de_trinta_minutos(operation):
    close = operation.close
    watchdog = PhaseCWatchdog.from_close(close)
    closed, fixed = _instant(close["close_confirmed_at"]), _instant(close["fixed_deadline"])
    assert fixed <= closed + timedelta(seconds=1800)
    assert watchdog.admit("phase-c-recovery", closed + timedelta(seconds=60), 300) is None
    late = fixed - timedelta(seconds=RECOVERY_SECONDS + 299)
    with pytest.raises(WindowCutoff) as refused:
        watchdog.admit("phase-c-audit", late, 300)
    decision = refused.value.decision
    assert decision["action"] == "converge-prior-routes"
    assert (decision["fence"], decision["reopen"]) == ("CLOSED", False)
    assert decision["step"] == "phase-c-audit"
    assert _instant(decision["choice_at"]) == late
    assert _instant(decision["recovery_deadline"]) <= fixed <= closed + timedelta(seconds=1800)
    assert decision["fixed_deadline"] == close["fixed_deadline"]
    with pytest.raises(WindowCutoff):
        PhaseCWatchdog.from_close(close).admit("reopen", fixed, 0)


def test_vps_sem_processor_pos_mig012_e_runbook_liga_aceite_de_rota():
    compose = yaml.safe_load((ROOT / "deploy/prod/docker-compose.prod.yml").read_text())
    assert not [name for name in compose["services"] if "processor" in name]
    runbook = (ROOT / "docs/runbooks/vps-processor-retirement.md").read_text()
    assert "scripts/promotion/accept_routing.py" in runbook
    assert "routes-open-operational-pending" in runbook
