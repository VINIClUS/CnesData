"""Phase C: oráculos por modo, canaries de recovery e audit, reabertura e watchdog."""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import json
import subprocess
from datetime import UTC, datetime, timedelta
from functools import partial, reduce
from pathlib import Path

import pytest
import yaml

from cnes_domain.control_plane.environment_gate import PermitHolder, ReleaseUnitPermit
from cnes_infra.aws.mode_canary import canonical_text
from scripts.promotion.accept_routing import (
    PhaseCWatchdog,
    WindowCutoff,
    _instant,
    accept_and_reopen,
    canary_status,
)
from scripts.promotion.environment_gate import acquire_initial_or_recovery
from scripts.promotion.mode_canary import execute_mode_canary
from scripts.promotion.phase_b import MACHINE, execute_reviewed_phase_b
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
from scripts.promotion.rollback_api_static import RECOVERY_SECONDS
from tests.production.delivery import test_phase_b_execution as base
from tests.production.delivery.test_mode_canary import _Context, _launched, _record, _stopped

activation, case, ddb, github = base.activation, base.case, base.ddb, base.github
operation = base.operation
ROOT = Path(__file__).resolve().parents[3]
ORDER = [("phase_c=order", lambda bad: None)]
COMMANDS = {"recovery": ["recover-once"], "audit": ["dispatch-outbox-once"]}
TASK = 'module.processing.aws_ecs_task_definition.processor["recovery"]'
EXTRA = [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]
DEADLINES = ("fixed_deadline", "stage_deadline", "canary_deadline", "expires_at")


@pytest.fixture(params=["initial", "established"])
def phase_c(request, operation, tmp_path, activation):
    if request.param == "established":
        base._establish(operation, tmp_path, activation)
    op, mp = operation, request.getfixturevalue("monkeypatch")
    evidence = execute_reviewed_phase_b(op.context, op.close, op.native, op.approval)
    op.phase_b, op.ctx = evidence, phase_c_context(op.context, evidence, op.close)
    op.mp, op.dirty, op.edit, op.stray = mp, False, lambda _: None, lambda _: None
    return op


def _schedule(ctx, mode, revision):
    canary = ctx.phase_b.canary
    network = {"assign_public_ip": True, "security_groups": [canary["network"]["security_group"]],
               "subnets": sorted(canary["network"]["subnets"])}
    ecs = {"task_definition_arn": revision, "launch_type": "FARGATE", "task_count": 1,
           "enable_ecs_managed_tags": False, "enable_execute_command": False,
           "network_configuration": [network], "propagate_tags": None, "tags": {}}
    role = f"arn:aws:iam::{canary['account_id']}:role/cnesdata-prod-scheduler-{mode}"
    target = {"arn": canary["cluster"], "role_arn": role, "ecs_parameters": [ecs], "input": None,
              "retry_policy": [{"maximum_retry_attempts": 0, "maximum_event_age_in_seconds": 900}]}
    return {"name": f"cnesdata-prod-{mode}", "group_name": f"cnesdata-prod-{mode}",
            "state": "ENABLED", "schedule_expression": "rate(30 minutes)", "description": None,
            "flexible_time_window": [{"mode": "OFF"}], "target": [target]}


def _policy(ctx, mode, revision, extra=()):
    canary = ctx.phase_b.canary
    role = f"arn:aws:iam::{canary['account_id']}:role/cnesdata-prod-processor-{mode}"
    run = [{"Effect": "Allow", "Action": "ecs:RunTask", "Resource": revision,
            "Condition": {"ArnEquals": {"ecs:cluster": canary["cluster"]}}}]
    passrole = {"Effect": "Allow", "Action": "iam:PassRole", "Resource": [role],
                "Condition": {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}}}
    statements = [*(run if revision else []), passrole, *extra]
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


def _tofu(op, mode):
    def run(args, **_):
        boundary = op.boundary
        if args[1] == "show":
            plan = _plan(op.ctx, mode)
            op.edit(plan)
            return subprocess.CompletedProcess(args, 0, json.dumps(plan).encode(), b"")
        if args[1] == "apply":
            boundary.serial += 1
            boundary.revisions[mode] = op.ctx.revisions[mode]
            op.stray(boundary)
            return subprocess.CompletedProcess(args, 0, b"phase c apply", b"")
        return subprocess.CompletedProcess(args, 2 if op.dirty else 0, b"plan", b"")
    return run


def _approval(op, mode, phase=None):
    binary, audit = (op.native.directory / f"phase-c-{mode}.{ext}" for ext in ("tfplan", "json"))
    binary.write_bytes(f"reviewed phase c {mode}".encode())
    report = {"decision": "ACCEPT", "phase": phase or f"phase-c-{mode}",
              "plan_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "checkpoint": op.native.checkpoint()[0],
              "source_sha": op.close["common"]["source_sha"]}
    audit.write_text(json.dumps(report))
    return {"plan_path": str(binary), "audit_path": str(audit),
            "audit_sha256": hashlib.sha256(audit.read_bytes()).hexdigest()}


def _apply(op, mode, approval=None):
    op.mp.setattr("scripts.promotion.verify_routes.subprocess.run", _tofu(op, mode))
    evidence = execute_reviewed_phase_c(op.ctx, op.native, approval or _approval(op, mode))
    op.ctx = op.ctx.accept(evidence)
    return evidence


def _readbacks(ctx):
    canary = ctx.phase_b.canary
    account, network = canary["account_id"], canary["network"]
    vpc = {"Subnets": sorted(network["subnets"]), "SecurityGroups": [network["security_group"]],
           "AssignPublicIp": "ENABLED"}
    ecs = {"TaskCount": 1, "LaunchType": "FARGATE", "PlatformVersion": "1.4.0",
           "NetworkConfiguration": {"awsvpcConfiguration": vpc}}
    schedules = {mode: {"Target": {
        "Arn": canary["cluster"], "RetryPolicy": {"MaximumRetryAttempts": 0,
                                                  "MaximumEventAgeInSeconds": 900},
        "RoleArn": f"arn:aws:iam::{account}:role/cnesdata-prod-scheduler-{mode}",
        "EcsParameters": {**copy.deepcopy(ecs), "TaskDefinitionArn": ctx.revisions[mode]},
    }} for mode in ("recovery", "audit")}
    tasks = {mode: {"taskDefinition": {
        "taskDefinitionArn": ctx.revisions[mode],
        "taskRoleArn": f"arn:aws:iam::{account}:role/cnesdata-prod-processor-{mode}",
        "containerDefinitions": [{"name": "processor", "command": COMMANDS.get(mode, [])}],
    }} for mode in ("unit", "recovery", "audit")}
    alarms = {mode: {
        "AlarmName": f"cnesdata-prod-scheduler-{mode}-repeated-failure",
        "Dimensions": [{"Name": "RuleName", "Value": f"cnesdata-prod-scheduler-{mode}-failed"}],
        "EvaluationPeriods": 2, "DatapointsToAlarm": 2, "ActionsEnabled": True,
        "AlarmActions": ["arn:aws:sns:us-east-2:123456789012:operator"],
    } for mode in ("recovery", "audit")}
    machine = ctx.phase_b.machine
    return {
        "state_machine": {"stateMachineArn": machine["arn"], "definition": machine["definition"]},
        "schedules": schedules, "task_definitions": tasks, "alarms": alarms,
    }


def _mode_run(ctx, mode, frozen=False, now=None):
    canary, common = ctx.phase_b.canary, ctx.close["common"]
    fixed = _instant(ctx.close["fixed_deadline"]).strftime("%Y-%m-%dT%H:%M:%SZ")
    bound = {key: canary[key] for key in ("candidate_sha256", "source_sha", "release_id")}
    record = _record(mode, activation_sha256=ctx.activation_sha256, canary_id=canary["canary_id"],
                     machine_arn=ctx.phase_b.machine["arn"], revisions=dict(ctx.revisions),
                     promotion_generation=common["promotion_generation"],
                     **dict.fromkeys(DEADLINES, fixed), **bound)
    observed = _readbacks(ctx)
    calls = _Context({"ecs:RunTask": _launched(mode), "ecs:DescribeTasks": _stopped(mode)})
    calls.now = lambda: now or datetime.now(UTC)
    for response in calls.responses.values():
        response["tasks"][0]["taskDefinitionArn"] = ctx.revisions[mode]
    originals = {"record": canonical_text(record).encode(), "readbacks": observed,
                 "readbacks_after": observed, "previous": None, "frozen": frozen,
                 "audit_event_ids": ["run.publishing.recovery-a"], "seed_sha256": "5" * 64,
                 "started_at": "2026-11-13T12:01:00Z"}
    return execute_mode_canary(mode, originals, calls)


def _at(plan, address):
    return next(item["change"] for item in plan["resource_changes"] if item["address"] == address)


def _ecs(plan, mode, side="after"):
    return _at(plan, SCHEDULE.format(mode))[side]["target"][0]["ecs_parameters"][0]


def _var(plan, name, value):
    plan["variables"][VARIABLES[name]]["value"] = value


def _foreign(plan, address, action="update"):
    changes = [item for item in plan["resource_changes"] if item["address"] != address]
    plan["resource_changes"] = [*changes, _change(address, [action], {"a": 1}, {"a": 2})]


def _rejects(review, plan, ctx, cases):
    for reason, edit in cases:
        bad = copy.deepcopy(plan)
        edit(bad)
        with pytest.raises(ValueError, match=reason):
            review(bad, ctx)


def _untouched(addresses):
    return [("delta=unexpected", lambda bad, a=a: _foreign(bad, a)) for a in addresses]


def _late(op, module="accept_routing", seconds=0):
    now = _instant(op.close["fixed_deadline"]) - timedelta(seconds=seconds)
    late = type("Late", (datetime,), {"now": classmethod(lambda cls, tz=None: now)})
    op.mp.setattr(f"scripts.promotion.{module}.datetime", late)


def test_plan_de_recovery_nao_muda_unit_nem_audit(phase_c):
    ctx, run, sched = phase_c.ctx, RUN_POLICY.format("recovery"), SCHEDULE.format("recovery")
    plan, revision, pol = _plan(ctx, "recovery"), ctx.revisions["recovery"], partial(
        _policy, ctx, "recovery")
    assert review_recovery_plan(plan, ctx) == ("initial" if ctx.initial else "established")
    cases = [
        *_untouched((MACHINE, SCHEDULE.format("audit"), RUN_POLICY.format("audit"))),
        ("plan=duplicate_address", lambda bad: bad["resource_changes"].append(
            copy.deepcopy(bad["resource_changes"][0]))),
        ("delta=task_revision", lambda bad: _foreign(bad, TASK, "create")),
        ("delta=action", lambda bad: _at(bad, run).update(actions=["delete", "create"])),
        ("variables=audit", lambda bad: _var(bad, "audit", ctx.revisions["audit"])),
        ("variables=unit", lambda bad: _var(bad, "unit", "x")),
        ("schedule=recovery_target", lambda bad: _ecs(bad, "recovery").update(task_count=2)),
        ("schedule=recovery_target", lambda bad: _ecs(bad, "recovery").update(propagate_tags="x")),
        ("schedule=recovery_target", lambda bad: _at(bad, sched)["after"].update(
            schedule_expression="rate(1 minute)")),
        ("schedule=recovery_target", lambda bad: _at(bad, sched)["after"]["target"][0].update(
            role_arn="x")),
        ("schedule=recovery_target", lambda bad: _at(bad, sched)["after"]["target"][0].update(
            input="{}")),
        ("run_policy=recovery_prior", lambda bad: _at(bad, run).update(before=pol("x"))),
        ("run_policy=recovery_grant", lambda bad: _at(bad, run).update(after=pol("x"))),
        ("run_policy=recovery_other", lambda bad: _at(bad, run).update(after=pol(revision, EXTRA))),
    ]
    if not ctx.initial:
        cases += [
            ("schedule=recovery_prior", lambda bad: _ecs(bad, "recovery", "before").update(
                task_definition_arn="x")),
            ("schedule=recovery_non_route", lambda bad: _at(bad, sched)["after"].update(
                description="x")),
        ]
    _rejects(review_recovery_plan, plan, ctx, cases)
    _rejects(review_audit_plan, _plan(ctx, "audit"), ctx, ORDER)


def test_plan_de_audit_nao_muda_unit_nem_recovery(phase_c):
    _rejects(review_audit_plan, _plan(phase_c.ctx, "audit"), phase_c.ctx, ORDER)
    _apply(phase_c, "recovery")
    ctx = phase_c.ctx
    plan = _plan(ctx, "audit")
    assert review_audit_plan(plan, ctx) == ("initial" if ctx.initial else "established")
    _rejects(review_audit_plan, plan, ctx, [
        *_untouched((MACHINE, SCHEDULE.format("recovery"), RUN_POLICY.format("recovery"))),
        ("variables=recovery", lambda bad: _var(bad, "recovery", ctx.prior["recovery"])),
        ("variables=unit", lambda bad: _var(bad, "unit", "x")),
        ("schedule=audit_target", lambda bad: _ecs(bad, "audit").update(task_count=2)),
    ])
    _rejects(review_recovery_plan, _plan(ctx, "recovery"), ctx, ORDER)
    good = {"kind": "phase-c-audit", "routes": ctx.routes_for("audit"), "before": ctx.checkpoint,
            **{key: ctx.phase_b_evidence[key] for key in ("common", "close_drain_sha256")}}
    assert ctx.accept(good).next_mode is None
    for key, value in (("routes", ctx.routes_for("recovery")), ("before", {"serial": 0})):
        with pytest.raises(ValueError, match="phase_c=evidence"):
            ctx.accept({**good, key: value})
    phase_c.stray = lambda boundary: boundary.revisions.update(
        recovery=boundary.revisions["recovery"] + "0")
    with pytest.raises(ValueError, match="phase_c=routes"):
        _apply(phase_c, "audit")


def test_applies_de_c_encadeiam_activation_checkpoint_e_rotas(phase_c):
    phase_b, close = phase_c.phase_b, phase_c.close
    for key, change, reason in (("common", {"activation_sha256": "0" * 64}, "activation_binding"),
                                ("routes", {"mode": "x"}, "phase_b_mode")):
        with pytest.raises(ValueError, match=f"phase_c={reason}"):
            phase_c_context(phase_c.context, {**phase_b, key: {**phase_b[key], **change}}, close)
    with pytest.raises(ValueError, match="phase_c=close_binding"):
        phase_c_context(phase_c.context, phase_b, {**close, "fixed_deadline": "x"})
    with pytest.raises(ValueError, match="exact_independent_audit_required"):
        _apply(phase_c, "recovery", _approval(phase_c, "recovery", "phase-c-audit"))
    phase_c.edit = lambda plan: _foreign(plan, MACHINE)
    with pytest.raises(ValueError, match="delta=unexpected"):
        _apply(phase_c, "recovery")
    phase_c.edit, phase_c.boundary.serial = lambda plan: None, phase_c.boundary.serial + 1
    with pytest.raises(ValueError, match="phase_c=checkpoint_chain"):
        _apply(phase_c, "recovery")
    phase_c.boundary.serial -= 1
    recovery, audit = _apply(phase_c, "recovery"), _apply(phase_c, "audit")
    assert (recovery["before"], audit["before"]) == (phase_b["after"], recovery["after"])
    assert [recovery["kind"], audit["kind"]] == ["phase-c-recovery", "phase-c-audit"]
    assert {evidence["common"]["activation_sha256"] for evidence in (recovery, audit)} == {
        phase_b["common"]["activation_sha256"]}
    assert recovery["routes"] == {**phase_c.ctx.revisions, "audit": phase_c.ctx.prior["audit"]}
    assert audit["routes"] == phase_c.ctx.revisions
    assert phase_c.adapter.observe_environment_gate("prod").fence.state.value == "CLOSED"


ROUTE_CASES = [
    ("task_definitions.recovery.taskDefinition.taskRoleArn", "x", "route=role"),
    ("schedules.recovery.Target.EcsParameters.TaskDefinitionArn", "x", "route=revision"),
    ("schedules.recovery.Target.RetryPolicy.MaximumEventAgeInSeconds", 3600, "route=deadline"),
    ("schedules.recovery.Target.EcsParameters.NetworkConfiguration.awsvpcConfiguration"
     ".SecurityGroups", ["sg-other"], "route=network"),
    ("alarms.recovery.Dimensions", [], "route=alarm"),
    ("alarms.recovery.AlarmActions", [], "route=alarm"),
]


def test_recovery_sobreposta_nao_passa_de_uma_task(phase_c):
    _apply(phase_c, "recovery")
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
    raw = _mode_run(ctx, "recovery")
    with pytest.raises(ValueError, match="canary=unit_in_flight"):
        accept_recovery_canary(raw, ctx, readbacks, held)
    assert gate.release_unit_permit(ReleaseUnitPermit("prod", held.permit.token, now))
    idle = gate.observe_environment_gate("prod")
    accepted = accept_recovery_canary(raw, ctx, readbacks, idle)
    assert (accepted.mode, accepted.task_receipts) == ("recovery", 1)
    duplicated = {**json.loads(raw), "task_receipts": json.loads(raw)["task_receipts"] * 2}
    other = dataclasses.replace(idle, fence=dataclasses.replace(idle.fence, promotion_id="x"))
    refusals = [("canary=single_task", canonical_text(duplicated).encode(), readbacks, idle),
                ("canary=fence", raw, readbacks, other),
                ("canary=routing", raw, {**readbacks, "extra": 1}, idle)]
    for path, value, reason in ROUTE_CASES:
        changed = copy.deepcopy(readbacks)
        *keys, last = path.split(".")
        reduce(dict.__getitem__, keys, changed)[last] = value
        refusals.append((reason, raw, changed, idle))
    for reason, result, observed, state in refusals:
        with pytest.raises(ValueError, match=reason):
            accept_recovery_canary(result, ctx, observed, state)


def test_audit_funciona_sob_freeze_simulado(phase_c):
    _apply(phase_c, "recovery")
    ctx, idle = phase_c.ctx, phase_c.adapter.observe_environment_gate("prod")
    with pytest.raises(ValueError, match="canary=route_not_accepted"):
        accept_audit_canary(_mode_run(ctx, "audit", frozen=True), ctx, _readbacks(ctx), idle)
    applied = _apply(phase_c, "audit")
    ctx, readbacks = phase_c.ctx, _readbacks(phase_c.ctx)
    raw = _mode_run(ctx, "audit", frozen=True)
    result = json.loads(raw)
    assert accept_audit_canary(raw, ctx, readbacks, idle).freeze_proof_sha256 == result[
        "freeze_proof_sha256"]
    stale = _instant(applied["observed_at"]) - timedelta(seconds=5)
    cut = _instant(phase_c.close["fixed_deadline"]) - timedelta(seconds=100)
    forged = [("canary=freeze_proof", json.loads(_mode_run(ctx, "audit"))),
              ("canary=pending", {**result, "pending": ["x"]}),
              *(("canary=common", {**result, "common": {**result["common"], key: 1}})
                for key in ("promotion_generation", "activation_sha256")),
              ("canary=window", {**result, "fixed_deadline": "2026-01-01T00:00:00Z"}),
              ("canary=window", json.loads(_mode_run(ctx, "audit", frozen=True, now=stale))),
              ("window=cutoff", json.loads(_mode_run(ctx, "audit", frozen=True, now=cut)))]
    for reason, value in forged:
        with pytest.raises(ValueError, match=reason):
            accept_audit_canary(canonical_text(value).encode(), ctx, readbacks, idle)
    other = dataclasses.replace(idle, fence=dataclasses.replace(idle.fence, promotion_id="x"))
    with pytest.raises(ValueError, match="canary=fence"):
        accept_audit_canary(raw, ctx, readbacks, other)


def _canaries(op, modes=("recovery", "audit")):
    ctx, readbacks = op.ctx, _readbacks(op.ctx)
    idle = op.adapter.observe_environment_gate("prod")
    accept = {"recovery": accept_recovery_canary, "audit": accept_audit_canary}
    return {mode: accept[mode](_mode_run(ctx, mode, frozen=mode == "audit"), ctx, readbacks,
                               idle) for mode in modes}


MIXED = {
    "incompleto": (None, "reopen=phase_c_incomplete"),
    "scheduler_fora_de_banda": (lambda op: setattr(op.boundary, "drift", True),
                                "routes=scheduler_readback"),
    "maquina_mista": (lambda op: setattr(op.boundary, "describe_state_machine", lambda **_: {
        "stateMachineArn": op.ctx.phase_b.machine["arn"], "definition": "{}"}),
        "routes=machine_readback"),
    "state_sujo": (lambda op: setattr(op, "dirty", True), "phase_b=post_apply_drift"),
    "checkpoint_mudou": (lambda op: setattr(op.boundary, "serial", op.boundary.serial + 1),
                         "reopen=checkpoint_changed"),
    "fora_da_janela": (_late, "window=cutoff step=reopen"),
}


@pytest.mark.parametrize("case_name", sorted(MIXED))
def test_estado_misto_nao_reabre_fence(phase_c, case_name):
    edit, reason = MIXED[case_name]
    _apply(phase_c, "recovery")
    if edit is None:
        canaries = _canaries(phase_c, ("recovery",))
    else:
        _apply(phase_c, "audit")
        canaries = _canaries(phase_c)
        edit(phase_c)
    with pytest.raises(ValueError, match=reason):
        accept_and_reopen(phase_c.ctx, phase_c.native, canaries)
    state = phase_c.adapter.observe_environment_gate("prod")
    assert state.fence.state.value == "CLOSED"
    assert state.fence.promotion_id == phase_c.close["fence"]["promotion_id"]


def test_rotas_aceitas_e_state_limpo_reabrem_fence(phase_c):
    _apply(phase_c, "recovery")
    _apply(phase_c, "audit")
    ctx, canaries = phase_c.ctx, _canaries(phase_c)
    drill = {mode: dataclasses.replace(item, deadline_drill_sha256="1" * 64)
             for mode, item in canaries.items()}
    assert canary_status(ctx, drill) == "accepted-open"
    audit = canaries["audit"]
    other = dataclasses.replace(audit, common={**audit.common, "activation_sha256": "0" * 64})
    for forged in (other, dataclasses.asdict(audit), dataclasses.replace(audit, task_receipts=2)):
        with pytest.raises(ValueError, match="reopen=canary_binding"):
            canary_status(ctx, {**canaries, "audit": forged})
    evidence = accept_and_reopen(ctx, phase_c.native, canaries)
    assert evidence["status"] == "routes-open-operational-pending"
    assert evidence["routes"] == ctx.revisions
    assert _instant(evidence["reopened_at"]) <= _instant(phase_c.close["fixed_deadline"])
    state = phase_c.adapter.observe_environment_gate("prod")
    assert (state.fence.state.value, state.permit) == ("OPEN", None)


def test_watchdog_converge_antes_de_trinta_minutos(phase_c):
    close, serial = phase_c.close, phase_c.boundary.serial
    watchdog = PhaseCWatchdog.from_close(close)
    closed, fixed = _instant(close["close_confirmed_at"]), _instant(close["fixed_deadline"])
    assert watchdog.admit("phase-c-recovery", closed + timedelta(seconds=60), 300) is None
    late = fixed - timedelta(seconds=RECOVERY_SECONDS + 299)
    with pytest.raises(WindowCutoff) as refused:
        watchdog.admit("phase-c-audit", late, 300)
    decision = refused.value.decision
    assert (decision["action"], decision["step"]) == ("converge-prior-routes", "phase-c-audit")
    assert (decision["fence"], decision["reopen"], decision["fixed_deadline"]) == (
        "CLOSED", False, close["fixed_deadline"])
    assert _instant(decision["choice_at"]) == late
    assert _instant(decision["recovery_deadline"]) <= fixed <= closed + timedelta(seconds=1800)
    with pytest.raises(WindowCutoff) as at_fixed:
        PhaseCWatchdog.from_close(close).admit("reopen", fixed, 0)
    assert at_fixed.value.decision["recovery_deadline"] == close["fixed_deadline"]
    beyond = (closed + timedelta(seconds=1801)).isoformat()
    with pytest.raises(ValueError, match="window=fixed"):
        PhaseCWatchdog.from_close({**close, "fixed_deadline": beyond})
    _late(phase_c, "phase_c_recovery", 300)
    with pytest.raises(WindowCutoff, match="step=phase-c-recovery"):
        _apply(phase_c, "recovery")
    assert phase_c.boundary.serial == serial
    assert phase_c.adapter.observe_environment_gate("prod").fence.state.value == "CLOSED"
    phase_c.mp.setattr("scripts.promotion.phase_c_recovery.datetime", datetime)
    phase_c.stray = lambda boundary: setattr(boundary, "serial", boundary.serial - 1)
    with pytest.raises(ValueError, match="phase_c=checkpoint_not_advanced"):
        _apply(phase_c, "recovery")


def test_vps_sem_processor_pos_mig012_e_runbook_liga_aceite_de_rota():
    compose = yaml.safe_load((ROOT / "deploy/prod/docker-compose.prod.yml").read_text())
    assert not [name for name in compose["services"] if "processor" in name]
    runbook = (ROOT / "docs/runbooks/vps-processor-retirement.md").read_text()
    assert all(text in runbook for text in ("scripts/promotion/accept_routing.py",
                                             "routes-open-operational-pending"))
