"""Phase C: oráculos por modo, canaries de recovery e audit, reabertura e watchdog."""

from __future__ import annotations

import copy
import hashlib
import json
import subprocess
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from functools import partial, reduce
from pathlib import Path

import pytest
import yaml

from cnes_domain.control_plane.environment_gate import PermitHolder, ReleaseUnitPermit
from cnes_infra.aws.mode_canary import canonical_text
from scripts.promotion import accept_routing as routing
from scripts.promotion import phase_c_recovery as rec
from scripts.promotion.environment_gate import acquire_initial_or_recovery
from scripts.promotion.mode_canary import execute_mode_canary
from scripts.promotion.phase_b import MACHINE, execute_reviewed_phase_b, object_hash
from scripts.promotion.phase_c_audit import accept_audit_canary, review_audit_plan
from scripts.promotion.rollback_api_static import RECOVERY_SECONDS
from tests.production.delivery import test_mode_canary as mc
from tests.production.delivery import test_phase_b_execution as base

activation, case, ddb, github = base.activation, base.case, base.ddb, base.github
SCHEDULE, RUN_POLICY, VARIABLES = rec.SCHEDULE, rec.RUN_POLICY, rec.VARIABLES
MODES, _instant, accept_and_reopen = routing.MODES, routing._instant, routing.accept_and_reopen
operation, ROOT = base.operation, Path(__file__).resolve().parents[3]
ORDER, PRIOR = ([(f"phase_c={reason}", lambda bad: None)] for reason in ("order", "prior_canary"))
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
    op.phase_b, op.ctx = evidence, rec.phase_c_context(op.context, evidence, op.close)
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
    run = [{"Effect": "Allow", "Action": "ecs:RunTask", "Resource": revision,
            "Condition": {"ArnEquals": {"ecs:cluster": ctx.phase_b.canary["cluster"]}}}]
    statements = [*(run if revision else []), *extra]
    return {"policy": json.dumps({"Version": "2012-10-17", "Statement": statements})}


def _change(address, actions, before, after):
    return {"mode": "managed", "address": address,
            "change": {"actions": actions, "before": before, "after": after, "after_unknown": {}}}


def _plan(ctx, mode):
    revision, prior, routes = ctx.revisions[mode], ctx.prior[mode], ctx.routes_for(mode)
    changes = [_change(MACHINE, ["no-op"], ctx.phase_b.machine, ctx.phase_b.machine)]
    if prior is None:
        for address in rec.INITIAL[mode]:
            after = _schedule(ctx, mode, revision) if address == SCHEDULE.format(mode) else {}
            changes.append(_change(address, ["create"], None, after))
    else:
        changes.append(_change(SCHEDULE.format(mode), ["update"],
                               _schedule(ctx, mode, prior), _schedule(ctx, mode, revision)))
    changes.append(_change(RUN_POLICY.format(mode), ["update"],
                           _policy(ctx, mode, prior), _policy(ctx, mode, revision)))
    variables = {VARIABLES[name]: {"value": value} for name, value in routes.items()}
    return {"variables": variables, "resource_changes": changes}


def _tofu(op, mode):
    def run(args, **_):
        if args[1] == "apply":
            op.boundary.serial += 1
            op.boundary.revisions[mode] = op.ctx.revisions[mode]
            op.stray(op.boundary)
        plan = _plan(op.ctx, mode)
        op.edit(plan)
        out = {"show": json.dumps(plan).encode(), "apply": b"phase c apply"}.get(args[1], b"plan")
        code = 2 if op.dirty and args[1] not in ("show", "apply") else 0
        return subprocess.CompletedProcess(args, code, out, b"")
    return run


def _approval(op, mode, **override):
    binary, audit = (op.native.directory / f"phase-c-{mode}.{ext}" for ext in ("tfplan", "json"))
    binary.write_bytes(f"reviewed phase c {mode}".encode())
    report = {"decision": "ACCEPT", "phase": f"phase-c-{mode}",
              "plan_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
              "checkpoint": op.native.checkpoint()[0],
              "source_sha": op.close["common"]["source_sha"], **override}
    audit.write_text(json.dumps(report))
    return {"plan_path": str(binary), "audit_path": str(audit),
            "audit_sha256": hashlib.sha256(audit.read_bytes()).hexdigest()}


def _apply(op, mode, approval=None):
    op.mp.setattr("scripts.promotion.verify_routes.subprocess.run", _tofu(op, mode))
    evidence = rec.execute_reviewed_phase_c(op.ctx, op.native, approval or _approval(op, mode))
    op.ctx = op.ctx.accept(evidence)
    return evidence


def _readbacks(ctx):
    canary, rev, machine = ctx.phase_b.canary, ctx.revisions, ctx.phase_b.machine
    network, role = canary["network"], f"arn:aws:iam::{canary['account_id']}:role/{{}}".format
    vpc = {"Subnets": sorted(network["subnets"]), "SecurityGroups": [network["security_group"]],
           "AssignPublicIp": "ENABLED"}
    ecs = {"TaskCount": 1, "LaunchType": "FARGATE", "PlatformVersion": "1.4.0",
           "NetworkConfiguration": {"awsvpcConfiguration": vpc}}
    sched = {mode: {"Target": {
        "Arn": canary["cluster"], "RoleArn": role(f"cnesdata-prod-scheduler-{mode}"),
        "RetryPolicy": {"MaximumRetryAttempts": 0, "MaximumEventAgeInSeconds": 900},
        "EcsParameters": {**copy.deepcopy(ecs), "TaskDefinitionArn": rev[mode]}}} for mode in MODES}
    tasks = {mode: {"taskDefinition": {
        "taskDefinitionArn": rev[mode], "taskRoleArn": role(f"cnesdata-prod-processor-{mode}"),
        "containerDefinitions": [{"name": "processor", "command": COMMANDS.get(mode, [])}],
    }} for mode in ("unit", *MODES)}
    alarms = {mode: {
        "AlarmName": f"cnesdata-prod-scheduler-{mode}-repeated-failure",
        "Dimensions": [{"Name": "RuleName", "Value": f"cnesdata-prod-scheduler-{mode}-failed"}],
        "EvaluationPeriods": 2, "DatapointsToAlarm": 2, "ActionsEnabled": True,
        "AlarmActions": ["arn:aws:sns:us-east-2:123456789012:operator"]} for mode in MODES}
    sm = {"stateMachineArn": machine["arn"], "definition": machine["definition"]}
    return {"state_machine": sm, "schedules": sched, "task_definitions": tasks, "alarms": alarms}


def _mode_run(ctx, mode, frozen=False, now=None):
    canary, common = ctx.phase_b.canary, ctx.close["common"]
    fixed = _instant(ctx.close["fixed_deadline"]).strftime("%Y-%m-%dT%H:%M:%SZ")
    record = mc._record(
        mode, activation_sha256=ctx.activation_sha256, machine_arn=ctx.phase_b.machine["arn"],
        revisions=dict(ctx.revisions), promotion_generation=common["promotion_generation"],
        **dict.fromkeys(DEADLINES, fixed), **{key: canary[key] for key in (
            "candidate_sha256", "source_sha", "release_id", "canary_id")})
    calls = mc._Context({"ecs:RunTask": mc._launched(mode), "ecs:DescribeTasks": mc._stopped(mode)})
    calls.now = lambda: now or datetime.now(UTC)
    for response in calls.responses.values():
        response["tasks"][0]["taskDefinitionArn"] = ctx.revisions[mode]
    observed = _readbacks(ctx)
    return execute_mode_canary(mode, mc._originals(
        mode, record=canonical_text(record).encode(), readbacks=observed, readbacks_after=observed,
        frozen=frozen), calls)


def _canary(op, mode, now=None):
    accept = accept_audit_canary if mode == "audit" else rec.accept_recovery_canary
    raw, gate = _mode_run(op.ctx, mode, mode == "audit", now), op.adapter
    op.ctx = accept(raw, op.ctx, _readbacks(op.ctx), gate.observe_environment_gate("prod"))


def _through(op, modes=MODES, now=None):
    for mode in modes:
        _apply(op, mode)
        _canary(op, mode, now)


def _at(plan, address):
    return next(item["change"] for item in plan["resource_changes"] if item["address"] == address)


def _ecs(plan, mode, side="after"):
    return _at(plan, SCHEDULE.format(mode))[side]["target"][0]["ecs_parameters"][0]


def _after(plan, mode="recovery"):
    return _at(plan, SCHEDULE.format(mode))["after"]


def _var(name, value):
    return lambda bad: bad["variables"][VARIABLES[name]].update(value=value)


def _foreign(plan, address, action="update"):
    changes = [item for item in plan["resource_changes"] if item["address"] != address]
    plan["resource_changes"] = [*changes, _change(address, [action], {"a": 1}, {"a": 2})]


def _rejects(review, plan, ctx, cases):
    for reason, edit in cases:
        bad = copy.deepcopy(plan)
        edit(bad)
        with pytest.raises(ValueError, match=reason):
            review(bad, ctx)


def _late(op, seconds=0, modules=("accept_routing",)):
    now = _instant(op.close["fixed_deadline"]) - timedelta(seconds=seconds)
    late = type("Late", (datetime,), {"now": classmethod(lambda cls, tz=None: now)})
    for module in modules:
        op.mp.setattr(f"scripts.promotion.{module}.datetime", late)
    return now


def test_plan_de_recovery_nao_muda_unit_nem_audit(phase_c):
    ctx, run = phase_c.ctx, RUN_POLICY.format("recovery")
    plan, revision = _plan(ctx, "recovery"), ctx.revisions["recovery"]
    pol = partial(_policy, ctx, "recovery")
    assert rec.review_recovery_plan(plan, ctx) == ("initial" if ctx.initial else "established")
    cases = [*(("delta=unexpected", partial(_foreign, address=address)) for address in (
        MACHINE, SCHEDULE.format("audit"), RUN_POLICY.format("audit"))), *((
        "schedule=recovery_target", edit) for edit in (
        lambda bad: _ecs(bad, "recovery").update(task_count=2),
        lambda bad: _ecs(bad, "recovery").update(propagate_tags="x"),
        lambda bad: _after(bad).update(schedule_expression="rate(1 minute)"),
        lambda bad: _after(bad)["flexible_time_window"][0].update(mode="FLEXIBLE"),
        lambda bad: _after(bad)["target"][0].update(role_arn="x"),
        lambda bad: _after(bad)["target"][0].update(input="{}"),
        lambda bad: _at(bad, SCHEDULE.format("recovery")).update(after_unknown={"target": 1}))),
        ("plan=duplicate_address", lambda bad: bad["resource_changes"].append(
            copy.deepcopy(bad["resource_changes"][0]))),
        ("delta=task_revision", lambda bad: _foreign(bad, TASK, "create")),
        ("delta=action", lambda bad: _at(bad, run).update(actions=["delete", "create"])),
        ("variables=audit", _var("audit", ctx.revisions["audit"])),
        ("variables=unit", _var("unit", "x")),
        ("run_policy=recovery_prior", lambda bad: _at(bad, run).update(before=pol("x"))),
        ("run_policy=recovery_grant", lambda bad: _at(bad, run).update(after=pol("x"))),
        ("run_policy=recovery_other", lambda bad: _at(bad, run).update(after=pol(revision, EXTRA))),
        *([] if ctx.initial else [
            ("schedule=recovery_prior", lambda bad: _ecs(bad, "recovery", "before").update(
                task_definition_arn="x")),
            ("schedule=recovery_non_route", lambda bad: _after(bad).update(description="x"))]),
    ]
    _rejects(rec.review_recovery_plan, plan, ctx, cases)
    _rejects(review_audit_plan, _plan(ctx, "audit"), ctx, ORDER)


def test_plan_de_audit_nao_muda_unit_nem_recovery(phase_c):
    _rejects(review_audit_plan, _plan(phase_c.ctx, "audit"), phase_c.ctx, ORDER)
    _apply(phase_c, "recovery")
    early = phase_c.ctx
    _rejects(review_audit_plan, _plan(early, "audit"), early, PRIOR)
    _canary(phase_c, "recovery")
    ctx, plan = phase_c.ctx, _plan(phase_c.ctx, "audit")
    assert review_audit_plan(plan, ctx) == ("initial" if ctx.initial else "established")
    _rejects(review_audit_plan, plan, ctx, [
        *(("delta=unexpected", partial(_foreign, address=address)) for address in (
            MACHINE, SCHEDULE.format("recovery"), RUN_POLICY.format("recovery"))),
        ("variables=recovery", _var("recovery", ctx.prior["recovery"])),
        ("variables=unit", _var("unit", "x")),
        ("schedule=audit_target", lambda bad: _ecs(bad, "audit").update(task_count=2)),
    ])
    _rejects(rec.review_recovery_plan, _plan(ctx, "recovery"), ctx, ORDER)
    good = {"kind": "phase-c-audit", "routes": ctx.routes_for("audit"), "before": ctx.checkpoint,
            **{key: ctx.phase_b_evidence[key] for key in ("common", "close_drain_sha256")}}
    assert ctx.accept(good).next_mode is None
    for context, evidence in [(early, good), *((ctx, {**good, key: value}) for key, value in (
            ("routes", ctx.routes_for("recovery")), ("before", {"serial": 0}),
            ("kind", "phase-c-recovery"), ("common", {}), ("close_drain_sha256", "0")))]:
        with pytest.raises(ValueError, match="phase_c=evidence"):
            context.accept(evidence)
    phase_c.stray = lambda boundary: boundary.revisions.update(
        recovery=boundary.revisions["recovery"] + "0")
    with pytest.raises(ValueError, match="phase_c=routes"):
        _apply(phase_c, "audit")


def test_applies_de_c_encadeiam_activation_checkpoint_e_rotas(phase_c):
    phase_b, boundary, close = phase_c.phase_b, phase_c.boundary, phase_c.close
    for key, change, closed, reason in (
            ("common", {"activation_sha256": "0"}, close, "activation_binding"),
            ("common", {"source_sha": "x"}, close, "close_binding"),
            ("common", {}, {**close, "fixed_deadline": "x"}, "close_binding"),
            ("routes", {"mode": "x"}, close, "phase_b_mode")):
        evidence = {**phase_b, key: {**phase_b[key], **change}}
        with pytest.raises(ValueError, match=f"phase_c={reason}"):
            rec.phase_c_context(phase_c.context, evidence, closed)
    for override in ({"phase": "phase-c-audit"}, {"checkpoint": {}}, {"source_sha": "x"},
                     {"decision": "REJECT"}, {"plan_sha256": "0"}):
        with pytest.raises(ValueError, match="exact_independent_audit_required"):
            _apply(phase_c, "recovery", _approval(phase_c, "recovery", **override))
    with pytest.raises(ValueError, match="phase_c=original_audit_hash"):
        _apply(phase_c, "recovery", {**_approval(phase_c, "recovery"), "audit_sha256": "0"})
    phase_c.edit = lambda plan: _foreign(plan, MACHINE)
    with pytest.raises(ValueError, match="delta=unexpected"):
        _apply(phase_c, "recovery")
    phase_c.edit, boundary.serial = lambda plan: None, boundary.serial + 1
    with pytest.raises(ValueError, match="phase_c=checkpoint_chain"):
        _apply(phase_c, "recovery")
    boundary.serial -= 1
    recovery = _apply(phase_c, "recovery")
    serial = boundary.serial
    with pytest.raises(ValueError, match="phase_c=prior_canary"):
        _apply(phase_c, "audit")
    assert boundary.serial == serial
    _canary(phase_c, "recovery")
    audit, ctx = _apply(phase_c, "audit"), phase_c.ctx
    assert [(e["kind"], e["before"], e["common"], e["routes"]) for e in (recovery, audit)] == [
        ("phase-c-recovery", phase_b["after"], phase_b["common"], ctx.routes_for("recovery")),
        ("phase-c-audit", recovery["after"], phase_b["common"], ctx.revisions)]


def test_recovery_sobreposta_nao_passa_de_uma_task(phase_c):
    _apply(phase_c, "recovery")
    ctx, gate = phase_c.ctx, phase_c.adapter
    release = phase_c.close["fence"]["promotion_id"]
    tenant = f"prod-canary-{ctx.phase_b.canary['canary_id']}"
    first, second = (PermitHolder(tenant, f"recovery-{n}", n * 16) for n in "ab")
    now, acquire = datetime.now(UTC), partial(acquire_initial_or_recovery, gate)
    assert [bool(acquire(holder, now, promotion)) for holder, promotion in (
        (first, release), (second, release), (first, release), (second, None))] == [1, 0, 1, 0]
    held = gate.observe_environment_gate("prod")
    assert getattr(held.permit, "holder", None) == first
    readbacks, raw = _readbacks(ctx), _mode_run(ctx, "recovery")
    with pytest.raises(ValueError, match="fence=permit_in_flight"):
        rec.accept_recovery_canary(raw, ctx, readbacks, held)
    assert gate.release_unit_permit(ReleaseUnitPermit("prod", held.permit.token, now))
    idle = gate.observe_environment_gate("prod")
    accepted = rec.accept_recovery_canary(raw, ctx, readbacks, idle).canaries[-1]
    assert (accepted.mode, accepted.task_receipts) == ("recovery", 1)
    assert accepted.phase_c_sha256 == object_hash(ctx.accepted[0])
    with pytest.raises(ValueError, match="canary=order"):
        rec.accept_recovery_canary(raw, replace(ctx, canaries=(accepted,)), readbacks, idle)
    duplicated = {**json.loads(raw), "task_receipts": json.loads(raw)["task_receipts"] * 2}
    fence = idle.fence
    refusals = [("canary=single_task", canonical_text(duplicated).encode(), readbacks, idle),
                ("canary=routing", raw, {**readbacks, "extra": 1}, idle),
                *(("fence=closed_readback", raw, readbacks, replace(idle, fence=replace(
                    fence, **change))) for change in ({"promotion_id": "x"}, {"state": type(
                        fence.state)("OPEN")}, {"version": fence.version + 1}))]
    target = "schedules.recovery.Target."
    for path, value, reason in (
            ("task_definitions.recovery.taskDefinition.taskRoleArn", "x", "route=role"),
            (target + "EcsParameters.TaskDefinitionArn", "x", "route=revision"),
            (target + "RetryPolicy.MaximumEventAgeInSeconds", 3600, "route=deadline"),
            (target + "EcsParameters.NetworkConfiguration.awsvpcConfiguration.SecurityGroups",
             ["sg-other"], "route=network"),
            ("alarms.recovery.Dimensions", [], "route=alarm"),
            ("alarms.recovery.AlarmActions", [], "route=alarm")):
        changed = copy.deepcopy(readbacks)
        *keys, last = path.split(".")
        reduce(dict.__getitem__, keys, changed)[last] = value
        refusals.append((reason, raw, changed, idle))
    for reason, result, observed, state in refusals:
        with pytest.raises(ValueError, match=reason):
            rec.accept_recovery_canary(result, ctx, observed, state)


def test_audit_funciona_sob_freeze_simulado(phase_c):
    _apply(phase_c, "recovery")
    ctx, idle = phase_c.ctx, phase_c.adapter.observe_environment_gate("prod")
    with pytest.raises(ValueError, match="canary=route_not_accepted"):
        accept_audit_canary(_mode_run(ctx, "audit", frozen=True), ctx, _readbacks(ctx), idle)
    _canary(phase_c, "recovery")
    applied = _apply(phase_c, "audit")
    ctx, readbacks = phase_c.ctx, _readbacks(phase_c.ctx)
    result = json.loads(raw := _mode_run(ctx, "audit", frozen=True))
    accepted = accept_audit_canary(raw, ctx, readbacks, idle).canaries[-1]
    assert accepted.freeze_proof_sha256 == result["freeze_proof_sha256"]
    stale = _instant(applied["observed_at"]) - timedelta(seconds=5)
    cut = _instant(phase_c.close["fixed_deadline"]) - timedelta(seconds=100)
    forged = [("canary=freeze_proof", json.loads(_mode_run(ctx, "audit"))),
              ("canary=pending", {**result, "pending": ["x"]}),
              ("canary=schema", {**result, "mode": "recovery"}),
              *(("canary=common", {**result, "common": {**result["common"], key: 1}})
                for key in ("promotion_generation", "activation_sha256", "release_id")),
              ("canary=window", {**result, "fixed_deadline": "2026-01-01T00:00:00Z"}),
              ("canary=window", json.loads(_mode_run(ctx, "audit", frozen=True, now=stale))),
              ("window=cutoff", json.loads(_mode_run(ctx, "audit", frozen=True, now=cut)))]
    for reason, value in forged:
        with pytest.raises(ValueError, match=reason):
            accept_audit_canary(canonical_text(value).encode(), ctx, readbacks, idle)


def _permit(op):
    acquire_initial_or_recovery(op.adapter, PermitHolder(
        f"prod-canary-{op.ctx.phase_b.canary['canary_id']}", "r", "0" * 16), datetime.now(UTC),
        op.close["fence"]["promotion_id"])


def _wrap(name, edit):
    return lambda op: setattr(op.boundary, name, partial(
        lambda read, **r: edit(op.boundary, read(**r)), getattr(op.boundary, name)))


def _stray(effect):
    return lambda op: setattr(op, "stray", partial(effect, op))


def _checkpoint(head=copy.copy, body=copy.copy):
    return lambda op, *_: op.mp.setattr(op.native, "checkpoint", partial(
        lambda read: (head(read()[0]), body(read()[1])), op.native.checkpoint))


APPLY, HALF = (), MODES[:1]
MIXED = {
    "antes_do_apply_com_permit": (APPLY, _permit, "fence=permit_in_flight"),
    "apply_com_permit_no_meio": (APPLY, _stray(lambda op, _: _permit(op)), "permit_in_flight"),
    "apply_sujo": (APPLY, _stray(lambda op, _: setattr(op, "dirty", True)), "post_apply_drift"),
    "apply_parou": (APPLY, _stray(lambda _, b: setattr(b, "serial", b.serial - 1)), "not_advanced"),
    "apply_lineage": (APPLY, _stray(_checkpoint(lambda h: dict(h, lineage="x"))), "not_advanced"),
    "antes_do_apply_no_corte": (APPLY, lambda op: _late(op, RECOVERY_SECONDS + 100, (
        "phase_c_recovery",)), "window=cutoff step=phase-c-recovery"),
    "incompleto": (HALF, lambda _: None, "reopen=phase_c_incomplete"),
    "sem_canary_de_audit": (MODES, lambda op: setattr(op, "ctx", replace(
        op.ctx, canaries=op.ctx.canaries[:1])), "reopen=canaries"),
    "scheduler_fora": (MODES, lambda op: setattr(op.boundary, "drift", True), "scheduler_readback"),
    "maquina_mista": (MODES, _wrap("describe_state_machine", lambda _, read: {
        **read, "definition": "{}"}), "routes=machine_readback"),
    "state_sujo": (MODES, lambda op: setattr(op, "dirty", True), "phase_b=post_apply_drift"),
    "serial_mudou": (MODES, lambda op: setattr(op.boundary, "serial", 99), "checkpoint_changed"),
    "checkpoint_muda_na_leitura": (MODES, _wrap("describe_state_machine", lambda b, read: (
        setattr(b, "serial", b.serial + 1), read)[1]), "reopen=checkpoint_changed"),
    "vazio": (MODES, _checkpoint(body=lambda s: {**s, "resources": []}), "recovery_missing"),
    "scheduler_duplicado": (MODES, _checkpoint(body=lambda s: {
        **s, "resources": s["resources"] * 2}), "routes=scheduler_mode"),
    "scheduler_em_outro_cluster": (MODES, _wrap("get_schedule", lambda _, read: {
        "Target": {**read["Target"], "Arn": "x"}}), "routes=scheduler_readback"),
    "task_divergente": (MODES, _wrap("describe_task_definition", lambda _, read: {
        "taskDefinition": {**read["taskDefinition"], "taskDefinitionArn": "x"}}), "task_readback"),
    "task_em_outra_maquina": (MODES, _wrap("describe_task_definition", lambda _, r: {
        "taskDefinition": {**r["taskDefinition"], "containerDefinitions": [{"environment": []}]}}),
        "routes=unit_environment_arn"),
    "permit_em_voo": (MODES, _permit, "fence=permit_in_flight"),
    "fora_da_janela": (MODES, _late, "window=cutoff step=reopen"),
}


@pytest.mark.parametrize("case_name", sorted(MIXED))
def test_estado_misto_nao_reabre_fence(phase_c, case_name):
    modes, edit, reason = MIXED[case_name]
    _through(phase_c, modes)
    edit(phase_c)
    act = (lambda: accept_and_reopen(phase_c.ctx, phase_c.native)) if modes else partial(
        _apply, phase_c, "recovery")
    with pytest.raises(ValueError, match=reason):
        act()
    applied = phase_c.boundary.revisions.get("recovery") == phase_c.ctx.revisions["recovery"]
    assert modes or applied != case_name.startswith("antes_do_apply")
    state = phase_c.adapter.observe_environment_gate("prod")
    assert state.fence.state.value == "CLOSED"
    assert state.fence.promotion_id == phase_c.close["fence"]["promotion_id"]


def test_rotas_aceitas_e_state_limpo_reabrem_fence(phase_c):
    now = _late(phase_c, 1000, ("accept_routing", "phase_c_recovery", "verify_routes"))
    _through(phase_c, MODES, now)
    ctx, audit = phase_c.ctx, phase_c.ctx.canaries[1]
    drill = replace(ctx, canaries=tuple(
        replace(item, deadline_drill_sha256="1" * 64) for item in ctx.canaries))
    assert routing.canary_status(drill) == "accepted-open"
    changes = ({"common": {**audit.common, "activation_sha256": "0" * 64}}, {"mode": "recovery"},
               {"common": {**audit.common, "promotion_generation": 0}}, {"task_receipts": 2},
               {"phase_c_sha256": "0" * 64}, {"freeze_proof_sha256": None})
    for forged in (asdict(audit), *(replace(audit, **change) for change in changes)):
        with pytest.raises(ValueError, match="reopen=canary_binding"):
            routing.canary_status(replace(ctx, canaries=(ctx.canaries[0], forged)))
    evidence = accept_and_reopen(ctx, phase_c.native)
    assert evidence["status"] == "routes-open-operational-pending"
    assert (evidence["routes"], _instant(evidence["reopened_at"])) == (ctx.revisions, now)
    state = phase_c.adapter.observe_environment_gate("prod")
    assert (state.fence.state.value, state.permit) == ("OPEN", None)
    compose = yaml.safe_load((ROOT / "deploy/prod/docker-compose.prod.yml").read_text())
    assert not [name for name in compose["services"] if "processor" in name]
    runbook = (ROOT / "docs/runbooks/vps-processor-retirement.md").read_text()
    assert all(text in runbook for text in (
        "scripts/promotion/accept_routing.py", evidence["status"]))


def test_watchdog_converge_antes_de_trinta_minutos(operation):
    close, utc = operation.close, routing._require_utc
    watchdog = routing.PhaseCWatchdog.from_close(close)
    closed, fixed = _instant(close["close_confirmed_at"]), _instant(close["fixed_deadline"])
    assert watchdog.admit("phase-c-recovery", closed + timedelta(seconds=60), 300) is None
    late = fixed - timedelta(seconds=RECOVERY_SECONDS + 299)
    with pytest.raises(routing.WindowCutoff, match="step=phase-c-audit") as refused:
        watchdog.admit("phase-c-audit", late, 300)
    with pytest.raises(routing.WindowCutoff, match="step=reopen") as at_fixed:
        watchdog.admit("reopen", fixed, 0)
    assert refused.value.decision == {
        "version": 1, "action": "converge-prior-routes", "step": "phase-c-audit", "fence": "CLOSED",
        "reopen": False, "choice_at": utc(late), "fixed_deadline": close["fixed_deadline"],
        "recovery_deadline": utc(late + timedelta(seconds=420)), "convergence_budget_seconds": 300}
    assert at_fixed.value.decision["recovery_deadline"] == close["fixed_deadline"]
    with pytest.raises(ValueError, match="window=fixed"):
        routing.PhaseCWatchdog.from_close({**close, "fixed_deadline": (
            closed + timedelta(seconds=1801)).isoformat()})
