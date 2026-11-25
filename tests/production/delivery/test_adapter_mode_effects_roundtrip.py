"""O mode-result de cada start é o insumo `mode-result` aceito pelo -observe do P13 (#465)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from scripts.promotion import promote_api
from scripts.promotion._adapter_abi import MODE_OPERATIONS
from scripts.promotion.mode_canary_seed import SeedBinding, build_mode_seed
from tests.production.delivery import test_adapter_mode as am
from tests.production.delivery import test_adapter_mode_effects as fx
from tests.production.delivery import test_mode_canary as mc
from tests.production.delivery import test_phase_c as pc
from tests.production.delivery.test_promote_close import canonical, sha

phase_c, operation, activation = pc.phase_c, pc.operation, pc.activation
case, ddb, github = pc.case, pc.ddb, pc.github
STAMP = "%Y-%m-%dT%H:%M:%SZ"
PROMOTION_ID = fx.OWNER_SHA


def _record(op: Any, mode: str) -> bytes:
    ctx, common = op.ctx, op.close["common"]
    fixed = pc._instant(op.close["fixed_deadline"]).strftime(STAMP)
    return canonical(mc._record(
        mode, activation_sha256=ctx.activation_sha256, machine_arn=op.context.machine["arn"],
        revisions=dict(ctx.revisions), promotion_generation=common["promotion_generation"],
        owner_sha256=fx.OWNER_SHA, promotion_id=fx.OWNER_SHA, run_ids=fx.runs(mode),
        fixture_manifest_sha256=sha(fx.fixtures()["fixture-manifest"]),
        **dict.fromkeys(pc.DEADLINES, fixed), **{key: op.context.canary[key] for key in (
            "candidate_sha256", "source_sha", "release_id", "canary_id")}))


def _seed(record: bytes, started: datetime) -> bytes:
    fields = json.loads(record)
    files = {fx.FILES[name]: raw for name, raw in fx.fixtures().items()}
    binding = SeedBinding(f"prod-canary-{fields['canary_id']}", tuple(fields["run_ids"]),
                          fields["owner_sha256"], started, "cnesdata-prod-data")
    return build_mode_seed(binding, files).manifest


def _request(op: Any, name: str, inputs: dict[str, bytes]) -> dict[str, Any]:
    now = int(datetime.now(UTC).timestamp())
    stage = fx.API | {"stage": MODE_OPERATIONS[name][1], "close": now - 120,
                      "fixed_deadline": now + 900, "admitted_at": now - 5, "deadline": now + 600}
    owner = fx.OWNER | {"generation": op.close["common"]["promotion_generation"]}
    return fx.request(name, inputs) | {"activation_sha256": sha(op.context.activation),
                                       "stage": stage, "owner": owner}


def _start(op: Any, mode: str, **extra: Any) -> fx.Keeper:
    started = datetime.now(UTC).replace(microsecond=0)
    record = _record(op, mode)
    seed_record = _record(op, "recovery") if mode == "audit" else record
    readbacks = pc._readbacks(op.ctx)
    inputs = {"record": record, "readbacks": canonical(readbacks), "previous": b"null\n",
              "mode-seed": _seed(seed_record, started),
              "mode-start": fx.start_doc(mode == "audit", started.strftime(STAMP))}
    if mode == "audit":
        inputs["audit-event-ids"] = canonical([f"run.publishing.{fx.runs(mode)[0]}"])
    responses = {"run_task": mc._launched(mode), "describe_tasks": mc._stopped(mode)}
    for response in responses.values():
        response["tasks"][0]["taskDefinitionArn"] = op.ctx.revisions.get(mode)
    context = fx.Keeper(inputs, responses, extra.get("unit"), readbacks)
    op.mp.setattr(promote_api, "CLOCK", lambda: datetime.now(UTC))
    name = f"{mode}-start"
    promote_api.execute_shared_operation(name, _request(op, name, inputs), context)
    return context


def _accepted(record: dict[str, Any]) -> bool:
    return (record["accepted"], record["checks"]) == (True, [])


def test_recovery_start_alimenta_o_recovery_observe(phase_c: Any) -> None:
    evidence = [pc._apply(phase_c, "recovery")]
    started = _start(phase_c, "recovery")
    raw = started.observed[-1][1]
    inputs = am._scheduled(phase_c, "recovery", raw, evidence)
    assert _accepted(am._observe(phase_c, "recovery-observe", inputs))


def test_audit_start_alimenta_o_audit_observe(phase_c: Any) -> None:
    evidence = [pc._apply(phase_c, "recovery")]
    recovery = am._scheduled(phase_c, "recovery", _start(phase_c, "recovery").observed[-1][1],
                             evidence)
    pc._canary(phase_c, "recovery")
    evidence.append(pc._apply(phase_c, "audit"))
    raw = _start(phase_c, "audit").observed[-1][1]
    inputs = am._scheduled(phase_c, "audit", raw, evidence) | {
        "recovery-result": recovery["mode-result"], "recovery-readbacks": recovery["readbacks"]}
    assert _accepted(am._observe(phase_c, "audit-observe", inputs))


def test_unit_start_alimenta_o_unit_observe(phase_c: Any) -> None:
    context = _start(phase_c, "unit", unit=am._unit_observed(phase_c))
    steps = [(kind, json.loads(raw)) for kind, raw in context.observed]
    observed = {"execution": next(body for kind, body in steps if kind == "unit-execution"),
                "tasks": [body for kind, body in steps if kind == "unit-task"],
                "outputs": [body for kind, body in steps if kind == "unit-output"]}
    inputs = {"activation": phase_c.context.activation, "close-drain": canonical(phase_c.close),
              "record": context.inputs["record"], "mode-result": context.observed[-1][1],
              "readbacks": context.inputs["readbacks"], "observed": canonical(observed)}
    assert _accepted(am._observe(phase_c, "unit-observe", inputs))
