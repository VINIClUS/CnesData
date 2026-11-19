"""Rollback de rota completa pela activation anterior retida e restore imediato de API/estático."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from scripts.promotion.accept_routing import MODES, _schedule_routes, assert_fence, observe_gate
from scripts.promotion.phase_b import (
    MACHINE,
    _assert_task_revisions_unchanged,
    _require_utc,
    object_hash,
)
from scripts.promotion.phase_c_recovery import (
    RUN_POLICY,
    SCHEDULE,
    VARIABLES,
    _advanced,
    _apply_reviewed,
    _changes,
    _run_task,
    _statements,
    _target,
)
from scripts.promotion.rollback_api_static import (
    HostStep,
    after_host,
    host_outcome,
    recovery_window,
    rollback_static,
)
from scripts.promotion.verify_routes import verify_task_environment_arns

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from scripts.promotion.phase_b import PhaseBContext
    from scripts.promotion.publish_dashboard import Inventory, PublicationReceipt, StaticChannel
    from scripts.promotion.verify_routes import NativePhaseB

Node = dict[str, Any]
RETAINED: Final = "processor_retained_unit_task_definition_arns"


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class RollbackContext:
    """Activation anterior autenticada, activation atual e a Phase B que as liga."""

    prior: PhaseBContext
    current: PhaseBContext
    phase_b: Node

    @property
    def restored(self) -> Node:
        return dict(self.prior.canary["revisions"])

    @property
    def tfvars(self) -> Node:
        old = self.restored
        return {
            VARIABLES["unit"]: old["unit"],
            RETAINED: sorted(self.current.phase_a["retained_after"]),
            VARIABLES["recovery"]: old["recovery"],
            VARIABLES["audit"]: old["audit"],
        }


def rollback_context(prior: PhaseBContext, current: PhaseBContext,
                     phase_b: Mapping[str, Any]) -> RollbackContext:
    """Returns: Contexto do rollback. Raises: ValueError: Activation anterior sem vínculo."""
    before = current.phase_a["active_before"]
    _require(before is not None, "rollback=no_prior")
    routes, unit = phase_b.get("routes") or {}, current.canary["revisions"]["unit"]
    _require(phase_b.get("kind") == "phase-b" and routes.get("unit_revision") == unit
             and (phase_b.get("common") or {}).get("activation_sha256")
             == _sha(current.activation or b""), "rollback=phase_b_binding")
    old = prior.canary["revisions"]
    _require(old["unit"] == before and old["unit"] != unit, "rollback=prior_unit")
    _require(old["unit"] in current.phase_a["retained_after"], "rollback=prior_retained")
    _require(routes.get("mode") == "established"
             and (routes.get("recovery_revision"), routes.get("audit_revision"))
             == (old["recovery"], old["audit"]), "rollback=prior_schedulers")
    _require(prior.machine["arn"] == current.machine["arn"], "rollback=machine")
    return RollbackContext(prior, current, dict(phase_b))


def _variables(plan: Mapping[str, Any], context: RollbackContext) -> None:
    values, expected = plan.get("variables") or {}, context.tfvars
    _require(all((values.get(name) or {}).get("value") == value
                 for name, value in expected.items())
             and context.current.canary["revisions"]["unit"] in expected[RETAINED],
             "rollback=variables")


def _machine(change: Node, context: RollbackContext) -> None:
    before, after = deepcopy(change.get("before") or {}), deepcopy(change.get("after") or {})

    def definition(value: Node) -> Any:
        return json.loads(value.pop("definition", None) or "null")

    _require(definition(before) == json.loads(context.current.machine["definition"])
             and definition(after) == json.loads(context.prior.machine["definition"])
             and before == after and not change.get("after_unknown"), "rollback=machine")


def _schedule(change: Node, mode: str, context: RollbackContext) -> None:
    now, then = context.current.canary["revisions"][mode], context.restored[mode]
    before, after = deepcopy(change.get("before") or {}), deepcopy(change.get("after") or {})
    routed = (_target(before).get("task_definition_arn"),
              _target(after).get("task_definition_arn")) == (now, then)
    _target(before)["task_definition_arn"] = then
    _require(routed and before == after
             and not (change.get("after_unknown") or {}).get("target"), "rollback=schedule")


def _run_policy(change: Node, mode: str, context: RollbackContext) -> None:
    cluster = context.current.canary["cluster"]
    now = _run_task(context.current.canary["revisions"][mode], cluster)
    then = _run_task(context.restored[mode], cluster)
    before, after = _statements(change.get("before")), _statements(change.get("after"))
    _require([s for s in before if s.get("Action") == "ecs:RunTask"] == now
             and [s for s in after if s.get("Action") == "ecs:RunTask"] == then
             and [s for s in before if s not in now] == [s for s in after if s not in then],
             "rollback=run_policy")


def review_rollback_plan(plan: Mapping[str, Any], context: RollbackContext) -> None:
    """Raises: ValueError: Plan parcial, com nova revisão ou fora das rotas anteriores."""
    _variables(plan, context)
    _assert_task_revisions_unchanged(plan)
    changes = _changes(plan)
    expected = {MACHINE, *(SCHEDULE.format(m) for m in MODES),
                *(RUN_POLICY.format(m) for m in MODES)}
    _require(set(changes) == expected
             and all(change.get("actions") == ["update"] for change in changes.values()),
             "rollback=delta")
    _machine(changes[MACHINE], context)
    for mode in MODES:
        _schedule(changes[SCHEDULE.format(mode)], mode, context)
        _run_policy(changes[RUN_POLICY.format(mode)], mode, context)


def _audited(approval: Mapping[str, Any], before: Node,
             close: Mapping[str, Any]) -> tuple[bytes, bytes]:
    raw = Path(approval["plan_path"]).read_bytes()
    audit = Path(approval["audit_path"]).read_bytes()
    _require(_sha(audit) == approval["audit_sha256"], "rollback=original_audit_hash")
    report = json.loads(audit)
    _require(report.get("decision") == "ACCEPT" and report.get("phase") == "rollback"
             and report.get("plan_sha256") == _sha(raw) and report.get("checkpoint") == before
             and report.get("source_sha") == close["common"]["source_sha"],
             "rollback=exact_independent_audit_required")
    return raw, audit


def read_rollback_routes(native: NativePhaseB, context: RollbackContext,
                         state: Mapping[str, Any]) -> Node:
    """Returns: Rotas lidas forte da máquina, dos Schedulers e do ambiente das tasks."""
    expected = context.prior.machine
    machine = native.clients["stepfunctions"].describe_state_machine(
        stateMachineArn=expected["arn"])
    _require(machine["stateMachineArn"] == expected["arn"]
             and json.loads(machine["definition"]) == json.loads(expected["definition"]),
             "rollback=machine_readback")
    routes = {"unit": context.restored["unit"], **_schedule_routes(native, state)}
    environments = {}
    for mode in ("unit", "recovery"):
        _require(routes[mode] is not None, f"rollback={mode}_missing")
        definition = native.clients["ecs"].describe_task_definition(
            taskDefinition=routes[mode])["taskDefinition"]
        _require(definition["taskDefinitionArn"] == routes[mode]
                 and len(definition["containerDefinitions"]) == 1, "rollback=task_readback")
        environments[mode] = {"environment": definition["containerDefinitions"][0]["environment"]}
    verify_task_environment_arns(environments, expected["arn"])
    return routes


def execute_reviewed_rollback(context: RollbackContext, native: NativePhaseB,
                              approval: Mapping[str, Any], close: Mapping[str, Any]) -> Node:
    """Aplica só o plan de rollback auditado, com o fence fechado, e lê as rotas anteriores."""
    _require(close["common"]["activation_sha256"] == _sha(context.current.activation or b""),
             "rollback=activation_binding")
    before, _ = native.checkpoint()
    raw, audit = _audited(approval, before, close)

    def review(plan: Node) -> None:
        review_rollback_plan(plan, context)
        assert_fence(observe_gate(native)[1], close)

    applied = _apply_reviewed(native, raw, review, before)
    native.assert_clean()
    after, state = native.checkpoint()
    _advanced(before, after)
    routes = read_rollback_routes(native, context, state)
    _require(routes == context.restored, "rollback=routes")
    assert_fence(observe_gate(native)[1], close)
    return {
        "version": 1, "kind": "rollback-processor", "common": dict(close["common"]),
        "close_drain_sha256": object_hash(close),
        "prior_activation_sha256": _sha(context.prior.activation or b""),
        "before": before, "after": after, "plan_sha256": _sha(raw), "audit_sha256": _sha(audit),
        "apply_sha256": _sha(applied), "routes": routes,
        "observed_at": _require_utc(datetime.now(UTC)),
    }


@dataclass(frozen=True)
class ApiStaticRollback:
    """Caller e closed-ready do host e o canal estático da release atual."""

    caller: Mapping[str, Any]
    closed_ready_sha256: str
    channel: StaticChannel
    inventory: Inventory
    publication: PublicationReceipt | None


def restore_api_static(handoff: bytes | None, target: ApiStaticRollback,
                       clock: Callable[[], datetime]) -> Node:
    """Returns: Ação de host e entrypoints restaurados. Raises: ValueError: Host sem recovery."""
    outcome = host_outcome(handoff, caller=target.caller,
                           closed_ready_sha256=target.closed_ready_sha256)
    plan = after_host(outcome)
    decision = plan.decision
    if plan.step is not HostStep.RECOVERY or decision is None:
        raise ValueError(f"rollback=host_step next={plan.step.value}")
    receipt = rollback_static(target.channel, target.inventory, target.publication,
                              recovery_window(decision, clock))
    return {"host_action": plan.host_action, "fence": plan.fence,
            "restored": [list(item) for item in receipt.restored],
            "residual": receipt.residual, "invalidation_id": receipt.invalidation_id}


def rollback_release(restore: Callable[[], Node], converge: Callable[[], Node],
                     journal: Callable[[Node], None]) -> Node:
    """Returns: Registro do rollback; API/estático ficam registrados antes do processor."""
    record = {"version": 1, "kind": "rollback", "api_static": restore(), "processor": "converging"}
    journal(record)
    record = {**record, "processor": converge()}
    journal(record)
    return record
