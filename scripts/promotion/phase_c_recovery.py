"""Phase C de recovery: oráculo por modo, apply revisado exato e aceite do canary de recovery."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Final

from scripts.promotion.phase_b import (
    PhaseBContext,
    _assert_task_revisions_unchanged,
    _context_from_admitted,
    _require_utc,
    object_hash,
)
from scripts.promotion.verify_routes import verify_task_environment_arns

if TYPE_CHECKING:
    from collections.abc import Mapping

    from scripts.promotion.verify_routes import NativePhaseB

Node = dict[str, Any]
MODES: Final = ("recovery", "audit")
SCHEDULE: Final = 'module.schedulers.aws_scheduler_schedule.mode["{}"]'
RUN_POLICY: Final = 'module.schedulers.aws_iam_role_policy.run["{}"]'
VARIABLES: Final = {
    "unit": "processor_active_unit_task_definition_arn",
    "recovery": "scheduler_recovery_task_definition_arn",
    "audit": "scheduler_audit_task_definition_arn",
}
_SCHEDULERS: Final = "module.schedulers."
_EVENTS: Final = {"recovery": ("recovery_failed",), "audit": ("audit_failed", "audit_succeeded")}
INITIAL: Final = {
    mode: (
        f'{_SCHEDULERS}aws_scheduler_schedule_group.mode["{mode}"]',
        SCHEDULE.format(mode),
        *(f'{_SCHEDULERS}aws_cloudwatch_event_rule.task_outcome["{e}"]' for e in events),
        f'{_SCHEDULERS}aws_cloudwatch_metric_alarm.repeated_failure["{mode}"]',
        f'{_SCHEDULERS}aws_cloudwatch_metric_alarm.lost_invocation["{mode}"]',
        *((f"{_SCHEDULERS}aws_cloudwatch_metric_alarm.outbox_backlog[0]",) * (mode == "audit")),
    )
    for mode, events in _EVENTS.items()
}
_RESULT_COMMON: Final = ("activation_sha256", "canary_id", "candidate_sha256", "release_id",
                         "source_sha")


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


@dataclass(frozen=True)
class PhaseCContext:
    """Contexto autenticado da Phase B e evidências das fases C aceitas, em ordem."""

    phase_b: PhaseBContext
    phase_b_evidence: Node
    accepted: tuple[Node, ...] = ()

    @property
    def activation_sha256(self) -> str:
        return hashlib.sha256(self.phase_b.activation or b"").hexdigest()

    @property
    def revisions(self) -> Node:
        return dict(self.phase_b.canary["revisions"])

    @property
    def initial(self) -> bool:
        return self.phase_b_evidence["routes"]["mode"] == "initial"

    @property
    def prior(self) -> dict[str, str | None]:
        routes = self.phase_b_evidence["routes"]
        return {mode: None if self.initial else routes[f"{mode}_revision"] for mode in MODES}

    @property
    def next_mode(self) -> str | None:
        return MODES[len(self.accepted)] if len(self.accepted) < len(MODES) else None

    @property
    def checkpoint(self) -> Node:
        return (self.accepted[-1] if self.accepted else self.phase_b_evidence)["after"]

    def routes_for(self, mode: str) -> dict[str, str | None]:
        """Returns: Rotas esperadas depois do apply do modo; o outro modo fica onde estava."""
        applied = MODES[: MODES.index(mode) + 1]
        return {
            "unit": self.revisions["unit"],
            **{m: self.revisions[m] if m in applied else self.prior[m] for m in MODES},
        }

    def accept(self, evidence: Mapping[str, Any]) -> PhaseCContext:
        """Raises: ValueError: Evidência fora da ordem, da cadeia de checkpoint ou das rotas."""
        mode = self.next_mode
        _require(
            mode is not None
            and evidence.get("kind") == f"phase-c-{mode}"
            and evidence.get("common") == self.phase_b_evidence["common"]
            and evidence.get("close_drain_sha256") == self.phase_b_evidence["close_drain_sha256"]
            and evidence.get("before") == self.checkpoint
            and evidence.get("routes") == self.routes_for(str(mode)),
            "phase_c=evidence",
        )
        return replace(self, accepted=(*self.accepted, deepcopy(dict(evidence))))


def phase_c_context(phase_b: PhaseBContext, evidence: Mapping[str, Any]) -> PhaseCContext:
    """Raises: ValueError: Handoff da Phase B alterado ou evidência de outra activation."""
    _require(phase_b.activation is not None, "phase_c=original_activation_required")
    _require(phase_b == _context_from_admitted(phase_b.canary, phase_b.activation),
             "phase_c=retained_handoff_changed")
    routes = evidence.get("routes") or {}
    _require(evidence.get("kind") == "phase-b", "phase_c=phase_b_kind")
    _require(
        evidence.get("common", {}).get("activation_sha256")
        == hashlib.sha256(phase_b.activation or b"").hexdigest(),
        "phase_c=activation_binding",
    )
    mode = "initial" if phase_b.phase_a["active_before"] is None else "established"
    _require(routes.get("mode") == mode, "phase_c=phase_b_mode")
    _require(routes.get("unit_revision") == phase_b.canary["revisions"]["unit"],
             "phase_c=phase_b_unit")
    return PhaseCContext(phase_b, deepcopy(dict(evidence)))


def _changes(plan: Mapping[str, Any]) -> dict[str, Node]:
    changes = plan.get("resource_changes")
    if not isinstance(changes, list):
        raise ValueError("plan=resource_changes")
    return {
        item["address"]: item["change"]
        for item in changes
        if item.get("mode") == "managed" and item["change"].get("actions") != ["no-op"]
    }


def _variables(plan: Mapping[str, Any], mode: str, context: PhaseCContext) -> None:
    values = plan.get("variables") or {}
    for name, expected in context.routes_for(mode).items():
        _require((values.get(VARIABLES[name]) or {}).get("value") == expected,
                 f"variables={name}")


def _target(value: Mapping[str, Any]) -> Node:
    return value["target"][0]["ecs_parameters"][0]


def _schedule_after(change: Node, mode: str, context: PhaseCContext) -> None:
    after = change.get("after") or {}
    target = after["target"][0]
    ecs = _target(after)
    canary = context.phase_b.canary
    _require(
        after.get("name") == f"cnesdata-prod-{mode}"
        and after.get("state") == "ENABLED"
        and target.get("arn") == canary["cluster"]
        and target["retry_policy"][0].get("maximum_retry_attempts") == 0
        and ecs.get("task_count") == 1
        and ecs.get("task_definition_arn") == context.revisions[mode]
        and not (change.get("after_unknown") or {}).get("target"),
        f"schedule={mode}_target",
    )


def _schedule_contract(change: Node, mode: str, context: PhaseCContext) -> None:
    _schedule_after(change, mode, context)
    prior = context.prior[mode]
    if prior is None:
        return
    before, after = deepcopy(change["before"]), deepcopy(change["after"])
    _require(_target(before).get("task_definition_arn") == prior, f"schedule={mode}_prior")
    _target(before)["task_definition_arn"] = context.revisions[mode]
    _require(before == after, f"schedule={mode}_non_route_change")


def _statements(value: Mapping[str, Any] | None) -> list[Node]:
    policy = (value or {}).get("policy") or "{}"
    document = json.loads(policy) if isinstance(policy, str) else policy
    return list(document.get("Statement", []))


def _run_task(revision: str | None, cluster: str) -> list[Node]:
    if revision is None:
        return []
    return [{"Effect": "Allow", "Action": "ecs:RunTask", "Resource": revision,
             "Condition": {"ArnEquals": {"ecs:cluster": cluster}}}]


def _run_policy_contract(change: Node, mode: str, context: PhaseCContext) -> None:
    cluster = context.phase_b.canary["cluster"]
    before, after = _statements(change.get("before")), _statements(change.get("after"))
    old, new = _run_task(context.prior[mode], cluster), _run_task(context.revisions[mode], cluster)
    _require([s for s in before if s.get("Action") == "ecs:RunTask"] == old,
             f"run_policy={mode}_prior_grant")
    _require([s for s in after if s.get("Action") == "ecs:RunTask"] == new,
             f"run_policy={mode}_grant")
    _require([s for s in before if s not in old] == [s for s in after if s not in new],
             f"run_policy={mode}_other_grants")


def review_scheduler_plan(plan: Mapping[str, Any], mode: str, context: PhaseCContext) -> str:
    """Returns: initial/established. Raises: ValueError: Delta fora do Scheduler do modo."""
    _require(mode == context.next_mode, "phase_c=order")
    _variables(plan, mode, context)
    _assert_task_revisions_unchanged(plan)
    changes = _changes(plan)
    initial = context.prior[mode] is None
    expected = {
        **dict.fromkeys(INITIAL[mode] if initial else (SCHEDULE.format(mode),),
                        ["create"] if initial else ["update"]),
        RUN_POLICY.format(mode): ["update"],
    }
    _require(set(changes) == set(expected), f"delta=unexpected resources={sorted(changes)!r}")
    _require(all(changes[a].get("actions") == expected[a] for a in expected), "delta=action")
    _schedule_contract(changes[SCHEDULE.format(mode)], mode, context)
    _run_policy_contract(changes[RUN_POLICY.format(mode)], mode, context)
    return "initial" if initial else "established"


def review_recovery_plan(plan: Mapping[str, Any], context: PhaseCContext) -> str:
    """Returns: initial/established. Raises: ValueError: Plan muda unit, audit ou fora de ordem."""
    return review_scheduler_plan(plan, "recovery", context)


def _bind_close(context: PhaseCContext, close: Mapping[str, Any]) -> None:
    common = close["common"]
    _require(common == context.phase_b_evidence["common"]
             and object_hash(close) == context.phase_b_evidence["close_drain_sha256"],
             "phase_c=close_binding")
    _require(common["activation_sha256"] == context.activation_sha256, "phase_c=activation_binding")


def _audited(approval: Mapping[str, Any], mode: str, close: Mapping[str, Any],
             before: Node) -> tuple[bytes, bytes]:
    raw = Path(approval["plan_path"]).read_bytes()
    audit = Path(approval["audit_path"]).read_bytes()
    _require(hashlib.sha256(audit).hexdigest() == approval["audit_sha256"],
             "phase_c=original_audit_hash")
    report = json.loads(audit)
    _require(
        report.get("decision") == "ACCEPT"
        and report.get("phase") == f"phase-c-{mode}"
        and report.get("plan_sha256") == hashlib.sha256(raw).hexdigest()
        and report.get("checkpoint") == before
        and report.get("source_sha") == close["common"]["source_sha"],
        "phase_c=exact_independent_audit_required",
    )
    return raw, audit


def _apply_reviewed(native: NativePhaseB, raw: bytes, review: Any, before: Node) -> bytes:
    with TemporaryDirectory(prefix="reviewed-phase-c-") as directory:
        reviewed = Path(directory) / "reviewed.tfplan"
        reviewed.write_bytes(raw)
        reviewed.chmod(0o400)
        review(native.show(reviewed))
        _require(native.checkpoint()[0] == before and reviewed.read_bytes() == raw,
                 "phase_c=checkpoint_or_binary_changed")
        applied = native.apply(reviewed)
        _require(reviewed.read_bytes() == raw, "phase_c=applied_binary_changed")
    return applied


def execute_reviewed_phase_c(context: PhaseCContext, close: Mapping[str, Any],
                             native: NativePhaseB, approval: Mapping[str, Any]) -> Node:
    """Aplica só o binário auditado do próximo modo sobre o checkpoint real anterior."""
    mode = context.next_mode
    _require(mode is not None, "phase_c=complete")
    _bind_close(context, close)
    native.assert_closed(close)
    before, _ = native.checkpoint()
    _require(before == context.checkpoint, "phase_c=checkpoint_chain")
    raw, audit = _audited(approval, str(mode), close, before)

    def review(plan: Node) -> None:
        review_scheduler_plan(plan, str(mode), context)
        native.assert_closed(close)

    applied = _apply_reviewed(native, raw, review, before)
    native.assert_clean()
    after, state = native.checkpoint()
    routes = read_routes(native, context, state)
    _require(routes == context.routes_for(str(mode)), "phase_c=routes")
    native.assert_closed(close)
    return {
        "version": 1, "kind": f"phase-c-{mode}", "common": dict(close["common"]),
        "close_drain_sha256": object_hash(close), "before": before, "after": after,
        "plan_sha256": hashlib.sha256(raw).hexdigest(),
        "audit_sha256": hashlib.sha256(audit).hexdigest(),
        "apply_sha256": hashlib.sha256(applied).hexdigest(), "routes": routes,
        "observed_at": _require_utc(datetime.now(UTC)),
    }


def _machine_route(native: NativePhaseB, context: PhaseCContext) -> str:
    expected = context.phase_b.machine
    machine = native.clients["stepfunctions"].describe_state_machine(
        stateMachineArn=expected["arn"])
    _require(machine["stateMachineArn"] == expected["arn"]
             and json.loads(machine["definition"]) == json.loads(expected["definition"]),
             "phase_c=machine_readback")
    return context.revisions["unit"]


def _schedule_routes(native: NativePhaseB, state: Mapping[str, Any]) -> dict[str, str | None]:
    routes: dict[str, str | None] = dict.fromkeys(MODES)
    for resource in state["resources"]:
        if resource.get("type") != "aws_scheduler_schedule":
            continue
        for instance in resource["instances"]:
            original = instance["attributes"]
            arn = _target(original)["task_definition_arn"]
            actual = native.clients["scheduler"].get_schedule(
                Name=original["name"], GroupName=original["group_name"])["Target"]
            _require(actual["EcsParameters"]["TaskDefinitionArn"] == arn
                     and actual["Arn"] == original["target"][0]["arn"],
                     "phase_c=scheduler_readback")
            modes = [mode for mode in MODES if f"processor-{mode}:" in arn]
            _require(len(modes) == 1 and routes[modes[0]] is None, "phase_c=scheduler_mode")
            routes[modes[0]] = arn
    return routes


def read_routes(native: NativePhaseB, context: PhaseCContext,
                state: Mapping[str, Any]) -> dict[str, str | None]:
    """Returns: Rotas lidas forte da máquina, dos Schedulers e do state; ambiente das tasks."""
    routes = {"unit": _machine_route(native, context), **_schedule_routes(native, state)}
    environments = {}
    for mode in ("unit", "recovery"):
        revision = routes[mode]
        _require(revision is not None, f"phase_c={mode}_route_missing")
        task = native.clients["ecs"].describe_task_definition(taskDefinition=revision)
        definition = task["taskDefinition"]
        _require(definition["taskDefinitionArn"] == revision
                 and len(definition["containerDefinitions"]) == 1, "phase_c=task_readback")
        environments[mode] = {"environment": definition["containerDefinitions"][0]["environment"]}
    verify_task_environment_arns(environments, context.phase_b.machine["arn"])
    return routes


def mode_result(raw: bytes, mode: str, context: PhaseCContext, readbacks: Node) -> Node:
    """Returns: mode-result aceito. Raises: ValueError: Result de outra activation ou incompleto."""
    from cnes_infra.aws.mode_canary import canonical_text
    from scripts.promotion.mode_canary import RESULT_FIELDS, RESULT_SCHEMA

    _require(mode in [e["kind"].removeprefix("phase-c-") for e in context.accepted],
             "canary=route_not_accepted")
    result = json.loads(raw)
    _require(canonical_text(result).encode() == raw and set(result) == set(RESULT_FIELDS)
             and result["schema"] == RESULT_SCHEMA and result["mode"] == mode, "canary=schema")
    canary = context.phase_b.canary
    expected = {**{key: canary[key] for key in _RESULT_COMMON[1:]},
                "activation_sha256": context.activation_sha256}
    _require(all(result["common"][key] == expected[key] for key in _RESULT_COMMON),
             "canary=common")
    _require(not result["pending"] and not result["unknown"], "canary=pending")
    routing = hashlib.sha256(canonical_text(readbacks).encode()).hexdigest()
    _require(result["routing_before_sha256"] == routing == result["routing_after_sha256"],
             "canary=routing")
    _require(len(result["task_receipts"]) == 1, "canary=single_task")
    return result


def _route_role_deadline(readbacks: Node, mode: str, context: PhaseCContext) -> None:
    canary = context.phase_b.canary
    target = readbacks["schedules"][mode]["Target"]
    definition = readbacks["task_definitions"][mode]["taskDefinition"]
    role = f"arn:aws:iam::{canary['account_id']}:role/cnesdata-prod-processor-{mode}"
    _require(definition.get("taskRoleArn") == role, "route=role")
    _require(target["RetryPolicy"] == {"MaximumRetryAttempts": 0,
                                       "MaximumEventAgeInSeconds": 900}, "route=deadline")
    network = canary["network"]
    vpc = target["EcsParameters"]["NetworkConfiguration"]["awsvpcConfiguration"]
    _require(vpc == {"Subnets": sorted(network["subnets"]),
                     "SecurityGroups": [network["security_group"]],
                     "AssignPublicIp": network["public_ip"]}, "route=network")


def verify_mode_route(readbacks: Node, mode: str, context: PhaseCContext) -> None:
    """Raises: ValueError: Rede, role, prazo ou alarme de falha fora do alvo exato do modo."""
    _route_role_deadline(readbacks, mode, context)
    alarm = readbacks["alarms"][mode]
    _require(
        alarm.get("AlarmName") == f"cnesdata-prod-scheduler-{mode}-repeated-failure"
        and alarm.get("Dimensions")
        == [{"Name": "RuleName", "Value": f"cnesdata-prod-scheduler-{mode}-failed"}]
        and (alarm.get("EvaluationPeriods"), alarm.get("DatapointsToAlarm")) == (2, 2)
        and alarm.get("ActionsEnabled") is True
        and bool(alarm.get("AlarmActions")),
        "route=alarm",
    )


def accept_recovery_canary(raw: bytes, context: PhaseCContext, readbacks: Node,
                           gate_state: Any) -> Node:
    """Returns: Aceite do canary de recovery. Raises: ValueError: Task unit ainda em voo."""
    verify_mode_route(readbacks, "recovery", context)
    result = mode_result(raw, "recovery", context, readbacks)
    fence = gate_state.fence
    _require(fence.state.value == "CLOSED"
             and fence.promotion_id == context.phase_b_evidence["common"]["promotion_id"],
             "canary=fence")
    _require(gate_state.permit is None, "canary=unit_in_flight")
    return summary(raw, result)


def summary(raw: bytes, result: Node) -> Node:
    """Returns: Resumo hashável do mode-result aceito."""
    return {
        "mode": result["mode"],
        "common": dict(result["common"]),
        "result_sha256": hashlib.sha256(raw).hexdigest(),
        "routing_sha256": result["routing_before_sha256"],
        "task_receipts": len(result["task_receipts"]),
        "freeze_proof_sha256": result["freeze_proof_sha256"],
        "deadline_drill_sha256": result["deadline_drill_sha256"],
        "operational_requirements": list(result["operational_requirements"]),
    }
