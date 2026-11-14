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

from cnes_infra.aws.mode_canary import canonical_text
from scripts.promotion.accept_routing import (
    MODES,
    CanaryAcceptance,
    PhaseCWatchdog,
    read_routes,
)
from scripts.promotion.mode_canary import RESULT_FIELDS, RESULT_SCHEMA
from scripts.promotion.phase_b import (
    PhaseBContext,
    _assert_task_revisions_unchanged,
    _context_from_admitted,
    _require_utc,
    object_hash,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from scripts.promotion.verify_routes import NativePhaseB

Node = dict[str, Any]
APPLY_SECONDS: Final = 300
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
_EMPTY_TARGET: Final = ("input", "dead_letter_config", "eventbridge_parameters",
                        "kinesis_parameters", "sagemaker_pipeline_parameters", "sqs_parameters")
_EMPTY_ECS: Final = ("capacity_provider_strategy", "placement_constraints", "placement_strategy",
                     "propagate_tags", "reference_id", "group", "tags")
_RESULT_COMMON: Final = ("canary_id", "candidate_sha256", "release_id", "source_sha")


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


def _second(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(microsecond=0)


@dataclass(frozen=True)
class PhaseCContext:
    """Phase B autenticada, close-drain original e evidências das fases C aceitas, em ordem."""

    phase_b: PhaseBContext
    phase_b_evidence: Node
    close: Node
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

    @property
    def watchdog(self) -> PhaseCWatchdog:
        return PhaseCWatchdog.from_close(self.close)

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


def phase_c_context(phase_b: PhaseBContext, evidence: Mapping[str, Any],
                    close: Mapping[str, Any]) -> PhaseCContext:
    """Raises: ValueError: Handoff alterado, outra activation ou close-drain de outra Phase B."""
    _require(phase_b.activation is not None, "phase_c=original_activation_required")
    _require(phase_b == _context_from_admitted(phase_b.canary, phase_b.activation),
             "phase_c=retained_handoff_changed")
    routes = evidence.get("routes") or {}
    _require(evidence.get("kind") == "phase-b", "phase_c=phase_b_kind")
    activation = hashlib.sha256(phase_b.activation or b"").hexdigest()
    _require(evidence.get("common", {}).get("activation_sha256") == activation,
             "phase_c=activation_binding")
    _require(object_hash(close) == evidence.get("close_drain_sha256")
             and close.get("common") == evidence.get("common"), "phase_c=close_binding")
    mode = "initial" if phase_b.phase_a["active_before"] is None else "established"
    _require(routes.get("mode") == mode, "phase_c=phase_b_mode")
    _require(routes.get("unit_revision") == phase_b.canary["revisions"]["unit"],
             "phase_c=phase_b_unit")
    PhaseCWatchdog.from_close(close)
    return PhaseCContext(phase_b, deepcopy(dict(evidence)), deepcopy(dict(close)))


def _changes(plan: Mapping[str, Any]) -> dict[str, Node]:
    changes = plan.get("resource_changes")
    if not isinstance(changes, list):
        raise ValueError("plan=resource_changes")
    managed = [item for item in changes if item.get("mode") == "managed"]
    addresses = [item["address"] for item in managed]
    _require(len(addresses) == len(set(addresses)), "plan=duplicate_address")
    return {
        item["address"]: item["change"]
        for item in managed
        if item["change"].get("actions") != ["no-op"]
    }


def _variables(plan: Mapping[str, Any], mode: str, context: PhaseCContext) -> None:
    values = plan.get("variables") or {}
    for name, expected in context.routes_for(mode).items():
        _require((values.get(VARIABLES[name]) or {}).get("value") == expected,
                 f"variables={name}")


def _target(value: Mapping[str, Any]) -> Node:
    return value["target"][0]["ecs_parameters"][0]


def _subset(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> bool:
    return all(actual.get(key) == value for key, value in expected.items())


def _empty(actual: Mapping[str, Any], keys: tuple[str, ...]) -> bool:
    return all(actual.get(key) in (None, [], {}, "") for key in keys)


def _schedule_after(change: Node, mode: str, context: PhaseCContext) -> None:
    after = change.get("after") or {}
    canary = context.phase_b.canary
    network = canary["network"]
    target = {
        "arn": canary["cluster"],
        "role_arn": f"arn:aws:iam::{canary['account_id']}:role/cnesdata-prod-scheduler-{mode}",
        "retry_policy": [{"maximum_retry_attempts": 0, "maximum_event_age_in_seconds": 900}],
    }
    ecs = {
        "task_definition_arn": context.revisions[mode], "launch_type": "FARGATE",
        "task_count": 1, "enable_execute_command": False,
        "network_configuration": [{
            "assign_public_ip": network["public_ip"] == "ENABLED",
            "security_groups": [network["security_group"]],
            "subnets": sorted(network["subnets"]),
        }],
    }
    names = {"name": f"cnesdata-prod-{mode}", "group_name": f"cnesdata-prod-{mode}",
             "state": "ENABLED", "schedule_expression": "rate(30 minutes)"}
    _require(
        _subset(after, names)
        and after["flexible_time_window"][0].get("mode") == "OFF"
        and _subset(after["target"][0], target) and _empty(after["target"][0], _EMPTY_TARGET)
        and _subset(_target(after), ecs) and _empty(_target(after), _EMPTY_ECS)
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


def _audited(approval: Mapping[str, Any], mode: str, context: PhaseCContext,
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
        and report.get("source_sha") == context.close["common"]["source_sha"],
        "phase_c=exact_independent_audit_required",
    )
    return raw, audit


def _apply_reviewed(native: NativePhaseB, raw: bytes, review: Callable[[Node], None],
                    before: Node) -> bytes:
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


def _advanced(before: Node, after: Node) -> None:
    _require(after["lineage"] == before["lineage"] and after["serial"] > before["serial"]
             and after["object_version"] != before["object_version"],
             "phase_c=checkpoint_not_advanced")


def execute_reviewed_phase_c(context: PhaseCContext, native: NativePhaseB,
                             approval: Mapping[str, Any]) -> Node:
    """Aplica só o binário auditado do próximo modo sobre o checkpoint real, antes do corte."""
    mode, close = context.next_mode, context.close
    _require(mode is not None, "phase_c=complete")
    native.assert_closed(close)
    before, _ = native.checkpoint()
    _require(before == context.checkpoint, "phase_c=checkpoint_chain")
    raw, audit = _audited(approval, str(mode), context, before)

    def review(plan: Node) -> None:
        review_scheduler_plan(plan, str(mode), context)
        native.assert_closed(close)
        context.watchdog.admit(f"phase-c-{mode}", datetime.now(UTC), APPLY_SECONDS)

    applied = _apply_reviewed(native, raw, review, before)
    native.assert_clean()
    after, state = native.checkpoint()
    _advanced(before, after)
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


def _bound(result: Node, mode: str, context: PhaseCContext) -> None:
    applied = [e for e in context.accepted if e["kind"] == f"phase-c-{mode}"]
    _require(len(applied) == 1, "canary=route_not_accepted")
    canary, common = context.phase_b.canary, context.close["common"]
    _require(
        all(result["common"][key] == canary[key] for key in _RESULT_COMMON)
        and result["common"]["activation_sha256"] == context.activation_sha256
        and result["common"]["promotion_generation"] == common["promotion_generation"],
        "canary=common",
    )
    observed = _second(result["observed_at"])
    _require(_second(result["fixed_deadline"]) == _second(context.close["fixed_deadline"])
             and observed >= _second(applied[0]["observed_at"]), "canary=window")
    context.watchdog.admit(f"canary-{mode}", observed, 0)


def mode_result(raw: bytes, mode: str, context: PhaseCContext, readbacks: Node) -> Node:
    """Returns: mode-result aceito. Raises: ValueError: Result de outra janela ou incompleto."""
    result = json.loads(raw)
    _require(canonical_text(result).encode() == raw and set(result) == set(RESULT_FIELDS)
             and result["schema"] == RESULT_SCHEMA and result["mode"] == mode, "canary=schema")
    _bound(result, mode, context)
    _require(not result["pending"] and not result["unknown"], "canary=pending")
    routing = hashlib.sha256(canonical_text(readbacks).encode()).hexdigest()
    _require(result["routing_before_sha256"] == routing == result["routing_after_sha256"],
             "canary=routing")
    _require(len(result["task_receipts"]) == 1, "canary=single_task")
    return result


def _route_role_deadline(readbacks: Node, mode: str, context: PhaseCContext) -> None:
    canary, revision = context.phase_b.canary, context.revisions[mode]
    target = readbacks["schedules"][mode]["Target"]
    definition = readbacks["task_definitions"][mode]["taskDefinition"]
    _require(target["EcsParameters"]["TaskDefinitionArn"] == revision
             and definition.get("taskDefinitionArn") == revision, "route=revision")
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
    """Raises: ValueError: Revisão, rede, role, prazo ou alarme de falha fora do alvo do modo."""
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


def fenced(context: PhaseCContext, gate_state: Any) -> None:
    """Raises: ValueError: Fence de outra promoção, aberto ou permit unit ainda em voo."""
    fence = gate_state.fence
    _require(fence.state.value == "CLOSED"
             and fence.promotion_id == context.close["fence"]["promotion_id"], "canary=fence")
    _require(gate_state.permit is None, "canary=unit_in_flight")


def accept_mode_canary(raw: bytes, mode: str, context: PhaseCContext,
                       evidence: Mapping[str, Any]) -> CanaryAcceptance:
    """Returns: Aceite tipado do mode-result. Raises: ValueError: Rota, janela ou fence."""
    verify_mode_route(evidence["readbacks"], mode, context)
    result = mode_result(raw, mode, context, evidence["readbacks"])
    fenced(context, evidence["gate"])
    return CanaryAcceptance(
        mode=mode, common=dict(result["common"]),
        result_sha256=hashlib.sha256(raw).hexdigest(),
        routing_sha256=result["routing_before_sha256"],
        task_receipts=len(result["task_receipts"]), observed_at=result["observed_at"],
        freeze_proof_sha256=result["freeze_proof_sha256"],
        deadline_drill_sha256=result["deadline_drill_sha256"],
        operational_requirements=tuple(result["operational_requirements"]),
    )


def accept_recovery_canary(raw: bytes, context: PhaseCContext, readbacks: Node,
                           gate_state: Any) -> CanaryAcceptance:
    """Returns: Aceite do canary de recovery. Raises: ValueError: Task unit ainda em voo."""
    return accept_mode_canary(raw, "recovery", context,
                              {"readbacks": readbacks, "gate": gate_state})
