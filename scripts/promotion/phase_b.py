"""Review and attest the two closed Phase B Terraform oracles."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

Node = dict[str, Any]
MACHINE = "module.processing.aws_sfn_state_machine.processor[0]"
API_POLICY = "module.runtime_iam.aws_iam_role_policy.api[0]"
SFN_ACTIONS = {
    "states:DescribeStateMachine",
    "states:StartExecution",
    "states:DescribeExecution",
    "states:StopExecution",
}
SCHEDULER_PREFIX = "module.schedulers."


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def object_hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _changes(plan: Mapping[str, Any]) -> list[Node]:
    changes = plan.get("resource_changes")
    if not isinstance(changes, list):
        raise ValueError("plan=resource_changes")
    return [item for item in changes if item.get("mode") == "managed"]


def _changed(plan: Mapping[str, Any], address: str) -> list[Node]:
    return [item for item in _changes(plan) if item.get("address") == address]


def _policy_actions(policy: Any) -> set[str]:
    if isinstance(policy, str):
        policy = json.loads(policy)
    statements = policy.get("Statement", [])
    actions: set[str] = set()
    for statement in statements:
        values = statement.get("Action", [])
        actions.update([values] if isinstance(values, str) else values)
    return actions


def _assert_no_scheduler_drift(plan: Mapping[str, Any]) -> None:
    changed = [
        item
        for item in _changes(plan)
        if item["address"].startswith(SCHEDULER_PREFIX)
        and item["change"].get("actions") != ["no-op"]
    ]
    if changed:
        raise ValueError("delta=scheduler")


def _assert_task_revisions_unchanged(plan: Mapping[str, Any]) -> None:
    changed = [
        item
        for item in _changes(plan)
        if "aws_ecs_task_definition" in item["address"]
        and item["change"].get("actions") != ["no-op"]
    ]
    if changed:
        raise ValueError("delta=task_revision")


def _initial_oracle(plan: Mapping[str, Any]) -> None:
    machine = _changed(plan, MACHINE)
    policy = _changed(plan, API_POLICY)
    if len(machine) != 1 or machine[0]["change"].get("actions") != ["create"]:
        raise ValueError("oracle=initial_machine_create")
    if len(policy) != 1 or policy[0]["change"].get("actions") not in (
        ["create"],
        ["update"],
    ):
        raise ValueError("oracle=initial_api_policy")
    before = policy[0]["change"].get("before") or {}
    after = policy[0]["change"].get("after") or {}
    old = _policy_actions(before.get("policy", {"Statement": []}))
    new = _policy_actions(after.get("policy", {"Statement": []}))
    if not new >= SFN_ACTIONS or not new - old >= SFN_ACTIONS:
        raise ValueError("oracle=initial_sfn_actions")


def _established_oracle(plan: Mapping[str, Any]) -> None:
    machine = _changed(plan, MACHINE)
    if len(machine) != 1 or machine[0]["change"].get("actions") != ["update"]:
        raise ValueError("oracle=established_machine_update")
    policy = _changed(plan, API_POLICY)
    if policy and policy[0]["change"].get("actions") != ["no-op"]:
        raise ValueError("oracle=established_policy_drift")


def review_phase_b_plan(plan: Mapping[str, Any], active_unit_arn: str | None) -> str:
    """Review a plan with the activation-null or established-machine oracle."""
    _assert_no_scheduler_drift(plan)
    _assert_task_revisions_unchanged(plan)
    mode = "initial" if active_unit_arn is None else "established"
    if mode == "initial":
        _initial_oracle(plan)
    else:
        _established_oracle(plan)
    allowed = {MACHINE, API_POLICY} if mode == "initial" else {MACHINE}
    drift = [
        item["address"]
        for item in _changes(plan)
        if item["change"].get("actions") != ["no-op"] and item["address"] not in allowed
    ]
    if drift:
        raise ValueError(f"delta=unexpected resources={sorted(drift)!r}")
    return mode


def _require_utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("reason=datetime_not_utc")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _routes(routes: Mapping[str, Any], expected_machine: str) -> Node:
    required = {
        "mode",
        "state_machine_arn",
        "unit_revision",
        "recovery_revision",
        "audit_revision",
        "unit_environment_state_machine_arn",
        "recovery_environment_state_machine_arn",
    }
    if set(routes) != required or routes["state_machine_arn"] != expected_machine:
        raise ValueError("routes=schema")
    if any(
        routes[key] != expected_machine
        for key in ("unit_environment_state_machine_arn", "recovery_environment_state_machine_arn")
    ):
        raise ValueError("routes=environment_arn")
    if routes["mode"] not in {"initial", "established"}:
        raise ValueError("routes=mode")
    return dict(routes)


def build_phase_b_evidence(
    close_drain: Mapping[str, Any],
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    plan: Any,
    audit: Any,
    applied: Any,
    routes: Mapping[str, Any],
    observed_at: datetime,
) -> Node:
    """Build a Phase B record from exact plan/audit/apply bytes and readback."""
    if close_drain.get("kind") != "close-drain" or close_drain.get("permit") is not None:
        raise ValueError("phase_b=close_drain")
    if before.get("lineage") != after.get("lineage") or after.get("serial", -1) <= before.get(
        "serial", -1
    ):
        raise ValueError("checkpoint=lineage_or_serial")
    route = _routes(routes, str(routes.get("state_machine_arn", "")))
    return {
        "version": 1,
        "kind": "phase-b",
        "common": dict(close_drain["common"]),
        "close_drain_sha256": object_hash(close_drain),
        "before": dict(before),
        "after": dict(after),
        "plan_sha256": object_hash(plan),
        "audit_sha256": object_hash(audit),
        "apply_sha256": object_hash(applied),
        "routes": route,
        "observed_at": _require_utc(observed_at),
    }


def run_phase_b(plan: Mapping[str, Any], active_unit_arn: str | None) -> str:
    """Compatibility entry point used by the operator adapter."""
    return review_phase_b_plan(plan, active_unit_arn)
