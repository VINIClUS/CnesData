"""Verify Phase B routing, task environment readback and closed handoff."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

Node = dict[str, Any]


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def object_hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("reason=datetime_not_utc")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def verify_task_environment_arns(
    task_definitions: Mapping[str, Mapping[str, Any]], expected_state_machine_arn: str
) -> None:
    """Require unit and recovery tasks to carry the exact machine ARN."""
    for mode in ("unit", "recovery"):
        environment = task_definitions.get(mode, {}).get("environment", {})
        if isinstance(environment, list):
            environment = {item["name"]: item.get("value") for item in environment}
        if environment.get("AWS_STATE_MACHINE_ARN") != expected_state_machine_arn:
            raise ValueError(f"routes={mode}_environment_arn")


def verify_routes(
    routes: Mapping[str, Any],
    expected_state_machine_arn: str,
    activation_unit_revision: str,
    previous_recovery_revision: str,
    previous_audit_revision: str,
    task_definitions: Mapping[str, Mapping[str, Any]],
) -> Node:
    """Verify route domains and exact ECS/SFN readback identities."""
    required = {
        "mode",
        "state_machine_arn",
        "unit_revision",
        "recovery_revision",
        "audit_revision",
        "unit_environment_state_machine_arn",
        "recovery_environment_state_machine_arn",
    }
    if set(routes) != required:
        raise ValueError("routes=schema")
    if routes["state_machine_arn"] != expected_state_machine_arn:
        raise ValueError("routes=state_machine_arn")
    if routes["unit_environment_state_machine_arn"] != expected_state_machine_arn:
        raise ValueError("routes=unit_arn")
    if routes["recovery_environment_state_machine_arn"] != expected_state_machine_arn:
        raise ValueError("routes=recovery_arn")
    if routes["unit_revision"] != activation_unit_revision:
        raise ValueError("routes=unit_revision")
    if routes["recovery_revision"] != previous_recovery_revision:
        raise ValueError("routes=recovery_revision")
    if routes["audit_revision"] != previous_audit_revision:
        raise ValueError("routes=audit_revision")
    if routes["mode"] not in {"initial", "established"}:
        raise ValueError("routes=mode")
    verify_task_environment_arns(task_definitions, expected_state_machine_arn)
    return dict(routes)


def _members(value: Iterable[Mapping[str, Any]]) -> list[Node]:
    result = [dict(item) for item in value]
    if len({item.get("dispatch_id") for item in result}) != len(result):
        raise ValueError("membership=duplicate")
    if any(
        item.get("resolution") not in {"TERMINAL", "ABSENT_PROVEN", "CANCEL_BEFORE_SEND"}
        for item in result
    ):
        raise ValueError("membership=unresolved")
    return sorted(result, key=lambda item: str(item["dispatch_id"]))


def _validate_closed_ready(
    close_drain: Mapping[str, Any],
    phase_b: Mapping[str, Any],
    current: list[Node],
    observed_at: datetime,
    owner: str,
    generation: int,
    admitted_at: datetime,
    deadline: datetime,
) -> None:
    if close_drain.get("kind") != "close-drain" or phase_b.get("kind") != "phase-b":
        raise ValueError("closed_ready=source_kind")
    if close_drain.get("permit") is not None:
        raise ValueError("closed_ready=permit")
    if phase_b.get("close_drain_sha256") != object_hash(close_drain):
        raise ValueError("closed_ready=close_hash")
    if object_hash(current) != close_drain.get("frozen_membership_sha256"):
        raise ValueError("closed_ready=membership_changed")
    if observed_at != admitted_at or deadline > admitted_at + timedelta(seconds=180):
        raise ValueError("closed_ready=stage_deadline")
    if not isinstance(generation, int) or generation < 1 or not owner:
        raise ValueError("closed_ready=exclusion")
    if close_drain["fence"]["state"] != "CLOSED":
        raise ValueError("closed_ready=fence")


def _closed_ready_document(
    close_drain: Mapping[str, Any],
    phase_b: Mapping[str, Any],
    current: list[Node],
    observed_at: datetime,
    owner: str,
    generation: int,
    admitted_at: datetime,
    deadline: datetime,
) -> Node:
    return {
        "version": 1,
        "kind": "closed-ready",
        "common": dict(close_drain["common"]),
        "close_drain_sha256": object_hash(close_drain),
        "phase_b_sha256": object_hash(phase_b),
        "fence": dict(close_drain["fence"]),
        "permit": None,
        "members": current,
        "proofs": dict(close_drain.get("proofs", {})),
        "observed_at": _utc(observed_at),
        "exclusion": {
            "owner": owner,
            "generation": generation,
            "membership_sha256": object_hash(current),
            "acquired_at": close_drain["close_confirmed_at"],
            "observed_at": _utc(observed_at),
        },
        "stage": {
            "name": "api",
            "admitted_at": _utc(admitted_at),
            "deadline": _utc(deadline),
            "later_seconds": 840,
        },
        "decision": None,
    }


def build_closed_ready(
    close_drain: Mapping[str, Any],
    phase_b: Mapping[str, Any],
    members: Iterable[Mapping[str, Any]],
    observed_at: datetime,
    owner: str,
    generation: int,
    admitted_at: datetime,
    deadline: datetime,
) -> Node:
    """Emit closed-ready only after post-B membership and fence readback."""
    current = _members(members)
    _validate_closed_ready(
        close_drain,
        phase_b,
        current,
        observed_at,
        owner,
        generation,
        admitted_at,
        deadline,
    )
    return _closed_ready_document(
        close_drain, phase_b, current, observed_at, owner, generation, admitted_at, deadline
    )


verify_task_arns = verify_task_environment_arns
