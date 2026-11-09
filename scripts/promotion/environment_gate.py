"""Close the production fence and consume only real drain observations."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    ClosePromotionFence,
    FenceState,
    PermitHolder,
)
from cnes_domain.ports.environment_gate import (
    ExecutionPresence,
    ExecutionProbePort,
)

Node = dict[str, Any]
Clock = Callable[[], datetime]


class MembershipSource(Protocol):
    def freeze_membership(self) -> Iterable[Mapping[str, Any]]: ...

    def proof_bytes(self, dispatch_id: str) -> bytes: ...


class FenceGatePort(Protocol):
    def close_promotion_fence(self, command: ClosePromotionFence) -> bool: ...

    def observe_environment_gate(self, environment: str) -> Any: ...


class DrainClock(Protocol):
    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...


class DrainError(RuntimeError):
    """Raised when the fence cannot be safely drained."""


def _canonical(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode()


def _sha(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("reason=datetime_not_utc")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _member(value: Mapping[str, Any]) -> Node:
    required = {
        "dispatch_id",
        "request_sha256",
        "generation",
        "initial_state",
        "execution_ref",
        "resolution",
        "proof_sha256",
        "observed_at",
        "send_disabled_at",
    }
    if set(value) != required:
        raise DrainError("membership=dispatch_schema")
    if not isinstance(value["dispatch_id"], str) or len(value["dispatch_id"]) != 16:
        raise DrainError("membership=dispatch_id")
    try:
        int(value["dispatch_id"], 16)
    except (TypeError, ValueError) as error:
        raise DrainError("membership=dispatch_id") from error
    for field in ("request_sha256", "proof_sha256"):
        if not isinstance(value[field], str) or len(value[field]) != 64:
            raise DrainError(f"membership={field}")
    if isinstance(value["generation"], bool) or not isinstance(value["generation"], int):
        raise DrainError("membership=generation")
    if value["resolution"] not in {"TERMINAL", "ABSENT_PROVEN", "CANCEL_BEFORE_SEND"}:
        raise DrainError("membership=unresolved")
    if value["resolution"] == "TERMINAL" and not value["execution_ref"]:
        raise DrainError("membership=terminal_ref")
    if value["resolution"] != "TERMINAL" and value["execution_ref"] is not None:
        raise DrainError("membership=execution_ref")
    if not isinstance(value["send_disabled_at"], str):
        raise DrainError("membership=send_exclusion")
    return dict(value)


def _common(value: Mapping[str, Any], promotion_id: str) -> Node:
    required = {
        "repository",
        "release_id",
        "source_sha",
        "candidate_sha256",
        "canary_id",
        "activation_sha256",
        "promotion_id",
        "promotion_generation",
        "caller",
    }
    if set(value) != required or value.get("repository") != "VINIClUS/CnesData":
        raise DrainError("common=schema")
    if value.get("promotion_id") != promotion_id:
        raise DrainError("common=promotion_binding")
    for field, length in (
        ("source_sha", 40),
        ("candidate_sha256", 64),
        ("activation_sha256", 64),
    ):
        if not isinstance(value.get(field), str) or len(value[field]) != length:
            raise DrainError(f"common={field}")
    if not isinstance(value.get("canary_id"), str) or len(value["canary_id"]) != 32:
        raise DrainError("common=canary_id")
    if not isinstance(value.get("caller"), str) or not value["caller"]:
        raise DrainError("common=caller")
    if not isinstance(value.get("promotion_generation"), int) or value["promotion_generation"] < 1:
        raise DrainError("common=promotion_generation")
    return dict(value)


def _source_members(
    source: MembershipSource | Callable[[], Iterable[Mapping[str, Any]]],
) -> list[Node]:
    if callable(source):
        raise DrainError("membership=source_capability")
    producer = source.freeze_membership
    members = [_member(item) for item in producer()]
    if len({item["dispatch_id"] for item in members}) != len(members):
        raise DrainError("membership=duplicate")
    for item in members:
        proof = source.proof_bytes(item["dispatch_id"])
        if (
            not isinstance(proof, bytes)
            or hashlib.sha256(proof).hexdigest() != item["proof_sha256"]
        ):
            raise DrainError("membership=proof_binding")
        try:
            evidence = json.loads(proof)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise DrainError("membership=proof_encoding") from error
        if (
            not isinstance(evidence, dict)
            or evidence.get("dispatch_id") != item["dispatch_id"]
            or evidence.get("resolution") != item["resolution"]
            or evidence.get("send_disabled_at") != item["send_disabled_at"]
        ):
            raise DrainError("membership=proof_content")
    return sorted(members, key=lambda item: item["dispatch_id"])


def _probe_member(member: Node, probe: ExecutionProbePort) -> None:
    observation = probe.probe(member["dispatch_id"])
    if member["resolution"] == "TERMINAL":
        if observation.presence is not ExecutionPresence.TERMINAL:
            raise DrainError("drain=terminal_not_observed")
        if observation.execution_ref != member["execution_ref"]:
            raise DrainError("drain=execution_ref_mismatch")
    elif observation.presence is not ExecutionPresence.ABSENT:
        raise DrainError("drain=absence_not_proven")


def _fixed_deadline(closed_at: datetime, expirations: Iterable[datetime]) -> datetime:
    values = [closed_at + timedelta(seconds=1800), *expirations]
    if any(value.tzinfo is None or value.utcoffset() != timedelta(0) for value in values):
        raise ValueError("reason=datetime_not_utc")
    return min(values)


def _close_fence(gate: FenceGatePort, environment: str, promotion_id: str, now: datetime) -> Any:
    command = ClosePromotionFence(environment, promotion_id, now)
    snapshot = getattr(gate, "close_promotion_fence_snapshot", None)
    if callable(snapshot):
        state = cast("Any", snapshot(command))
        if not state.fence.closed_membership_sha256:
            raise DrainError("membership=fence_snapshot_missing")
    else:
        if not gate.close_promotion_fence(command):
            raise DrainError("fence=close_conflict")
        state = gate.observe_environment_gate(environment)
    if state.fence.state is not FenceState.CLOSED or state.fence.promotion_id != promotion_id:
        raise DrainError("fence=closed_readback")
    return state


def _drain(
    gate: FenceGatePort,
    probe: ExecutionProbePort,
    members: list[Node],
    environment: str,
    started: datetime,
    deadline: datetime,
    clock: DrainClock,
) -> None:
    limit = clock.monotonic() + max(0.0, (deadline - started).total_seconds())
    while True:
        for member in members:
            _probe_member(member, probe)
        if gate.observe_environment_gate(environment).permit is None:
            return
        if clock.monotonic() >= limit:
            raise DrainError("drain=deadline_exceeded")
        clock.sleep(min(1.0, max(0.0, limit - clock.monotonic())))


def close_and_drain(
    gate: FenceGatePort,
    probe: ExecutionProbePort,
    membership: MembershipSource | Callable[[], Iterable[Mapping[str, Any]]],
    environment: str,
    promotion_id: str,
    now: datetime,
    expirations: Iterable[datetime] = (),
    clock: DrainClock | None = None,
    common: Mapping[str, Any] | None = None,
) -> Node:
    """Close a fence and emit a canonical close/drain observation.

    The membership producer must provide original proofs; absence of a producer or
    an unresolved dispatch fails closed and never reopens the fence.
    """
    if common is None:
        raise DrainError("common=original_binding_required")
    validated_common = _common(common, promotion_id)
    state = _close_fence(gate, environment, promotion_id, now)
    fixed_deadline = _fixed_deadline(now, expirations)
    members = _source_members(membership)
    active_clock = clock or _SystemDrainClock()
    _drain(gate, probe, members, environment, now, fixed_deadline, active_clock)
    post_state = gate.observe_environment_gate(environment)
    if (
        post_state.fence.state is not FenceState.CLOSED
        or post_state.fence.promotion_id != promotion_id
        or post_state.fence.closed_membership_sha256 != state.fence.closed_membership_sha256
    ):
        raise DrainError("fence=post_drain_mismatch")
    observed_at = _utc(now)
    return {
        "version": 1,
        "kind": "close-drain",
        "common": validated_common,
        "fence": {
            "environment": environment,
            "state": FenceState.CLOSED.value,
            "promotion_id": promotion_id,
            "version": state.fence.version,
            "changed_at": _utc(cast("datetime", state.fence.changed_at)),
        },
        "close_confirmed_at": _utc(now),
        "drain_observed_at": observed_at,
        "frozen_membership_sha256": _sha(members),
        "closed_membership_sha256": state.fence.closed_membership_sha256,
        "members": members,
        "proofs": {item["proof_sha256"]: item for item in members},
        "permit": None,
        "input_expirations": [],
        "fixed_deadline": _utc(fixed_deadline),
        "decision": None,
    }


def acquire_initial_or_recovery(
    gate: Any,
    holder: PermitHolder,
    now: datetime,
    canary_promotion_id: str | None = None,
    environment: str = "prod",
) -> bool:
    """Attempt a normal acquire; callers can only admit the bound canary."""
    outcome = gate.acquire_unit_permit(
        AcquireUnitPermit(
            environment=environment,
            holder=holder,
            now=now,
            lease_seconds=300,
            canary_promotion_id=canary_promotion_id,
        )
    )
    return outcome.status.name in {"ACQUIRED", "REENTERED"}


class _SystemDrainClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


close_fence_and_drain = close_and_drain
