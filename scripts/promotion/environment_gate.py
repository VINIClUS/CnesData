"""Close the production fence and consume only real drain observations."""

from __future__ import annotations

import base64
import hashlib
import json
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    ClosePromotionFence,
    EnvironmentGateState,
    FenceState,
    PermitHolder,
)
from cnes_domain.ports.environment_gate import (
    ExecutionObservation,
    ExecutionPresence,
    ExecutionProbePort,
)

Node = dict[str, Any]
Clock = Callable[[], datetime]


class MembershipSource(Protocol):
    def freeze_membership(self) -> Iterable[Mapping[str, Any]]: ...

    def proof_bytes(self, dispatch_id: str) -> bytes: ...


class PersistedMembershipSource:
    """Consome o snapshot original e a liquidação persistida pelo recovery nativo."""

    def __init__(self, gate: FenceGatePort, state: Any) -> None:
        self._gate = gate
        self._state = state
        self._snapshot = tuple(dict(item) for item in state.closed_membership)
        if not state.fence.closed_membership_sha256:
            raise DrainError("membership=fence_snapshot_missing")
        if _sha(self._snapshot) != state.fence.closed_membership_sha256:
            raise DrainError("membership=fence_snapshot_mismatch")
        if len(self._snapshot) > 1:
            raise DrainError("membership=capacity")
        self._proofs: dict[str, bytes] = {}

    def freeze_membership(self) -> Iterable[Mapping[str, Any]]:
        state = self._gate.observe_environment_gate(self._state.environment)
        if state.fence != self._state.fence:
            raise DrainError("membership=fence_changed")
        if not self._snapshot:
            return ()
        if state.permit is not None or state.settlement is None:
            raise DrainError("membership=pending")
        item = self._snapshot[0]
        proof = state.settlement
        receipt = json.loads(proof)
        for key in ("tenant_id", "run_id", "dispatch_id", "token", "generation", "request_sha256"):
            if receipt.get(key) != item.get(key) or item.get(key) is None:
                raise DrainError("membership=settlement_binding")
        resolution = receipt.get("resolution")
        if resolution not in {"TERMINAL", "CANCEL_BEFORE_SEND"}:
            raise DrainError("membership=settlement_resolution")
        if resolution == "TERMINAL" and not receipt.get("observation"):
            raise DrainError("membership=original_observation_required")
        if resolution == "CANCEL_BEFORE_SEND" and not receipt.get("cancellation"):
            raise DrainError("membership=original_cancellation_required")
        self._validate_receipt(receipt)
        self._proofs[item["dispatch_id"]] = proof
        return (
            {
                "dispatch_id": item["dispatch_id"],
                "generation": item["generation"],
                "request_sha256": item["request_sha256"],
                "initial_state": {
                    "RESERVED": "NOT_SENT",
                    "STARTING": "OUTCOME_UNKNOWN",
                    "STARTED": "ACKNOWLEDGED",
                }[item["initial_state"]],
                **{
                    key: receipt[key]
                    for key in ("execution_ref", "resolution", "observed_at", "send_disabled_at")
                },
                "proof_sha256": hashlib.sha256(proof).hexdigest(),
            },
        )

    @staticmethod
    def _validate_receipt(receipt: Node) -> None:
        from cnes_domain.control_plane.dispatch_start import request_sha256

        if receipt["resolution"] == "TERMINAL":
            native = receipt["observation"]
            if (
                native.get("executionArn") != receipt["execution_ref"]
                or native.get("status") not in {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}
                or request_sha256(json.loads(native["input"])) != receipt["request_sha256"]
            ):
                raise DrainError("membership=terminal_proof_content")
        else:
            canceled = receipt["cancellation"]
            claim = canceled.get("start_claim") or {}
            if (
                canceled.get("state") != "TERMINAL"
                or canceled.get("terminal_outcome") != "CANCELED"
                or canceled.get("dispatch_id") != receipt["dispatch_id"]
                or claim.get("permit_token") != receipt["token"]
                or claim.get("generation") != receipt["generation"]
                or claim.get("request_sha256") != receipt["request_sha256"]
            ):
                raise DrainError("membership=cancel_proof_content")

    def proof_bytes(self, dispatch_id: str) -> bytes:
        return self._proofs[dispatch_id]


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
    if value["resolution"] not in {
        "PENDING",
        "TERMINAL",
        "ABSENT_PROVEN",
        "CANCEL_BEFORE_SEND",
    }:
        raise DrainError("membership=unresolved")
    if value["resolution"] == "TERMINAL" and not value["execution_ref"]:
        raise DrainError("membership=terminal_ref")
    if value["resolution"] not in {"PENDING", "TERMINAL"} and value["execution_ref"] is not None:
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
    caller = value.get("caller")
    required_caller = {
        "repository",
        "source_sha",
        "ref",
        "workflow_path",
        "workflow_sha",
        "run_id",
        "run_attempt",
    }
    if not isinstance(caller, dict) or set(caller) != required_caller:
        raise DrainError("common=caller")
    if caller["repository"] != value["repository"] or caller["source_sha"] != value["source_sha"]:
        raise DrainError("common=caller_binding")
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


def _probe_member(member: Node, probe: ExecutionProbePort) -> ExecutionObservation:
    observation = probe.probe(member["dispatch_id"])
    if member["resolution"] == "TERMINAL":
        if observation.presence is not ExecutionPresence.TERMINAL:
            raise DrainError("drain=terminal_not_observed")
        if observation.execution_ref != member["execution_ref"]:
            raise DrainError("drain=execution_ref_mismatch")
        if observation.request_sha256 not in {None, member["request_sha256"]}:
            raise DrainError("drain=request_mismatch")
    elif member["resolution"] == "CANCEL_BEFORE_SEND":
        if observation.presence is not ExecutionPresence.ABSENT:
            raise DrainError("drain=absence_not_proven")
    else:
        raise DrainError("drain=unresolved")
    return observation


def _fixed_deadline(closed_at: datetime, expirations: Iterable[Mapping[str, Any]]) -> datetime:
    values = [closed_at + timedelta(seconds=1800)]
    identities = []
    for item in expirations:
        if not isinstance(item, Mapping) or set(item) != {"kind", "object_sha256", "expires_at"}:
            raise DrainError("expiry=original_descriptor_required")
        if item["kind"] not in {
            "envelope",
            "artifact",
            "platform",
            "cost",
            "credential",
            "activation",
            "plan",
        }:
            raise DrainError("expiry=kind")
        if len(item["object_sha256"]) != 64:
            raise DrainError("expiry=hash")
        values.append(datetime.fromisoformat(item["expires_at"].replace("Z", "+00:00")))
        identities.append((item["kind"], item["object_sha256"]))
    if not identities or identities != sorted(set(identities)):
        raise DrainError("expiry=complete_unique_originals")
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
    source: MembershipSource,
    environment: str,
    limit: float,
    clock: DrainClock,
) -> list[Node]:
    while True:
        if clock.monotonic() >= limit:
            raise DrainError("drain=deadline_exceeded")
        try:
            members = _source_members(source)
        except DrainError as error:
            if str(error) != "membership=pending":
                raise
        else:
            for member in members:
                _probe_member(member, probe)
            if gate.observe_environment_gate(environment).permit is None:
                if clock.monotonic() >= limit:
                    raise DrainError("drain=deadline_exceeded")
                return members
        clock.sleep(min(1.0, max(0.0, limit - clock.monotonic())))


def _original_expirations(
    state: EnvironmentGateState, expirations: Iterable[Mapping[str, Any]]
) -> tuple[Mapping[str, Any], ...]:
    receipt = state.fence.close_receipt
    if not receipt or receipt["closed_membership_sha256"] != state.fence.closed_membership_sha256:
        raise DrainError("fence=original_close_receipt_required")
    expiry = {
        "kind": "envelope",
        "object_sha256": _sha(receipt),
        "expires_at": receipt["expires_at"],
    }
    return tuple(
        sorted([*expirations, expiry], key=lambda item: (item["kind"], item["object_sha256"]))
    )


def close_and_drain(
    gate: FenceGatePort,
    probe: ExecutionProbePort,
    membership: MembershipSource | Callable[[], Iterable[Mapping[str, Any]]] | None,
    environment: str,
    promotion_id: str,
    now: datetime,
    expirations: Iterable[Mapping[str, Any]] = (),
    clock: DrainClock | None = None,
    common: Mapping[str, Any] | None = None,
) -> Node:
    """Fecha e drena usando membership persistida e provas originais do recovery."""
    if common is None:
        raise DrainError("common=original_binding_required")
    validated_common = _common(common, promotion_id)
    active_clock = clock or _SystemDrainClock()
    started = active_clock.monotonic()
    expirations = tuple(expirations)
    if _fixed_deadline(now, expirations) <= now:
        raise DrainError("drain=deadline_exceeded")
    state = _close_fence(gate, environment, promotion_id, now)
    observed_close = now + timedelta(seconds=active_clock.monotonic() - started)
    confirm = getattr(gate, "confirm_promotion_close", None)
    if not callable(confirm):
        raise DrainError("fence=native_confirmation_required")
    state = cast("EnvironmentGateState", confirm(environment, promotion_id, observed_close))
    closed_at = cast("datetime", state.fence.confirmed_at)
    expirations = _original_expirations(state, expirations)
    fixed_deadline = _fixed_deadline(closed_at, expirations)
    limit = started + (fixed_deadline - now).total_seconds()
    if membership is not None and not isinstance(membership, PersistedMembershipSource):
        raise DrainError("membership=source_capability")
    source = PersistedMembershipSource(gate, state)
    if callable(source):
        raise DrainError("membership=source_capability")
    members = _drain(gate, probe, source, environment, limit, active_clock)
    if any(item["generation"] >= validated_common["promotion_generation"] for item in members):
        raise DrainError("membership=promotion_generation")
    post_state = gate.observe_environment_gate(environment)
    if post_state.fence != state.fence or post_state.permit is not None:
        raise DrainError("fence=post_drain_mismatch")
    observed_at = now + timedelta(seconds=active_clock.monotonic() - started)
    if observed_at >= fixed_deadline:
        raise DrainError("drain=deadline_exceeded")
    return _drain_document(
        state,
        validated_common,
        members,
        source,
        closed_at,
        observed_at,
        expirations,
        fixed_deadline,
    )


def _drain_document(
    state: Any,
    common: Node,
    members: list[Node],
    source: MembershipSource,
    closed_at: datetime,
    observed_at: datetime,
    expirations: Iterable[Mapping[str, Any]],
    deadline: datetime,
) -> Node:
    return {
        "version": 1,
        "kind": "close-drain",
        "common": common,
        "fence": {
            "environment": state.environment,
            "state": "CLOSED",
            "promotion_id": state.fence.promotion_id,
            "version": state.fence.version,
            "changed_at": _utc(state.fence.changed_at),
        },
        "close_confirmed_at": _utc(closed_at),
        "drain_observed_at": _utc(observed_at),
        "frozen_membership_sha256": _sha(members),
        "members": members,
        "proofs": [
            {
                "sha256": item["proof_sha256"],
                "bytes_base64": base64.b64encode(source.proof_bytes(item["dispatch_id"])).decode(),
            }
            for item in members
        ],
        "permit": None,
        "input_expirations": [dict(value) for value in expirations],
        "fixed_deadline": _utc(deadline),
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
