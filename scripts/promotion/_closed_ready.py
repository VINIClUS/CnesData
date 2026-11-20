"""Handoff closed-ready original: reobserva FENCE/SEM e cada execução congelada após B."""

from __future__ import annotations

import base64
import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

Node = dict[str, Any]


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def object_hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("reason=datetime_not_utc")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _identity(member: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(
        member[key]
        for key in (
            "dispatch_id",
            "request_sha256",
            "generation",
            "initial_state",
            "execution_ref",
            "resolution",
        )
    )


def _post_members(
    gate: Any, probe: Any, close: Mapping[str, Any], observed_at: datetime
) -> tuple[list[Node], list[Node]]:
    from scripts.promotion.environment_gate import (
        PersistedMembershipSource,
        _probe_member,
        _source_members,
    )

    state = gate.observe_environment_gate("prod")
    fence = {
        "environment": "prod",
        "state": state.fence.state.value,
        "promotion_id": state.fence.promotion_id,
        "version": state.fence.version,
        "changed_at": utc_text(state.fence.changed_at),
    }
    if fence != close["fence"] or state.permit is not None:
        raise ValueError("closed_ready=strong_fence_or_permit")
    source = PersistedMembershipSource(gate, state)
    current = _source_members(source)
    if [_identity(item) for item in current] != [_identity(item) for item in close["members"]]:
        raise ValueError("closed_ready=membership_changed")
    proofs = []
    for member in current:
        observation = _probe_member(member, probe)
        if observation.evidence is None:
            raise ValueError("closed_ready=original_probe_required")
        original = json.loads(source.proof_bytes(member["dispatch_id"]))
        original["post_b_observation"] = json.loads(observation.evidence)
        original["observed_at"] = utc_text(observed_at)
        raw = _canonical(original)
        member["observed_at"] = utc_text(observed_at)
        member["proof_sha256"] = hashlib.sha256(raw).hexdigest()
        proofs.append(
            {"sha256": member["proof_sha256"], "bytes_base64": base64.b64encode(raw).decode()}
        )
    if gate.observe_environment_gate("prod") != state:
        raise ValueError("closed_ready=strong_readback_changed")
    return current, proofs


def build_closed_ready(
    close_drain: Mapping[str, Any],
    phase_b: Mapping[str, Any],
    gate: Any,
    probe: Any,
    observed_at: datetime,
    owner: Mapping[str, Any],
    generation: int,
    admitted_at: datetime,
    deadline: datetime,
) -> Node:
    """Reobserva FENCE/SEM e cada execução antes de emitir o handoff original."""
    if close_drain.get("kind") != "close-drain" or phase_b.get("kind") != "phase-b":
        raise ValueError("closed_ready=source_kind")
    if phase_b.get("close_drain_sha256") != object_hash(close_drain):
        raise ValueError("closed_ready=close_hash")
    common = close_drain["common"]
    if (
        common != phase_b["common"]
        or owner != common["caller"]
        or generation != common["promotion_generation"]
    ):
        raise ValueError("closed_ready=original_exclusion")
    fixed = datetime.fromisoformat(close_drain["fixed_deadline"].replace("Z", "+00:00"))
    after = datetime.fromisoformat(phase_b["observed_at"].replace("Z", "+00:00"))
    if (
        observed_at != admitted_at
        or observed_at < after
        or not admitted_at < deadline
        or deadline > min(fixed, admitted_at + timedelta(seconds=180))
        or (fixed - admitted_at).total_seconds() < 1440
    ):
        raise ValueError("closed_ready=stage_deadline")
    current, proofs = _post_members(gate, probe, close_drain, observed_at)
    return {
        "version": 1,
        "kind": "closed-ready",
        "common": dict(common),
        "close_drain_sha256": object_hash(close_drain),
        "phase_b_sha256": object_hash(phase_b),
        "fence": dict(close_drain["fence"]),
        "permit": None,
        "members": current,
        "proofs": proofs,
        "observed_at": utc_text(observed_at),
        "exclusion": {
            "owner": dict(owner),
            "generation": generation,
            "membership_sha256": object_hash(current),
            "acquired_at": close_drain["close_confirmed_at"],
            "observed_at": utc_text(observed_at),
        },
        "stage": {
            "name": "api",
            "admitted_at": utc_text(admitted_at),
            "deadline": utc_text(deadline),
            "later_seconds": 840,
        },
        "decision": None,
    }
