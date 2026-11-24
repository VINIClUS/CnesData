"""Construtor puro do dispatch nativo do unit: wave, dispatch_id, item RUNDISPATCH e start."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Final

from cnes_infra.aws.mode_canary_frames import refuse
from cnes_infra.control_plane.dynamodb_keys import dispatch_key, key_component

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from cnes_domain.orchestration.mode_canary import ModeCanaryBinding

DISPATCH_ENTITY: Final = "RUNDISPATCH"
# Why: the native unit dispatch (gate acquire, claim, bind, renew) touches exactly these
# environment items; mode_dispatch IAM already grants these operations, nothing else.
UNIT_DISPATCH_GATE: Final = (
    ("ENV#prod#FENCE", "FENCE", ("GetItem", "ConditionCheckItem")),
    ("ENV#prod#SEMAPHORE", "UNIT", ("GetItem", "UpdateItem")),
    ("ENV#prod#ATTEMPTS", "MONTH#<YYYY-MM>", ("UpdateItem",)),
)
FIRST_GENERATION: Final = 1
_CANDIDATES: Final = frozenset({"PENDING", "FAILED_RETRYABLE", "LEASED"})
_SATISFIED: Final = frozenset({"SUCCEEDED", "SUCCEEDED_DEGRADED"})
_SHAPES: Final = {"RESERVED": (False, False), "STARTING": (True, False), "STARTED": (True, True)}
_SEPARATOR: Final = "\x1f"


@dataclass(frozen=True, slots=True)
class UnitDispatch:
    """Dispatch da primeira geração do run do unit, como a reserva nativa o grava."""

    tenant_id: str
    run_id: str
    wave_id: str
    dispatch_id: str
    unit_ids: tuple[str, ...]
    max_concurrency: int


@dataclass(frozen=True, slots=True)
class DispatchStep:
    """Estado do item RUNDISPATCH: RESERVED, STARTING com claim ou STARTED com execution_ref."""

    state: str
    lease_until: datetime
    start_claim: Mapping[str, Any] | None = None
    execution_ref: str | None = None


def unit_dispatch(locator: ModeCanaryBinding, units: Sequence[Mapping[str, Any]],
                  max_concurrency: int, now: datetime) -> UnitDispatch:
    """Returns: Dispatch das unidades prontas do run do unit, com wave e dispatch_id da reserva.

    Raises: SyntheticContextRequired: dispatch_locator, dispatch_units, dispatch_concurrency
    ou clock_not_utc.
    """
    if locator.phase != "unit" or len(locator.run_ids) != 1:
        raise refuse("dispatch_locator")
    tenant, run = locator.tenant_id, locator.run_ids[0]
    if any((unit["tenant_id"], unit["run_id"]) != (tenant, run) for unit in units):
        raise refuse("dispatch_units")
    unit_ids = _ready(units, _utc(now))
    if not unit_ids:
        raise refuse("dispatch_units")
    if type(max_concurrency) is not int or not 1 <= max_concurrency <= len(unit_ids):
        raise refuse("dispatch_concurrency")
    wave = sha256(_SEPARATOR.join(unit_ids).encode()).hexdigest()[:16]
    raw = _SEPARATOR.join((tenant, run, wave, str(FIRST_GENERATION)))
    return UnitDispatch(tenant, run, wave, sha256(raw.encode()).hexdigest()[:16], unit_ids,
                        max_concurrency)


def start_execution(dispatch: UnitDispatch) -> dict[str, str]:
    """Returns: name e input do StartExecution do executor nativo para o dispatch."""
    return {"name": dispatch.dispatch_id, "input": _request_json(dispatch)}


def start_request_sha256(dispatch: UnitDispatch) -> str:
    """Returns: request_sha256 do StartRunExecution ligado ao claim do start."""
    return sha256(_request_json(dispatch).encode()).hexdigest()


def start_claim(dispatch: UnitDispatch, permit_token: int, fence_version: int,
                promotion_id: str | None) -> dict[str, Any]:
    """Returns: DispatchStartClaim do dispatch na ordem dos campos do modelo."""
    return {"dispatch_id": dispatch.dispatch_id, "generation": FIRST_GENERATION,
            "permit_token": permit_token, "fence_version": fence_version,
            "request_sha256": start_request_sha256(dispatch), "promotion_id": promotion_id}


def run_dispatch_item(dispatch: UnitDispatch, step: DispatchStep) -> dict[str, Any]:
    """Returns: Item RUNDISPATCH do adapter no estado do passo.

    Raises: SyntheticContextRequired: dispatch_state ou clock_not_utc.
    """
    shape = (step.start_claim is not None, step.execution_ref is not None)
    if _SHAPES.get(step.state) != shape:
        raise refuse("dispatch_state")
    lease = _utc(step.lease_until).isoformat().replace("+00:00", "Z")
    payload = {
        "tenant_id": dispatch.tenant_id, "run_id": dispatch.run_id,
        "wave_id": dispatch.wave_id, "dispatch_id": dispatch.dispatch_id,
        "generation": FIRST_GENERATION, "unit_ids": list(dispatch.unit_ids),
        "state": step.state, "lease_until": lease, "execution_ref": step.execution_ref,
        "terminal_outcome": None,
        "start_claim": None if step.start_claim is None else dict(step.start_claim),
        "canceled_before_send": False,
    }
    pk, sk = dispatch_key(dispatch.tenant_id, dispatch.run_id)
    index = f"RUN_ITEMS#{key_component(dispatch.tenant_id)}#{key_component(dispatch.run_id)}"
    text = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    return {"pk": {"S": pk}, "sk": {"S": sk}, "entity": {"S": DISPATCH_ENTITY},
            "payload": {"S": text}, "gsi5pk": {"S": index}, "gsi5sk": {"S": "DISPATCH#ACTIVE"}}


def _utc(moment: datetime) -> datetime:
    if moment.utcoffset() != timedelta(0):
        raise refuse("clock_not_utc")
    return moment


def _live(unit: Mapping[str, Any], now: datetime) -> bool:
    lease = unit["lease_until"]
    return unit["state"] == "LEASED" and lease is not None and datetime.fromisoformat(lease) > now


def _ready(units: Sequence[Mapping[str, Any]], now: datetime) -> tuple[str, ...]:
    if any(_live(unit, now) for unit in units):
        return ()
    states = {unit["unit_id"]: unit["state"] for unit in units}
    return tuple(sorted(
        unit["unit_id"] for unit in units if unit["state"] in _CANDIDATES
        and all(states.get(dependency) in _SATISFIED for dependency in unit["depends_on_unit_ids"])
    ))


def _request_json(dispatch: UnitDispatch) -> str:
    return json.dumps({
        "tenant_id": dispatch.tenant_id, "run_id": dispatch.run_id, "wave_id": dispatch.wave_id,
        "dispatch_id": dispatch.dispatch_id, "unit_ids": list(dispatch.unit_ids),
        "max_concurrency": dispatch.max_concurrency,
    }, separators=(",", ":"), sort_keys=True)


__all__ = [
    "DISPATCH_ENTITY",
    "FIRST_GENERATION",
    "UNIT_DISPATCH_GATE",
    "DispatchStep",
    "UnitDispatch",
    "run_dispatch_item",
    "start_claim",
    "start_execution",
    "start_request_sha256",
    "unit_dispatch",
]
