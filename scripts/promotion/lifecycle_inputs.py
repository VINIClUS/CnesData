"""Gerador do inputs.json por step: static_promotion ligado à inventory e mode_canary."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Final

from scripts.promotion._api_static_records import read_plan
from scripts.promotion._canonical import canonical_bytes
from scripts.promotion.mode_canary_contract import ContextManifest, session_names
from scripts.promotion.prepare_mode_canary import check_mode_canary_input

INPUTS_LIMIT: Final = 131072
OPERATIONAL: Final = "operational"
_RELEASE_ID: Final = re.compile(r"[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?")
_HEX40: Final = re.compile(r"[0-9a-f]{40}")
_TIMESTAMP: Final = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_PLAN_FIELDS: Final = ("candidate_keys", "rollback_versions", "retire_versions", "multipart_keys")
_RESERVED: Final = ("static_promotion", "mode_canary")
_MAX_ID: Final = 2**53 - 1


@dataclass(frozen=True, slots=True)
class StaticBinding:
    """Binding #498 por run das sessões estáticas, sem o sha do plano."""

    release_id: str
    run_id: int
    run_attempt: int
    generation: int
    product_source_sha: str
    shared_source_sha: str


@dataclass(frozen=True, slots=True)
class StaticPromotion:
    """Pedido do input static_promotion: modo, fragmento do application e janela."""

    mode: str
    application_promotion: dict[str, Any]
    binding: StaticBinding
    enabled_until: str


def _require(condition: bool, field: str) -> None:
    if not condition:
        raise ValueError(f"lifecycle_inputs field={field}")


def _positive(value: Any) -> bool:
    return type(value) is int and 1 <= value <= _MAX_ID


def _check_binding(binding: StaticBinding) -> None:
    _require(type(binding.release_id) is str and bool(_RELEASE_ID.fullmatch(binding.release_id)),
             "release_id=closed_format")
    for name in ("run_id", "run_attempt", "generation"):
        _require(_positive(getattr(binding, name)), f"{name}=positive_integer")
    for name in ("product_source_sha", "shared_source_sha"):
        value = getattr(binding, name)
        _require(type(value) is str and bool(_HEX40.fullmatch(value)), f"{name}=hex40")


def _check_request(request: StaticPromotion) -> None:
    _require(request.mode == OPERATIONAL, "mode=operational_only")
    _require(type(request.application_promotion) is dict, "application_promotion=object")
    _require(type(request.enabled_until) is str
             and bool(_TIMESTAMP.fullmatch(request.enabled_until)), "enabled_until=utc_instant")
    _check_binding(request.binding)


def static_promotion_input(request: StaticPromotion, plan: bytes, inventory: bytes) -> dict[str, Any]:
    """Returns: o input `static_promotion` de nove campos, ligado ao plano e à inventory.

    Raises: ValueError: pedido fora do contrato; PublicationError: plano de outra inventory.
    """
    _check_request(request)
    record = read_plan(plan, inventory)
    binding = {
        "release_id": request.binding.release_id, "run_id": request.binding.run_id,
        "run_attempt": request.binding.run_attempt, "generation": request.binding.generation,
        "product_source_sha": request.binding.product_source_sha,
        "shared_source_sha": request.binding.shared_source_sha,
        "static_plan_sha256": hashlib.sha256(plan).hexdigest(),
    }
    return {
        "mode": request.mode, "application_promotion": request.application_promotion,
        "binding": binding, "inventory_sha256": record["inventory_sha256"],
        "enabled_until": request.enabled_until, **{name: record[name] for name in _PLAN_FIELDS},
    }


def mode_canary_input(
    manifest: ContextManifest, resources: dict[str, Any], principal: str, enabled_until: str
) -> dict[str, Any]:
    """Returns: o input `mode_canary` de oito campos, validado pelo oráculo do produto.

    Raises: ContractError: recursos, principal ou sessões divergentes do manifest.
    """
    value = {
        "schema_version": 2, "context_kind": manifest.fields["context_kind"],
        "application_principal_arn": principal, "owner": manifest.fields["owner"],
        "context_manifest": {"canonical_utf8": manifest.canonical_utf8, "sha256": manifest.sha256},
        "delegation_sessions": session_names(manifest), "resources": resources,
        "enabled_until": enabled_until,
    }
    check_mode_canary_input(value)
    return value


def inputs_bytes(
    base: dict[str, Any], static_promotion: dict[str, Any] | None = None,
    mode_canary: dict[str, Any] | None = None,
) -> bytes:
    """Returns: bytes UTF-8/LF do var-file, com static_promotion e mode_canary explícitos.

    Raises: ValueError: chave reservada já presente na base ou arquivo acima do limite.
    """
    _require(not any(name in base for name in _RESERVED), "inputs=reserved_key")
    document = base | {"static_promotion": static_promotion, "mode_canary": mode_canary}
    raw = canonical_bytes(document, 4 * INPUTS_LIMIT)
    _require(len(raw) <= INPUTS_LIMIT, "inputs=size")
    return raw
