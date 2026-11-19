"""Codecs fechados do canary de modo: sessões, context manifest e registro."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final, cast

from cnes_domain.orchestration.mode_canary import ModeCanaryRecord
from scripts.promotion._canonical import canonical_bytes, parse_document

REPOSITORY: Final = "VINIClUS/CnesData"
ALIASES: Final = ("mode_seed", "mode_dispatch", "mode_audit_launch", "mode_observe")
SESSION_OPERATIONS: Final[dict[str, tuple[str, str, str, str]]] = {
    "unit-seed": ("us", "mode_seed", "unit", "promotion"),
    "unit-start": ("ux", "mode_dispatch", "unit", "promotion"),
    "unit-observe": ("uo", "mode_observe", "unit", "promotion"),
    "recovery-seed": ("rs", "mode_seed", "recovery", "promotion"),
    "recovery-start": ("rx", "mode_dispatch", "recovery", "promotion"),
    "recovery-observe": ("ro", "mode_observe", "recovery", "promotion"),
    "audit-start": ("ax", "mode_audit_launch", "audit", "promotion"),
    "audit-observe": ("ao", "mode_observe", "audit", "promotion"),
    "drill-seed": ("ds", "mode_seed", "readiness", "readiness"),
    "drill-unit-start": ("dx", "mode_dispatch", "readiness", "readiness"),
    "drill-recovery-start": ("dr", "mode_dispatch", "readiness", "readiness"),
    "drill-hang-first": ("h1", "mode_dispatch", "readiness", "readiness"),
    "drill-hang-second": ("h2", "mode_dispatch", "readiness", "readiness"),
    "drill-observe": ("do", "mode_observe", "readiness", "readiness"),
}
READINESS_MAX_SECONDS: Final = 14400
_HEX40: Final = re.compile(r"[0-9a-f]{40}")
_HEX64: Final = re.compile(r"[0-9a-f]{64}")
_ASCII_TOKEN: Final = re.compile(r"[!-~]+")
_TIMESTAMP: Final = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_OWNER_FIELDS: Final = ("repository", "run_id", "run_attempt", "generation", "owner_sha256")
_SOURCE_FIELDS: Final = (
    "product_source_sha", "product_source_closure_sha256", "shared_source_sha",
    "shared_source_closure_sha256", "release_id", "activation_sha256",
)
_MANIFEST_FIELDS: Final = frozenset({
    "schema_version", "context_kind", "owner", *_SOURCE_FIELDS, "resources_sha256",
    "fixture_manifest_sha256", "scenario_plan_sha256", "predecessor_terminal_sha256",
    "admitted_at", "deadline",
})
_BINDING_FIELDS: Final = frozenset({
    "schema_version", "context_kind", "operation", "phase", *_SOURCE_FIELDS,
    *_OWNER_FIELDS, "context_manifest_sha256",
})


class ContractError(ValueError):
    """Documento fora do contrato fechado do canary de modo."""


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ContractError(f"reason=invalid field={reason}")


def _positive(value: Any) -> bool:
    return type(value) is int and value > 0


def _matches(pattern: re.Pattern[str], value: Any) -> bool:
    return type(value) is str and pattern.fullmatch(value) is not None


def _check_owner(owner: Any) -> None:
    _require(type(owner) is dict and set(cast("dict[str, Any]", owner)) == set(_OWNER_FIELDS),
             "owner")
    fields = cast("dict[str, Any]", owner)
    _require(fields["repository"] == REPOSITORY, "repository")
    for name in ("run_id", "run_attempt", "generation"):
        _require(_positive(fields[name]), name)
    _require(_matches(_HEX64, fields["owner_sha256"]), "owner_sha256")


def _check_sources(fields: dict[str, Any]) -> None:
    for name in ("product_source_sha", "shared_source_sha"):
        _require(_matches(_HEX40, fields[name]), name)
    for name in ("product_source_closure_sha256", "shared_source_closure_sha256",
                 "activation_sha256"):
        _require(_matches(_HEX64, fields[name]), name)
    _require(_matches(_ASCII_TOKEN, fields["release_id"]), "release_id")


def session_binding_bytes(binding: dict[str, Any]) -> bytes:
    """Returns: B canônico ASCII com LF da sessão `mc-`.

    Raises: ContractError: Campo fora dos 16 fechados, operação ou valor inválido.
    """
    _require(type(binding) is dict and set(binding) == _BINDING_FIELDS, "binding")
    entry = SESSION_OPERATIONS.get(binding["operation"])
    _require(entry is not None, "operation")
    _, _, phase, kind = cast("tuple[str, str, str, str]", entry)
    _require(binding["phase"] == phase and binding["context_kind"] == kind, "phase")
    _require(binding["schema_version"] == 2 and _positive(binding["schema_version"]),
             "schema_version")
    _check_owner({name: binding[name] for name in _OWNER_FIELDS})
    _check_sources(binding)
    _require(_matches(_HEX64, binding["context_manifest_sha256"]), "context_manifest_sha256")
    text = json.dumps(binding, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"{text}\n".encode("ascii")


def session_name(binding: dict[str, Any]) -> str:
    """Returns: `mc-<code>-<40 hex>` da operação; Raises: ContractError."""
    digest = hashlib.sha256(session_binding_bytes(binding)).hexdigest()
    return f"mc-{SESSION_OPERATIONS[binding['operation']][0]}-{digest[:40]}"


@dataclass(frozen=True, slots=True)
class ContextManifest:
    """Manifest de admissão validado: texto canônico, digest interno e 15 campos."""

    canonical_utf8: str
    sha256: str
    fields: dict[str, Any]


def _timestamp(value: Any, field: str) -> datetime:
    _require(_matches(_TIMESTAMP, value), field)
    try:
        return datetime.strptime(cast("str", value), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError as error:
        raise ContractError(f"reason=invalid field={field}") from error


def _check_manifest(fields: dict[str, Any]) -> None:
    _require(set(fields) == _MANIFEST_FIELDS, "manifest")
    _require(fields["schema_version"] == 2 and _positive(fields["schema_version"]),
             "schema_version")
    kind = fields["context_kind"]
    _require(kind in ("promotion", "readiness"), "context_kind")
    _check_owner(fields["owner"])
    _check_sources(fields)
    for name in ("resources_sha256", "fixture_manifest_sha256", "scenario_plan_sha256"):
        _require(_matches(_HEX64, fields[name]), name)
    predecessor = fields["predecessor_terminal_sha256"]
    if kind == "promotion":
        _require(predecessor is None, "predecessor_terminal_sha256")
    else:
        _require(_matches(_HEX64, predecessor), "predecessor_terminal_sha256")
    admitted = _timestamp(fields["admitted_at"], "admitted_at")
    deadline = _timestamp(fields["deadline"], "deadline")
    _require(admitted < deadline, "deadline")
    ceiling = admitted + timedelta(seconds=READINESS_MAX_SECONDS)
    _require(kind == "promotion" or deadline <= ceiling, "deadline")


def parse_context_manifest(wrapper: Any) -> ContextManifest:
    """Returns: Manifest de admissão do wrapper `{canonical_utf8,sha256}`.

    Raises: ContractError: Wrapper alternativo, JSON não canônico, digest ou semântica inválidos.
    """
    _require(type(wrapper) is dict and set(cast("dict[str, Any]", wrapper))
             == {"canonical_utf8", "sha256"}, "context_manifest")
    text, digest = cast("dict[str, Any]", wrapper)["canonical_utf8"], wrapper["sha256"]
    _require(type(text) is str and _matches(_HEX64, digest), "context_manifest")
    raw = cast("str", text).encode("utf-8")
    _require(hashlib.sha256(raw).hexdigest() == digest, "sha256")
    try:
        fields = parse_document(raw)
    except ValueError as error:
        raise ContractError("reason=invalid field=canonical_utf8") from error
    _check_manifest(fields)
    return ContextManifest(cast("str", text), cast("str", digest), fields)


def session_names(manifest: ContextManifest) -> dict[str, list[str]]:
    """Returns: Sessões ordenadas por alias para o contexto do manifest."""
    fields = manifest.fields
    base = {name: fields[name] for name in _SOURCE_FIELDS} | fields["owner"]
    base |= {"schema_version": 2, "context_kind": fields["context_kind"],
             "context_manifest_sha256": manifest.sha256}
    names: dict[str, list[str]] = {alias: [] for alias in ALIASES}
    for operation, (_, alias, phase, kind) in SESSION_OPERATIONS.items():
        if kind == fields["context_kind"]:
            names[alias].append(session_name(base | {"operation": operation, "phase": phase}))
    return {alias: sorted(values) for alias, values in names.items()}


def record_bytes(record: dict[str, Any]) -> bytes:
    """Returns: Payload canônico (UTF-8, LF) do registro `cnesdata.mode-canary/v2`.

    Raises: ContractError: Registro fora dos 26 campos ou da união fechada.
    """
    try:
        ModeCanaryRecord.from_mapping(record)
    except ValueError as error:
        raise ContractError(f"reason=invalid field=record detail={error}") from error
    return canonical_bytes(record)


__all__ = [
    "ALIASES",
    "SESSION_OPERATIONS",
    "ContextManifest",
    "ContractError",
    "parse_context_manifest",
    "record_bytes",
    "session_binding_bytes",
    "session_name",
    "session_names",
]
