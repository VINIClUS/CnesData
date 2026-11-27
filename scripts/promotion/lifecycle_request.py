"""Descriptor do request de lifecycle e binding do caller original do application."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Final

from scripts.promotion._canonical import canonical_bytes

REPOSITORY: Final = "VINIClUS/CnesData"
SCHEMA_VERSION: Final = 1
REQUEST_LIMIT: Final = 65536
_CALLERS: Final = {
    "promotion": ("promote-production.yml", "promotion"),
    "mode-readiness-drill": ("readiness-production.yml", "readiness"),
}
_STEP: Final = re.compile(r"[a-z][a-z0-9-]{0,63}")
_HEX40: Final = re.compile(r"[0-9a-f]{40}")
_HEX64: Final = re.compile(r"[0-9a-f]{64}")
_REFERENCE_FIELDS: Final = frozenset({"step"})
_MAX_ID: Final = 2**53 - 1


@dataclass(frozen=True, slots=True)
class CallerClaim:
    """Observações originais do caller: propósito, fonte, execução e produtor do gate."""

    application_purpose: str
    source_sha: str
    run_id: int
    run_attempt: int
    producer_workflow_sha: str
    producer_source_closure_sha256: str


def _require(condition: bool, field: str) -> None:
    if not condition:
        raise ValueError(f"lifecycle_request field={field}")


def _positive(value: Any) -> bool:
    return type(value) is int and 1 <= value <= _MAX_ID


def _matches(pattern: re.Pattern[str], value: Any) -> bool:
    return type(value) is str and pattern.fullmatch(value) is not None


def _check_claim(claim: CallerClaim) -> None:
    _require(claim.application_purpose in _CALLERS, "application_purpose=closed_set")
    _require(_matches(_HEX40, claim.source_sha), "source_sha=hex40")
    _require(_positive(claim.run_id), "run_id=positive_integer")
    _require(_positive(claim.run_attempt), "run_attempt=positive_integer")
    _require(_matches(_HEX40, claim.producer_workflow_sha), "producer_workflow_sha=hex40")
    _require(_matches(_HEX64, claim.producer_source_closure_sha256),
             "producer_source_closure_sha256=hex64")


def caller_binding(claim: CallerClaim) -> dict[str, Any]:
    """Returns: os 11 campos do binding do caller original do application.

    Raises: ValueError: propósito fora do conjunto fechado ou observação malformada.
    """
    _check_claim(claim)
    workflow, context = _CALLERS[claim.application_purpose]
    return {
        "application_purpose": claim.application_purpose, "context_kind": context,
        "repository": REPOSITORY, "workflow_path": f".github/workflows/{workflow}",
        "ref": "refs/heads/main", "environment": "production", "source_sha": claim.source_sha,
        "run_id": claim.run_id, "run_attempt": claim.run_attempt,
        "producer_workflow_sha": claim.producer_workflow_sha,
        "producer_source_closure_sha256": claim.producer_source_closure_sha256,
    }


def caller_binding_sha256(claim: CallerClaim) -> str:
    """Returns: sha256 dos bytes canônicos (com LF) do binding do caller."""
    return hashlib.sha256(canonical_bytes(caller_binding(claim))).hexdigest()


def _check_reference(reference: Any) -> None:
    _require(type(reference) is dict and set(reference) == _REFERENCE_FIELDS, "payloads=reference")
    _require(_matches(_STEP, reference["step"]), "payloads=reference")


def _check_payloads(payloads: dict[str, Any], input_jobs: frozenset[str]) -> None:
    _require(set(payloads) == input_jobs, "payloads=complete_inventory")
    for name, reference in payloads.items():
        _require(_matches(_STEP, name), "payloads=name")
        _check_reference(reference)


def request_descriptor(
    claim: CallerClaim, step: str, payloads: dict[str, dict[str, Any]], input_jobs: frozenset[str]
) -> bytes:
    """Returns: bytes canônicos do descriptor de 7 campos; o sha256 deles vai ao reusable.

    Raises: ValueError: step, payloads (≠ input_jobs do catálogo) ou claim fora da forma fechada.
    """
    _require(_matches(_STEP, step), "step=name")
    _check_payloads(payloads, input_jobs)
    descriptor = {
        "schema_version": SCHEMA_VERSION, "repository": REPOSITORY,
        "source_sha": claim.source_sha, "step": step, "payloads": payloads,
        "application_purpose": claim.application_purpose,
        "caller_binding_sha256": caller_binding_sha256(claim),
    }
    return canonical_bytes(descriptor, REQUEST_LIMIT)


def descriptor_sha256(descriptor: bytes) -> str:
    """Returns: sha256 dos bytes do descriptor, o `request_descriptor_sha256` do reusable."""
    return hashlib.sha256(descriptor).hexdigest()
