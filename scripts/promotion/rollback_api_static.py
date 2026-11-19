"""Rollback API/estático: handoff público do host, decisão de recovery e entrypoints."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from scripts.promotion.publish_dashboard import (
    ENTRYPOINTS,
    INVALIDATION_PATHS,
    Inventory,
    PublicationError,
    PublicationReceipt,
    StaticChannel,
    UploadWindow,
    call_within,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from cnes_domain.orchestration.static_release import VersionRef

HANDOFF_FIELDS = frozenset(
    ["version", "kind", "caller", "producer_sha", "closed_ready_sha256", "original_request_sha256", "status", "response_sha256", "proof_sha256", "receipt_sha256", "witness_sha256", "operation_id", "binding_sha256", "generation", "recovery_decision", "recovery_proof_sha256"]
)
DECISION_FIELDS = frozenset(
    ["version", "operation_id", "binding_sha256", "generation", "choice_at", "choice_monotonic_ms", "clock_boot_id", "fixed_deadline", "recovery_deadline", "allocated_seconds", "convergence_budget_seconds", "finalization_budget_seconds", "uncertainty_budget_seconds", "checkpoint_sha256"]
)
RECOVERY_SECONDS = 420
CONVERGENCE_SECONDS = 300
_DIGEST = re.compile(r"[0-9a-f]{64}")
_INSTANT = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ")


class HostStep(StrEnum):
    PUBLISH_STATIC = "publish-static"
    FENCED_FAILURE = "fenced-failure"
    RECOVERY = "rollback-api-static"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class HostOutcome:
    status: str
    decision: dict[str, Any] | None


@dataclass(frozen=True)
class HostPlan:
    step: HostStep
    fence: str
    decision: dict[str, Any] | None
    host_action: str | None = None


@dataclass(frozen=True)
class RollbackReceipt:
    restored: tuple[tuple[str, str], ...]
    residual: bool
    invalidation_id: str | None


def host_outcome(
    raw: bytes | None, *, caller: Mapping[str, Any], closed_ready_sha256: str
) -> HostOutcome:
    """Returns:
    Status do handoff público autenticado; divergência ou ausência vira unknown.
    """
    if raw is None:
        return HostOutcome("unknown", None)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return HostOutcome("unknown", None)
    if not _valid_handoff(value, caller, closed_ready_sha256):
        return HostOutcome("unknown", None)
    return HostOutcome(value["status"], value["recovery_decision"])


def after_host(outcome: HostOutcome) -> HostPlan:
    """Returns:
    Próximo passo do produto; o fence fica CLOSED em todos os ramos desta tarefa.
    """
    if outcome.status == "accepted":
        return HostPlan(HostStep.PUBLISH_STATIC, "CLOSED", None)
    if outcome.status == "rolled_back":
        return HostPlan(HostStep.FENCED_FAILURE, "CLOSED", None)
    if outcome.status == "recovery_required" and outcome.decision is not None:
        return HostPlan(HostStep.RECOVERY, "CLOSED", outcome.decision, "rollback-previous")
    return HostPlan(HostStep.UNKNOWN, "CLOSED", None)


def recovery_window(decision: Mapping[str, Any], clock: Callable[[], datetime]) -> UploadWindow:
    """Returns:
    Janela do rollback: min(D_rec, choice_at+300), sem reset da decisão original.
    Raises:
    PublicationError: decisão fora do contrato original de recovery.
    """
    if not _valid_decision(decision):
        raise PublicationError("decision=invalid")
    choice = _instant(decision["choice_at"])
    deadline = min(
        _instant(decision["recovery_deadline"]), choice + timedelta(seconds=CONVERGENCE_SECONDS)
    )
    return UploadWindow(clock, deadline)


def rollback_static(
    channel: StaticChannel,
    inventory: Inventory,
    publication: PublicationReceipt | None,
    window: UploadWindow,
) -> RollbackReceipt:
    """Returns:
    Entrypoints restaurados da release current; sem par anterior, residual sem restaurar.
    Raises:
    PublicationError: checksum da cópia diverge da versão anterior conhecida.
    UploadDeadlineError: chamada sem prazo dentro da janela de recovery.
    """
    known = {version.ref: version.sha256 for version in inventory.snapshot.versions}
    targets = tuple((ref, known.get(ref)) for ref in inventory.rollback_versions)
    return restore_entrypoints(channel, targets, publication, window)


def restore_entrypoints(
    channel: StaticChannel,
    targets: tuple[tuple[VersionRef, str | None], ...],
    publication: PublicationReceipt | None,
    window: UploadWindow,
) -> RollbackReceipt:
    """Returns:
    Entrypoints restaurados para (versão, sha256) planejados; sem par completo, residual.
    Raises:
    PublicationError: sha256 desconhecido ou cópia divergente.
    UploadDeadlineError: chamada sem prazo dentro da janela de recovery.
    """
    written = {key for key, _ in publication.uploaded} if publication is not None else set()
    if not written & set(ENTRYPOINTS):
        return RollbackReceipt((), False, None)
    refs = {ref.key: (ref, expected) for ref, expected in targets}
    if set(refs) != set(ENTRYPOINTS):
        return RollbackReceipt((), True, None)
    restored = []
    for key in ENTRYPOINTS:
        ref, expected = refs[key]
        if expected is None:
            raise PublicationError(f"rollback=unknown_checksum key={key}")
        restored.append((key, _copy_version(channel, ref, expected, window)))
    invalidation = _invalidate(channel, restored, window)
    return RollbackReceipt(tuple(restored), False, invalidation)


def _copy_version(
    channel: StaticChannel, ref: VersionRef, expected: str, window: UploadWindow
) -> str:
    key, version = ref.key, ref.version_id
    parameters = {
        "Bucket": channel.target.bucket,
        "Key": key,
        "CopySource": {"Bucket": channel.target.bucket, "Key": key, "VersionId": version},
        "ChecksumAlgorithm": "SHA256",
        "MetadataDirective": "COPY",
    }
    result = call_within(channel.port, window, ("recovery", "s3", "CopyObject"), parameters)
    copied = result.get("CopyObjectResult", {}).get("ChecksumSHA256")
    if _hex(copied) != expected:
        raise PublicationError(f"rollback=checksum key={key}")
    restored = result.get("VersionId")
    if not isinstance(restored, str) or not restored:
        raise PublicationError(f"rollback=version key={key}")
    return restored


def _invalidate(channel: StaticChannel, restored: list[tuple[str, str]], window: UploadWindow) -> str:
    reference = hashlib.sha256(json.dumps(["rollback", *restored]).encode()).hexdigest()[:32]
    parameters = {
        "DistributionId": channel.target.distribution_id,
        "InvalidationBatch": {
            "Paths": {"Quantity": len(INVALIDATION_PATHS), "Items": list(INVALIDATION_PATHS)},
            "CallerReference": reference,
        },
    }
    request = ("recovery", "cloudfront", "CreateInvalidation")
    return str(call_within(channel.port, window, request, parameters)["Invalidation"]["Id"])


def _hex(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        raw = base64.b64decode(value, validate=True)
    except binascii.Error:
        return None
    return raw.hex() if len(raw) == 32 else None


def _valid_handoff(value: Any, caller: Mapping[str, Any], ready: str) -> bool:
    if not isinstance(value, dict) or set(value) != HANDOFF_FIELDS:
        return False
    if value["version"] != 1 or value["kind"] != "host-handoff":
        return False
    if value["caller"] != dict(caller) or value["closed_ready_sha256"] != ready:
        return False
    status, recovery = value["status"], value["recovery_decision"]
    if status not in {"accepted", "rolled_back", "recovery_required"}:
        return False
    if status == "recovery_required" and recovery is None:
        return False
    return recovery is None or _valid_decision(recovery)


def _valid_decision(value: Any) -> bool:
    if not isinstance(value, dict) or set(value) != DECISION_FIELDS or value["version"] != 1:
        return False
    if not all(_is_digest(value[key]) for key in ("operation_id", "binding_sha256",
                                                    "checkpoint_sha256", "clock_boot_id")):
        return False
    if not all(isinstance(value[k], str) and _INSTANT.fullmatch(value[k])
               for k in ("choice_at", "fixed_deadline", "recovery_deadline")):
        return False
    choice, fixed = _instant(value["choice_at"]), _instant(value["fixed_deadline"])
    deadline = min(fixed, choice + timedelta(seconds=RECOVERY_SECONDS))
    return (
        _instant(value["recovery_deadline"]) == deadline
        and value["allocated_seconds"] == max(0, int((deadline - choice).total_seconds()))
        and value["convergence_budget_seconds"] == CONVERGENCE_SECONDS
        and value["finalization_budget_seconds"] == value["uncertainty_budget_seconds"] == 60
    )


def _is_digest(value: Any) -> bool:
    return isinstance(value, str) and _DIGEST.fullmatch(value) is not None


def _instant(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
