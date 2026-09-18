"""Claim -> process -> validate -> fenced commit for a single RunUnit attempt."""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from io import BytesIO
from typing import TYPE_CHECKING

from cnes_contracts.manifests.validation import manifest_sha256
from cnes_domain.control_plane.commands import CommitRunUnit, FailRunUnit
from cnes_domain.control_plane.entities import ManifestRef, OutboxEvent
from cnes_domain.control_plane.errors import ControlPlaneErrorCode, LeaseLost
from data_processor.orchestration.attempt_store import (
    AttemptObjectStore,
    attempt_object_key,
    unit_attempt_prefix,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_contracts.manifests.outputs import OutputManifest
    from cnes_domain.control_plane.commands import ClaimRunUnit
    from cnes_domain.control_plane.entities import RunUnit
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort

logger = logging.getLogger(__name__)

_ERROR_CODE_PATTERN = re.compile(r"[^A-Za-z0-9_.:-]")


def _noop(unit: RunUnit) -> None:
    return None


def _sanitize_error_code(exc: Exception) -> str:
    return _ERROR_CODE_PATTERN.sub("_", type(exc).__name__)[:64]


def _validate_outputs(
    unit: RunUnit, manifests: tuple[OutputManifest, ...], attempt_store: AttemptObjectStore
) -> None:
    if not manifests:
        raise ValueError("output_manifests_required")
    manifest_ids = [manifest.manifest_id for manifest in manifests]
    object_keys = [manifest.object_key for manifest in manifests]
    if len(set(manifest_ids)) != len(manifest_ids):
        raise ValueError("duplicate_manifest_id")
    if len(set(object_keys)) != len(object_keys):
        raise ValueError("duplicate_object_key")
    identity = (unit.tenant_id, unit.run_id, unit.unit_id, unit.attempt)
    for manifest in manifests:
        if (manifest.tenant_id, manifest.run_id, manifest.unit_id, manifest.attempt) != identity:
            raise ValueError("output_identity_mismatch")
        stat = attempt_store.stat(manifest.object_key)
        if stat is None or stat.sha256 != manifest.object_sha256:
            raise ValueError("output_not_found_or_hash_mismatch")


@dataclass(frozen=True, slots=True)
class UnitWorkerDependencies:
    control_plane: ControlPlanePort
    store: ObjectStorePort
    processor: Callable[[RunUnit, ObjectStorePort], tuple[OutputManifest, ...]]
    clock: Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class UnitWorkerPolicy:
    max_attempts: int = 3
    after_persist: Callable[[RunUnit], None] = _noop

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts_must_be_positive")


class UnitWorker:
    def __init__(
        self, dependencies: UnitWorkerDependencies, policy: UnitWorkerPolicy | None = None
    ) -> None:
        self._dependencies = dependencies
        self._policy = policy or UnitWorkerPolicy()

    def execute(self, command: ClaimRunUnit) -> RunUnit:
        deps = self._dependencies
        unit = deps.control_plane.claim_run_unit(command)
        if unit is None:
            raise LeaseLost(ControlPlaneErrorCode.UNIT_NOT_LEASED)
        prefix = unit_attempt_prefix(unit)
        attempt_store = AttemptObjectStore(delegate=deps.store, prefix=prefix)
        try:
            manifests = deps.processor(unit, attempt_store)
        except Exception as exc:
            retryable = unit.attempt < self._policy.max_attempts
            return self._fail(command, unit, exc, retryable=retryable)
        try:
            _validate_outputs(unit, manifests, attempt_store)
        except ValueError as exc:
            return self._fail(command, unit, exc, retryable=False)
        return self._commit(command, unit, prefix, manifests)

    def _fail(
        self, command: ClaimRunUnit, unit: RunUnit, exc: Exception, *, retryable: bool
    ) -> RunUnit:
        logger.exception(
            "unit_attempt_failed tenant_id=%s run_id=%s unit_id=%s attempt=%d retryable=%s",
            unit.tenant_id, unit.run_id, unit.unit_id, unit.attempt, retryable,
        )
        fail_command = FailRunUnit(
            tenant_id=command.tenant_id, run_id=command.run_id, unit_id=command.unit_id,
            dispatch_id=command.dispatch_id, owner=command.owner,
            fencing_token=unit.fencing_token, error_code=_sanitize_error_code(exc),
            retryable=retryable,
        )
        event = self._build_event(unit, "run_unit.failed")
        persisted = self._dependencies.control_plane.fail_run_unit(fail_command, event)
        self._policy.after_persist(persisted)
        return persisted

    def _commit(
        self,
        command: ClaimRunUnit,
        unit: RunUnit,
        prefix: str,
        manifests: tuple[OutputManifest, ...],
    ) -> RunUnit:
        ordered = sorted(manifests, key=lambda manifest: manifest.object_key)
        refs = tuple(self._write_sidecar(prefix, manifest) for manifest in ordered)
        commit_command = CommitRunUnit(
            tenant_id=command.tenant_id, run_id=command.run_id, unit_id=command.unit_id,
            dispatch_id=command.dispatch_id, owner=command.owner,
            fencing_token=unit.fencing_token, output_manifests=refs,
        )
        event = self._build_event(unit, "run_unit.succeeded")
        persisted = self._dependencies.control_plane.commit_run_unit(commit_command, event)
        self._policy.after_persist(persisted)
        return persisted

    def _write_sidecar(self, prefix: str, manifest: OutputManifest) -> ManifestRef:
        sidecar_key = attempt_object_key(
            prefix, f"manifests/{manifest.manifest_id}/manifest.json"
        )
        payload = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
        self._dependencies.store.put(sidecar_key, BytesIO(payload), manifest_sha256(manifest))
        return ManifestRef(manifest_id=manifest.manifest_id, manifest_key=sidecar_key)

    def _build_event(self, unit: RunUnit, event_type: str) -> OutboxEvent:
        event_id = f"{event_type}:{unit.tenant_id}:{unit.run_id}:{unit.unit_id}:{unit.attempt}"
        return OutboxEvent(
            tenant_id=unit.tenant_id, event_id=event_id, event_type=event_type,
            aggregate_id=unit.unit_id,
            payload={"run_id": unit.run_id, "unit_id": unit.unit_id, "attempt": unit.attempt},
            created_at=self._dependencies.clock(), delivered_at=None,
        )
