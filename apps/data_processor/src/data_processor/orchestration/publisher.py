"""Promotes committed unit outputs to an immutable dataset publication."""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING

from cnes_contracts.manifests.outputs import OutputManifest, RunManifest
from cnes_domain.control_plane.commands import PublicationPermit, PublishDataset
from cnes_domain.control_plane.entities import DatasetVersion, OutboxEvent
from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
from data_processor.orchestration.attempt_store import attempt_object_key, unit_attempt_prefix

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.control_plane.entities import DatasetPointer, ManifestRef, Run, RunUnit
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort

logger = logging.getLogger(__name__)

type PublicationPolicy = Callable[[Run], PublicationPermit]

_PUBLISHED_EVENT_TYPE = "reconciliation.published"


def allow_publication(run: Run) -> PublicationPermit:
    return PublicationPermit(
        tenant_id=run.tenant_id, run_id=run.run_id, policy_version=0, fencing_token=0
    )


@dataclass(frozen=True, slots=True)
class PublishRequest:
    run: Run
    units: tuple[RunUnit, ...]
    expected_version_id: str | None
    now: datetime


@dataclass(frozen=True, slots=True)
class PublishResult:
    version: DatasetVersion
    pointer: DatasetPointer
    run_manifest: RunManifest


def _validate_unit_state(unit: RunUnit) -> None:
    if unit.state not in {RunUnitState.SUCCEEDED, RunUnitState.SUCCEEDED_DEGRADED}:
        raise ValueError("unit_not_succeeded")
    if unit.state is RunUnitState.SUCCEEDED and not unit.output_manifests:
        raise ValueError("succeeded_unit_missing_outputs")
    if unit.state is RunUnitState.SUCCEEDED_DEGRADED and unit.output_manifests:
        raise ValueError("degraded_unit_outputs_forbidden")


def _validate_units(run: Run, units: tuple[RunUnit, ...]) -> None:
    if run.state is not RunState.PUBLISHING:
        raise ValueError("run_not_publishing")
    for unit in units:
        _validate_unit_state(unit)
    materialize_count = sum(
        1
        for unit in units
        if unit.stage is RunStage.MATERIALIZE and unit.state is RunUnitState.SUCCEEDED
    )
    if materialize_count != 1:
        raise ValueError("materialize_unit_count_invalid")


def _read_manifest(store: ObjectStorePort, ref: ManifestRef) -> OutputManifest:
    with store.open(ref.manifest_key) as stream:
        payload = stream.read()
    manifest = OutputManifest.model_validate_json(payload)
    if manifest.manifest_id != ref.manifest_id:
        raise ValueError("manifest_id_mismatch")
    canonical = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    if canonical != payload:
        raise ValueError("manifest_not_canonical")
    return manifest


def _require_unique_manifests(pairs: list[tuple[RunUnit, OutputManifest]]) -> None:
    manifest_ids = [manifest.manifest_id for _, manifest in pairs]
    object_keys = [manifest.object_key for _, manifest in pairs]
    if len(set(manifest_ids)) != len(manifest_ids):
        raise ValueError("duplicate_manifest_id")
    if len(set(object_keys)) != len(object_keys):
        raise ValueError("duplicate_object_key")


def _load_manifests(
    store: ObjectStorePort, units: tuple[RunUnit, ...]
) -> tuple[tuple[RunUnit, OutputManifest], ...]:
    refs = [(unit, ref) for unit in units for ref in unit.output_manifests]
    pairs = [(unit, _read_manifest(store, ref)) for unit, ref in refs]
    _require_unique_manifests(pairs)
    return tuple(sorted(pairs, key=lambda pair: pair[1].object_key))


def _promote(store: ObjectStorePort, unit: RunUnit, manifest: OutputManifest) -> None:
    source_key = attempt_object_key(unit_attempt_prefix(unit), manifest.object_key)
    store.promote(source_key, manifest.object_key, manifest.object_sha256)
    stat = store.stat(manifest.object_key)
    if stat is None or stat.sha256 != manifest.object_sha256:
        raise ValueError("promoted_object_hash_mismatch")


def _run_manifest_key(run: Run) -> str:
    return f"reconciliation/{run.tenant_id}/{run.competencia}/{run.run_id}/run-manifest.json"


def _write_run_manifest(
    store: ObjectStorePort, run: Run, manifests: tuple[OutputManifest, ...], now: datetime
) -> tuple[str, RunManifest]:
    run_manifest = RunManifest(
        manifest_version=1,
        tenant_id=run.tenant_id,
        dataset_name=run.dataset_name,
        run_id=run.run_id,
        competencia=run.competencia,
        outputs=manifests,
        missing_sources=run.missing_sources,
        published_at=now,
    )
    key = _run_manifest_key(run)
    payload = run_manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    store.put(key, BytesIO(payload), sha256(payload).hexdigest())
    return key, run_manifest


def _build_version(run: Run, run_manifest_key: str, now: datetime) -> DatasetVersion:
    return DatasetVersion(
        tenant_id=run.tenant_id,
        dataset_name=run.dataset_name,
        version_id=run.run_id,
        run_id=run.run_id,
        run_manifest_key=run_manifest_key,
        created_at=now,
    )


def _validate_permit(run: Run, permit: PublicationPermit) -> None:
    if permit.tenant_id != run.tenant_id or permit.run_id != run.run_id:
        raise ValueError("publication_permit_identity_mismatch")


def _build_event(run: Run, now: datetime) -> OutboxEvent:
    return OutboxEvent(
        tenant_id=run.tenant_id,
        event_id=f"{_PUBLISHED_EVENT_TYPE}:{run.tenant_id}:{run.run_id}",
        event_type=_PUBLISHED_EVENT_TYPE,
        aggregate_id=run.run_id,
        payload={"dataset_name": run.dataset_name, "version_id": run.run_id},
        created_at=now,
        delivered_at=None,
    )


def _build_command(
    request: PublishRequest, version: DatasetVersion, permit: PublicationPermit
) -> PublishDataset:
    run = request.run
    final_state = RunState.PUBLISHED_DEGRADED if run.missing_sources else RunState.PUBLISHED
    return PublishDataset(
        version=version,
        pointer_name="current",
        expected_version_id=request.expected_version_id,
        final_state=final_state,
        missing_sources=run.missing_sources,
        publication_permit=permit,
        event=_build_event(run, request.now),
    )


class DatasetPublisher:
    def __init__(
        self,
        store: ObjectStorePort,
        control_plane: ControlPlanePort,
        publication_policy: PublicationPolicy = allow_publication,
    ) -> None:
        self._store = store
        self._control_plane = control_plane
        self._publication_policy = publication_policy

    def publish(self, request: PublishRequest) -> PublishResult:
        run = request.run
        _validate_units(run, request.units)
        pairs = _load_manifests(self._store, request.units)
        for unit, manifest in pairs:
            _promote(self._store, unit, manifest)
        manifests = tuple(manifest for _, manifest in pairs)
        key, run_manifest = _write_run_manifest(self._store, run, manifests, request.now)
        version = _build_version(run, key, request.now)
        permit = self._publication_policy(run)
        _validate_permit(run, permit)
        pointer = self._control_plane.publish_dataset(
            _build_command(request, version, permit)
        )
        logger.info(
            "dataset_published tenant_id=%s run_id=%s version_id=%s outputs=%d",
            run.tenant_id, run.run_id, version.version_id, len(manifests),
        )
        return PublishResult(version=version, pointer=pointer, run_manifest=run_manifest)
