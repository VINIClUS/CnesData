"""Immutable control-plane commands."""

from __future__ import annotations

from typing import Literal

from pydantic import field_validator, model_validator

from cnes_domain.control_plane.entities import (
    ControlPlaneModel,
    DatasetVersion,
    IdempotencyRecord,
    ManifestRef,
    OutboxEvent,
    RawManifestRecord,
    RunUnit,
    UtcDatetime,
    optional_error_code,
    optional_non_blank,
    optional_sha256,
    require_dispatch_id,
    require_non_blank,
    require_sha256,
    require_unique_refs,
    require_utc,
    unique_non_blank,
)
from cnes_domain.control_plane.enums import (
    DispatchOutcome,
    RunStage,
    RunState,
    RunUnitState,
)


def _require_positive(value: int) -> int:
    if value < 1:
        raise ValueError("positive_value_required")
    return value


def _require_non_negative(value: int) -> int:
    if value < 0:
        raise ValueError("negative_counter")
    return value


def _require_dispatch_field(value: str) -> str:
    return require_dispatch_id(value, "dispatch_id")


def _require_wave_field(value: str) -> str:
    return require_dispatch_id(value, "wave_id")


def _unique_missing_sources(values: tuple[str, ...]) -> tuple[str, ...]:
    return unique_non_blank(values, "duplicate_missing_source")


def _validate_dispatch_units(values: tuple[str, ...]) -> tuple[str, ...]:
    if not values:
        raise ValueError("unit_ids_required")
    unique_non_blank(values, "duplicate_unit_id")
    if values != tuple(sorted(values)):
        raise ValueError("unit_ids_not_ordered")
    return values


class ClaimJob(ControlPlaneModel):
    tenant_id: str
    job_id: str
    owner: str
    now: UtcDatetime
    lease_seconds: int

    _strings = field_validator("tenant_id", "job_id", "owner")(require_non_blank)
    _now_utc = field_validator("now")(require_utc)
    _lease_positive = field_validator("lease_seconds")(_require_positive)


class RenewJobLease(ControlPlaneModel):
    tenant_id: str
    job_id: str
    owner: str
    fencing_token: int
    now: UtcDatetime
    lease_seconds: int

    _strings = field_validator("tenant_id", "job_id", "owner")(require_non_blank)
    _fence = field_validator("fencing_token")(_require_non_negative)
    _now_utc = field_validator("now")(require_utc)
    _lease_positive = field_validator("lease_seconds")(_require_positive)


class CompleteJob(ControlPlaneModel):
    tenant_id: str
    job_id: str
    owner: str
    fencing_token: int
    manifest: RawManifestRecord
    expected_head_manifest_id: str | None = None

    _strings = field_validator("tenant_id", "job_id", "owner")(require_non_blank)
    _expected_head = field_validator("expected_head_manifest_id")(optional_non_blank)
    _fence = field_validator("fencing_token")(_require_non_negative)

    @model_validator(mode="after")
    def _manifest_identity(self) -> CompleteJob:
        if self.manifest.tenant_id != self.tenant_id:
            raise ValueError("manifest_identity_mismatch")
        return self


class FailJob(ControlPlaneModel):
    tenant_id: str
    job_id: str
    owner: str
    fencing_token: int
    error_code: str
    retryable: bool
    rejected_manifest_sha256: str | None = None
    expected_head_manifest_id: str | None = None
    expected_resync_marker: bool | None = None

    _strings = field_validator("tenant_id", "job_id", "owner")(require_non_blank)
    _error = field_validator("error_code")(optional_error_code)
    _fence = field_validator("fencing_token")(_require_non_negative)
    _rejected_hash = field_validator("rejected_manifest_sha256")(optional_sha256)
    _expected_head = field_validator("expected_head_manifest_id")(optional_non_blank)

    @model_validator(mode="after")
    def _validate_rejected_hash(self) -> FailJob:
        is_resync = not self.retryable and self.error_code.startswith("RAW_RESYNC_")
        if is_resync and self.rejected_manifest_sha256 is None:
            raise ValueError("resync_hash_required")
        guarded = (
            self.expected_head_manifest_id is not None
            or self.expected_resync_marker is not None
        )
        if guarded and not is_resync:
            raise ValueError("resync_guard_forbidden")
        if (
            self.expected_head_manifest_id is not None
            and self.expected_resync_marker is not False
        ):
            raise ValueError("head_guard_requires_absent_marker")
        if not is_resync and self.rejected_manifest_sha256 is not None:
            raise ValueError("resync_hash_forbidden")
        return self


class CancelJob(ControlPlaneModel):
    tenant_id: str
    job_id: str
    requested_by: str

    _strings = field_validator("tenant_id", "job_id", "requested_by")(require_non_blank)


class TransitionRun(ControlPlaneModel):
    tenant_id: str
    run_id: str
    expected_state: RunState
    new_state: RunState
    missing_sources: tuple[str, ...] = ()

    _strings = field_validator("tenant_id", "run_id")(require_non_blank)
    _missing_unique = field_validator("missing_sources")(_unique_missing_sources)


def _validate_unit_identity(command: PutRunUnits) -> None:
    for unit in command.units:
        if unit.tenant_id != command.tenant_id or unit.run_id != command.run_id:
            raise ValueError("unit_identity_mismatch")
        initial = (
            unit.state is RunUnitState.PENDING
            and unit.attempt == 0
            and unit.fencing_token == 0
            and unit.lease_owner is None
            and unit.lease_until is None
            and unit.dispatch_id is None
            and not unit.output_manifests
            and unit.error_code is None
        )
        if not initial:
            raise ValueError("unit_not_initial")


def _validate_unit_graph(units: tuple[RunUnit, ...]) -> None:
    by_id = {unit.unit_id: unit for unit in units}
    if len(by_id) != len(units):
        raise ValueError("duplicate_unit_id")
    predecessors = {
        RunStage.NORMALIZE: None,
        RunStage.RECONCILE: RunStage.NORMALIZE,
        RunStage.MATERIALIZE: RunStage.RECONCILE,
    }
    for unit in units:
        expected = predecessors[unit.stage]
        for dependency_id in unit.depends_on_unit_ids:
            dependency = by_id.get(dependency_id)
            if dependency is None:
                raise ValueError("unknown_dependency")
            if dependency.stage is not expected:
                raise ValueError("invalid_stage_progression")


class PutRunUnits(ControlPlaneModel):
    tenant_id: str
    run_id: str
    expected_run_state: RunState
    units: tuple[RunUnit, ...]

    _strings = field_validator("tenant_id", "run_id")(require_non_blank)

    @model_validator(mode="after")
    def _validate_units(self) -> PutRunUnits:
        if not self.units:
            raise ValueError("units_required")
        _validate_unit_identity(self)
        _validate_unit_graph(self.units)
        return self


class ClaimRunUnit(ControlPlaneModel):
    tenant_id: str
    run_id: str
    unit_id: str
    dispatch_id: str
    owner: str
    now: UtcDatetime
    lease_seconds: int

    _strings = field_validator("tenant_id", "run_id", "unit_id", "owner")(require_non_blank)
    _dispatch = field_validator("dispatch_id")(_require_dispatch_field)
    _now_utc = field_validator("now")(require_utc)
    _lease_positive = field_validator("lease_seconds")(_require_positive)


class CommitRunUnit(ControlPlaneModel):
    tenant_id: str
    run_id: str
    unit_id: str
    dispatch_id: str
    owner: str
    fencing_token: int
    output_manifests: tuple[ManifestRef, ...]

    _strings = field_validator("tenant_id", "run_id", "unit_id", "owner")(require_non_blank)
    _dispatch = field_validator("dispatch_id")(_require_dispatch_field)
    _fence = field_validator("fencing_token")(_require_non_negative)

    @field_validator("output_manifests")
    @classmethod
    def _outputs_required(cls, values: tuple[ManifestRef, ...]) -> tuple[ManifestRef, ...]:
        if not values:
            raise ValueError("output_manifests_required")
        require_unique_refs(values)
        return values


class FailRunUnit(ControlPlaneModel):
    tenant_id: str
    run_id: str
    unit_id: str
    dispatch_id: str
    owner: str
    fencing_token: int
    error_code: str
    retryable: bool

    _strings = field_validator("tenant_id", "run_id", "unit_id", "owner")(require_non_blank)
    _error = field_validator("error_code")(optional_error_code)
    _dispatch = field_validator("dispatch_id")(_require_dispatch_field)
    _fence = field_validator("fencing_token")(_require_non_negative)


class FinalizeRunCancellation(ControlPlaneModel):
    tenant_id: str
    run_id: str
    expected_state: Literal[RunState.CANCEL_REQUESTED]
    canceled_at: UtcDatetime

    _strings = field_validator("tenant_id", "run_id")(require_non_blank)
    _canceled_utc = field_validator("canceled_at")(require_utc)


class ReserveRunDispatch(ControlPlaneModel):
    tenant_id: str
    run_id: str
    wave_id: str
    unit_ids: tuple[str, ...]
    now: UtcDatetime
    lease_seconds: int

    _strings = field_validator("tenant_id", "run_id")(require_non_blank)
    _wave = field_validator("wave_id")(_require_wave_field)
    _units = field_validator("unit_ids")(_validate_dispatch_units)
    _now_utc = field_validator("now")(require_utc)
    _lease_positive = field_validator("lease_seconds")(_require_positive)


class BindRunDispatch(ControlPlaneModel):
    tenant_id: str
    run_id: str
    dispatch_id: str
    execution_ref: str
    now: UtcDatetime
    lease_seconds: int

    _strings = field_validator("tenant_id", "run_id", "execution_ref")(require_non_blank)
    _dispatch = field_validator("dispatch_id")(_require_dispatch_field)
    _now_utc = field_validator("now")(require_utc)
    _lease_positive = field_validator("lease_seconds")(_require_positive)


class FinishRunDispatch(ControlPlaneModel):
    tenant_id: str
    run_id: str
    dispatch_id: str
    outcome: DispatchOutcome
    finished_at: UtcDatetime

    _strings = field_validator("tenant_id", "run_id")(require_non_blank)
    _dispatch = field_validator("dispatch_id")(_require_dispatch_field)
    _finished_utc = field_validator("finished_at")(require_utc)


class BeginIdempotency(ControlPlaneModel):
    tenant_id: str
    scope: str
    key: str
    request_hash: str
    resource_id: str
    now: UtcDatetime
    expires_at: UtcDatetime

    _strings = field_validator("tenant_id", "scope", "key", "resource_id")(require_non_blank)
    _request_hash = field_validator("request_hash")(require_sha256)
    _datetimes = field_validator("now", "expires_at")(require_utc)

    @model_validator(mode="after")
    def _validate_expiry(self) -> BeginIdempotency:
        if self.expires_at <= self.now:
            raise ValueError("invalid_expiry")
        return self


class IdempotencyOutcome(ControlPlaneModel):
    record: IdempotencyRecord
    created: bool


class PublicationPermit(ControlPlaneModel):
    tenant_id: str
    run_id: str
    policy_version: int
    fencing_token: int
    binding_context: object | None = None

    _strings = field_validator("tenant_id", "run_id")(require_non_blank)
    _counters = field_validator("policy_version", "fencing_token")(_require_non_negative)


class PublishDataset(ControlPlaneModel):
    version: DatasetVersion
    pointer_name: str
    expected_version_id: str | None
    final_state: RunState
    missing_sources: tuple[str, ...]
    publication_permit: PublicationPermit
    event: OutboxEvent

    _pointer = field_validator("pointer_name")(require_non_blank)
    _expected_version = field_validator("expected_version_id")(optional_non_blank)
    _missing_unique = field_validator("missing_sources")(_unique_missing_sources)

    @model_validator(mode="after")
    def _validate_publication(self) -> PublishDataset:
        allowed = {RunState.PUBLISHED, RunState.PUBLISHED_DEGRADED}
        if self.final_state not in allowed:
            raise ValueError("invalid_final_state")
        identity = (self.version.tenant_id, self.version.run_id)
        if identity != (self.publication_permit.tenant_id, self.publication_permit.run_id):
            raise ValueError("publication_permit_mismatch")
        if (
            self.event.tenant_id != self.version.tenant_id
            or self.event.aggregate_id != self.version.run_id
        ):
            raise ValueError("publication_event_mismatch")
        if self.final_state is RunState.PUBLISHED and self.missing_sources:
            raise ValueError("published_missing_sources_forbidden")
        if self.final_state is RunState.PUBLISHED_DEGRADED and not self.missing_sources:
            raise ValueError("degraded_missing_sources_required")
        return self
