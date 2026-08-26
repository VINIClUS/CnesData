"""Immutable control-plane commands."""

from __future__ import annotations

from typing import Literal

from pydantic import field_validator, model_validator

from cnes_domain.control_plane.entities import (
    DatasetVersion,
    IdempotencyRecord,
    ManifestRef,
    OutboxEvent,
    RawManifestRecord,
    RunUnit,
    _ControlPlaneModel,
    _optional_error_code,
    _optional_non_blank,
    _require_dispatch_id,
    _require_non_blank,
    _require_sha256,
    _require_unique_refs,
    _require_utc,
    _unique_non_blank,
    _UtcDatetime,
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


def _validate_dispatch_units(values: tuple[str, ...]) -> tuple[str, ...]:
    if not values:
        raise ValueError("unit_ids_required")
    _unique_non_blank(values, "duplicate_unit_id")
    if values != tuple(sorted(values)):
        raise ValueError("unit_ids_not_ordered")
    return values


class ClaimJob(_ControlPlaneModel):
    tenant_id: str
    job_id: str
    owner: str
    now: _UtcDatetime
    lease_seconds: int

    _strings = field_validator("tenant_id", "job_id", "owner")(_require_non_blank)
    _now_utc = field_validator("now")(_require_utc)
    _lease_positive = field_validator("lease_seconds")(_require_positive)


class RenewJobLease(_ControlPlaneModel):
    tenant_id: str
    job_id: str
    owner: str
    fencing_token: int
    now: _UtcDatetime
    lease_seconds: int

    _strings = field_validator("tenant_id", "job_id", "owner")(_require_non_blank)
    _fence = field_validator("fencing_token")(_require_non_negative)
    _now_utc = field_validator("now")(_require_utc)
    _lease_positive = field_validator("lease_seconds")(_require_positive)


class CompleteJob(_ControlPlaneModel):
    tenant_id: str
    job_id: str
    owner: str
    fencing_token: int
    manifest: RawManifestRecord

    _strings = field_validator("tenant_id", "job_id", "owner")(_require_non_blank)
    _fence = field_validator("fencing_token")(_require_non_negative)

    @model_validator(mode="after")
    def _manifest_identity(self) -> CompleteJob:
        if self.manifest.tenant_id != self.tenant_id:
            raise ValueError("manifest_identity_mismatch")
        return self


class FailJob(_ControlPlaneModel):
    tenant_id: str
    job_id: str
    owner: str
    fencing_token: int
    error_code: str
    retryable: bool

    _strings = field_validator("tenant_id", "job_id", "owner")(_require_non_blank)
    _error = field_validator("error_code")(_optional_error_code)
    _fence = field_validator("fencing_token")(_require_non_negative)


class CancelJob(_ControlPlaneModel):
    tenant_id: str
    job_id: str
    requested_by: str

    _strings = field_validator("tenant_id", "job_id", "requested_by")(_require_non_blank)


class TransitionRun(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    expected_state: RunState
    new_state: RunState
    missing_sources: tuple[str, ...] = ()

    _strings = field_validator("tenant_id", "run_id")(_require_non_blank)
    _missing_unique = field_validator("missing_sources")(
        lambda values: _unique_non_blank(values, "duplicate_missing_source")
    )


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


class PutRunUnits(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    expected_run_state: RunState
    units: tuple[RunUnit, ...]

    _strings = field_validator("tenant_id", "run_id")(_require_non_blank)

    @model_validator(mode="after")
    def _validate_units(self) -> PutRunUnits:
        if not self.units:
            raise ValueError("units_required")
        _validate_unit_identity(self)
        _validate_unit_graph(self.units)
        return self


class ClaimRunUnit(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    unit_id: str
    dispatch_id: str
    owner: str
    now: _UtcDatetime
    lease_seconds: int

    _strings = field_validator("tenant_id", "run_id", "unit_id", "owner")(_require_non_blank)
    _dispatch = field_validator("dispatch_id")(
        lambda value: _require_dispatch_id(value, "dispatch_id")
    )
    _now_utc = field_validator("now")(_require_utc)
    _lease_positive = field_validator("lease_seconds")(_require_positive)


class CommitRunUnit(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    unit_id: str
    dispatch_id: str
    owner: str
    fencing_token: int
    output_manifests: tuple[ManifestRef, ...]

    _strings = field_validator("tenant_id", "run_id", "unit_id", "owner")(_require_non_blank)
    _dispatch = field_validator("dispatch_id")(
        lambda value: _require_dispatch_id(value, "dispatch_id")
    )
    _fence = field_validator("fencing_token")(_require_non_negative)

    @field_validator("output_manifests")
    @classmethod
    def _outputs_required(cls, values: tuple[ManifestRef, ...]) -> tuple[ManifestRef, ...]:
        if not values:
            raise ValueError("output_manifests_required")
        _require_unique_refs(values)
        return values


class FailRunUnit(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    unit_id: str
    dispatch_id: str
    owner: str
    fencing_token: int
    error_code: str
    retryable: bool

    _strings = field_validator("tenant_id", "run_id", "unit_id", "owner")(_require_non_blank)
    _error = field_validator("error_code")(_optional_error_code)
    _dispatch = field_validator("dispatch_id")(
        lambda value: _require_dispatch_id(value, "dispatch_id")
    )
    _fence = field_validator("fencing_token")(_require_non_negative)


class FinalizeRunCancellation(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    expected_state: Literal[RunState.CANCEL_REQUESTED]
    canceled_at: _UtcDatetime

    _strings = field_validator("tenant_id", "run_id")(_require_non_blank)
    _canceled_utc = field_validator("canceled_at")(_require_utc)


class ReserveRunDispatch(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    wave_id: str
    unit_ids: tuple[str, ...]
    now: _UtcDatetime
    lease_seconds: int

    _strings = field_validator("tenant_id", "run_id")(_require_non_blank)
    _wave = field_validator("wave_id")(lambda value: _require_dispatch_id(value, "wave_id"))
    _units = field_validator("unit_ids")(_validate_dispatch_units)
    _now_utc = field_validator("now")(_require_utc)
    _lease_positive = field_validator("lease_seconds")(_require_positive)


class BindRunDispatch(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    dispatch_id: str
    execution_ref: str
    now: _UtcDatetime
    lease_seconds: int

    _strings = field_validator("tenant_id", "run_id", "execution_ref")(_require_non_blank)
    _dispatch = field_validator("dispatch_id")(
        lambda value: _require_dispatch_id(value, "dispatch_id")
    )
    _now_utc = field_validator("now")(_require_utc)
    _lease_positive = field_validator("lease_seconds")(_require_positive)


class FinishRunDispatch(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    dispatch_id: str
    outcome: DispatchOutcome
    finished_at: _UtcDatetime

    _strings = field_validator("tenant_id", "run_id")(_require_non_blank)
    _dispatch = field_validator("dispatch_id")(
        lambda value: _require_dispatch_id(value, "dispatch_id")
    )
    _finished_utc = field_validator("finished_at")(_require_utc)


class BeginIdempotency(_ControlPlaneModel):
    tenant_id: str
    scope: str
    key: str
    request_hash: str
    resource_id: str
    now: _UtcDatetime
    expires_at: _UtcDatetime

    _strings = field_validator("tenant_id", "scope", "key", "resource_id")(_require_non_blank)
    _request_hash = field_validator("request_hash")(_require_sha256)
    _datetimes = field_validator("now", "expires_at")(_require_utc)

    @model_validator(mode="after")
    def _validate_expiry(self) -> BeginIdempotency:
        if self.expires_at <= self.now:
            raise ValueError("invalid_expiry")
        return self


class IdempotencyOutcome(_ControlPlaneModel):
    record: IdempotencyRecord
    created: bool


class PublicationPermit(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    policy_version: int
    fencing_token: int
    binding_context: object | None = None

    _strings = field_validator("tenant_id", "run_id")(_require_non_blank)
    _counters = field_validator("policy_version", "fencing_token")(_require_non_negative)


class PublishDataset(_ControlPlaneModel):
    version: DatasetVersion
    pointer_name: str
    expected_version_id: str | None
    final_state: RunState
    missing_sources: tuple[str, ...]
    publication_permit: PublicationPermit
    event: OutboxEvent

    _pointer = field_validator("pointer_name")(_require_non_blank)
    _expected_version = field_validator("expected_version_id")(_optional_non_blank)
    _missing_unique = field_validator("missing_sources")(
        lambda values: _unique_non_blank(values, "duplicate_missing_source")
    )

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
