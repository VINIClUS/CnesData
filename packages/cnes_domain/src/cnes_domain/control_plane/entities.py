from __future__ import annotations

import re
from copy import deepcopy
from datetime import datetime, timedelta
from math import isfinite
from typing import Literal

from pydantic import BaseModel, ConfigDict, NonNegativeInt, field_validator, model_validator

from cnes_domain.control_plane.enums import (
    AccessRequestState,
    AgentState,
    DispatchOutcome,
    DispatchState,
    JobState,
    RunStage,
    RunState,
    RunUnitState,
)

type JsonValue = bool | int | float | str | list[JsonValue] | dict[str, JsonValue] | None
type _UtcDatetime = datetime
_COMPETENCIA = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_LOWER_HEX_16 = re.compile(r"^[0-9a-f]{16}$")
_LOWER_HEX_64 = re.compile(r"^[0-9a-f]{64}$")
_ERROR_CODE = re.compile(r"^[A-Za-z0-9_.:-]+$")


def _require_non_blank(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("blank_value")
    return value


def _optional_non_blank(value: str | None) -> str | None:
    return _require_non_blank(value) if value is not None else value


def _require_key_component(value: str) -> str:
    _require_non_blank(value)
    if value in {".", ".."} or any(char in value for char in "#/\\"):
        raise ValueError("invalid_key_component")
    return value


def _require_competencia(value: str) -> str:
    if not isinstance(value, str) or not _COMPETENCIA.fullmatch(value):
        raise ValueError("invalid_competencia")
    return value


def _require_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("datetime_not_utc")
    return value


def _optional_utc(value: datetime | None) -> datetime | None:
    return _require_utc(value) if value is not None else value


def _require_sha256(value: str) -> str:
    if not _LOWER_HEX_64.fullmatch(value):
        raise ValueError("invalid_sha256")
    return value


def _optional_sha256(value: str | None) -> str | None:
    return _require_sha256(value) if value is not None else value


def _optional_error_code(value: str | None) -> str | None:
    if value is not None and not _ERROR_CODE.fullmatch(value):
        raise ValueError("invalid_error_code")
    return value


def _require_dispatch_id(value: str, name: str) -> str:
    if not _LOWER_HEX_16.fullmatch(value):
        raise ValueError(f"invalid_{name}")
    return value


def _require_sidecar_key(value: str) -> str:
    if value.startswith("/") or "//" in value or "\\" in value:
        raise ValueError("invalid_manifest_key")
    parts = value.split("/")
    invalid_parts = any(part in {"", ".", ".."} for part in parts)
    if len(parts) < 3 or parts[-1] not in {"manifest.json", "run-manifest.json"} or invalid_parts:
        raise ValueError("invalid_manifest_key")
    return value


def _require_finite_json(value: JsonValue) -> JsonValue:
    if isinstance(value, float) and not isfinite(value):
        raise ValueError("non_finite_json_float")
    if isinstance(value, list):
        for item in value:
            _require_finite_json(item)
    elif isinstance(value, dict):
        for item in value.values():
            _require_finite_json(item)
    return value


def _unique_non_blank(values: tuple[str, ...], duplicate_message: str) -> tuple[str, ...]:
    for value in values:
        _require_non_blank(value)
    if len(set(values)) != len(values):
        raise ValueError(duplicate_message)
    return values


def _require_unique_refs(refs: tuple[ManifestRef, ...]) -> None:
    ids = {ref.manifest_id for ref in refs}
    keys = {ref.manifest_key for ref in refs}
    if len(ids) != len(refs) or len(keys) != len(refs):
        raise ValueError("duplicate_manifest_ref")


class _ControlPlaneModel(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")


class Tenant(_ControlPlaneModel):
    tenant_id: str
    municipality_name: str
    created_at: datetime
    _strings = field_validator("tenant_id", "municipality_name")(_require_non_blank)
    _created_utc = field_validator("created_at")(_require_utc)


class Membership(_ControlPlaneModel):
    tenant_id: str
    user_id: str
    role: str
    created_at: datetime
    _strings = field_validator("tenant_id", "user_id", "role")(_require_non_blank)
    _created_utc = field_validator("created_at")(_require_utc)


class Agent(_ControlPlaneModel):
    tenant_id: str
    agent_id: str
    state: AgentState
    version: str
    certificate_fingerprint: str
    last_seen_at: datetime | None
    created_at: datetime
    _strings = field_validator("tenant_id", "agent_id", "version")(_require_non_blank)
    _fingerprint = field_validator("certificate_fingerprint")(_require_sha256)
    _datetimes = field_validator("last_seen_at", "created_at")(_optional_utc)


class Job(_ControlPlaneModel):
    tenant_id: str
    job_id: str
    agent_id: str
    source_type: str
    file_subtype: str
    competencia: str
    requested_snapshot_mode: Literal["FULL", "DELTA"]
    state: JobState
    attempt: NonNegativeInt
    fencing_token: NonNegativeInt
    lease_owner: str | None
    lease_until: datetime | None
    result_manifest_id: str | None
    result_manifest_key: str | None
    error_code: str | None
    created_at: datetime
    _strings = field_validator("tenant_id", "job_id", "agent_id", "source_type", "file_subtype")(
        _require_key_component
    )
    _competencia_value = field_validator("competencia")(_require_competencia)
    _datetimes = field_validator("lease_until", "created_at")(_optional_utc)
    _optional_ids = field_validator("lease_owner", "result_manifest_id")(_optional_non_blank)
    _error_value = field_validator("error_code")(_optional_error_code)
    @model_validator(mode="after")
    def _validate_consistency(self) -> Job:
        if (self.lease_owner is None) != (self.lease_until is None):
            raise ValueError("lease_pair_required")
        if (self.result_manifest_id is None) != (self.result_manifest_key is None):
            raise ValueError("result_manifest_pair_required")
        if self.state is JobState.SUCCEEDED and self.result_manifest_id is None:
            raise ValueError("succeeded_manifest_required")
        self._validate_result_key()
        return self

    def _validate_result_key(self) -> None:
        if self.result_manifest_key is None:
            return
        _require_sidecar_key(self.result_manifest_key)
        parts = self.result_manifest_key.split("/")
        expected = ("raw", self.tenant_id, self.source_type, self.competencia)
        if len(parts) != 6 or tuple(parts[:4]) != expected:
            raise ValueError("invalid_result_manifest_key")
        if parts[-1] != "manifest.json":
            raise ValueError("invalid_result_manifest_key")


class RunDependency(_ControlPlaneModel):
    source_type: str
    file_subtype: str
    required: bool
    _components = field_validator("source_type", "file_subtype")(_require_key_component)


class Run(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    competencia: str
    dataset_name: str
    state: RunState
    dependencies: tuple[RunDependency, ...]
    missing_sources: tuple[str, ...]
    created_at: datetime
    _strings = field_validator("tenant_id", "run_id", "dataset_name")(_require_non_blank)
    _competencia_value = field_validator("competencia")(_require_competencia)
    _created_utc = field_validator("created_at")(_require_utc)
    @field_validator("missing_sources")
    @classmethod
    def _missing_unique(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        return _unique_non_blank(values, "duplicate_missing_source")

    @model_validator(mode="after")
    def _dependencies_unique(self) -> Run:
        if not self.dependencies:
            raise ValueError("dependencies_required")
        identities = {(item.source_type, item.file_subtype) for item in self.dependencies}
        if len(identities) != len(self.dependencies):
            raise ValueError("duplicate_dependency")
        return self


class ManifestRef(_ControlPlaneModel):
    manifest_id: str
    manifest_key: str
    _identifier = field_validator("manifest_id")(_require_non_blank)
    _sidecar = field_validator("manifest_key")(_require_sidecar_key)


class RawManifestRecord(_ControlPlaneModel):
    tenant_id: str
    manifest_id: str
    manifest_key: str
    agent_id: str
    source_type: str
    file_subtype: str
    competencia: str
    snapshot_mode: Literal["FULL", "DELTA"]
    snapshot_id: str
    base_snapshot_id: str | None
    sequence: int
    previous_manifest_sha256: str | None
    manifest_sha256: str
    created_at: datetime
    _strings = field_validator(
        "tenant_id",
        "manifest_id",
        "agent_id",
        "source_type",
        "file_subtype",
        "snapshot_id",
    )(_require_key_component)
    _competencia_value = field_validator("competencia")(_require_competencia)
    _manifest_key_value = field_validator("manifest_key")(_require_sidecar_key)
    _manifest_hash = field_validator("manifest_sha256")(_require_sha256)
    _created_utc = field_validator("created_at")(_require_utc)
    _base_snapshot = field_validator("base_snapshot_id")(
        lambda value: _require_key_component(value) if value is not None else value
    )
    _previous_hash = field_validator("previous_manifest_sha256")(_optional_sha256)
    @model_validator(mode="after")
    def _validate_chain(self) -> RawManifestRecord:
        if self.sequence < 1:
            raise ValueError("invalid_sequence")
        if self.snapshot_mode == "FULL":
            if self.sequence != 1 or self.base_snapshot_id or self.previous_manifest_sha256:
                raise ValueError("invalid_full_chain")
        elif self.sequence < 2 or not self.base_snapshot_id or not self.previous_manifest_sha256:
            raise ValueError("invalid_delta_chain")
        expected = f"raw/{self.tenant_id}/{self.source_type}/{self.competencia}/"
        if self.manifest_key != f"{expected}{self.snapshot_id}/manifest.json":
            raise ValueError("invalid_manifest_key")
        return self


class RunUnit(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    unit_id: str
    stage: RunStage
    source_type: str | None
    file_subtype: str | None
    partition: str
    depends_on_unit_ids: tuple[str, ...]
    input_manifests: tuple[ManifestRef, ...]
    state: RunUnitState
    attempt: NonNegativeInt
    fencing_token: NonNegativeInt
    lease_owner: str | None
    lease_until: datetime | None
    dispatch_id: str | None
    output_manifests: tuple[ManifestRef, ...]
    error_code: str | None
    _strings = field_validator("tenant_id", "run_id", "unit_id", "partition")(_require_non_blank)
    _optional_components = field_validator("source_type", "file_subtype")(
        lambda value: _require_key_component(value) if value is not None else value
    )
    _lease_owner_value = field_validator("lease_owner")(_optional_non_blank)
    _lease_utc = field_validator("lease_until")(_optional_utc)

    @field_validator("dispatch_id")
    @classmethod
    def _dispatch_value(cls, value: str | None) -> str | None:
        return _require_dispatch_id(value, "dispatch_id") if value is not None else value

    _error_value = field_validator("error_code")(_optional_error_code)

    @model_validator(mode="after")
    def _validate_unit(self) -> RunUnit:
        _unique_non_blank(self.depends_on_unit_ids, "duplicate_dependency_id")
        if self.unit_id in self.depends_on_unit_ids:
            raise ValueError("self_dependency")
        if (self.lease_owner is None) != (self.lease_until is None):
            raise ValueError("lease_pair_required")
        if self.state is RunUnitState.PENDING and self.dispatch_id is not None:
            raise ValueError("initial_dispatch_forbidden")
        if self.state is RunUnitState.SUCCEEDED_DEGRADED:
            self._validate_degraded()
        self._validate_stage()
        self._validate_manifest_refs()
        return self

    def _validate_stage(self) -> None:
        if self.stage is RunStage.NORMALIZE:
            if not self.source_type or not self.file_subtype or not self.input_manifests:
                raise ValueError("normalize_inputs_required")
            if self.depends_on_unit_ids:
                raise ValueError("normalize_dependencies_forbidden")
            return
        if self.source_type is not None or self.file_subtype is not None:
            raise ValueError("downstream_source_forbidden")
        if self.input_manifests:
            raise ValueError("downstream_direct_inputs_forbidden")
        if not self.depends_on_unit_ids:
            raise ValueError("downstream_dependencies_required")

    def _validate_manifest_refs(self) -> None:
        refs = (*self.input_manifests, *self.output_manifests)
        _require_unique_refs(refs)

    def _validate_degraded(self) -> None:
        if self.stage is not RunStage.NORMALIZE or not self.error_code:
            raise ValueError("invalid_degraded_unit")
        if self.output_manifests:
            raise ValueError("degraded_outputs_forbidden")


class RunDispatch(_ControlPlaneModel):
    tenant_id: str
    run_id: str
    wave_id: str
    dispatch_id: str
    generation: int
    unit_ids: tuple[str, ...]
    state: DispatchState
    lease_until: datetime
    execution_ref: str | None = None
    terminal_outcome: DispatchOutcome | None = None
    _strings = field_validator("tenant_id", "run_id")(_require_non_blank)
    _lease_utc = field_validator("lease_until")(_require_utc)
    _execution_ref_value = field_validator("execution_ref")(_optional_non_blank)
    @field_validator("wave_id", "dispatch_id")
    @classmethod
    def _dispatch_ids(cls, value: str, info: object) -> str:
        return _require_dispatch_id(value, info.field_name)

    @model_validator(mode="after")
    def _validate_dispatch(self) -> RunDispatch:
        if self.generation < 1:
            raise ValueError("generation_positive")
        if not self.unit_ids:
            raise ValueError("unit_ids_required")
        _unique_non_blank(self.unit_ids, "duplicate_unit_id")
        if self.unit_ids != tuple(sorted(self.unit_ids)):
            raise ValueError("unit_ids_not_ordered")
        if self.state is DispatchState.STARTED and not self.execution_ref:
            raise ValueError("execution_ref_required")
        if self.state is DispatchState.RESERVED and self.execution_ref is not None:
            raise ValueError("execution_ref_forbidden")
        if self.state is DispatchState.TERMINAL and self.terminal_outcome is None:
            raise ValueError("terminal_outcome_required")
        if self.state is not DispatchState.TERMINAL and self.terminal_outcome is not None:
            raise ValueError("terminal_outcome_forbidden")
        return self


class DatasetVersion(_ControlPlaneModel):
    tenant_id: str
    dataset_name: str
    version_id: str
    run_id: str
    run_manifest_key: str
    created_at: datetime
    _components = field_validator("tenant_id", "version_id", "run_id")(_require_key_component)
    _dataset = field_validator("dataset_name")(_require_non_blank)
    _created_utc = field_validator("created_at")(_require_utc)

    @model_validator(mode="after")
    def _validate_version(self) -> DatasetVersion:
        if self.version_id != self.run_id:
            raise ValueError("version_run_mismatch")
        pattern = re.compile(
            rf"^reconciliation/{re.escape(self.tenant_id)}/"
            rf"\d{{4}}-(?:0[1-9]|1[0-2])/{re.escape(self.run_id)}/run-manifest\.json$"
        )
        if not pattern.fullmatch(self.run_manifest_key):
            raise ValueError("invalid_run_manifest_key")
        return self


class DatasetPointer(_ControlPlaneModel):
    tenant_id: str
    dataset_name: str
    pointer_name: str
    version_id: str
    updated_at: datetime
    _strings = field_validator("tenant_id", "dataset_name", "pointer_name", "version_id")(
        _require_non_blank
    )
    _updated_utc = field_validator("updated_at")(_require_utc)


class AccessRequest(_ControlPlaneModel):
    tenant_id: str
    request_id: str
    user_id: str
    state: AccessRequestState
    decided_by: str | None
    decided_at: datetime | None
    _strings = field_validator("tenant_id", "request_id", "user_id")(_require_non_blank)

    _decided_by_value = field_validator("decided_by")(_optional_non_blank)
    _decided_utc = field_validator("decided_at")(_optional_utc)

    @model_validator(mode="after")
    def _validate_decision(self) -> AccessRequest:
        decided = self.decided_by is not None and self.decided_at is not None
        partial = (self.decided_by is None) != (self.decided_at is None)
        if partial:
            raise ValueError("decision_pair_required")
        if self.state is AccessRequestState.PENDING and decided:
            raise ValueError("decision_forbidden")
        if self.state is not AccessRequestState.PENDING and not decided:
            raise ValueError("decision_required")
        return self


class IdempotencyRecord(_ControlPlaneModel):
    tenant_id: str
    scope: str
    key: str
    request_hash: str
    status: str
    resource_id: str
    created_at: datetime
    expires_at: datetime
    _strings = field_validator("tenant_id", "scope", "key", "status", "resource_id")(
        _require_non_blank
    )
    _request_hash = field_validator("request_hash")(_require_sha256)
    _datetimes = field_validator("created_at", "expires_at")(_require_utc)

    @model_validator(mode="after")
    def _validate_expiry(self) -> IdempotencyRecord:
        if self.expires_at <= self.created_at:
            raise ValueError("invalid_expiry")
        return self


class OutboxEvent(_ControlPlaneModel):
    tenant_id: str
    event_id: str
    event_type: str
    aggregate_id: str
    payload: dict[str, JsonValue]
    created_at: datetime
    delivered_at: datetime | None
    _strings = field_validator("tenant_id", "event_id", "event_type", "aggregate_id")(
        _require_non_blank
    )
    _payload_value = field_validator("payload")(_require_finite_json)
    _datetimes = field_validator("created_at", "delivered_at")(_optional_utc)

    def __getattribute__(self, name: str) -> object:
        value = super().__getattribute__(name)
        return deepcopy(value) if name == "payload" else value
