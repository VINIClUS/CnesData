"""Versioned output and serving manifest contracts."""

from __future__ import annotations

from datetime import datetime  # noqa: TC003
from math import isfinite
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cnes_contracts.manifests.raw import SourceType  # noqa: TC001
from cnes_contracts.manifests.validation import (
    _COMPETENCIA_PATTERN,
    _HASH_PATTERN,
    _validate_utc,
    validate_object_key,
)

type JsonValue = bool | int | float | str | list[JsonValue] | dict[str, JsonValue] | None

_SCHEMA_PATTERN = r"^[a-z0-9-]+-v[1-9][0-9]*$"
_DOCUMENT_PATTERN = r"^[a-z0-9][a-z0-9_-]*$"


def _is_json_value(value: object) -> bool:
    if value is None or isinstance(value, bool | int | str):
        return True
    if isinstance(value, float):
        return isfinite(value)
    if isinstance(value, list):
        return all(_is_json_value(item) for item in value)
    if isinstance(value, dict):
        return all(isinstance(key, str) and _is_json_value(item) for key, item in value.items())
    return False


class OutputManifest(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")

    manifest_version: Literal[1]
    manifest_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    layer: Literal["normalized", "reconciliation", "serving"]
    source_type: SourceType | None
    competencia: str = Field(pattern=_COMPETENCIA_PATTERN)
    run_id: str = Field(min_length=1)
    unit_id: str = Field(min_length=1)
    attempt: int = Field(gt=0)
    schema_version: str = Field(min_length=1)
    object_key: str = Field(min_length=1)
    object_sha256: str = Field(pattern=_HASH_PATTERN)
    row_count: int = Field(ge=0)
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: datetime) -> datetime:
        return _validate_utc(value)

    @model_validator(mode="after")
    def validate_layer(self) -> Self:
        if self.layer == "normalized" and self.source_type is None:
            raise ValueError("source_type_required")
        if self.layer != "normalized" and self.source_type is not None:
            raise ValueError("source_type_forbidden")
        validate_object_key(self)
        return self


class RunManifest(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")

    manifest_version: Literal[1]
    tenant_id: str = Field(min_length=1)
    dataset_name: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    competencia: str = Field(pattern=_COMPETENCIA_PATTERN)
    outputs: tuple[OutputManifest, ...]
    missing_sources: tuple[str, ...]
    published_at: datetime

    @field_validator("published_at")
    @classmethod
    def validate_published_at(cls, value: datetime) -> datetime:
        return _validate_utc(value)

    @model_validator(mode="after")
    def validate_outputs(self) -> Self:
        if not self.outputs:
            raise ValueError("outputs_required")
        manifest_ids = {item.manifest_id for item in self.outputs}
        object_keys = {item.object_key for item in self.outputs}
        if len(manifest_ids) != len(self.outputs) or len(object_keys) != len(self.outputs):
            raise ValueError("outputs_unique")
        expected = (self.tenant_id, self.run_id, self.competencia)
        if any(
            (item.tenant_id, item.run_id, item.competencia) != expected for item in self.outputs
        ):
            raise ValueError("output_identity")
        if len(set(self.missing_sources)) != len(self.missing_sources):
            raise ValueError("missing_sources_unique")
        return self


class ServingDocument(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")

    schema_version: str = Field(pattern=_SCHEMA_PATTERN)
    document_name: str = Field(pattern=_DOCUMENT_PATTERN)
    tenant_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    generated_at: datetime
    payload: dict[str, JsonValue]

    @field_validator("generated_at")
    @classmethod
    def validate_generated_at(cls, value: datetime) -> datetime:
        return _validate_utc(value)

    @field_validator("payload", mode="before")
    @classmethod
    def validate_payload(cls, value: object) -> object:
        if not isinstance(value, dict) or not _is_json_value(value):
            raise ValueError("payload_json_required")
        return value
