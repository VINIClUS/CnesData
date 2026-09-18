"""Versioned raw manifest contracts."""

from __future__ import annotations

from datetime import datetime  # noqa: TC003
from enum import StrEnum
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from cnes_contracts.manifests.validation import (
    _COMPETENCIA_PATTERN,
    _HASH_PATTERN,
    _validate_utc,
    validate_object_key,
)


class SourceType(StrEnum):
    CNES_LOCAL = "CNES_LOCAL"
    CNES_NACIONAL = "CNES_NACIONAL"
    SIHD = "SIHD"
    BPA_MAG = "BPA_MAG"
    SIA_LOCAL = "SIA_LOCAL"


class SnapshotMode(StrEnum):
    FULL = "FULL"
    DELTA = "DELTA"


class RawManifest(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")

    manifest_version: Literal[1]
    manifest_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    source_type: SourceType
    file_subtype: str = Field(min_length=1)
    competencia: str = Field(pattern=_COMPETENCIA_PATTERN)
    agent_id: str = Field(min_length=1)
    agent_version: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    snapshot_mode: SnapshotMode
    snapshot_id: str = Field(min_length=1)
    base_snapshot_id: str | None
    sequence: int = Field(gt=0)
    previous_manifest_sha256: str | None = Field(pattern=_HASH_PATTERN)
    object_sha256: str = Field(pattern=_HASH_PATTERN)
    row_count: int = Field(ge=0)
    size_bytes: int = Field(gt=0)
    object_key: str = Field(min_length=1)
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def validate_created_at(cls, value: datetime) -> datetime:
        return _validate_utc(value)

    @model_validator(mode="after")
    def validate_chain(self) -> Self:
        if self.snapshot_mode is SnapshotMode.FULL:
            if self.sequence != 1 or self.base_snapshot_id is not None:
                raise ValueError("full_chain_invalid")
            if self.previous_manifest_sha256 is not None:
                raise ValueError("full_chain_invalid")
        elif (
            self.sequence < 2 or not self.base_snapshot_id or self.previous_manifest_sha256 is None
        ):
            raise ValueError("delta_chain_required")
        validate_object_key(self)
        return self
