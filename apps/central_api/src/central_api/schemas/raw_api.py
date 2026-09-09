"""Schemas HTTP do protocolo raw do Edge Agent."""

from __future__ import annotations

import json
from typing import Any

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from cnes_contracts import RawManifest


class _FrozenSchema(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")


class EdgeIdentity(_FrozenSchema):
    """Identidade autenticada do certificado mTLS."""

    tenant_id: str = Field(min_length=1)
    agent_id: str = Field(min_length=1)
    certificate_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")


class EdgeJobResponse(_FrozenSchema):
    """Job adquirido pelo Edge Agent."""

    job_id: str
    source_type: str
    file_subtype: str
    competencia: str
    requested_snapshot_mode: str
    fencing_token: int
    lease_until: AwareDatetime
    raw_upload_path: str


class HeartbeatRequest(_FrozenSchema):
    """Fence apresentado para renovação do lease."""

    fencing_token: int = Field(ge=0)


class HeartbeatResponse(_FrozenSchema):
    """Lease renovado do job."""

    job_id: str
    fencing_token: int
    lease_until: AwareDatetime


class RawUploadResponse(_FrozenSchema):
    """Recibo do objeto raw persistido."""

    object_key: str
    object_sha256: str
    size_bytes: int


class RawManifestSubmission(_FrozenSchema):
    """Envelope autenticado de submissão do manifesto raw."""

    job_id: str = Field(min_length=1)
    fencing_token: int = Field(ge=0)
    manifest: RawManifest

    @field_validator("manifest", mode="before")
    @classmethod
    def normalize_manifest(cls, value: Any) -> RawManifest:
        if isinstance(value, RawManifest):
            return value
        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        return RawManifest.model_validate_json(payload)


class RawManifestResponse(_FrozenSchema):
    """Resultado canônico do registro do manifesto raw."""

    accepted: bool
    manifest_id: str
    manifest_sha256: str
    full_resync_required: bool
    reason: str | None
