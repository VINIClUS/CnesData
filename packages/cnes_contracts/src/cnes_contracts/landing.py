"""Landing table contracts."""
from __future__ import annotations

from datetime import datetime  # noqa: TC003
from typing import Literal
from uuid import UUID  # noqa: TC003

from pydantic import BaseModel, ConfigDict, Field

from cnes_contracts.jobs import JobStatus  # noqa: TC001

FonteSistema = Literal[
    "CNES_LOCAL",
    "CNES_NACIONAL",
    "SIHD",
    "SIA_APA",
    "SIA_BPI",
    "BPA_C",
    "BPA_I",
]


class Extraction(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    id: UUID
    job_id: UUID
    tenant_id: str = Field(pattern=r"^\d{6}$")
    fonte_sistema: FonteSistema
    tipo_extracao: str
    competencia: int = Field(ge=200001, le=209912)
    object_key: str | None = None
    row_count: int | None = Field(default=None, ge=0)
    sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    schema_version: int = Field(default=1, ge=1)
    status: JobStatus
    agent_version: str | None = None
    machine_id: str | None = None
    lease_owner: str | None = None
    lease_until: datetime | None = None
    retry_count: int = Field(default=0, ge=0)
    error_detail: str | None = None
    created_at: datetime
    uploaded_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None


class ExtractionRegisterPayload(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    tenant_id: str = Field(pattern=r"^\d{6}$")
    fonte_sistema: FonteSistema
    tipo_extracao: str
    competencia: int = Field(ge=200001, le=209912)
    job_id: UUID
    agent_version: str
    machine_id: str
