"""Landing table contracts."""
from __future__ import annotations

from datetime import date, datetime  # noqa: TC003
from typing import Literal
from uuid import UUID  # noqa: TC003

from pydantic import BaseModel, ConfigDict, Field

from cnes_contracts.jobs import JobStatus  # noqa: TC001

FATO_SUBTYPE = Literal[
    "CNES_VINCULO", "SIHD_INTERNACAO", "SIHD_PROC_AIH",
    "BPA_C", "BPA_I",
    "SIA_APA", "SIA_BPI", "SIA_BPIHST",
    "DIM_SIGTAP", "DIM_MUNICIPIO",
]

SOURCE_TYPE = Literal[
    "CNES_LOCAL", "CNES_NACIONAL", "SIHD", "BPA_MAG", "SIA_LOCAL",
]


class FileManifest(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    minio_key: str = Field(pattern=r"^[\w\-./]+\.parquet\.gz$")
    fato_subtype: FATO_SUBTYPE
    size_bytes: int = Field(gt=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class Extraction(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    job_id: UUID
    tenant_id: str = Field(min_length=1, max_length=64)
    source_type: SOURCE_TYPE
    competencia: date
    files: list[FileManifest] = Field(min_length=1)
    depends_on: list[UUID] = Field(default_factory=list)
    status: JobStatus
    lease_until: datetime | None = None
    created_at: datetime
    registered_at: datetime | None = None


class ExtractionRegisterPayload(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    job_id: UUID
    files: list[FileManifest] = Field(min_length=1)
