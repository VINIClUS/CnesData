"""Modelos Pydantic para a API de ingestão."""

import uuid
from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class IngestPayload(BaseModel):
    """Payload de ingestão recebido via POST."""

    tenant_id: str = Field(min_length=6, max_length=6)
    competencia: str = Field(min_length=7, max_length=7)
    registros: list[dict]

    @field_validator("tenant_id")
    @classmethod
    def tenant_id_numerico(cls, v: str) -> str:
        if not v.isdigit():
            raise ValueError("tenant_id deve conter apenas dígitos")
        return v

    @field_validator("competencia")
    @classmethod
    def competencia_formato(cls, v: str) -> str:
        parts = v.split("-")
        if len(parts) != 2 or len(parts[0]) != 4 or len(parts[1]) != 2:
            raise ValueError("competencia deve estar no formato YYYY-MM")
        return v


class IngestResponse(BaseModel):
    job_id: uuid.UUID
    mensagem: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    source_system: str
    tenant_id: str
    created_at: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    error_detail: str | None = None


class HealthResponse(BaseModel):
    status: str
    db_connected: bool
    timestamp: datetime
