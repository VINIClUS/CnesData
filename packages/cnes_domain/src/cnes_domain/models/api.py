"""Modelos Pydantic para a API de jobs e streaming."""

import uuid
from datetime import datetime

from pydantic import BaseModel, Field


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    source_system: str
    tenant_id: str
    created_at: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    error_detail: str | None = None


class AcquireJobRequest(BaseModel):
    machine_id: str = Field(min_length=1, max_length=128)
    source_system: str | None = None


class AcquireJobResponse(BaseModel):
    job_id: uuid.UUID
    source_system: str
    tenant_id: str
    upload_url: str
    object_key: str
    lease_expires_at: datetime


class HeartbeatRequest(BaseModel):
    machine_id: str = Field(min_length=1, max_length=128)


class HeartbeatResponse(BaseModel):
    renewed: bool
    lease_expires_at: datetime | None = None


class CompleteUploadRequest(BaseModel):
    machine_id: str = Field(min_length=1, max_length=128)
    object_key: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)


class HealthResponse(BaseModel):
    status: str
    db_connected: bool
    timestamp: datetime
