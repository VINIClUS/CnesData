"""Job status enum + transition events."""
from __future__ import annotations

from datetime import datetime  # noqa: TC003
from enum import StrEnum
from uuid import UUID  # noqa: TC003

from pydantic import BaseModel, ConfigDict


class JobStatus(StrEnum):
    PENDING = "PENDING"
    UPLOADED = "UPLOADED"
    PROCESSING = "PROCESSING"
    INGESTED = "INGESTED"
    FAILED = "FAILED"
    DLQ = "DLQ"


class JobTransitionEvent(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    extraction_id: UUID
    from_status: JobStatus
    to_status: JobStatus
    actor: str
    reason: str | None = None
    at: datetime
