"""POST /extractions/enqueue — aposentada (MIG-012): responde 410 legacy_ingestion_retired."""
from __future__ import annotations

from datetime import date  # noqa: TC003
from uuid import UUID  # noqa: TC003

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, ConfigDict

from central_api.deps import legacy_ingestion_retired, require_admin_token
from cnes_contracts.landing import SOURCE_TYPE  # noqa: TC001

router = APIRouter(prefix="/extractions", tags=["extractions"])


class EnqueueRequest(BaseModel):
    model_config = ConfigDict(strict=False)

    source_type: SOURCE_TYPE
    tenant_id: str
    competencia: date


class EnqueueResponse(BaseModel):
    job_ids: list[UUID]


@router.post(
    "/enqueue",
    response_model=EnqueueResponse,
    status_code=status.HTTP_201_CREATED,
)
def enqueue(
    req: EnqueueRequest,
    _: None = Depends(require_admin_token),
    retired: None = Depends(legacy_ingestion_retired),
) -> EnqueueResponse:
    legacy_ingestion_retired()
