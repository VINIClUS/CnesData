"""Rotas /api/v1/jobs/* — lifecycle de landing.extractions (Gold v2)."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID  # noqa: TC003 - runtime FastAPI path param

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from central_api.deps import get_conn, get_minio
from cnes_contracts.landing import SOURCE_TYPE, ExtractionRegisterPayload
from cnes_infra.storage import extractions_repo

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

    from central_api.deps import MinioWrapper

logger = logging.getLogger(__name__)

router = APIRouter(tags=["jobs"])


class RegisterRequest(BaseModel):
    tenant_id: str = Field(pattern=r"^\d{6}$")
    fonte_sistema: SOURCE_TYPE
    tipo_extracao: str
    competencia: int = Field(ge=200001, le=209912)
    job_id: UUID
    agent_version: str
    machine_id: str


class RegisterResponse(BaseModel):
    extraction_id: UUID
    upload_url: str


class CompletePayload(BaseModel):
    sha256: str
    row_count: int


class FailPayload(BaseModel):
    error: str


@router.post(
    "/jobs/register",
    response_model=RegisterResponse,
    status_code=status.HTTP_201_CREATED,
)
def register_extraction(
    payload: RegisterRequest,
    conn: Connection = Depends(get_conn),
    minio: MinioWrapper = Depends(get_minio),
) -> RegisterResponse:
    contract = ExtractionRegisterPayload.model_validate(payload.model_dump())
    extraction_id, object_key = extractions_repo.register(
        conn, contract, bucket=minio.bucket,
    )
    try:
        upload_url = minio.presigned_put(object_key, expires=3600)
    except Exception as exc:
        logger.exception("minio_presign_failed err=%s", exc)
        raise HTTPException(
            status_code=503, detail="minio_presign_failed",
        ) from exc
    return RegisterResponse(
        extraction_id=extraction_id, upload_url=upload_url,
    )


@router.get("/jobs/next")
def next_extraction(
    processor_id: str,
    lease_secs: int = 300,
    conn: Connection = Depends(get_conn),
) -> dict:
    ext = extractions_repo.claim_next(
        conn, processor_id=processor_id, lease_secs=lease_secs,
    )
    if ext is None:
        return {"extraction": None}
    return {"extraction": ext.model_dump(mode="json")}


@router.post("/jobs/{extraction_id}/complete")
def complete_extraction(
    extraction_id: UUID,
    payload: CompletePayload,
    conn: Connection = Depends(get_conn),
) -> dict:
    extractions_repo.mark_uploaded(
        conn, extraction_id, payload.sha256, payload.row_count,
    )
    return {"status": "UPLOADED"}


@router.post("/jobs/{extraction_id}/fail")
def fail_extraction(
    extraction_id: UUID,
    payload: FailPayload,
    conn: Connection = Depends(get_conn),
) -> dict:
    extractions_repo.fail(conn, extraction_id, payload.error)
    return {"status": "FAILED"}


@router.post("/jobs/{extraction_id}/heartbeat")
def heartbeat_extraction(
    extraction_id: UUID,
    processor_id: str,
    conn: Connection = Depends(get_conn),
) -> dict:
    extractions_repo.heartbeat(conn, extraction_id, processor_id)
    return {"status": "heartbeat_ok"}
