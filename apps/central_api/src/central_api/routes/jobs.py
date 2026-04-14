"""Rotas de jobs: status, acquire, heartbeat, complete-upload."""

import logging
import uuid
from datetime import UTC

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from sqlalchemy.engine import Engine

from central_api.deps import get_engine, get_object_storage
from cnes_domain.models.api import (
    AcquireJobRequest,
    AcquireJobResponse,
    CompleteUploadRequest,
    HeartbeatRequest,
    HeartbeatResponse,
    JobStatusResponse,
)
from cnes_infra.storage.job_queue import (
    acquire_for_agent,
    complete_upload,
    get_status,
    renew_heartbeat,
    transition_to_streaming,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["jobs"])

_LEASE_MINUTES = 15


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
def get_job_status(
    job_id: str,
    engine: Engine = Depends(get_engine),
) -> JobStatusResponse:
    result = get_status(engine, job_id)
    if result is None:
        raise HTTPException(status_code=404, detail="job_not_found")
    return JobStatusResponse(**result)


@router.post("/jobs/acquire", response_model=None)
def acquire_job(
    body: AcquireJobRequest,
    engine: Engine = Depends(get_engine),
    storage: object = Depends(get_object_storage),
) -> AcquireJobResponse | Response:
    from cnes_infra import config

    job = acquire_for_agent(
        engine, body.machine_id, body.source_system,
    )
    if job is None:
        return Response(status_code=204)

    obj_key = (
        f"{job.tenant_id}/{job.source_system}/{job.id}.parquet.gz"
    )
    upload_url = storage.generate_presigned_upload_url(
        config.MINIO_BUCKET, obj_key,
    )

    from datetime import datetime, timedelta
    lease_exp = datetime.now(UTC) + timedelta(
        minutes=_LEASE_MINUTES,
    )

    return AcquireJobResponse(
        job_id=job.id,
        source_system=job.source_system,
        tenant_id=job.tenant_id,
        upload_url=upload_url,
        object_key=obj_key,
        lease_expires_at=lease_exp,
    )


@router.post("/jobs/{job_id}/heartbeat")
def heartbeat(
    job_id: uuid.UUID,
    body: HeartbeatRequest,
    engine: Engine = Depends(get_engine),
) -> HeartbeatResponse:
    renewed = renew_heartbeat(engine, job_id, body.machine_id)
    if not renewed:
        raise HTTPException(
            status_code=409, detail="lease_not_found_or_mismatch",
        )
    from datetime import datetime, timedelta
    lease_exp = datetime.now(UTC) + timedelta(
        minutes=_LEASE_MINUTES,
    )
    return HeartbeatResponse(renewed=True, lease_expires_at=lease_exp)


@router.post("/jobs/{job_id}/streaming")
def start_streaming(
    job_id: uuid.UUID,
    body: HeartbeatRequest,
    engine: Engine = Depends(get_engine),
) -> Response:
    ok = transition_to_streaming(engine, job_id, body.machine_id)
    if not ok:
        raise HTTPException(
            status_code=409, detail="transition_failed",
        )
    return Response(status_code=200)


@router.post("/jobs/{job_id}/complete-upload")
def complete_upload_route(
    job_id: uuid.UUID,
    body: CompleteUploadRequest,
    engine: Engine = Depends(get_engine),
) -> Response:
    ok = complete_upload(
        engine, job_id, body.machine_id, body.object_key,
    )
    if not ok:
        raise HTTPException(
            status_code=409, detail="complete_failed",
        )
    logger.info("upload_accepted job_id=%s key=%s", job_id, body.object_key)
    return Response(status_code=200)
