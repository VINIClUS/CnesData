"""Rotas administrativas dos jobs raw."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from central_api.deps import require_admin_token
from central_api.routes.raw_jobs import get_control_plane
from central_api.services.raw_enqueue import RawEnqueueRequest, RawEnqueueService
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.ports.control_plane import ControlPlanePort  # noqa: TC001

router = APIRouter(prefix="/api/v1/admin/raw-jobs", tags=["admin-raw"])


class RawEnqueueResponse(BaseModel):
    job_ids: list[str]


@router.post("/enqueue", response_model=RawEnqueueResponse, status_code=201)
def enqueue_raw_jobs(
    request: RawEnqueueRequest,
    key: Annotated[str, Header(alias="Idempotency-Key")],
    control: Annotated[ControlPlanePort, Depends(get_control_plane)],
    _: Annotated[None, Depends(require_admin_token)],
) -> RawEnqueueResponse:
    if not key.strip():
        raise HTTPException(status_code=422, detail="idempotency_key_required")
    try:
        job_ids = RawEnqueueService(control, _utc_now).enqueue(request, key)
        return RawEnqueueResponse(job_ids=job_ids)
    except Conflict as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


def _utc_now():
    from datetime import UTC, datetime

    return datetime.now(UTC)
