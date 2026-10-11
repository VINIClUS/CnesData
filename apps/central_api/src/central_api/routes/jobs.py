"""Rotas /api/v1/jobs/* — aposentadas (MIG-012): respondem 410 legacy_ingestion_retired."""
from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID  # noqa: TC003

from fastapi import APIRouter, Body, Depends

from central_api.agent_auth import AgentCertIdentity, agent_identity_if_required
from central_api.deps import legacy_ingestion_retired

router = APIRouter(tags=["jobs"])


_Caller = Annotated[AgentCertIdentity | None, Depends(agent_identity_if_required)]
_Retired = Annotated[None, Depends(legacy_ingestion_retired)]


@router.post("/jobs/upload-url", status_code=201)
def mint_upload_url(
    body: Annotated[dict[str, Any], Body()],
    caller: _Caller,
    retired: _Retired,
) -> dict[Any, Any]:
    legacy_ingestion_retired()


@router.post("/jobs/register")
def register_job(
    body: Annotated[dict[str, Any], Body()],
    caller: _Caller,
    retired: _Retired,
) -> dict[Any, Any]:
    legacy_ingestion_retired()


@router.post("/jobs/{job_id}/fail")
def fail_job(
    job_id: UUID,
    body: Annotated[dict[str, Any], Body()],
    caller: _Caller,
    retired: _Retired,
) -> dict[Any, Any]:
    legacy_ingestion_retired()
