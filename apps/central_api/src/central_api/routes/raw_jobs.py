"""Rotas autenticadas de jobs e upload raw do Edge Agent."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hmac import compare_digest
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response

from central_api.schemas.raw_api import (
    EdgeIdentity,
    EdgeJobResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    RawUploadResponse,
)
from central_api.services.raw_ingestion import RawIngestionService  # noqa: TC001
from central_api.services.raw_upload import (
    RawUploadConflict,
    RawUploadEmpty,
    RawUploadError,
    RawUploadIdentityRejected,
    RawUploadKeyRejected,
    RawUploadNotFound,
    RawUploadRequest,
    RawUploadService,
    RawUploadTooLarge,
)
from cnes_domain.control_plane.commands import ClaimJob, RenewJobLease
from cnes_domain.control_plane.enums import AgentState
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost, NotFound
from cnes_domain.ports.control_plane import ControlPlanePort  # noqa: TC001

if TYPE_CHECKING:
    from cnes_domain.control_plane.entities import Job

EDGE_JOB_LEASE_SECONDS = 300
router = APIRouter(prefix="/api/v1/edge", tags=["edge-raw"])
_ERROR_ALIASES = {
    "fence_mismatch": "job_fence_rejected",
    "owner_mismatch": "job_owner_lost",
    "lease_expired": "job_lease_expired",
    "manifest_identity_mismatch": "manifest_identity_conflict",
}


@dataclass(frozen=True, slots=True)
class _RawUploadBody:
    request: Request
    fencing_token: int
    object_key: str


def _raw_upload_body(
    request: Request,
    x_fencing_token: Annotated[str, Header(alias="X-Fencing-Token")],
    x_object_key: Annotated[str, Header(alias="X-Object-Key")],
    content_type: Annotated[str, Header(alias="Content-Type")],
) -> _RawUploadBody:
    if content_type != "application/octet-stream":
        raise HTTPException(status_code=415, detail="media_type_unsupported")
    try:
        token = int(x_fencing_token)
    except ValueError as error:
        raise HTTPException(status_code=422, detail="fencing_token_required") from error
    if token < 0:
        raise HTTPException(status_code=422, detail="fencing_token_required")
    return _RawUploadBody(request, token, x_object_key)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def get_edge_identity() -> EdgeIdentity:
    """Falha fechado até o terminador mTLS fornecer a identidade."""

    raise HTTPException(status_code=401, detail="mtls_required")


def get_control_plane() -> ControlPlanePort:
    """Falha fechado até a composição fornecer o control plane."""

    raise HTTPException(status_code=503, detail="control_plane_not_configured")


def get_raw_upload_service() -> RawUploadService:
    """Falha fechado até a composição fornecer o upload raw."""

    raise HTTPException(status_code=503, detail="raw_upload_not_configured")


def get_raw_ingestion_service() -> RawIngestionService:
    """Falha fechado até a composição fornecer a ingestão raw."""

    raise HTTPException(status_code=503, detail="raw_ingestion_not_configured")


def require_edge_agent(
    identity: Annotated[EdgeIdentity, Depends(get_edge_identity)],
    control_plane: Annotated[ControlPlanePort, Depends(get_control_plane)],
) -> EdgeIdentity:
    """Valida o agente persistido contra a identidade mTLS."""

    agent = control_plane.get_agent(identity.tenant_id, identity.agent_id)
    if agent is None:
        raise HTTPException(status_code=403, detail="agent_missing")
    if agent.state is AgentState.REVOKED:
        raise HTTPException(status_code=403, detail="agent_revoked")
    if (agent.tenant_id, agent.agent_id) != (identity.tenant_id, identity.agent_id):
        raise HTTPException(status_code=403, detail="agent_identity_mismatch")
    if not compare_digest(agent.certificate_fingerprint, identity.certificate_fingerprint):
        raise HTTPException(status_code=403, detail="certificate_fingerprint_mismatch")
    return identity


@router.get("/jobs/next", response_model=EdgeJobResponse, responses={204: {}})
def next_job(
    identity: Annotated[EdgeIdentity, Depends(require_edge_agent)],
    control_plane: Annotated[ControlPlanePort, Depends(get_control_plane)],
) -> EdgeJobResponse | Response:
    """Adquire por CAS o primeiro entre até dez jobs elegíveis."""

    candidates = control_plane.list_claimable_jobs(identity.tenant_id, identity.agent_id, 10)
    now = _utc_now()
    for candidate in candidates:
        if (candidate.tenant_id, candidate.agent_id) != (
            identity.tenant_id,
            identity.agent_id,
        ):
            continue
        claimed = control_plane.claim_job(
            ClaimJob(
                tenant_id=identity.tenant_id,
                job_id=candidate.job_id,
                owner=identity.agent_id,
                now=now,
                lease_seconds=EDGE_JOB_LEASE_SECONDS,
            )
        )
        if claimed is not None:
            return _job_response(claimed, identity)
    return Response(status_code=204)


@router.post("/jobs/{job_id}/heartbeat", response_model=HeartbeatResponse)
def heartbeat(
    job_id: str,
    body: HeartbeatRequest,
    identity: Annotated[EdgeIdentity, Depends(require_edge_agent)],
    control_plane: Annotated[ControlPlanePort, Depends(get_control_plane)],
) -> HeartbeatResponse:
    """Renova por 300 segundos o owner e fence apresentados."""

    job = control_plane.get_job(identity.tenant_id, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="job_missing")
    if (job.tenant_id, job.agent_id) != (identity.tenant_id, identity.agent_id):
        raise HTTPException(status_code=403, detail="job_identity_mismatch")
    command = RenewJobLease(
        tenant_id=identity.tenant_id,
        job_id=job_id,
        owner=identity.agent_id,
        fencing_token=body.fencing_token,
        now=_utc_now(),
        lease_seconds=EDGE_JOB_LEASE_SECONDS,
    )
    try:
        renewed = control_plane.renew_job_lease(command)
    except NotFound as error:
        raise HTTPException(status_code=404, detail=_error_code(error)) from error
    except (FenceRejected, LeaseLost, Conflict) as error:
        raise HTTPException(status_code=409, detail=_error_code(error)) from error
    if renewed.lease_until is None:
        raise HTTPException(status_code=409, detail="job_not_leased")
    return HeartbeatResponse(
        job_id=renewed.job_id,
        fencing_token=renewed.fencing_token,
        lease_until=renewed.lease_until,
    )


@router.put("/jobs/{job_id}/raw-object", response_model=RawUploadResponse)
async def upload_raw_object(
    job_id: str,
    identity: Annotated[EdgeIdentity, Depends(require_edge_agent)],
    body: Annotated[_RawUploadBody, Depends(_raw_upload_body)],
    service: Annotated[RawUploadService, Depends(get_raw_upload_service)],
) -> RawUploadResponse:
    """Transmite um objeto raw sem carregar o payload inteiro em memória."""

    upload = RawUploadRequest(
        tenant_id=identity.tenant_id,
        agent_id=identity.agent_id,
        job_id=job_id,
        fencing_token=body.fencing_token,
        object_key=body.object_key,
    )
    try:
        stat = await service.upload(upload, body.request.stream())
    except RawUploadError as error:
        raise HTTPException(status_code=_upload_status(error), detail=error.code) from error
    return RawUploadResponse(
        object_key=stat.key,
        object_sha256=stat.sha256,
        size_bytes=stat.size_bytes,
    )


def _job_response(job: Job, identity: EdgeIdentity) -> EdgeJobResponse:
    if (job.tenant_id, job.agent_id) != (identity.tenant_id, identity.agent_id):
        raise HTTPException(status_code=409, detail="job_identity_mismatch")
    if job.lease_until is None:
        raise HTTPException(status_code=409, detail="job_not_leased")
    return EdgeJobResponse(
        job_id=job.job_id,
        source_type=job.source_type,
        file_subtype=job.file_subtype,
        competencia=job.competencia,
        requested_snapshot_mode=job.requested_snapshot_mode,
        fencing_token=job.fencing_token,
        lease_until=job.lease_until,
        raw_upload_path=f"/api/v1/edge/jobs/{job.job_id}/raw-object",
    )


def _upload_status(error: RawUploadError) -> int:
    if isinstance(error, RawUploadTooLarge):
        return 413
    if isinstance(error, RawUploadEmpty):
        return 422
    if isinstance(error, RawUploadConflict):
        return 409
    if isinstance(error, RawUploadNotFound):
        return 404
    if isinstance(error, (RawUploadIdentityRejected, RawUploadKeyRejected)):
        return 409
    return 409


def _error_code(error: Exception) -> str:
    code = getattr(error, "code", None)
    normalized = getattr(code, "value", code) or str(error)
    return _ERROR_ALIASES.get(normalized, normalized)
