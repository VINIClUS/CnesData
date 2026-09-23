"""Rotas /api/v1/jobs/* — lifecycle de landing.extractions (Gold v2)."""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any
from uuid import UUID  # noqa: TC003

from fastapi import APIRouter, Body, Depends, Header, HTTPException
from pydantic import ValidationError

from central_api.agent_auth import AgentCertIdentity, agent_identity_if_required
from central_api.deps import get_engine, get_object_storage
from central_api.validation_errors import validation_error
from cnes_contracts.landing import (
    ExtractionFailPayload,
    ExtractionRegisterPayload,
    UploadUrlRequest,
    UploadUrlResponse,
)
from cnes_domain.tenant import set_tenant_id
from cnes_infra import config
from cnes_infra.storage import extractions_repo

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

router = APIRouter(tags=["jobs"])



@dataclass(frozen=True, slots=True)
class _JobCaller:
    identity: AgentCertIdentity | None
    header_tenant_id: str | None

    @property
    def tenant_id(self) -> str | None:
        return self.identity.tenant_id if self.identity else self.header_tenant_id


def _job_caller(
    identity: Annotated[AgentCertIdentity | None, Depends(agent_identity_if_required)],
    x_tenant_id: Annotated[
        str | None, Header(alias="X-Tenant-Id", include_in_schema=False)
    ] = None,
) -> _JobCaller:
    return _JobCaller(identity=identity, header_tenant_id=x_tenant_id)


_Caller = Annotated[_JobCaller, Depends(_job_caller)]


_FATO_SUBTYPE_FOR: dict[tuple[str, str], str] = {
    ("CNES_LOCAL", "cnes_profissionais"): "CNES_VINCULO",
    ("CNES_LOCAL", "cnes_estabelecimentos"): "CNES_VINCULO",
    ("CNES_LOCAL", "cnes_equipes"): "CNES_VINCULO",
    ("CNES_NACIONAL", "cnes_profissionais"): "CNES_VINCULO",
    ("SIHD", "sihd_producao"): "SIHD_INTERNACAO",
}

_UPLOAD_URL_TTL_SECONDS: int = 3600


def _object_storage():
    return get_object_storage()


def _resolve_fato_subtype(source_type: str, intent: str) -> str:
    subtype = _FATO_SUBTYPE_FOR.get((source_type, intent))
    if subtype is None:
        raise validation_error(
            f"unsupported_source_intent={source_type}/{intent}",
            loc=["body", "intent"],
        )
    return subtype


def _bind_identity(
    caller: _JobCaller,
    *,
    tenant_id: str | None,
    machine_id: str | None,
) -> str | None:
    identity = caller.identity
    if identity is None:
        # AGENT_MTLS_REQUIRED=false (local stack only): no credential exists,
        # so the body/header tenant is the only tenant source.
        unauthenticated_tenant = tenant_id or caller.header_tenant_id
        if unauthenticated_tenant:
            set_tenant_id(unauthenticated_tenant)
        return machine_id
    set_tenant_id(identity.tenant_id)
    if tenant_id is not None and tenant_id != identity.tenant_id:
        raise HTTPException(status_code=403, detail="agent_identity_mismatch")
    # Not a 403: on Linux machine_id is per-user state, so `register` (CN) and
    # the service can legitimately disagree. The cert CN is what gets recorded.
    if machine_id is not None and machine_id != identity.machine_id:
        logger.warning(
            "agent_machine_id_mismatch agent_id=%s body=%s cert=%s",
            identity.agent_id, machine_id, identity.machine_id,
        )
    return identity.machine_id


def _build_minio_key(payload: UploadUrlRequest, fato_subtype: str) -> str:
    return (
        f"{payload.tenant_id}/{fato_subtype}/{payload.competencia}/"
        f"{payload.job_id}.parquet.gz"
    )


@router.post("/jobs/upload-url", status_code=201)
def mint_upload_url(
    body: Annotated[dict[str, Any], Body()],
    caller: _Caller,
    engine: Engine = Depends(get_engine),
) -> dict:
    try:
        payload = UploadUrlRequest.model_validate(body, strict=False)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc
    machine_id = _bind_identity(
        caller, tenant_id=payload.tenant_id, machine_id=payload.machine_id,
    )

    fato_subtype = _resolve_fato_subtype(payload.source_type, payload.intent)
    minio_key = _build_minio_key(payload, fato_subtype)
    inserted = extractions_repo.mint_upload_url(
        engine,
        job_id=payload.job_id,
        tenant_id=payload.tenant_id,
        source_type=payload.source_type,
        competencia=payload.competencia,
        fato_subtype=fato_subtype,
        minio_key=minio_key,
        agent_version=payload.agent_version,
        machine_id=machine_id,
    )
    if inserted is None:
        raise HTTPException(status_code=409, detail="duplicate_job_id")

    upload_url = _object_storage().generate_presigned_upload_url(
        config.S3_BUCKET, minio_key, expires_secs=_UPLOAD_URL_TTL_SECONDS,
    )
    response = UploadUrlResponse(
        extraction_id=payload.job_id,
        upload_url=upload_url,
        minio_key=minio_key,
        fato_subtype=fato_subtype,
    )
    return response.model_dump(mode="json")


@router.post("/jobs/register")
def register_job(
    body: Annotated[dict[str, Any], Body()],
    caller: _Caller,
    engine: Engine = Depends(get_engine),
) -> dict:
    try:
        payload = ExtractionRegisterPayload.model_validate(
            body, strict=False,
        )
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc
    machine_id = _bind_identity(
        caller, tenant_id=None, machine_id=payload.machine_id,
    )
    result = extractions_repo.register(
        engine,
        job_id=payload.job_id,
        files=[f.model_dump() for f in payload.files],
        agent_version=payload.agent_version,
        machine_id=machine_id,
        sha256=payload.sha256,
        tenant_id=caller.tenant_id,
    )
    if result is None:
        raise HTTPException(
            status_code=404, detail="job_not_found_or_invalid_state",
        )
    return {"job_id": str(result), "status": "REGISTERED"}


@router.post("/jobs/{job_id}/fail")
def fail_job(
    job_id: UUID,
    body: Annotated[dict[str, Any], Body()],
    caller: _Caller,
    engine: Engine = Depends(get_engine),
) -> dict:
    try:
        payload = ExtractionFailPayload.model_validate(body, strict=False)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc
    _bind_identity(caller, tenant_id=None, machine_id=None)
    result = extractions_repo.mark_failed(
        engine, job_id=job_id, reason=payload.error,
        tenant_id=caller.tenant_id,
    )
    if result is None:
        raise HTTPException(
            status_code=404, detail="job_not_found_or_invalid_state",
        )
    return {"job_id": str(result), "status": "FAILED"}
