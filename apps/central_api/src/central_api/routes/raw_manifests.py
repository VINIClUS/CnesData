"""Rota autenticada de registro de manifestos raw."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse

from central_api.routes.raw_jobs import (
    _error_code,
    _utc_now,
    get_raw_ingestion_service,
    require_edge_agent,
)
from central_api.schemas.raw_api import (
    EdgeIdentity,
    RawManifestResponse,
    RawManifestSubmission,
)
from central_api.services.raw_ingestion import RawIngestionService, RegisterRawManifest
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost, NotFound

router = APIRouter(prefix="/api/v1/edge", tags=["edge-raw"])


@router.post(
    "/raw-manifests",
    response_model=RawManifestResponse,
    responses={
        409: {
            "content": {
                "application/json": {
                    "schema": {
                        "oneOf": [
                            {"$ref": "#/components/schemas/RawManifestResponse"},
                            {
                                "type": "object",
                                "properties": {"detail": {"type": "string"}},
                                "required": ["detail"],
                                "additionalProperties": False,
                            },
                        ]
                    }
                }
            }
        }
    },
)
def register_raw_manifest(
    body: RawManifestSubmission,
    identity: Annotated[EdgeIdentity, Depends(require_edge_agent)],
    service: Annotated[RawIngestionService, Depends(get_raw_ingestion_service)],
) -> RawManifestResponse | JSONResponse:
    """Registra o manifesto usando somente a identidade autenticada."""

    canonical = body.manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    command = RegisterRawManifest(
        tenant_id=identity.tenant_id,
        agent_id=identity.agent_id,
        job_id=body.job_id,
        owner=identity.agent_id,
        fencing_token=body.fencing_token,
        manifest=body.manifest,
        manifest_bytes=canonical,
        now=_utc_now(),
    )
    try:
        acceptance = service.register(command)
    except NotFound as error:
        raise HTTPException(status_code=404, detail=_error_code(error)) from error
    except (FenceRejected, LeaseLost, Conflict) as error:
        raise HTTPException(status_code=409, detail=_error_code(error)) from error
    response = RawManifestResponse(
        accepted=acceptance.accepted,
        manifest_id=acceptance.manifest_id,
        manifest_sha256=acceptance.manifest_sha256,
        full_resync_required=acceptance.full_resync_required,
        reason=acceptance.reason.value if acceptance.reason is not None else None,
    )
    if response.full_resync_required:
        return JSONResponse(status_code=409, content=response.model_dump(mode="json"))
    return response
