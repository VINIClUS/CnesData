"""POST /extractions/enqueue — admin enqueue with depends_on wiring."""
from __future__ import annotations

from datetime import date  # noqa: TC003
from typing import TYPE_CHECKING
from uuid import UUID  # noqa: TC003

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel, ConfigDict

from central_api.deps import get_engine
from cnes_contracts.landing import SOURCE_TYPE  # noqa: TC001
from cnes_infra.storage import extractions_repo

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


router = APIRouter(prefix="/extractions", tags=["extractions"])


_SOURCE_MANIFEST: dict[str, list[tuple[str, bool]]] = {
    "CNES_LOCAL": [("CNES_VINCULO", False)],
    "CNES_NACIONAL": [("CNES_VINCULO", False)],
    "SIHD": [("SIHD_INTERNACAO", False), ("SIHD_PROC_AIH", False)],
    "BPA_MAG": [("BPA_C", False), ("BPA_I", False)],
    "SIA_LOCAL": [
        ("DIM_SIGTAP", True),
        ("DIM_MUNICIPIO", True),
        ("SIA_APA", False),
        ("SIA_BPI", False),
        ("SIA_BPIHST", False),
    ],
}


_ADMIN_TOKEN = "test-admin"  # noqa: S105


class EnqueueRequest(BaseModel):
    model_config = ConfigDict(strict=False)

    source_type: SOURCE_TYPE
    tenant_id: str
    competencia: date


class EnqueueResponse(BaseModel):
    job_ids: list[UUID]


def _require_admin(x_admin_token: str | None = Header(None)) -> None:
    if x_admin_token != _ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="admin_token_required")


@router.post(
    "/enqueue",
    response_model=EnqueueResponse,
    status_code=status.HTTP_201_CREATED,
)
def enqueue(
    req: EnqueueRequest,
    _: None = Depends(_require_admin),
    engine: Engine = Depends(get_engine),
) -> EnqueueResponse:
    manifest = _SOURCE_MANIFEST.get(req.source_type)
    if manifest is None:
        raise HTTPException(
            status_code=422,
            detail=f"unsupported_source_type={req.source_type}",
        )

    dim_ids: list[UUID] = []
    fato_ids: list[UUID] = []

    for fato_subtype, is_dim in manifest:
        placeholder = {
            "minio_key": (
                f"placeholder/{req.source_type}/{req.competencia}/"
                f"{fato_subtype.lower()}.parquet.gz"
            ),
            "fato_subtype": fato_subtype,
            "size_bytes": 1,
            "sha256": "0" * 64,
        }
        deps = [] if is_dim else dim_ids
        job_id = extractions_repo.enqueue(
            engine,
            tenant_id=req.tenant_id,
            source_type=req.source_type,
            competencia=req.competencia,
            files=[placeholder],
            depends_on=deps,
        )
        (dim_ids if is_dim else fato_ids).append(job_id)

    return EnqueueResponse(job_ids=dim_ids + fato_ids)
