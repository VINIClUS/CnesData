"""Rotas /api/v1/jobs/* — lifecycle de landing.extractions (Gold v2)."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException
from pydantic import ValidationError

from central_api.deps import get_engine
from cnes_contracts.landing import ExtractionRegisterPayload
from cnes_infra.storage import extractions_repo

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

router = APIRouter(tags=["jobs"])


@router.post("/jobs/register")
def register_job(
    body: Annotated[dict[str, Any], Body()],
    engine: Engine = Depends(get_engine),
) -> dict:
    try:
        payload = ExtractionRegisterPayload.model_validate(
            body, strict=False,
        )
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc
    result = extractions_repo.register(
        engine,
        job_id=payload.job_id,
        files=[f.model_dump() for f in payload.files],
    )
    if result is None:
        raise HTTPException(
            status_code=404, detail="job_not_found_or_invalid_state",
        )
    return {"job_id": str(result), "status": "REGISTERED"}
