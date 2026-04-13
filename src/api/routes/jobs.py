"""Rotas de consulta de jobs."""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.engine import Engine

from api.deps import get_engine
from api.models import JobStatusResponse
from storage.job_queue import get_status

router = APIRouter(tags=["jobs"])


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
def get_job_status(
    job_id: str,
    engine: Engine = Depends(get_engine),
) -> JobStatusResponse:
    result = get_status(engine, job_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Job não encontrado")
    return JobStatusResponse(**result)
