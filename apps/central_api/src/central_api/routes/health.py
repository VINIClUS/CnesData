"""Rota de health check."""

from datetime import datetime, timezone

from cnes_domain.models.api import HealthResponse
from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.engine import Engine

from central_api.deps import get_engine

router = APIRouter(tags=["sistema"])


@router.get("/system/health", response_model=HealthResponse)
def health_check(
    engine: Engine = Depends(get_engine),
) -> HealthResponse:
    db_ok = False
    try:
        with engine.connect() as con:
            con.execute(text("SELECT 1"))
            db_ok = True
    except Exception:
        pass
    return HealthResponse(
        status="ok" if db_ok else "degraded",
        db_connected=db_ok,
        timestamp=datetime.now(timezone.utc),
    )
