"""Rota de health check."""

from datetime import UTC, datetime

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.engine import Engine

from central_api.deps import get_health_engine
from cnes_domain.models.api import HealthResponse

router = APIRouter(tags=["sistema"])


@router.get("/system/health", response_model=HealthResponse)
def health_check(
    engine: Engine | None = Depends(get_health_engine),
) -> HealthResponse:
    db_ok = engine is None
    if engine is not None:
        try:
            with engine.connect() as con:
                con.execute(text("SELECT 1"))
                db_ok = True
        except Exception:
            pass
    return HealthResponse(
        status="ok" if db_ok else "degraded",
        db_connected=db_ok,
        timestamp=datetime.now(UTC),
    )
