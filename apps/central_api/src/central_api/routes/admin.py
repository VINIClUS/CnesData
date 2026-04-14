"""Rotas administrativas — reaper de leases, diagnóstico."""

import logging

from fastapi import APIRouter, Depends
from sqlalchemy.engine import Engine

from central_api.deps import get_engine
from cnes_infra.storage.job_queue import reap_expired_leases

logger = logging.getLogger(__name__)

router = APIRouter(tags=["admin"])


@router.post("/admin/reap-leases")
def reap_leases(
    engine: Engine = Depends(get_engine),
) -> dict:
    count = reap_expired_leases(engine)
    return {"reaped": count}
