"""Rotas administrativas — reaper de leases, diagnóstico."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends

from central_api.deps import get_engine
from cnes_infra.storage import extractions_repo

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

router = APIRouter(tags=["admin"])


@router.post("/admin/reap-leases")
def reap_leases(
    engine: Engine = Depends(get_engine),
) -> dict:
    count = extractions_repo.reap_expired(engine)
    return {"reaped": count}
