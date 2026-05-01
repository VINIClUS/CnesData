"""Rotas administrativas — reaper de leases, diagnóstico."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends

from central_api.deps import get_conn
from cnes_infra.storage import extractions_repo

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)

router = APIRouter(tags=["admin"])


@router.post("/admin/reap-leases")
def reap_leases(
    conn: Connection = Depends(get_conn),
) -> dict:
    count = extractions_repo.reap_expired(conn)
    return {"reaped": count}
