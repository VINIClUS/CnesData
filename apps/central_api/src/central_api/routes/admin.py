"""Rotas administrativas — reap-leases aposentada (MIG-012): responde 410."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends

from central_api.deps import legacy_ingestion_retired, require_admin_token

router = APIRouter(tags=["admin"], dependencies=[Depends(require_admin_token)])


@router.post("/admin/reap-leases")
def reap_leases() -> dict[Any, Any]:
    legacy_ingestion_retired()
