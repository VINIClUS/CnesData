"""Rotas administrativas — reap-leases aposentada (MIG-012): responde 410."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from central_api.deps import require_admin_token

router = APIRouter(tags=["admin"], dependencies=[Depends(require_admin_token)])


@router.post("/admin/reap-leases")
def reap_leases() -> dict[Any, Any]:
    raise HTTPException(status_code=410, detail="legacy_ingestion_retired")
