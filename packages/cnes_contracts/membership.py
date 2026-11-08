"""Contrato de membership compartilhado entre Identity e Control Plane."""
from datetime import datetime

from pydantic import field_validator

from cnes_contracts.schema import (
    ControlPlaneModel,
    optional_non_blank,
    require_non_blank,
    require_utc,
)


class Membership(ControlPlaneModel):
    tenant_id: str
    user_id: str
    role: str
    created_at: datetime
    oidc_issuer: str | None = None
    _strings = field_validator("tenant_id", "user_id", "role")(require_non_blank)
    _oidc_issuer = field_validator("oidc_issuer")(optional_non_blank)
    _created_utc = field_validator("created_at")(require_utc)
