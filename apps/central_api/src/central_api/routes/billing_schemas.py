"""Modelos de requisição e resposta das rotas de billing."""

from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from cnes_domain.billing.revocation_models import REASON_CODE_PATTERN

_Key = Annotated[str, Field(min_length=16, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")]
_Id = Annotated[str, Field(min_length=1, max_length=128)]


class _Request(BaseModel):
    model_config = ConfigDict(extra="forbid")


class BillingAccountCreate(_Request):
    idempotency_key: _Key


class BillingAccountTransfer(_Request):
    new_owner_user_id: _Id
    reason_code: str = Field(pattern=REASON_CODE_PATTERN)


class CheckoutCreate(_Request):
    billing_account_id: _Id
    plan_version_id: _Id
    idempotency_key: _Key


class PortalCreate(_Request):
    billing_account_id: _Id
    idempotency_key: _Key


class HostedSessionOut(BaseModel):
    session_id: str
    url: HttpUrl


class BillingAccountOut(BaseModel):
    billing_account_id: str
    owner_user_id: str
    stripe_customer_id: str | None


class BillingStatusOut(BaseModel):
    state: str
    plan_version_id: str | None = None
    billing_account_id: str | None = None
    cancel_at_period_end: bool | None = None
    period_end: datetime | None = None
    grace_until: datetime | None = None
    entitlement_version: int | None = None
    features: list[str] | None = None
