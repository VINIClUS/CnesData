"""Contrato de configuração de billing independente da composição Runtime."""

from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from cnes_contracts.modes import BillingMode, RuntimeProfile


class BillingProfile(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    profile: RuntimeProfile = RuntimeProfile.LOCAL
    tenant_id: str | None = Field(default=None, pattern=r"^[0-9]{6}$")
    billing_mode: BillingMode = BillingMode.DISABLED

    @model_validator(mode="after")
    def _validate_modes(self) -> Self:
        if self.profile is RuntimeProfile.LOCAL and self.tenant_id is None:
            raise ValueError("code=tenant_id_required")
        if self.profile is RuntimeProfile.LOCAL and self.billing_mode is BillingMode.STRIPE:
            raise ValueError("code=local_billing_disabled")
        return self
