"""Configuração imutável de profiles de execução."""

from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RuntimeProfile(StrEnum):
    LOCAL = "local"
    AWS = "aws"


class AuthMode(StrEnum):
    LOCAL = "local"
    OIDC = "oidc"


class BillingMode(StrEnum):
    DISABLED = "disabled"
    STRIPE = "stripe"


class ProfileSettings(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    profile: RuntimeProfile = RuntimeProfile.LOCAL
    tenant_id: str = Field(pattern=r"^[0-9]{6}$")
    data_dir: Path = Path("data")
    auth_mode: AuthMode = AuthMode.LOCAL
    billing_mode: BillingMode = BillingMode.DISABLED
    oidc_issuer: str | None = None

    @model_validator(mode="after")
    def _validate_modes(self) -> Self:
        if self.profile is RuntimeProfile.LOCAL and self.billing_mode is BillingMode.STRIPE:
            raise ValueError("code=local_billing_disabled")
        if self.auth_mode is AuthMode.OIDC and not (self.oidc_issuer or "").strip():
            raise ValueError("code=oidc_issuer_required")
        return self


_ENV_FIELDS = {
    "AUTH_MODE": "auth_mode",
    "BILLING_MODE": "billing_mode",
    "DATA_DIR": "data_dir",
    "OIDC_ISSUER": "oidc_issuer",
    "PROFILE": "profile",
    "TENANT_ID": "tenant_id",
}


def parse_profile(env: Mapping[str, str]) -> ProfileSettings:
    """Converte variáveis selecionadas em configuração.
    Args: env: Variáveis ambientais disponíveis.
    Returns: Configuração validada e imutável.
    Raises: ValidationError: Quando a configuração é inválida.
    """
    values = {field: env[name] for name, field in _ENV_FIELDS.items() if name in env}
    return ProfileSettings.model_validate(values)


__all__ = [
    "AuthMode",
    "BillingMode",
    "ProfileSettings",
    "RuntimeProfile",
    "parse_profile",
]
