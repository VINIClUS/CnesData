"""Configuração imutável de profiles de execução."""

from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RuntimeProfile(StrEnum):
    LOCAL = "local"
    AWS = "aws"


class ProfileNotImplemented(NotImplementedError):
    pass


class AuthMode(StrEnum):
    LOCAL = "local"
    OIDC = "oidc"


class BillingMode(StrEnum):
    DISABLED = "disabled"
    STRIPE = "stripe"


_LOCAL_STATE_DB_SEGMENTS = ("state", "cnesdata.sqlite3")
_LOCAL_OBJECTS_SEGMENT = "objects"


def local_state_db(data_dir: Path) -> Path:
    """Args: data_dir: Diretório raiz do profile local.
    Returns: Caminho do SQLite de control plane + credenciais locais.
    """
    return data_dir.joinpath(*_LOCAL_STATE_DB_SEGMENTS)


def local_state_db_arcname() -> str:
    """Returns: Segmento relativo de `local_state_db`, para uso em arquivos tar."""
    return "/".join(_LOCAL_STATE_DB_SEGMENTS)


def local_objects_dir(data_dir: Path) -> Path:
    """Args: data_dir: Diretório raiz do profile local.
    Returns: Caminho da raiz do object store filesystem.
    """
    return data_dir / _LOCAL_OBJECTS_SEGMENT


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

    @property
    def state_db(self) -> Path:
        return local_state_db(self.data_dir)

    @property
    def objects_dir(self) -> Path:
        return local_objects_dir(self.data_dir)


_ENV_FIELDS = {
    "AUTH_MODE": "auth_mode",
    "BILLING_MODE": "billing_mode",
    "DATA_DIR": "data_dir",
    "OIDC_ISSUER": "oidc_issuer",
    "PROFILE": "profile",
    "TENANT_ID": "tenant_id",
}


def parse_profile(env: Mapping[str, str]) -> ProfileSettings:
    """Args: env: Variáveis ambientais disponíveis.
    Returns: Configuração validada e imutável.
    Raises: ValidationError: Quando a configuração é inválida.
    """
    values = {field: env[name] for name, field in _ENV_FIELDS.items() if name in env}
    return ProfileSettings.model_validate(values)


__all__ = [
    "AuthMode",
    "BillingMode",
    "ProfileNotImplemented",
    "ProfileSettings",
    "RuntimeProfile",
    "local_objects_dir",
    "local_state_db",
    "local_state_db_arcname",
    "parse_profile",
]
