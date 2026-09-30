"""Settings imutáveis de billing."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Self

from pydantic import ValidationError

from cnes_domain.billing.models import BillingEnforcementMode
from cnes_domain.profiles import BillingMode, parse_profile

_PROFILE_KEYS = ("PROFILE", "BILLING_MODE", "TENANT_ID")
_CODE_PREFIX = "code="
_DEFAULT_PROFILE_CODE = "billing_profile_invalid"
_DEFAULT_TTL_SECONDS = 60
_MAX_TTL_SECONDS = 60


class BillingConfigurationError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(f"code={code}")
        self.code = code


@dataclass(frozen=True, slots=True)
class BillingSettings:
    mode: BillingMode
    enforcement_mode: BillingEnforcementMode
    cache_ttl_seconds: int

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> Self:
        """Args: values: Variáveis ambientais disponíveis.
        Returns: Configuração de billing validada.
        Raises: BillingConfigurationError: Quando a configuração é inválida.
        """
        return cls(
            mode=_mode(values),
            enforcement_mode=_enforcement_mode(values),
            cache_ttl_seconds=_cache_ttl(values),
        )

    @property
    def enforced(self) -> bool:
        return (
            self.mode is BillingMode.STRIPE
            and self.enforcement_mode is BillingEnforcementMode.ENFORCE
        )

    @property
    def execution_mode(self) -> BillingMode:
        """Returns: STRIPE só com enforce; senão DISABLED (companion opcional)."""
        return BillingMode.STRIPE if self.enforced else BillingMode.DISABLED


LOCAL_BILLING_SETTINGS = BillingSettings(
    BillingMode.DISABLED, BillingEnforcementMode.OFF, _DEFAULT_TTL_SECONDS
)


def _error_code(error: ValidationError) -> str:
    for item in error.errors():
        message = str(item.get("ctx", {}).get("error", ""))
        if message.startswith(_CODE_PREFIX):
            return message.removeprefix(_CODE_PREFIX)
    return _DEFAULT_PROFILE_CODE


def _mode(values: Mapping[str, str]) -> BillingMode:
    try:
        profile = parse_profile({key: values[key] for key in _PROFILE_KEYS if key in values})
    except ValidationError as error:
        raise BillingConfigurationError(_error_code(error)) from error
    return profile.billing_mode


def _enforcement_mode(values: Mapping[str, str]) -> BillingEnforcementMode:
    raw = values.get("BILLING_ENFORCEMENT_MODE", BillingEnforcementMode.OFF.value)
    try:
        return BillingEnforcementMode(raw)
    except ValueError as error:
        raise BillingConfigurationError("billing_enforcement_mode_invalid") from error


def _cache_ttl(values: Mapping[str, str]) -> int:
    raw = values.get("BILLING_CACHE_TTL_SECONDS")
    if raw is None:
        return _DEFAULT_TTL_SECONDS
    try:
        ttl = int(raw)
    except ValueError as error:
        raise BillingConfigurationError("billing_cache_ttl_invalid") from error
    if not 0 <= ttl <= _MAX_TTL_SECONDS:
        raise BillingConfigurationError("billing_cache_ttl_out_of_range")
    return ttl


__all__ = ["LOCAL_BILLING_SETTINGS", "BillingConfigurationError", "BillingSettings"]
