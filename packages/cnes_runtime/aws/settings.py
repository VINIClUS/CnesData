"""Settings imutáveis do profile aws."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import NamedTuple, Self
from urllib.parse import urlsplit

from pydantic import ValidationError

from cnes_domain.profiles import AuthMode, ProfileSettings, RuntimeProfile, parse_profile

_PROFILE_KEYS = ("PROFILE", "AUTH_MODE", "OIDC_ISSUER")


class AwsRuntimeConfigurationError(ValueError):
    pass


class _Limit(NamedTuple):
    env: str
    field: str
    default: int | None
    minimum: int
    maximum: int | None
    error: str


_LIMITS = (
    _Limit(
        "AWS_SERVING_URL_TTL_SECONDS", "serving_url_ttl_seconds", 300, 30, 900,
        "serving_ttl_out_of_range",
    ),
    _Limit(
        "AWS_AUDIT_RETENTION_DAYS", "audit_retention_days", None, 1, None,
        "audit_retention_days_invalid",
    ),
    _Limit(
        "AWS_PROCESSOR_MAX_CONCURRENCY", "processor_max_concurrency", 8, 1, 40,
        "processor_concurrency_invalid",
    ),
    _Limit(
        "AWS_PROCESSOR_LEASE_SECONDS", "processor_lease_seconds", 300, 30, 3600,
        "processor_lease_invalid",
    ),
    _Limit(
        "AWS_PROCESSOR_RECOVERY_BATCH_SIZE", "processor_recovery_batch_size", 100, 1, 1000,
        "recovery_batch_invalid",
    ),
)


@dataclass(frozen=True, slots=True)
class AwsRuntimeSettings:
    region: str
    control_plane_table: str
    data_bucket: str
    audit_bucket: str
    state_machine_arn: str
    processor_container_name: str
    oidc_issuer: str
    oidc_audience: str
    serving_url_ttl_seconds: int
    audit_retention_days: int
    processor_max_concurrency: int
    processor_lease_seconds: int
    processor_recovery_batch_size: int
    dynamodb_endpoint_url: str | None
    service_endpoint_url: str | None

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> Self:
        """Args: values: Variáveis ambientais disponíveis.
        Returns: Configuração validada, sem credenciais estáticas.
        Raises: AwsRuntimeConfigurationError: Quando a configuração é inválida.
        """
        profile = _profile(values)
        service_endpoint_url = values.get("AWS_ENDPOINT_URL") or None
        return cls(
            region=_required(values, "AWS_REGION"),
            control_plane_table=_required(values, "AWS_CONTROL_PLANE_TABLE"),
            data_bucket=_required(values, "AWS_DATA_BUCKET"),
            audit_bucket=_required(values, "AWS_AUDIT_BUCKET"),
            state_machine_arn=_required(values, "AWS_STATE_MACHINE_ARN"),
            processor_container_name=_required(values, "AWS_PROCESSOR_CONTAINER_NAME"),
            oidc_issuer=_issuer(
                profile.oidc_issuer or "", allow_http=service_endpoint_url is not None,
            ),
            oidc_audience=_required(values, "OIDC_AUDIENCE"),
            dynamodb_endpoint_url=values.get("DYNAMODB_ENDPOINT_URL") or None,
            service_endpoint_url=service_endpoint_url,
            **{limit.field: _bounded(values, limit) for limit in _LIMITS},
        )


def _profile(values: Mapping[str, str]) -> ProfileSettings:
    if values.get("PROFILE") != RuntimeProfile.AWS:
        raise AwsRuntimeConfigurationError("profile_must_be_aws")
    try:
        profile = parse_profile({key: values[key] for key in _PROFILE_KEYS if key in values})
    except ValidationError as error:
        raise AwsRuntimeConfigurationError("profile_settings_invalid") from error
    if profile.auth_mode is not AuthMode.OIDC:
        raise AwsRuntimeConfigurationError("auth_mode_must_be_oidc")
    return profile


def _required(values: Mapping[str, str], key: str) -> str:
    value = values.get(key, "").strip()
    if not value:
        raise AwsRuntimeConfigurationError(f"missing={key}")
    return value


def _integer(values: Mapping[str, str], key: str, default: int | None) -> int:
    raw = values.get(key)
    if raw is None:
        if default is None:
            raise AwsRuntimeConfigurationError(f"missing={key}")
        return default
    try:
        return int(raw)
    except ValueError as error:
        raise AwsRuntimeConfigurationError(f"integer_invalid={key}") from error


def _bounded(values: Mapping[str, str], limit: _Limit) -> int:
    value = _integer(values, limit.env, limit.default)
    too_large = limit.maximum is not None and value > limit.maximum
    if value < limit.minimum or too_large:
        raise AwsRuntimeConfigurationError(limit.error)
    return value


def _issuer(raw: str, allow_http: bool) -> str:
    value = raw.strip()
    parts = urlsplit(value)
    schemes = {"https", "http"} if allow_http else {"https"}
    has_suffix = "?" in value or "#" in value
    if parts.scheme not in schemes or not parts.netloc or has_suffix:
        raise AwsRuntimeConfigurationError("oidc_issuer_invalid")
    return value.removesuffix("/")


__all__ = ["AwsRuntimeConfigurationError", "AwsRuntimeSettings"]
