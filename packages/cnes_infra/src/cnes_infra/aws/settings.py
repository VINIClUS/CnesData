"""Settings imutáveis do profile aws."""

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import NamedTuple, Self
from urllib.parse import urlsplit

from pydantic import ValidationError

from cnes_domain.profiles import AuthMode, ProfileSettings, RuntimeProfile, parse_profile

_PROFILE_KEYS = ("PROFILE", "AUTH_MODE", "OIDC_ISSUER")
_POLICY_FLAGS = {"true": True, "false": False}
_DEFAULT_ENVIRONMENT = "default"
_ENVIRONMENT_NAME = re.compile(r"[a-z][a-z0-9-]{0,31}")
_SUBNET_ID = re.compile(r"subnet-[0-9a-f]{8,17}")
_SECURITY_GROUP_ID = re.compile(r"sg-[0-9a-f]{8,17}")
_PRODUCTION_ONLY_KEYS = ("AWS_PUBLIC_SUBNET_IDS", "AWS_SECURITY_GROUP_ID")
_PRODUCTION_REGION = "us-east-2"
_PRODUCTION_LEASE_SECONDS = 3600
_PRODUCTION_SERVING_TTL_SECONDS = 300


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
    production_policy: bool
    environment: str
    public_subnet_ids: tuple[str, ...]
    security_group_id: str | None

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> Self:
        """Args: values: Variáveis ambientais disponíveis.
        Returns: Configuração validada, sem credenciais estáticas.
        Raises: AwsRuntimeConfigurationError: Quando a configuração é inválida.
        """
        profile = _profile(values)
        production = production_policy_enabled(values)
        if not production:
            _forbid_production_settings(values)
        service_endpoint_url = values.get("AWS_ENDPOINT_URL") or None
        settings = cls(
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
            production_policy=production,
            environment=_environment(values, production),
            public_subnet_ids=_subnet_ids(values),
            security_group_id=_security_group_id(values),
            **{limit.field: _bounded(values, limit) for limit in _LIMITS},
        )
        if production:
            _validate_production(settings)
        return settings


def production_policy_enabled(values: Mapping[str, str]) -> bool:
    """Args: values: Variáveis ambientais disponíveis.
    Returns: True com AWS_PRODUCTION_POLICY="true"; False com "false" ou ausente.
    Raises: AwsRuntimeConfigurationError: Valor diferente de "true" e "false".
    """
    raw = values.get("AWS_PRODUCTION_POLICY", "false")
    if raw not in _POLICY_FLAGS:
        raise AwsRuntimeConfigurationError("production_policy_invalid")
    return _POLICY_FLAGS[raw]


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


def _forbid_production_settings(values: Mapping[str, str]) -> None:
    for key in _PRODUCTION_ONLY_KEYS:
        if values.get(key):
            raise AwsRuntimeConfigurationError(f"production_setting_forbidden={key}")


def _environment(values: Mapping[str, str], production: bool) -> str:
    raw = values.get("AWS_ENVIRONMENT")
    if raw is None:
        if production:
            raise AwsRuntimeConfigurationError("environment_required")
        return _DEFAULT_ENVIRONMENT
    if not _ENVIRONMENT_NAME.fullmatch(raw):
        raise AwsRuntimeConfigurationError("environment_invalid")
    return raw


def _subnet_ids(values: Mapping[str, str]) -> tuple[str, ...]:
    raw = values.get("AWS_PUBLIC_SUBNET_IDS")
    if not raw:
        return ()
    subnet_ids = tuple(raw.split(","))
    if not all(_SUBNET_ID.fullmatch(subnet_id) for subnet_id in subnet_ids):
        raise AwsRuntimeConfigurationError("public_subnet_invalid")
    if subnet_ids != tuple(sorted(set(subnet_ids))):
        raise AwsRuntimeConfigurationError("public_subnets_not_normalized")
    return subnet_ids


def _security_group_id(values: Mapping[str, str]) -> str | None:
    raw = values.get("AWS_SECURITY_GROUP_ID")
    if not raw:
        return None
    if not _SECURITY_GROUP_ID.fullmatch(raw):
        raise AwsRuntimeConfigurationError("security_group_invalid")
    return raw


def _validate_production(settings: AwsRuntimeSettings) -> None:
    violations = (
        (settings.region != _PRODUCTION_REGION, "production_region_must_be_us_east_2"),
        (settings.processor_max_concurrency != 1, "production_concurrency_must_be_one"),
        (
            settings.processor_lease_seconds != _PRODUCTION_LEASE_SECONDS,
            "production_lease_must_be_3600",
        ),
        (
            settings.serving_url_ttl_seconds != _PRODUCTION_SERVING_TTL_SECONDS,
            "production_serving_ttl_must_be_300",
        ),
        (not settings.public_subnet_ids, "production_subnets_required"),
        (settings.security_group_id is None, "production_security_group_required"),
    )
    for violated, error in violations:
        if violated:
            raise AwsRuntimeConfigurationError(error)


__all__ = ["AwsRuntimeConfigurationError", "AwsRuntimeSettings", "production_policy_enabled"]
