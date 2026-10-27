"""Settings imutáveis dos modos de unidade e recuperação AWS."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Self

from cnes_infra.aws.settings import (
    _PRODUCTION_LEASE_SECONDS,  # pyright: ignore[reportPrivateUsage]
    _PRODUCTION_REGION,  # pyright: ignore[reportPrivateUsage]
    AwsRuntimeConfigurationError,
    _bounded,  # pyright: ignore[reportPrivateUsage]
    _environment,  # pyright: ignore[reportPrivateUsage]
    _integer,  # pyright: ignore[reportPrivateUsage]
    _Limit,  # pyright: ignore[reportPrivateUsage]
    _required,  # pyright: ignore[reportPrivateUsage]
    _security_group_id,  # pyright: ignore[reportPrivateUsage]
    _subnet_ids,  # pyright: ignore[reportPrivateUsage]
    production_policy_enabled,
)
from cnes_infra.executor.step_functions import (
    _require_unqualified_state_machine_arn,  # pyright: ignore[reportPrivateUsage]
)

_CONCURRENCY = _Limit(
    "AWS_PROCESSOR_MAX_CONCURRENCY", "processor_max_concurrency", 8, 1, 40,
    "processor_max_concurrency=invalid",
)
_UNIT_LEASE = _Limit(
    "AWS_PROCESSOR_LEASE_SECONDS", "processor_lease_seconds", 300, 30, 3600,
    "processor_lease_seconds=invalid",
)
_RECOVERY_BATCH_SIZE = 100


def _state_machine_arn(values: Mapping[str, str]) -> str:
    value = _required(values, "AWS_STATE_MACHINE_ARN")
    try:
        _require_unqualified_state_machine_arn(value)
    except ValueError as error:
        raise AwsRuntimeConfigurationError(str(error)) from error
    if value.split(":", 3)[2:3] != ["states"]:
        raise AwsRuntimeConfigurationError("state_machine_arn=invalid_service")
    return value


def _fixed(values: Mapping[str, str], name: str, expected: int) -> int:
    value = _integer(values, name, expected)
    if value != expected:
        raise AwsRuntimeConfigurationError(f"fixed={name} expected={expected}")
    return value


@dataclass(frozen=True, slots=True)
class AwsUnitSettings:
    region: str
    control_plane_table: str
    data_bucket: str
    state_machine_arn: str
    processor_container_name: str
    processor_max_concurrency: int
    processor_lease_seconds: int
    dynamodb_endpoint_url: str | None
    service_endpoint_url: str | None
    production_policy: bool
    environment: str
    public_subnet_ids: tuple[str, ...]
    security_group_id: str | None

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> Self:
        """Args: values: Variáveis ambientais do worker.
        Returns: Configuração validada sem campos de API ou credenciais.
        Raises: AwsRuntimeConfigurationError: Configuração inválida.
        """
        production = production_policy_enabled(values)
        settings = cls(
            region=_required(values, "AWS_REGION"),
            control_plane_table=_required(values, "AWS_CONTROL_PLANE_TABLE"),
            data_bucket=_required(values, "AWS_DATA_BUCKET"),
            state_machine_arn=_state_machine_arn(values),
            processor_container_name=_required(values, "AWS_PROCESSOR_CONTAINER_NAME"),
            processor_max_concurrency=_bounded(values, _CONCURRENCY),
            processor_lease_seconds=cls._lease_seconds(values),
            dynamodb_endpoint_url=values.get("DYNAMODB_ENDPOINT_URL") or None,
            service_endpoint_url=values.get("AWS_ENDPOINT_URL") or None,
            production_policy=production,
            environment=_environment(values, production),
            public_subnet_ids=_subnet_ids(values),
            security_group_id=_security_group_id(values),
        )
        if production:
            _validate_production(settings)
        return settings

    @staticmethod
    def _lease_seconds(values: Mapping[str, str]) -> int:
        return _bounded(values, _UNIT_LEASE)


@dataclass(frozen=True, slots=True)
class AwsRecoverySettings(AwsUnitSettings):
    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> Self:
        """Args: values: Variáveis ambientais do worker.
        Returns: Configuração de recuperação com batch 100 e lease 3600.
        Raises: AwsRuntimeConfigurationError: Configuração inválida.
        """
        _fixed(values, "AWS_PROCESSOR_RECOVERY_BATCH_SIZE", _RECOVERY_BATCH_SIZE)
        return super().from_mapping(values)

    @staticmethod
    def _lease_seconds(values: Mapping[str, str]) -> int:
        return _fixed(values, "AWS_PROCESSOR_LEASE_SECONDS", _PRODUCTION_LEASE_SECONDS)


def _validate_production(settings: AwsUnitSettings) -> None:
    violations = (
        (settings.region != _PRODUCTION_REGION, "production_region=invalid"),
        (settings.processor_max_concurrency != 1, "production_processor_max_concurrency=invalid"),
        (
            settings.processor_lease_seconds != _PRODUCTION_LEASE_SECONDS,
            "production_processor_lease_seconds=invalid",
        ),
        (not settings.public_subnet_ids, "production_public_subnet_ids=required"),
        (settings.security_group_id is None, "production_security_group_id=required"),
    )
    for violated, error in violations:
        if violated:
            raise AwsRuntimeConfigurationError(error)


__all__ = ["AwsRecoverySettings", "AwsUnitSettings"]
