"""ECS boundary of the processor: one run-unit envelope or one recover-once pass."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from cnes_domain.ports.processing import RunUnitMessage

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from data_processor.composition import AwsProcessorServices, ProcessorRuntimeComponents

_ENVELOPE_NAMES = (
    "TENANT_ID", "RUN_ID", "WAVE_ID", "DISPATCH_ID", "UNIT_ID", "EXECUTION_OWNER",
    "LEASE_SECONDS",
)
_HEX_ID = re.compile(r"[0-9a-f]{16}")
_EXECUTION_OWNER_PREFIX = "arn:aws:states:"
_MIN_LEASE_SECONDS = 30
_MAX_LEASE_SECONDS = 3600
_RECOVER_ONCE = ("recover-once",)


class EntrypointConfigurationError(ValueError):
    pass


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _required(values: Mapping[str, str], name: str) -> str:
    value = values.get(name, "").strip()
    if not value:
        raise EntrypointConfigurationError(f"missing={name}")
    return value


def _hex_id(values: Mapping[str, str], name: str) -> str:
    value = _required(values, name)
    if not _HEX_ID.fullmatch(value):
        raise EntrypointConfigurationError(f"invalid={name}")
    return value


def _lease_seconds(values: Mapping[str, str]) -> int:
    raw = _required(values, "LEASE_SECONDS")
    if not raw.isdecimal() or not _MIN_LEASE_SECONDS <= int(raw) <= _MAX_LEASE_SECONDS:
        raise EntrypointConfigurationError("invalid=LEASE_SECONDS")
    return int(raw)


@dataclass(frozen=True, slots=True)
class EcsUnitEnvelope:
    tenant_id: str
    run_id: str
    wave_id: str
    dispatch_id: str
    unit_id: str
    execution_owner: str
    lease_seconds: int

    @classmethod
    def from_mapping(cls, values: Mapping[str, str]) -> EcsUnitEnvelope:
        """Args: values: ambiente do task ECS com as sete variáveis do envelope.
        Returns: Envelope validado.
        Raises: EntrypointConfigurationError: variável ausente ou inválida.
        """
        envelope = cls(
            tenant_id=_required(values, "TENANT_ID"),
            run_id=_required(values, "RUN_ID"),
            wave_id=_hex_id(values, "WAVE_ID"),
            dispatch_id=_hex_id(values, "DISPATCH_ID"),
            unit_id=_required(values, "UNIT_ID"),
            execution_owner=_required(values, "EXECUTION_OWNER"),
            lease_seconds=_lease_seconds(values),
        )
        if not envelope.execution_owner.startswith(_EXECUTION_OWNER_PREFIX):
            raise EntrypointConfigurationError("invalid=EXECUTION_OWNER")
        return envelope

    def message(self, now: datetime) -> RunUnitMessage:
        """Returns: Mensagem canônica que o handler converte em ClaimRunUnit."""
        return RunUnitMessage(
            tenant_id=self.tenant_id,
            run_id=self.run_id,
            wave_id=self.wave_id,
            dispatch_id=self.dispatch_id,
            unit_id=self.unit_id,
            owner=self.execution_owner,
            now=now,
            lease_seconds=self.lease_seconds,
        )


def _required_aws_services(runtime: ProcessorRuntimeComponents) -> AwsProcessorServices:
    if runtime.services is None:
        raise EntrypointConfigurationError("aws_services=missing")
    return runtime.services


def _has_partial_unit_envelope(values: Mapping[str, str]) -> bool:
    return any(values.get(name, "").strip() for name in _ENVELOPE_NAMES)


def run_aws_entrypoint(
    runtime: ProcessorRuntimeComponents, values: Mapping[str, str], argv: Sequence[str],
) -> int:
    """Args: runtime: componentes aws; values: ambiente; argv: argumentos do comando.
    Returns: 0 após exatamente uma unidade ou uma passada de recovery.
    Raises: EntrypointConfigurationError; erros do handler ou do recovery propagam.
    """
    services = _required_aws_services(runtime)
    if values.get("UNIT_ID"):
        if argv:
            raise EntrypointConfigurationError("unit_mode=argv_forbidden")
        envelope = EcsUnitEnvelope.from_mapping(values)
        runtime.unit_handler.handle(envelope.message(_utc_now()))
        return 0
    if _has_partial_unit_envelope(values):
        raise EntrypointConfigurationError("unit_envelope=partial")
    if tuple(argv) != _RECOVER_ONCE:
        raise EntrypointConfigurationError("command=recover_once_required")
    services.recovery.run_once(limit=services.recovery_batch_size)
    return 0


__all__ = ["EcsUnitEnvelope", "EntrypointConfigurationError", "run_aws_entrypoint"]
