"""ECS boundary for the processor's single execution modes."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING

from cnes_domain.ports.processing import RunUnitMessage
from data_processor.aws_composition import AwsAuditRuntime, AwsRecoveryRuntime, AwsUnitRuntime

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from data_processor.aws_composition import AwsModeRuntime

_ENVELOPE_NAMES = (
    "TENANT_ID",
    "RUN_ID",
    "WAVE_ID",
    "DISPATCH_ID",
    "UNIT_ID",
    "EXECUTION_OWNER",
    "LEASE_SECONDS",
)
_HEX_ID = re.compile(r"[0-9a-f]{16}")
_EXECUTION_OWNER_PREFIX = "arn:aws:states:"
_MIN_LEASE_SECONDS = 30
_MAX_LEASE_SECONDS = 3600
_RECOVER_ONCE = ("recover-once",)
_DISPATCH_OUTBOX_ONCE = ("dispatch-outbox-once",)


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


class AwsMode(StrEnum):
    RUN_UNIT = "run-unit"
    RECOVER_ONCE = "recover-once"
    DISPATCH_OUTBOX_ONCE = "dispatch-outbox-once"


@dataclass(frozen=True, slots=True)
class AwsInvocation:
    mode: AwsMode
    envelope: EcsUnitEnvelope | None


def select_aws_invocation(values: Mapping[str, str], argv: Sequence[str]) -> AwsInvocation:
    """Args: values: ambiente; argv: argumentos do comando.
    Returns: Invocação validada sem executar I/O.
    Raises: EntrypointConfigurationError: envelope ou argumentos inválidos.
    """
    if tuple(argv) == _DISPATCH_OUTBOX_ONCE:
        if any(name in values for name in _ENVELOPE_NAMES):
            raise EntrypointConfigurationError("unit_envelope=forbidden")
        return AwsInvocation(AwsMode.DISPATCH_OUTBOX_ONCE, None)
    if "UNIT_ID" in values:
        if argv:
            raise EntrypointConfigurationError("unit_mode=argv_forbidden")
        return AwsInvocation(AwsMode.RUN_UNIT, EcsUnitEnvelope.from_mapping(values))
    if any(name in values for name in _ENVELOPE_NAMES):
        raise EntrypointConfigurationError("unit_envelope=partial")
    if tuple(argv) != _RECOVER_ONCE:
        raise EntrypointConfigurationError("command=recover_once_required")
    return AwsInvocation(AwsMode.RECOVER_ONCE, None)


def run_aws_entrypoint(invocation: AwsInvocation, runtime: AwsModeRuntime) -> int:
    """Args: invocation: Modo validado. runtime: Runtime específico do modo.
    Returns: 0 após unidade/recovery ou despacho sem falhas; 1 se o despacho falhar.
    Raises: EntrypointConfigurationError; erros do handler ou recovery propagam.
    """
    if invocation.mode is AwsMode.RUN_UNIT:
        if not isinstance(runtime, AwsUnitRuntime) or invocation.envelope is None:
            raise EntrypointConfigurationError("unit_runtime=required")
        runtime.unit_handler.handle(invocation.envelope.message(_utc_now()))
        return 0
    if invocation.mode is AwsMode.RECOVER_ONCE:
        if not isinstance(runtime, AwsRecoveryRuntime) or invocation.envelope is not None:
            raise EntrypointConfigurationError("recovery_runtime=required")
        runtime.recovery.run_once(limit=100)
        return 0
    if invocation.mode is not AwsMode.DISPATCH_OUTBOX_ONCE:
        raise EntrypointConfigurationError("audit_runtime=required")
    from data_processor.audit_dispatch import run_audit_dispatch_once

    if not isinstance(runtime, AwsAuditRuntime):
        raise EntrypointConfigurationError("audit_runtime=required")
    return run_audit_dispatch_once(runtime)


__all__ = [
    "AwsInvocation",
    "AwsMode",
    "EcsUnitEnvelope",
    "EntrypointConfigurationError",
    "run_aws_entrypoint",
    "select_aws_invocation",
]
