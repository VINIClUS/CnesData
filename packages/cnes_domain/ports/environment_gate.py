"""Portas do gate de ambiente: fence, semáforo e quota mensal, probe e admissão de dispatch."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from cnes_domain.control_plane.environment_gate import require_environment

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.control_plane.environment_gate import (
        AcquireUnitPermit,
        BindUnitPermit,
        ClosePromotionFence,
        EnvironmentGateState,
        PermitAcquisition,
        PermitHolder,
        ReanchorUnitPermit,
        ReleaseUnitPermit,
        RenewUnitPermit,
        ReopenPromotionFence,
    )
    from cnes_domain.control_plane.execution_quota import (
        AttemptOutcome,
        ConsumeExecutionAttempt,
    )


@runtime_checkable
class EnvironmentGatePort(Protocol):
    """Fence, semáforo de unidade e tentativas do mês: leitura forte e mutações condicionais."""

    def observe_environment_gate(self, environment: str) -> EnvironmentGateState: ...
    def acquire_unit_permit(self, command: AcquireUnitPermit) -> PermitAcquisition: ...
    def bind_unit_permit(self, command: BindUnitPermit) -> bool: ...
    def renew_unit_permit(self, command: RenewUnitPermit) -> bool: ...
    def release_unit_permit(self, command: ReleaseUnitPermit) -> bool: ...
    def reanchor_unit_permit(self, command: ReanchorUnitPermit) -> bool: ...
    def close_promotion_fence(self, command: ClosePromotionFence) -> bool: ...
    def reopen_promotion_fence(self, command: ReopenPromotionFence) -> bool: ...
    def consume_execution_attempt(self, command: ConsumeExecutionAttempt) -> AttemptOutcome: ...


class ExecutionPresence(StrEnum):
    RUNNING = "RUNNING"
    TERMINAL = "TERMINAL"
    ABSENT = "ABSENT"


@dataclass(frozen=True, slots=True)
class ExecutionObservation:
    """Presença da execução nomeada pelo dispatch_id; ABSENT exige prova do executor."""

    presence: ExecutionPresence
    execution_ref: str | None

    def __post_init__(self) -> None:
        if (self.presence is ExecutionPresence.ABSENT) != (self.execution_ref is None):
            raise ValueError("reason=execution_ref_presence_mismatch")


@runtime_checkable
class ExecutionProbePort(Protocol):
    def probe(self, dispatch_id: str) -> ExecutionObservation: ...


class DeferralReason(StrEnum):
    FENCE_CLOSED = "fence_closed"
    PERMIT_HELD = "permit_held"
    MONTHLY_ATTEMPTS_EXHAUSTED = "monthly_attempts_exhausted"


@dataclass(frozen=True, slots=True)
class DispatchDeferred:
    """Dispatch adiado sem chamar o executor: resultado tipado, nunca exceção."""

    reason: DeferralReason


@dataclass(frozen=True, slots=True)
class UnitAdmission:
    """Permit admitido para o dispatch; ambiente e token nulos quando não há gate."""

    environment: str | None
    token: int | None

    def __post_init__(self) -> None:
        if self.environment is None and self.token is None:
            return
        require_environment(self.environment)
        token = self.token
        if isinstance(token, bool) or not isinstance(token, int) or token < 1:
            raise ValueError("reason=out_of_range field=token")


@runtime_checkable
class DispatchAdmissionPort(Protocol):
    """Admissão do dispatch no gate do ambiente em volta do start do executor.

    `reconcile` liquida o detentor atual na passada de recovery, sem adquirir nem iniciar.
    """

    def admit(self, holder: PermitHolder, now: datetime) -> UnitAdmission | DispatchDeferred: ...
    def confirm(self, admission: UnitAdmission, execution_ref: str, now: datetime) -> None: ...
    def settle(self, holder: PermitHolder, now: datetime) -> None: ...
    def reconcile(self, now: datetime) -> None: ...


class _UngatedAdmission:
    def admit(self, holder: PermitHolder, now: datetime) -> UnitAdmission | DispatchDeferred:
        return UnitAdmission(None, None)

    def confirm(self, admission: UnitAdmission, execution_ref: str, now: datetime) -> None:
        pass

    def settle(self, holder: PermitHolder, now: datetime) -> None:
        pass

    def reconcile(self, now: datetime) -> None:
        pass


UNGATED_ADMISSION: Final[DispatchAdmissionPort] = _UngatedAdmission()
