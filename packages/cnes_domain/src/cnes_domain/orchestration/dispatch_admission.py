"""Admissão do dispatch no gate do ambiente: acquire, reconciliação por probe e liquidação."""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, Final, cast

from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    AcquisitionStatus,
    BindUnitPermit,
    PermitHolder,
    ReleaseUnitPermit,
    RenewUnitPermit,
    require_environment,
)
from cnes_domain.ports.environment_gate import (
    DeferralReason,
    DispatchDeferred,
    ExecutionPresence,
    UnitAdmission,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import datetime

    from cnes_domain.control_plane.entities import RunDispatch, RunUnit
    from cnes_domain.control_plane.environment_gate import PermitAcquisition, UnitPermit
    from cnes_domain.ports.environment_gate import (
        DispatchAdmissionPort,
        EnvironmentGatePort,
        ExecutionObservation,
        ExecutionProbePort,
    )

ABSENCE_PROOF_GRACE_SECONDS: Final = 900
# A younger permit may belong to a start still in flight: its absence proves nothing yet.
_ABSENCE_PROOF_GRACE: Final = timedelta(seconds=ABSENCE_PROOF_GRACE_SECONDS)
_DEFERRALS: Final = {
    AcquisitionStatus.FENCE_CLOSED: DeferralReason.FENCE_CLOSED,
    AcquisitionStatus.HELD: DeferralReason.PERMIT_HELD,
}
logger = logging.getLogger(__name__)


class UnitPermitLost(RuntimeError):
    """O permit admitido mudou de dono ou de execução antes da confirmação do start."""


def dispatch_holder(dispatch: RunDispatch) -> PermitHolder:
    """Returns: Holder do permit; o dispatch_id é o nome da execução do dispatch."""
    return PermitHolder(dispatch.tenant_id, dispatch.run_id, dispatch.dispatch_id)


def settle_run_dispatches(
    admission: DispatchAdmissionPort, units: Iterable[RunUnit], now: datetime,
) -> None:
    """Liquida uma vez cada dispatch registrado nas unidades de um run terminal.

    Args: admission: Admissão do runtime; units: Unidades do run; now: Instante UTC.
    """
    holders = {
        PermitHolder(unit.tenant_id, unit.run_id, unit.dispatch_id)
        for unit in units if unit.dispatch_id is not None
    }
    for holder in sorted(holders, key=lambda item: item.dispatch_id):
        admission.settle(holder, now)


def _require_lease(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("reason=out_of_range field=lease_seconds")
    return value


def _proves_end(observation: ExecutionObservation, permit: UnitPermit, now: datetime) -> bool:
    if observation.presence is ExecutionPresence.TERMINAL:
        return True
    absent = observation.presence is ExecutionPresence.ABSENT
    return absent and now - permit.acquired_at >= _ABSENCE_PROOF_GRACE


class EnvironmentAdmission:
    """`DispatchAdmissionPort` sobre o gate do ambiente e o probe do executor."""

    def __init__(
        self, gate: EnvironmentGatePort, probe: ExecutionProbePort, environment: str,
        lease_seconds: int,
    ) -> None:
        """Raises: ValueError: Ambiente inválido ou lease menor que 1 segundo."""
        self._gate = gate
        self._probe = probe
        self._environment = require_environment(environment)
        self._lease_seconds = _require_lease(lease_seconds)

    def admit(self, holder: PermitHolder, now: datetime) -> UnitAdmission | DispatchDeferred:
        """Adquire o permit; o de outro holder só cede com a prova do probe.

        Returns: UnitAdmission com o token; DispatchDeferred com fence fechado ou permit ocupado.
        Raises: Erro do gate ou do probe, sem liberar nada.
        """
        command = AcquireUnitPermit(self._environment, holder, now, self._lease_seconds)
        acquisition = self._gate.acquire_unit_permit(command)
        held = acquisition.status is AcquisitionStatus.HELD
        if held and self._reconcile(cast("UnitPermit", acquisition.permit), now):
            acquisition = self._gate.acquire_unit_permit(command)
        return self._admission(holder, acquisition)

    def confirm(self, admission: UnitAdmission, execution_ref: str, now: datetime) -> None:
        """Vincula a execução iniciada ao permit admitido e renova o lease.

        Raises: ValueError: Admissão de outro ambiente.
            UnitPermitLost: Bind ou renovação recusados pelo gate.
        """
        if admission.environment != self._environment:
            raise ValueError("reason=environment_mismatch")
        token = cast("int", admission.token)
        bind = BindUnitPermit(self._environment, token, execution_ref, now)
        renew = RenewUnitPermit(self._environment, token, now, self._lease_seconds)
        if not (self._gate.bind_unit_permit(bind) and self._gate.renew_unit_permit(renew)):
            raise UnitPermitLost("reason=unit_permit_lost")

    def settle(self, holder: PermitHolder, now: datetime) -> None:
        """Libera o permit do holder só com a prova do probe; execução viva o mantém.

        Raises: Erro do gate ou do probe, sem liberar nada.
        """
        permit = self._gate.observe_environment_gate(self._environment).permit
        if permit is not None and permit.holder == holder:
            self._reconcile(permit, now)

    def _admission(
        self, holder: PermitHolder, acquisition: PermitAcquisition,
    ) -> UnitAdmission | DispatchDeferred:
        reason = _DEFERRALS.get(acquisition.status)
        if reason is None:
            return UnitAdmission(self._environment, cast("UnitPermit", acquisition.permit).token)
        logger.info(
            "dispatch_deferred environment=%s tenant_id=%s run_id=%s dispatch_id=%s reason=%s",
            self._environment, holder.tenant_id, holder.run_id, holder.dispatch_id, reason,
        )
        return DispatchDeferred(reason)

    def _reconcile(self, permit: UnitPermit, now: datetime) -> bool:
        observation = self._probe.probe(permit.holder.dispatch_id)
        if _proves_end(observation, permit, now):
            self._release(permit, observation.presence, now)
            return True
        if observation.presence is ExecutionPresence.RUNNING:
            self._keep_alive(permit, observation, now)
        return False

    def _release(self, permit: UnitPermit, presence: ExecutionPresence, now: datetime) -> None:
        command = ReleaseUnitPermit(self._environment, permit.token, now)
        applied = self._gate.release_unit_permit(command)
        logger.info(
            "unit_permit_released environment=%s dispatch_id=%s token=%d presence=%s applied=%s",
            self._environment, permit.holder.dispatch_id, permit.token, presence, applied,
        )

    def _keep_alive(
        self, permit: UnitPermit, observation: ExecutionObservation, now: datetime,
    ) -> None:
        if permit.execution_ref is None:
            execution_ref = cast("str", observation.execution_ref)
            bind = BindUnitPermit(self._environment, permit.token, execution_ref, now)
            self._gate.bind_unit_permit(bind)
            return
        renew = RenewUnitPermit(self._environment, permit.token, now, self._lease_seconds)
        self._gate.renew_unit_permit(renew)


__all__ = [
    "ABSENCE_PROOF_GRACE_SECONDS",
    "EnvironmentAdmission",
    "UnitPermitLost",
    "dispatch_holder",
    "settle_run_dispatches",
]
