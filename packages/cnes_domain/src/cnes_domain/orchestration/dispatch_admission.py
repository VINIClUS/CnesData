"""Admissão do dispatch no gate do ambiente: acquire, quota mensal, probe e liquidação."""

from __future__ import annotations

import json
import logging
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING, Final, cast

from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    CancelReservedRunDispatch,
    FinishRunDispatch,
)
from cnes_domain.control_plane.dispatch_start import DispatchStartClaim, request_sha256
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState
from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    AcquisitionStatus,
    BindUnitPermit,
    FenceState,
    PermitHolder,
    ReanchorUnitPermit,
    ReleaseUnitPermit,
    RenewUnitPermit,
    require_environment,
)
from cnes_domain.control_plane.execution_quota import (
    AttemptStatus,
    ConsumeExecutionAttempt,
    execution_period,
)
from cnes_domain.orchestration.mode_canary import (
    SyntheticContextRequired,
    is_mode_canary_namespace,
)
from cnes_domain.ports.environment_gate import (
    DeferralReason,
    DispatchDeferred,
    ExecutionObservation,
    ExecutionPresence,
    UnitAdmission,
)

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import datetime

    from cnes_domain.control_plane.entities import RunDispatch, RunUnit
    from cnes_domain.control_plane.environment_gate import PermitAcquisition, UnitPermit
    from cnes_domain.orchestration.mode_canary import ModeCanaryAdmissionProvider
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.environment_gate import (
        DispatchAdmissionPort,
        EnvironmentGatePort,
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


def dispatch_holder(dispatch: RunDispatch, request: object | None = None) -> PermitHolder:
    """Returns: Holder do permit; o dispatch_id é o nome da execução do dispatch."""
    return PermitHolder(
        dispatch.tenant_id,
        dispatch.run_id,
        dispatch.dispatch_id,
        dispatch.generation,
        request_sha256(request) if request is not None else None,
    )


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
    # A bound permit names an execution that existed: only a terminal status ends it.
    if observation.presence is not ExecutionPresence.ABSENT or permit.execution_ref is not None:
        return False
    return now - permit.acquired_at >= _ABSENCE_PROOF_GRACE


def _unbound_reentry(status: AcquisitionStatus, permit: UnitPermit) -> bool:
    return status is AcquisitionStatus.REENTERED and permit.execution_ref is None


class EnvironmentAdmission:
    """`DispatchAdmissionPort` sobre o gate do ambiente, sua quota mensal e o probe do executor."""

    def __init__(
        self,
        gate: EnvironmentGatePort,
        probe: ExecutionProbePort,
        environment: str,
        lease_seconds: int,
        control_plane: ControlPlanePort | None = None,
        *,
        mode_canary: ModeCanaryAdmissionProvider | None = None,
    ) -> None:
        """Raises: ValueError: Ambiente inválido ou lease menor que 1 segundo."""
        self._gate = gate
        self._probe = probe
        self._environment = require_environment(environment)
        self._lease_seconds = _require_lease(lease_seconds)
        self._control_plane: ControlPlanePort | None = control_plane
        self._mode_canary = mode_canary

    def admit(self, holder: PermitHolder, now: datetime) -> UnitAdmission | DispatchDeferred:
        """Adquire o permit e só então conta a tentativa do mês; outro holder cede pelo probe.

        Returns: UnitAdmission com o token; DispatchDeferred com fence fechado (inclusive na
            reentrada sem bind sem execução), permit ocupado, reancoragem recusada ou quota
            mensal esgotada, que libera o permit adquirido nesta chamada.
        Raises: Erro do gate ou do probe, sem liberar nada.
        """
        command = AcquireUnitPermit(self._environment, holder, now, self._lease_seconds)
        command = replace(command, canary_promotion_id=self._promotion_id(holder, now))
        acquisition = self._gate.acquire_unit_permit(command)
        held = acquisition.status is AcquisitionStatus.HELD
        if held and self._reconcile(cast("UnitPermit", acquisition.permit), now):
            acquisition = self._gate.acquire_unit_permit(command)
        return self._admission(holder, acquisition, now)

    def _promotion_id(self, holder: PermitHolder, now: datetime) -> str | None:
        if not is_mode_canary_namespace(holder.tenant_id):
            return None
        if self._mode_canary is None:
            raise SyntheticContextRequired("reason=synthetic_context_required field=tenant_id")
        return self._mode_canary.binding_for(holder, now).promotion_id

    def _may_mutate(self, permit: UnitPermit, now: datetime) -> bool:
        holder = permit.holder
        if not is_mode_canary_namespace(holder.tenant_id):
            return True
        try:
            self._promotion_id(holder, now)
        except SyntheticContextRequired:
            logger.info(
                "synthetic_permit_untouched environment=%s dispatch_id=%s",
                self._environment, holder.dispatch_id,
            )
            return False
        return True

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

    def reconcile(self, now: datetime) -> None:
        """Libera o permit do detentor atual só com a prova do probe; independe do fence.

        Nunca adquire nem inicia execução; execução viva sem bind é vinculada.
        Raises: Erro do gate ou do probe, sem liberar nada.
        """
        permit = self._gate.observe_environment_gate(self._environment).permit
        if permit is not None:
            self._reconcile(permit, now)

    def _admission(
        self, holder: PermitHolder, acquisition: PermitAcquisition, now: datetime,
    ) -> UnitAdmission | DispatchDeferred:
        reason = _DEFERRALS.get(acquisition.status)
        permit = cast("UnitPermit", acquisition.permit)
        if reason is None and _unbound_reentry(acquisition.status, permit):
            reason = self._reentry_deferral(permit, now)
        if reason is None:
            reason = self._attempt_deferral(acquisition.status, permit, now)
        if reason is None:
            version = None
            if self._control_plane is not None:
                version = self._gate.observe_environment_gate(self._environment).fence.version
            return UnitAdmission(
                self._environment, permit.token, version, permit.canary_promotion_id
            )
        logger.info(
            "dispatch_deferred environment=%s tenant_id=%s run_id=%s dispatch_id=%s reason=%s",
            self._environment, holder.tenant_id, holder.run_id, holder.dispatch_id, reason,
        )
        return DispatchDeferred(reason)

    def _reentry_deferral(self, permit: UnitPermit, now: datetime) -> DeferralReason | None:
        # Behind a closed fence only an execution that already exists may resume: StartExecution
        # by the same name returns it, while an absent one would start anew after the close.
        if self._fence_closed() and self._absent(permit):
            return DeferralReason.FENCE_CLOSED
        # A late re-entry may start the execution only now: the absence grace restarts here.
        reanchor = ReanchorUnitPermit(self._environment, permit.token, now)
        applied = self._gate.reanchor_unit_permit(reanchor)
        logger.info(
            "unit_permit_reanchored environment=%s dispatch_id=%s token=%d applied=%s",
            self._environment, permit.holder.dispatch_id, permit.token, applied,
        )
        return None if applied else DeferralReason.PERMIT_HELD

    def _attempt_deferral(
        self, status: AcquisitionStatus, permit: UnitPermit, now: datetime,
    ) -> DeferralReason | None:
        # Only a held permit spends an attempt: every gate deferral above leaves the month intact.
        command = ConsumeExecutionAttempt(
            self._environment, execution_period(now), permit.holder.dispatch_id, now,
        )
        outcome = self._gate.consume_execution_attempt(command)
        logger.info(
            "execution_attempt_consumed environment=%s dispatch_id=%s period=%s status=%s "
            "attempts=%d limit=%d",
            self._environment, command.dispatch_id, command.period, outcome.status,
            outcome.attempts, outcome.limit,
        )
        if outcome.status is not AttemptStatus.EXHAUSTED:
            return None
        # A re-entered permit may cover a start still in flight: only the reconcile settles it.
        if status is AcquisitionStatus.ACQUIRED:
            self._release_unstarted(permit, now)
        return DeferralReason.MONTHLY_ATTEMPTS_EXHAUSTED

    def _release_unstarted(self, permit: UnitPermit, now: datetime) -> None:
        if self._control_plane is not None:
            if self._cancel_reserved(permit, now):
                self._release(
                    permit,
                    ExecutionObservation(ExecutionPresence.ABSENT, None),
                    now,
                    "CANCEL_BEFORE_SEND",
                )
            return
        # The claim of this call wrote `now` as the anchor, while the permit read afterwards may
        # already carry a rival re-entry's re-anchor: only the untouched claim is released.
        command = ReleaseUnitPermit(self._environment, permit.token, now, now)
        applied = self._gate.release_unit_permit(command)
        logger.info(
            "unit_permit_released environment=%s dispatch_id=%s token=%d reason=%s applied=%s",
            self._environment, permit.holder.dispatch_id, permit.token,
            DeferralReason.MONTHLY_ATTEMPTS_EXHAUSTED, applied,
        )

    def _fence_closed(self) -> bool:
        fence = self._gate.observe_environment_gate(self._environment).fence
        return fence.state is FenceState.CLOSED

    def _absent(self, permit: UnitPermit) -> bool:
        observation = self._probe.probe(permit.holder.dispatch_id)
        return observation.presence is ExecutionPresence.ABSENT

    def _reconcile(self, permit: UnitPermit, now: datetime) -> bool:
        if not self._may_mutate(permit, now):
            return False
        observation = self._probe.probe(permit.holder.dispatch_id)
        if observation.presence is ExecutionPresence.TERMINAL:
            return self._settle_terminal(permit, observation, now)
        if observation.presence is ExecutionPresence.ABSENT:
            if self._control_plane is not None:
                if not self._cancel_reserved(permit, now):
                    return False
                return self._release(permit, observation, now, "CANCEL_BEFORE_SEND")
            if permit.generation is None and _proves_end(observation, permit, now):
                return self._release(permit, observation, now, "ABSENT_PROVEN")
        if observation.presence is ExecutionPresence.RUNNING:
            self._keep_alive(permit, observation, now)
        elif permit.execution_ref is not None:
            logger.warning(
                "unit_permit_bound_absent environment=%s dispatch_id=%s token=%d",
                self._environment,
                permit.holder.dispatch_id,
                permit.token,
            )
        return False

    def _settle_terminal(
        self, permit: UnitPermit, observation: ExecutionObservation, now: datetime
    ) -> bool:
        if (
            permit.execution_ref not in {None, observation.execution_ref}
            or observation.request_sha256 not in {None, permit.request_sha256}
            or not self._terminal_dispatch(permit, observation, now)
        ):
            return False
        return self._release(permit, observation, now, "TERMINAL")

    def _terminal_dispatch(
        self, permit: UnitPermit, observation: ExecutionObservation, now: datetime
    ) -> bool:
        if self._control_plane is None:
            return True
        dispatch = self._control_plane.get_run_dispatch(
            permit.holder.tenant_id, permit.holder.run_id
        )
        if dispatch is None or dispatch.dispatch_id != permit.holder.dispatch_id:
            return False
        claim = dispatch.start_claim
        if (
            claim is None
            or claim.permit_token != permit.token
            or claim.generation != permit.generation
        ):
            return False
        if observation.evidence is None:
            return True
        outcomes = {
            "SUCCEEDED": DispatchOutcome.SUCCEEDED,
            "FAILED": DispatchOutcome.FAILED,
            "TIMED_OUT": DispatchOutcome.FAILED,
            "ABORTED": DispatchOutcome.CANCELED,
        }
        status = json.loads(observation.evidence or b"{}").get("status")
        if status not in outcomes:
            return False
        if dispatch.state is not DispatchState.TERMINAL:
            self._control_plane.finish_run_dispatch(
                FinishRunDispatch(
                    tenant_id=dispatch.tenant_id,
                    run_id=dispatch.run_id,
                    dispatch_id=dispatch.dispatch_id,
                    outcome=outcomes[status],
                    finished_at=now,
                    claim=claim,
                )
            )
        return True

    def _release(
        self,
        permit: UnitPermit,
        observation: ExecutionObservation,
        now: datetime,
        resolution: str,
    ) -> bool:
        evidence = {
            "dispatch_id": permit.holder.dispatch_id,
            "tenant_id": permit.holder.tenant_id,
            "run_id": permit.holder.run_id,
            "token": permit.token,
            "generation": permit.generation,
            "request_sha256": permit.request_sha256,
            "execution_ref": observation.execution_ref,
            "resolution": resolution,
            "observed_at": now.isoformat().replace("+00:00", "Z"),
            "send_disabled_at": now.isoformat().replace("+00:00", "Z"),
            "observation": json.loads(observation.evidence) if observation.evidence else None,
        }
        if resolution == "CANCEL_BEFORE_SEND" and self._control_plane is not None:
            canceled = self._control_plane.get_run_dispatch(
                permit.holder.tenant_id, permit.holder.run_id
            )
            evidence["cancellation"] = canceled.model_dump(mode="json") if canceled else None
        proof = (json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n").encode()
        anchor = permit.acquired_at if observation.presence is ExecutionPresence.ABSENT else None
        return self._gate.release_unit_permit(
            ReleaseUnitPermit(
                self._environment,
                permit.token,
                now,
                anchor,
                permit.generation,
                resolution,
                proof,
            )
        )

    def _keep_alive(
        self, permit: UnitPermit, observation: ExecutionObservation, now: datetime,
    ) -> None:
        if observation.request_sha256 not in {None, permit.request_sha256}:
            return
        if self._control_plane is not None:
            dispatch = self._control_plane.get_run_dispatch(
                permit.holder.tenant_id, permit.holder.run_id
            )
            if dispatch is None or dispatch.dispatch_id != permit.holder.dispatch_id:
                return
            if dispatch.state is DispatchState.STARTING:
                if (
                    dispatch.start_claim is None
                    or dispatch.start_claim.permit_token != permit.token
                ):
                    return
                self._control_plane.bind_run_dispatch(
                    BindRunDispatch(
                        tenant_id=dispatch.tenant_id,
                        run_id=dispatch.run_id,
                        dispatch_id=dispatch.dispatch_id,
                        execution_ref=cast("str", observation.execution_ref),
                        now=now,
                        lease_seconds=self._lease_seconds,
                        claim=dispatch.start_claim,
                    )
                )
        if permit.execution_ref is None:
            execution_ref = cast("str", observation.execution_ref)
            bind = BindUnitPermit(self._environment, permit.token, execution_ref, now)
            self._gate.bind_unit_permit(bind)
            return
        renew = RenewUnitPermit(self._environment, permit.token, now, self._lease_seconds)
        self._gate.renew_unit_permit(renew)

    def _cancel_reserved(self, permit: UnitPermit, now: datetime) -> bool:
        if self._control_plane is None or permit.start_state != "RESERVED":
            return False
        dispatch = self._control_plane.get_run_dispatch(
            permit.holder.tenant_id, permit.holder.run_id
        )
        if (
            dispatch is None
            or dispatch.dispatch_id != permit.holder.dispatch_id
            or dispatch.generation != permit.generation
            or permit.request_sha256 is None
        ):
            return False
        claim = DispatchStartClaim(
            dispatch_id=dispatch.dispatch_id,
            generation=dispatch.generation,
            permit_token=permit.token,
            fence_version=permit.fence_version or 0,
            request_sha256=permit.request_sha256,
            promotion_id=permit.canary_promotion_id,
        )
        if dispatch.state is DispatchState.TERMINAL:
            return (
                dispatch.terminal_outcome is DispatchOutcome.CANCELED
                and dispatch.start_claim == claim
                and dispatch.canceled_before_send
            )
        canceled = self._control_plane.cancel_reserved_run_dispatch(
            CancelReservedRunDispatch(
                tenant_id=dispatch.tenant_id,
                run_id=dispatch.run_id,
                dispatch_id=dispatch.dispatch_id,
                claim=claim,
                canceled_at=now,
            )
        )
        return canceled is not None


__all__ = [
    "ABSENCE_PROOF_GRACE_SECONDS",
    "EnvironmentAdmission",
    "UnitPermitLost",
    "dispatch_holder",
    "settle_run_dispatches",
]
