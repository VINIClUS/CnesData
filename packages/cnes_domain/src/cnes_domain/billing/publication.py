"""Publication and unit-commit fences over the billing companion."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from cnes_domain.billing.commands import PublishGateRequest
from cnes_domain.billing.errors import EntitlementDenied, PublishDenied
from cnes_domain.billing.execution import PublicationGuard
from cnes_domain.billing.models import SubscriptionStatus
from cnes_domain.control_plane.commands import PublicationPermit
from cnes_domain.profiles import BillingMode

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.billing.execution import RunBillingState
    from cnes_domain.billing.models import EntitlementDecision, EntitlementSnapshot
    from cnes_domain.billing.ports import ClockPort
    from cnes_domain.control_plane.entities import Run


def _require_guard(state: RunBillingState, guard: PublicationGuard) -> None:
    if guard.billing_account_id != state.billing_account_id:
        raise PublishDenied("reason=billing_account_mismatch")
    if guard.expected_run_fencing_token != state.fencing_token:
        raise PublishDenied("reason=stale_fence")


def require_publication_companion(
    state: RunBillingState | None, permit: PublicationPermit, mode: BillingMode,
) -> None:
    """Args: state: Companion lido na transação; permit: Permit do publisher; mode: Execução.
    Raises: PublishDenied: Guard inválido, companion ausente, divergente, cancelado ou velho.
    """
    guard = permit.binding_context
    if mode is BillingMode.STRIPE and not isinstance(guard, PublicationGuard):
        raise PublishDenied("reason=publication_guard_invalid")
    if state is None:
        if mode is BillingMode.STRIPE:
            raise PublishDenied("reason=run_billing_state_missing")
        return
    if (state.tenant_id, state.run_id) != (permit.tenant_id, permit.run_id):
        raise PublishDenied("reason=run_billing_state_mismatch")
    if state.cancel_requested:
        raise PublishDenied("reason=run_cancel_requested")
    if state.fencing_token != permit.fencing_token:
        raise PublishDenied("reason=stale_fence")
    if isinstance(guard, PublicationGuard):
        _require_guard(state, guard)


def require_publication_snapshot(
    snapshot: EntitlementSnapshot | None, guard: PublicationGuard, now: datetime,
) -> None:
    """Args: snapshot: Snapshot lido na transação; guard: Guard do permit; now: Instante.
    Raises: PublishDenied: Snapshot ausente, revogado, expirado ou de outra versão.
    """
    if snapshot is None:
        raise PublishDenied("reason=snapshot_missing")
    if snapshot.subscription_status is SubscriptionStatus.ADMIN_REVOKED:
        raise PublishDenied("reason=admin_revoked")
    if snapshot.entitlement_version != guard.expected_entitlement_version:
        raise PublishDenied("reason=stale_entitlement")
    if snapshot.valid_until <= now:
        raise PublishDenied("reason=snapshot_expired")


def unit_companion_allows(
    state: RunBillingState | None, dispatch_id: str, mode: BillingMode,
) -> bool:
    """Args: state: Companion atual; dispatch_id: Dispatch do comando; mode: Execução.
    Returns: True se commit/fail da unidade pode prosseguir.
    """
    if state is None:
        return mode is not BillingMode.STRIPE
    if state.cancel_requested:
        return False
    return mode is not BillingMode.STRIPE or state.execution_dispatch_id == dispatch_id


@runtime_checkable
class RunBillingStateReader(Protocol):
    def get_run_billing_state(self, tenant_id: str, run_id: str) -> RunBillingState | None:
        raise NotImplementedError


@runtime_checkable
class PublishAuthorizer(Protocol):
    def authorize_publish_run(self, request: PublishGateRequest) -> EntitlementDecision:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class PublicationPolicyDependencies:
    control_plane: RunBillingStateReader
    gate: PublishAuthorizer
    clock: ClockPort
    mode: BillingMode


def _legacy_permit(run: Run) -> PublicationPermit:
    return PublicationPermit(
        tenant_id=run.tenant_id, run_id=run.run_id, policy_version=0, fencing_token=0
    )


def _require_publishable(state: RunBillingState, run: Run) -> None:
    if (state.tenant_id, state.run_id) != (run.tenant_id, run.run_id):
        raise PublishDenied("reason=run_billing_state_mismatch")
    if state.cancel_requested:
        raise PublishDenied("reason=run_cancel_requested")


class BillingPublicationPolicy:
    def __init__(self, dependencies: PublicationPolicyDependencies) -> None:
        self._dependencies = dependencies

    def __call__(self, run: Run) -> PublicationPermit:
        dependencies = self._dependencies
        state = dependencies.control_plane.get_run_billing_state(run.tenant_id, run.run_id)
        if state is None:
            if dependencies.mode is BillingMode.STRIPE:
                raise PublishDenied("reason=run_billing_state_missing")
            return _legacy_permit(run)
        _require_publishable(state, run)
        if dependencies.mode is not BillingMode.STRIPE:
            return PublicationPermit(
                tenant_id=run.tenant_id,
                run_id=run.run_id,
                policy_version=state.authorization.entitlement_version,
                fencing_token=state.fencing_token,
            )
        return self._stripe_permit(run, state)

    def _stripe_permit(self, run: Run, state: RunBillingState) -> PublicationPermit:
        request = PublishGateRequest(
            billing_account_id=state.billing_account_id,
            tenant_id=state.tenant_id,
            run_id=state.run_id,
            expected_entitlement_version=state.authorization.entitlement_version,
            expected_fencing_token=state.fencing_token,
        )
        try:
            decision = self._dependencies.gate.authorize_publish_run(request)
        except EntitlementDenied as error:
            raise PublishDenied(str(error)) from error
        guard = PublicationGuard(
            billing_account_id=state.billing_account_id,
            expected_entitlement_version=decision.entitlement_version,
            expected_run_fencing_token=state.fencing_token,
            checked_at=self._dependencies.clock(),
        )
        return PublicationPermit(
            tenant_id=run.tenant_id,
            run_id=run.run_id,
            policy_version=decision.entitlement_version,
            fencing_token=state.fencing_token,
            binding_context=guard,
        )
