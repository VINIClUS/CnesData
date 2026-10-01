"""Immediate entitlement revocation service and Stripe access-loss enforcement."""

import logging
from dataclasses import dataclass, field, replace
from typing import TypeGuard

from cnes_domain.billing.commands import SnapshotWrite
from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.execution import RunBillingState
from cnes_domain.billing.models import (
    AccessLevel,
    BillingAuditEvent,
    EntitlementAction,
    EntitlementSnapshot,
    ReadConsistency,
    SubscriptionStatus,
)
from cnes_domain.billing.policy import EntitlementPolicy
from cnes_domain.billing.revocation_models import (
    DEFAULT_SETTINGS,
    MAX_REASON_CODE_LENGTH,
    PUBLICATION_DENIABLE_RUN_STATES,
    REVOCABLE_RUN_STATES,
    REVOKED_REASON_CODE,
    CancelRunUnitsCommand,
    CancelRunUnitsResult,
    FailDeniedPublicationCommand,
    ImmediateRevocationCommand,
    RevocableRunPage,
    RevocationDependencies,
    RevocationPhase,
    RevocationProgress,
    RevocationResult,
    RevocationSettings,
    RevocationStorePort,
    RevokeRunCommand,
)
from cnes_domain.billing.validation import require_id
from cnes_domain.control_plane.entities import OutboxEvent, RunDispatch
from cnes_domain.control_plane.enums import DispatchState
from cnes_domain.ports.processing import CancelRunExecution
from cnes_domain.profiles import BillingMode

__all__ = [
    "DEFAULT_SETTINGS",
    "MAX_REASON_CODE_LENGTH",
    "PUBLICATION_DENIABLE_RUN_STATES",
    "REVOCABLE_RUN_STATES",
    "REVOKED_REASON_CODE",
    "CancelRunUnitsCommand",
    "CancelRunUnitsResult",
    "FailDeniedPublicationCommand",
    "ImmediateRevocationCommand",
    "ImmediateRevocationService",
    "RevocableRunPage",
    "RevocationDependencies",
    "RevocationPhase",
    "RevocationProgress",
    "RevocationResult",
    "RevocationSettings",
    "RevocationStorePort",
    "RevokeRunCommand",
]

logger = logging.getLogger(__name__)
_ATTEMPTS = 3
_NEXT_PHASE = {
    RevocationPhase.FENCING: RevocationPhase.CANCELING,
    RevocationPhase.CANCELING: RevocationPhase.FINALIZING,
    RevocationPhase.FINALIZING: RevocationPhase.COMPLETE,
}


@dataclass(slots=True)
class _Context:
    actor_id: str
    fenced: list[str]
    failures: list[str]
    guarded: bool = False
    failed: list[str] = field(default_factory=list)


class ImmediateRevocationService:
    def __init__(
        self,
        dependencies: RevocationDependencies,
        settings: RevocationSettings = DEFAULT_SETTINGS,
    ) -> None:
        self._deps = dependencies
        self._settings = settings
        self._policy = EntitlementPolicy(BillingMode.STRIPE)

    def revoke(self, command: ImmediateRevocationCommand) -> RevocationResult:
        """Revoga o entitlement, fenceia runs e cancela execuções.

        Args: command: Revogação administrativa idempotente.
        Returns: Versão revogada, runs fenceadas e falhas de cancelamento.
        Raises: RetryableBillingError, PermanentBillingError, BillingDisabledError.
        """
        snapshot, fresh = self._revoked_snapshot(command)
        return self._enforce(self._progress(snapshot, fresh), _Context(command.actor_id, [], []))

    def enforce_access_loss(self, snapshot: EntitlementSnapshot, actor_id: str) -> RevocationResult:
        """Fenceia runs e cancela execuções após perda de acesso já gravada no snapshot.

        Args: snapshot: Snapshot já persistido sem acesso pleno; actor_id: Ator auditado.
        Returns: Versão aplicada, runs fenceadas e falhas de cancelamento.
        Raises: RetryableBillingError, PermanentBillingError, BillingDisabledError.
        """
        require_id(actor_id, "actor_id")
        return self._converge(self._progress(snapshot, True), _Context(actor_id, [], [], True))

    def resume_pending(self, billing_account_id: str, actor_id: str) -> RevocationResult | None:
        """Retoma o progresso de revogação incompleto; versão superada não fenceia runs novas.

        Args: billing_account_id: Conta; actor_id: Ator auditado.
        Returns: Resultado da retomada, ou None sem progresso pendente.
        Raises: RetryableBillingError, PermanentBillingError, BillingDisabledError.
        """
        require_id(actor_id, "actor_id")
        stored = self._deps.store.get_revocation_progress(billing_account_id)
        if stored is None or stored.phase is RevocationPhase.COMPLETE:
            return None
        return self._converge(stored, _Context(actor_id, [], [], True))

    def _converge(self, progress: RevocationProgress, context: _Context) -> RevocationResult:
        for _ in range(_ATTEMPTS):
            result = self._enforce(progress, context)
            current = self._current(progress.billing_account_id)
            if current is None or not self._newer_denial(current, progress):
                return result
            progress = self._progress(current, True)
        raise RetryableBillingError("access_loss_enforcement_unstable")

    def _newer_denial(self, current: EntitlementSnapshot, progress: RevocationProgress) -> bool:
        if current.entitlement_version <= progress.entitlement_version:
            return False
        if current.subscription_status is SubscriptionStatus.ADMIN_REVOKED:
            return False
        return self._level(current) is not AccessLevel.FULL

    def _level(self, snapshot: EntitlementSnapshot) -> AccessLevel:
        return self._policy.evaluate(
            snapshot, EntitlementAction.SERVING_ACCESS, self._deps.clock()
        ).access_level

    def _current(self, billing_account_id: str) -> EntitlementSnapshot | None:
        return self._deps.projection.get_snapshot(billing_account_id, ReadConsistency.STRONG)

    def _enforce(self, progress: RevocationProgress, context: _Context) -> RevocationResult:
        while progress.phase is not RevocationPhase.COMPLETE:
            progress = self._advance(progress, context)
        return RevocationResult(
            progress.entitlement_version,
            tuple(context.fenced),
            tuple(context.failures),
            tuple(context.failed),
        )

    def _revoked_snapshot(
        self, command: ImmediateRevocationCommand
    ) -> tuple[EntitlementSnapshot, bool]:
        account = command.billing_account_id
        for _ in range(_ATTEMPTS):
            current = self._deps.projection.get_snapshot(account, ReadConsistency.STRONG)
            if current is None:
                raise PermanentBillingError("entitlement_snapshot_missing")
            if current.subscription_status is SubscriptionStatus.ADMIN_REVOKED:
                return current, False
            write = self._snapshot_write(current, command)
            if self._deps.projection.compare_and_set_snapshot(write):
                return write.snapshot, True
        raise RetryableBillingError("revocation_snapshot_contended")

    def _snapshot_write(
        self, current: EntitlementSnapshot, command: ImmediateRevocationCommand
    ) -> SnapshotWrite:
        now = self._deps.clock()
        account = command.billing_account_id
        version = current.entitlement_version + 1
        revoked = replace(
            current,
            subscription_status=SubscriptionStatus.ADMIN_REVOKED,
            entitlement_version=version,
            updated_at=now,
            valid_until=max(current.valid_until, now),
            source_event_id=f"revocation:{version}",
        )
        audit = BillingAuditEvent(
            event_id=f"entitlement.revoked:{account}:{version}",
            event_type="entitlement.revoked",
            aggregate_id=account,
            actor_id=command.actor_id,
            reason_code=command.reason_code,
            occurred_at=now,
            attributes={
                "entitlement_version": version,
                "previous_status": current.subscription_status.value,
            },
        )
        return SnapshotWrite(current.entitlement_version, revoked, (audit,))

    def _progress(self, snapshot: EntitlementSnapshot, fresh: bool) -> RevocationProgress:
        account = snapshot.billing_account_id
        store = self._deps.store
        stored = store.get_revocation_progress(account)
        superseded = fresh and stored is not None and (
            stored.entitlement_version < snapshot.entitlement_version
        )
        if stored is not None and not superseded:
            return stored
        start = RevocationProgress(
            account,
            snapshot.entitlement_version,
            RevocationPhase.FENCING,
            None,
            self._deps.clock(),
        )
        if store.save_revocation_progress(None, start):
            return start
        winner = store.get_revocation_progress(account)
        if winner is None:
            raise RetryableBillingError("revocation_progress_contended")
        return winner

    def _advance(self, progress: RevocationProgress, context: _Context) -> RevocationProgress:
        if progress.phase is RevocationPhase.FENCING:
            return self._fencing(progress, context)
        if progress.phase is RevocationPhase.CANCELING:
            return self._canceling(progress, context)
        return self._finalizing(progress, context)

    def _save(
        self, expected: RevocationProgress, replacement: RevocationProgress
    ) -> RevocationProgress:
        if not self._deps.store.save_revocation_progress(expected, replacement):
            raise RetryableBillingError("revocation_progress_contended")
        return replacement

    def _advanced(
        self, progress: RevocationProgress, next_cursor: str | None
    ) -> RevocationProgress:
        now = self._deps.clock()
        if next_cursor is not None:
            return replace(progress, run_cursor=next_cursor, updated_at=now)
        return replace(
            progress,
            phase=_NEXT_PHASE[progress.phase],
            run_cursor=None,
            updated_at=now,
        )

    def _page(self, progress: RevocationProgress, limit: int) -> RevocableRunPage:
        return self._deps.store.list_revocable_runs(
            progress.billing_account_id, limit, progress.run_cursor
        )

    def _skip_fencing(self, progress: RevocationProgress) -> RevocationProgress:
        return self._save(progress, self._advanced(replace(progress, run_cursor=None), None))

    def _superseded(self, progress: RevocationProgress, context: _Context) -> bool:
        if not context.guarded:
            return False
        current = self._current(progress.billing_account_id)
        if current is None:
            return True
        newer = current.entitlement_version != progress.entitlement_version
        return newer and self._level(current) is AccessLevel.FULL

    def _fencing(self, progress: RevocationProgress, context: _Context) -> RevocationProgress:
        if self._superseded(progress, context):
            logger.info("access_loss_superseded account=%s", progress.billing_account_id)
            return self._skip_fencing(progress)
        page = self._page(progress, self._settings.run_page_size)
        for state in page.runs:
            self._fence(state, progress, context)
        return self._save(progress, self._advanced(progress, page.next_cursor))

    def _fence(
        self, state: RunBillingState, progress: RevocationProgress, context: _Context
    ) -> None:
        for _ in range(_ATTEMPTS):
            try:
                return self._try_fence(state, progress, context)
            except RetryableBillingError as error:
                if error.code != "run_revocation_stale":
                    raise
        raise RetryableBillingError("run_revocation_contended")

    def _try_fence(
        self, state: RunBillingState, progress: RevocationProgress, context: _Context
    ) -> None:
        store = self._deps.store
        tenant, run_id = state.tenant_id, state.run_id
        current = store.get_run_billing_state(tenant, run_id)
        run = store.get_run(tenant, run_id)
        if current is None or run is None:
            raise PermanentBillingError("run_revocation_missing")
        if current.cancel_requested:
            return
        if run.state in PUBLICATION_DENIABLE_RUN_STATES:
            if self._fail_publication(current, progress):
                context.failed.append(run_id)
            return
        if run.state not in REVOCABLE_RUN_STATES:
            return
        now = self._deps.clock()
        command = RevokeRunCommand(
            tenant, run_id, run.state, current.fencing_token, REVOKED_REASON_CODE, now
        )
        store.request_run_revocation(command, self._fence_event(current, progress))
        context.fenced.append(run_id)

    def _fail_publication(self, current: RunBillingState, progress: RevocationProgress) -> bool:
        tenant, run_id = current.tenant_id, current.run_id
        now = self._deps.clock()
        command = FailDeniedPublicationCommand(
            tenant, run_id, current.fencing_token, REVOKED_REASON_CODE, now
        )
        event = OutboxEvent(
            tenant_id=tenant,
            event_id=f"run.failed.revoked:{tenant}:{run_id}",
            event_type="run.failed",
            aggregate_id=run_id,
            payload={
                "billing_account_id": progress.billing_account_id,
                "entitlement_version": progress.entitlement_version,
                "fencing_token": current.fencing_token,
                "reason_code": REVOKED_REASON_CODE,
            },
            created_at=now,
            delivered_at=None,
        )
        return self._deps.store.fail_denied_publication(command, event)

    def _fence_event(self, current: RunBillingState, progress: RevocationProgress) -> OutboxEvent:
        tenant, run_id = current.tenant_id, current.run_id
        version = progress.entitlement_version
        return OutboxEvent(
            tenant_id=tenant,
            event_id=f"run.cancel_requested:{tenant}:{run_id}:{version}",
            event_type="run.cancel_requested",
            aggregate_id=run_id,
            payload={
                "billing_account_id": progress.billing_account_id,
                "entitlement_version": version,
                "fencing_token": current.fencing_token + 1,
                "reason_code": REVOKED_REASON_CODE,
            },
            created_at=self._deps.clock(),
            delivered_at=None,
        )

    def _canceling(self, progress: RevocationProgress, context: _Context) -> RevocationProgress:
        page = self._page(progress, self._settings.run_page_size)
        for request in self._cancel_targets(page):
            self._cancel(request, context)
        return self._save(progress, self._advanced(progress, page.next_cursor))

    def _cancel_targets(self, page: RevocableRunPage) -> list[CancelRunExecution]:
        targets: list[CancelRunExecution] = []
        for state in page.runs:
            if not state.cancel_requested:
                continue
            dispatch = self._deps.store.get_run_dispatch(state.tenant_id, state.run_id)
            if self._executing(dispatch):
                targets.append(
                    CancelRunExecution(
                        tenant_id=state.tenant_id,
                        run_id=state.run_id,
                        execution_ref=dispatch.execution_ref,
                    )
                )
        return targets

    @staticmethod
    def _executing(dispatch: RunDispatch | None) -> TypeGuard[RunDispatch]:
        return (
            dispatch is not None
            and dispatch.state is not DispatchState.TERMINAL
            and dispatch.execution_ref is not None
        )

    def _cancel(self, request: CancelRunExecution, context: _Context) -> None:
        try:
            self._deps.executor.cancel(request)
        except Exception:
            logger.warning(
                "revocation_cancel_failed tenant_id=%s run_id=%s",
                request.tenant_id,
                request.run_id,
            )
            context.failures.append(request.run_id)

    def _finalizing(self, progress: RevocationProgress, context: _Context) -> RevocationProgress:
        page = self._page(progress, self._settings.run_page_size)
        for state in page.runs:
            if state.cancel_requested:
                self._settle(state)
                self._audit_canceled(state, progress, context.actor_id)
        return self._save(progress, self._advanced(progress, page.next_cursor))

    def _settle(self, state: RunBillingState) -> None:
        cursor: str | None = None
        while True:
            result = self._deps.store.cancel_run_units(
                CancelRunUnitsCommand(
                    state.tenant_id,
                    state.run_id,
                    state.fencing_token,
                    self._settings.unit_batch_size,
                    cursor,
                    self._deps.clock(),
                )
            )
            if result.run_canceled:
                return
            cursor = result.next_cursor

    def _audit_canceled(
        self,
        state: RunBillingState,
        progress: RevocationProgress,
        actor_id: str,
    ) -> None:
        tenant, run_id = state.tenant_id, state.run_id
        version = progress.entitlement_version
        self._deps.audit.append(
            BillingAuditEvent(
                event_id=f"run.canceled:{progress.billing_account_id}:{tenant}:{run_id}",
                event_type="run.canceled",
                aggregate_id=run_id,
                actor_id=actor_id,
                reason_code=REVOKED_REASON_CODE,
                occurred_at=self._deps.clock(),
                attributes={
                    "tenant_id": tenant,
                    "billing_account_id": progress.billing_account_id,
                    "entitlement_version": version,
                },
            )
        )
