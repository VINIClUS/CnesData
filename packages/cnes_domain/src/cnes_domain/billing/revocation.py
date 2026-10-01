"""Immediate entitlement revocation: commands, store capability and service."""

import logging
from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable

from cnes_domain.billing.commands import SnapshotWrite
from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.execution import RunBillingState
from cnes_domain.billing.models import (
    BillingAuditEvent,
    EntitlementSnapshot,
    ReadConsistency,
    SubscriptionStatus,
)
from cnes_domain.billing.ports import BillingAuditPort, ClockPort, EntitlementProjectionPort
from cnes_domain.billing.validation import (
    optional_id,
    require_fields,
    require_id,
    require_non_negative,
    require_positive,
    require_utc,
)
from cnes_domain.control_plane.entities import OutboxEvent, Run, RunDispatch
from cnes_domain.control_plane.enums import RunState
from cnes_domain.ports.processing import CancelRunExecution, ProcessorExecutorPort

logger = logging.getLogger(__name__)
_ATTEMPTS = 3

MAX_REASON_CODE_LENGTH = 128
REVOKED_REASON_CODE = "revoked"
REVOCABLE_RUN_STATES = frozenset(
    {RunState.PLANNED, RunState.WAITING_INPUTS, RunState.PROCESSING, RunState.CANCEL_REQUESTED}
)


def _check_reason(reason_code: str) -> None:
    require_id(reason_code, "reason_code")
    if len(reason_code) > MAX_REASON_CODE_LENGTH:
        raise ValueError("reason=reason_code_too_long")


@dataclass(frozen=True, slots=True)
class ImmediateRevocationCommand:
    billing_account_id: str
    actor_id: str
    reason_code: str
    requested_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("billing_account_id", "actor_id"))
        _check_reason(self.reason_code)
        require_utc(self.requested_at, "requested_at")


@dataclass(frozen=True, slots=True)
class RevocationResult:
    entitlement_version: int
    fenced_run_ids: tuple[str, ...]
    cancel_failures: tuple[str, ...]

    def __post_init__(self) -> None:
        require_positive(self.entitlement_version, "entitlement_version")


@dataclass(frozen=True, slots=True)
class RevokeRunCommand:
    tenant_id: str
    run_id: str
    expected_state: RunState
    expected_fencing_token: int
    reason_code: str
    requested_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("tenant_id", "run_id"))
        if self.expected_state not in REVOCABLE_RUN_STATES:
            raise ValueError("reason=run_state_not_revocable")
        require_non_negative(self.expected_fencing_token, "expected_fencing_token")
        _check_reason(self.reason_code)
        require_utc(self.requested_at, "requested_at")


@dataclass(frozen=True, slots=True)
class RevocableRunPage:
    runs: tuple[RunBillingState, ...]
    next_cursor: str | None

    def __post_init__(self) -> None:
        optional_id(self.next_cursor, "next_cursor")


@dataclass(frozen=True, slots=True)
class CancelRunUnitsCommand:
    tenant_id: str
    run_id: str
    expected_run_fencing_token: int
    limit: int
    cursor: str | None
    canceled_at: datetime

    def __post_init__(self) -> None:
        require_fields(self, require_id, ("tenant_id", "run_id"))
        require_non_negative(self.expected_run_fencing_token, "expected_run_fencing_token")
        require_positive(self.limit, "limit")
        optional_id(self.cursor, "cursor")
        require_utc(self.canceled_at, "canceled_at")


@dataclass(frozen=True, slots=True)
class CancelRunUnitsResult:
    canceled_unit_ids: tuple[str, ...]
    next_cursor: str | None
    run_canceled: bool

    def __post_init__(self) -> None:
        optional_id(self.next_cursor, "next_cursor")
        if self.run_canceled and self.next_cursor is not None:
            raise ValueError("reason=canceled_run_has_cursor")


class RevocationPhase(StrEnum):
    FENCING = "fencing"
    CANCELING = "canceling"
    FINALIZING = "finalizing"
    COMPLETE = "complete"


@dataclass(frozen=True, slots=True)
class RevocationProgress:
    billing_account_id: str
    entitlement_version: int
    phase: RevocationPhase
    run_cursor: str | None
    updated_at: datetime

    def __post_init__(self) -> None:
        require_id(self.billing_account_id, "billing_account_id")
        require_positive(self.entitlement_version, "entitlement_version")
        RevocationPhase(self.phase)
        optional_id(self.run_cursor, "run_cursor")
        if self.phase is RevocationPhase.COMPLETE and self.run_cursor is not None:
            raise ValueError("reason=complete_progress_has_cursor")
        require_utc(self.updated_at, "updated_at")


@runtime_checkable
class RevocationStorePort(Protocol):
    def get_run(self, tenant_id: str, run_id: str) -> Run | None: ...
    def get_run_billing_state(self, tenant_id: str, run_id: str) -> RunBillingState | None: ...
    def get_active_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None: ...
    def list_revocable_runs(
        self, billing_account_id: str, limit: int, cursor: str | None,
    ) -> RevocableRunPage: ...
    def request_run_revocation(
        self, command: RevokeRunCommand, event: OutboxEvent,
    ) -> RunBillingState: ...
    def cancel_run_units(self, command: CancelRunUnitsCommand) -> CancelRunUnitsResult: ...
    def get_revocation_progress(self, billing_account_id: str) -> RevocationProgress | None: ...
    def save_revocation_progress(
        self, expected: RevocationProgress | None, replacement: RevocationProgress,
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class RevocationDependencies:
    projection: EntitlementProjectionPort
    store: RevocationStorePort
    executor: ProcessorExecutorPort
    audit: BillingAuditPort
    clock: ClockPort


@dataclass(frozen=True, slots=True)
class RevocationSettings:
    run_page_size: int = 25
    unit_batch_size: int = 98

    def __post_init__(self) -> None:
        require_positive(self.run_page_size, "run_page_size")
        require_positive(self.unit_batch_size, "unit_batch_size")


DEFAULT_SETTINGS = RevocationSettings()
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
    guard: EntitlementSnapshot | None = None


class ImmediateRevocationService:
    def __init__(
        self,
        dependencies: RevocationDependencies,
        settings: RevocationSettings = DEFAULT_SETTINGS,
    ) -> None:
        self._deps = dependencies
        self._settings = settings

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
        Returns: Versão do snapshot, runs fenceadas e falhas de cancelamento.
        Raises: RetryableBillingError, PermanentBillingError, BillingDisabledError.
        """
        require_id(actor_id, "actor_id")
        context = _Context(actor_id, [], [], snapshot)
        return self._enforce(self._progress(snapshot, True), context)

    def _enforce(self, progress: RevocationProgress, context: _Context) -> RevocationResult:
        while progress.phase is not RevocationPhase.COMPLETE:
            progress = self._advance(progress, context)
        return RevocationResult(
            progress.entitlement_version, tuple(context.fenced), tuple(context.failures)
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

    def _require_current(self, guard: EntitlementSnapshot | None) -> None:
        if guard is None:
            return
        account = guard.billing_account_id
        current = self._deps.projection.get_snapshot(account, ReadConsistency.STRONG)
        if current is None or current.entitlement_version != guard.entitlement_version:
            raise RetryableBillingError("access_loss_snapshot_superseded")

    def _fencing(self, progress: RevocationProgress, context: _Context) -> RevocationProgress:
        self._require_current(context.guard)
        page = self._page(progress, self._settings.run_page_size)
        for state in page.runs:
            if self._fence(state, progress):
                context.fenced.append(state.run_id)
        return self._save(progress, self._advanced(progress, page.next_cursor))

    def _fence(self, state: RunBillingState, progress: RevocationProgress) -> bool:
        for _ in range(_ATTEMPTS):
            try:
                return self._try_fence(state, progress)
            except RetryableBillingError as error:
                if error.code != "run_revocation_stale":
                    raise
        raise RetryableBillingError("run_revocation_contended")

    def _try_fence(self, state: RunBillingState, progress: RevocationProgress) -> bool:
        store = self._deps.store
        tenant, run_id = state.tenant_id, state.run_id
        current = store.get_run_billing_state(tenant, run_id)
        run = store.get_run(tenant, run_id)
        if current is None or run is None:
            raise PermanentBillingError("run_revocation_missing")
        if current.cancel_requested or run.state not in REVOCABLE_RUN_STATES:
            return False
        now = self._deps.clock()
        command = RevokeRunCommand(
            tenant, run_id, run.state, current.fencing_token, REVOKED_REASON_CODE, now
        )
        store.request_run_revocation(command, self._fence_event(current, progress))
        return True

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
            dispatch = self._deps.store.get_active_run_dispatch(state.tenant_id, state.run_id)
            if dispatch is not None and dispatch.execution_ref is not None:
                targets.append(
                    CancelRunExecution(
                        tenant_id=state.tenant_id,
                        run_id=state.run_id,
                        execution_ref=dispatch.execution_ref,
                    )
                )
        return targets

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
