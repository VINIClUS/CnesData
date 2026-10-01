"""Fakes em memória para os testes de revogação imediata."""

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from cnes_domain.billing.errors import BillingDisabledError, RetryableBillingError
from cnes_domain.billing.execution import RunBillingState
from cnes_domain.billing.models import (
    BillingAuditEvent,
    EntitlementSnapshot,
    QuotaLimits,
    ReadConsistency,
    RunAuthorization,
    SubscriptionStatus,
)
from cnes_domain.billing.revocation import (
    REVOCABLE_RUN_STATES,
    CancelRunUnitsCommand,
    CancelRunUnitsResult,
    ImmediateRevocationCommand,
    ImmediateRevocationService,
    RevocableRunPage,
    RevocationDependencies,
    RevocationPhase,
    RevocationProgress,
    RevocationResult,
    RevocationSettings,
    RevokeRunCommand,
)
from cnes_domain.control_plane.entities import OutboxEvent, Run, RunDependency, RunDispatch
from cnes_domain.control_plane.enums import DispatchState, RunState
from cnes_domain.ports.processing import CancelRunExecution

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
LATER = datetime(2026, 10, 30, 12, tzinfo=UTC)
TENANT = "tenant-1"
ACCOUNT = "acct-1"
WAVE = "0123456789abcdef"
DISPATCH = "fedcba9876543210"
REASON = "fraud_review"


def _snapshot(**overrides: Any) -> EntitlementSnapshot:
    values: dict[str, Any] = {
        "billing_account_id": ACCOUNT,
        "stripe_subscription_id": "sub_1",
        "subscription_status": SubscriptionStatus.ACTIVE,
        "cancel_at_period_end": False,
        "plan_version_id": "plan-1",
        "features": frozenset(),
        "quotas": QuotaLimits(None, None, None, 4, None, None),
        "period_start": NOW,
        "period_end": LATER,
        "grace_until": None,
        "valid_until": LATER,
        "entitlement_version": 3,
        "updated_at": NOW,
        "source_event_id": "evt-1",
    }
    return EntitlementSnapshot(**{**values, **overrides})


def _run(run_id: str, state: RunState = RunState.PROCESSING) -> Run:
    return Run(
        tenant_id=TENANT,
        run_id=run_id,
        competencia="2026-09",
        dataset_name="cnes",
        state=state,
        dependencies=(RunDependency(source_type="cnes", file_subtype="pf", required=True),),
        missing_sources=(),
        created_at=NOW,
    )


def _state(run_id: str, **overrides: Any) -> RunBillingState:
    values: dict[str, Any] = {
        "billing_account_id": ACCOUNT,
        "tenant_id": TENANT,
        "run_id": run_id,
        "authorization": RunAuthorization(ACCOUNT, "plan-1", 3, 4, "res-1", NOW),
        "execution_generation": 0,
        "execution_wave_id": None,
        "execution_dispatch_id": None,
        "execution_ref": None,
        "execution_unit_ids": (),
        "execution_status": None,
        "execution_terminal_outcome": None,
        "fencing_token": 7,
        "cancel_requested": False,
        "updated_at": NOW,
    }
    return RunBillingState(**{**values, **overrides})


def _dispatch(run_id: str, ref: str | None = None) -> RunDispatch:
    started = ref is not None
    return RunDispatch(
        tenant_id=TENANT,
        run_id=run_id,
        wave_id=WAVE,
        dispatch_id=DISPATCH,
        generation=1,
        unit_ids=("u1",),
        state=DispatchState.STARTED if started else DispatchState.RESERVED,
        lease_until=NOW,
        execution_ref=ref,
    )


class FakeProjection:
    def __init__(self, snapshot: EntitlementSnapshot | None, calls: list[str]) -> None:
        self.snapshot = snapshot
        self.calls = calls
        self.lose_cas = 0
        self.disabled = False
        self.writes: list[Any] = []
        self.consistencies: list[ReadConsistency] = []

    def get_snapshot(self, billing_account_id: str, consistency: ReadConsistency):
        self.consistencies.append(consistency)
        return self.snapshot

    def compare_and_set_snapshot(self, command: Any) -> bool:
        if self.disabled:
            raise BillingDisabledError("billing_disabled")
        if self.lose_cas > 0:
            self.lose_cas -= 1
            return False
        if command.expected_version != self.snapshot.entitlement_version:
            return False
        self.snapshot = command.snapshot
        self.writes.append(command)
        self.calls.append("snapshot_admin_revoked")
        return True


class FakeStore:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.runs: dict[str, Run] = {}
        self.states: dict[str, RunBillingState] = {}
        self.dispatches: dict[str, RunDispatch] = {}
        self.progress: RevocationProgress | None = None
        self.saved: list[RevocationProgress] = []
        self.units_needed: dict[str, int] = {}
        self.unit_calls: dict[str, int] = {}
        self.unit_cursors: list[str | None] = []
        self.get_run_state: dict[str, RunState] = {}
        self.hide_run: set[str] = set()
        self.hide_state: set[str] = set()
        self.stale_times = 0
        self.reject_after: int | None = None
        self.rival: RevocationProgress | None = None
        self.reject_first = False
        self.fail_on: dict[str, Exception] = {}
        self.fence_requests = 0
        self.crash_save_phase: RevocationPhase | None = None
        self.crash_on_unit_call: int | None = None

    def add_run(self, run_id: str, state: RunState = RunState.PROCESSING, ref: str | None = None):
        self.runs[run_id] = _run(run_id, state)
        self.states[run_id] = _state(run_id)
        self.dispatches[run_id] = _dispatch(run_id, ref)

    def _maybe_fail(self, name: str) -> None:
        error = self.fail_on.pop(name, None)
        if error is not None:
            raise error

    def get_run(self, tenant_id: str, run_id: str) -> Run | None:
        if run_id in self.hide_run:
            return None
        run = self.runs[run_id]
        override = self.get_run_state.get(run_id)
        return run if override is None else run.model_copy(update={"state": override})

    def get_run_billing_state(self, tenant_id: str, run_id: str) -> RunBillingState | None:
        return None if run_id in self.hide_state else self.states[run_id]

    def get_active_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None:
        return self.dispatches.get(run_id)

    def _listable(self, run_id: str, run: Run) -> bool:
        fenced_cancel = run.state is RunState.CANCELED and self.states[run_id].cancel_requested
        return run.state in REVOCABLE_RUN_STATES or fenced_cancel

    def list_revocable_runs(self, billing_account_id: str, limit: int, cursor: str | None):
        ids = sorted(
            run_id
            for run_id, run in self.runs.items()
            if self._listable(run_id, run) and (cursor is None or run_id > cursor)
        )
        page = ids[:limit]
        more = len(ids) > limit
        return RevocableRunPage(
            tuple(self.states[run_id] for run_id in page), page[-1] if more else None
        )

    def request_run_revocation(self, command: RevokeRunCommand, event: OutboxEvent):
        self._maybe_fail("request_run_revocation")
        current = self.states[command.run_id]
        if self.stale_times > 0:
            self.stale_times -= 1
            raise RetryableBillingError("run_revocation_stale")
        if command.expected_fencing_token != current.fencing_token:
            raise RetryableBillingError("run_revocation_stale")
        self.fence_requests += 1
        self.events = [*getattr(self, "events", []), event]
        self.commands = [*getattr(self, "commands", []), command]
        updated = replace(current, fencing_token=current.fencing_token + 1, cancel_requested=True)
        self.states[command.run_id] = updated
        self.calls.append(f"{command.run_id}_fence_incremented")
        return updated

    def cancel_run_units(self, command: CancelRunUnitsCommand) -> CancelRunUnitsResult:
        self._maybe_fail("cancel_run_units")
        run_id = command.run_id
        count = self.unit_calls.get(run_id, 0) + 1
        self.unit_calls[run_id] = count
        self.unit_cursors.append(command.cursor)
        self.calls.append(f"units_{run_id}")
        if count == self.crash_on_unit_call:
            raise RuntimeError("crash")
        if count < self.units_needed.get(run_id, 1):
            return CancelRunUnitsResult((f"u{count}",), f"u{count}", False)
        self.runs[run_id] = self.runs[run_id].model_copy(update={"state": RunState.CANCELED})
        return CancelRunUnitsResult((f"u{count}",), None, True)

    def get_revocation_progress(self, billing_account_id: str) -> RevocationProgress | None:
        return self.progress

    def save_revocation_progress(self, expected, replacement) -> bool:
        if expected is not None and expected.phase is self.crash_save_phase:
            self.crash_save_phase = None
            raise RuntimeError("crash")
        if expected is None and self.rival is not None:
            self.progress, self.rival = self.rival, None
            return False
        if self.reject_first:
            self.reject_first = False
            return False
        if self.reject_after is not None and len(self.saved) >= self.reject_after:
            return False
        self.progress = replacement
        self.saved.append(replacement)
        return True


class FakeExecutor:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.requests: list[CancelRunExecution] = []
        self.fail = False
        self.on_cancel: Any = None

    def start(self, request: Any) -> str:
        raise AssertionError

    def cancel(self, request: CancelRunExecution) -> None:
        self.calls.append(f"executor_{request.run_id}_cancel")
        self.requests.append(request)
        if self.on_cancel is not None:
            self.on_cancel(request)
        if self.fail:
            raise RuntimeError("step_functions_unavailable")

    def status(self, execution_ref: str) -> Any:
        raise AssertionError


class FakeAudit:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.events: list[BillingAuditEvent] = []

    def append(self, event: BillingAuditEvent) -> None:
        self.events.append(event)
        self.calls.append(f"audit_{event.event_type}")


class Harness:
    def __init__(self, snapshot: EntitlementSnapshot | None = None, page: int = 25) -> None:
        self.calls: list[str] = []
        self.projection = FakeProjection(snapshot or _snapshot(), self.calls)
        self.store = FakeStore(self.calls)
        self.executor = FakeExecutor(self.calls)
        self.audit = FakeAudit(self.calls)
        deps = RevocationDependencies(
            self.projection, self.store, self.executor, self.audit, lambda: NOW
        )
        self.service = ImmediateRevocationService(deps, RevocationSettings(run_page_size=page))

    def revoke(self) -> RevocationResult:
        return self.service.revoke(_command())


def _command(**overrides: Any) -> ImmediateRevocationCommand:
    values: dict[str, Any] = {
        "billing_account_id": ACCOUNT,
        "actor_id": "admin-1",
        "reason_code": REASON,
        "requested_at": NOW,
    }
    return ImmediateRevocationCommand(**{**values, **overrides})


def _two_runs(page: int = 25) -> Harness:
    harness = Harness(page=page)
    harness.store.add_run("run_01", ref="exec-1")
    harness.store.add_run("run_02", ref="exec-2")
    return harness
