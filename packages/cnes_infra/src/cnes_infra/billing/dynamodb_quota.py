"""DynamoDB transactional quota and budget reservations."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from cnes_domain.billing.commands import (
    CreateRunRequest,
    ReserveAnalyticsCommand,
    ReserveRunCommand,
)
from cnes_domain.billing.errors import EntitlementDenied, PermanentBillingError, QuotaExceeded
from cnes_domain.billing.execution import RunBillingState
from cnes_domain.billing.models import (
    AnalyticsAuthorization,
    EntitlementSnapshot,
    QuotaReservation,
    ReservationKind,
    ReservationStatus,
    RunAuthorization,
)
from cnes_domain.billing.ports import ClockPort
from cnes_domain.control_plane.entities import IdempotencyRecord, Run
from cnes_domain.control_plane.enums import RunState
from cnes_infra.billing.dynamodb_items import (
    decode_snapshot,
    get_item,
    outbox_item,
    put_new,
    transact,
)
from cnes_infra.billing.dynamodb_quota_capacity import DynamoQuotaCapacityMixin
from cnes_infra.billing.dynamodb_quota_items import (
    ANALYTICS_SCOPE,
    CONSUMED_RUNS,
    IDEMPOTENCY_TTL,
    RUN_SCOPE,
    SnapshotExpectation,
    UsageGuard,
    decode_analytics_result,
    decode_run_result,
    encode_reservation,
    encode_run_billing_state,
    encode_run_lookup,
    idempotency_put,
    quota_event,
    read_replay,
    scan_attributes,
    snapshot_check,
    usage_counter,
    usage_update,
)
from cnes_infra.billing.dynamodb_quota_recovery import DynamoQuotaRecoveryMixin
from cnes_infra.billing.dynamodb_quota_settlement import DynamoQuotaSettlementMixin
from cnes_infra.billing.keys import entitlement_snapshot_key, usage_key
from cnes_infra.control_plane.dynamodb_codec import Action, Item
from cnes_infra.control_plane.dynamodb_run_codec import run_dependency_actions, run_item

RUN_FIXED_ACTIONS = 8
CONFLICT_CODE = "quota_reservation_conflict"
EXPIRY_CODE = "invalid_reservation_expiry"


@dataclass(frozen=True, slots=True)
class _RunPlan:
    command: ReserveRunCommand
    authorization: RunAuthorization
    run: Run
    reservation: QuotaReservation
    state: RunBillingState
    now: datetime


def _run_concurrency(command: ReserveRunCommand) -> int:
    limits = [command.request.requested_concurrency, command.deployment_max_concurrency]
    plan_limit = command.snapshot.quotas.max_concurrency
    return min(limits if plan_limit is None else [*limits, plan_limit])


def _run_authorization(command: ReserveRunCommand, now: datetime) -> RunAuthorization:
    return RunAuthorization(
        billing_account_id=command.request.billing_account_id,
        plan_version_id=command.snapshot.plan_version_id,
        entitlement_version=command.snapshot.entitlement_version,
        max_concurrency=_run_concurrency(command),
        budget_reservation_id=command.reservation_id,
        authorized_at=now,
    )


def _canonical_run(request: CreateRunRequest, now: datetime) -> Run:
    missing = sorted(
        f"{item.source_type}/{item.file_subtype}" for item in request.dependencies if item.required
    )
    return Run(
        tenant_id=request.tenant_id,
        run_id=request.run_id,
        competencia=request.competencia,
        dataset_name=request.dataset_name,
        state=RunState.WAITING_INPUTS,
        dependencies=request.dependencies,
        missing_sources=tuple(missing),
        created_at=now,
    )


def _run_reservation(command: ReserveRunCommand, now: datetime) -> QuotaReservation:
    request = command.request
    return QuotaReservation(
        reservation_id=command.reservation_id,
        billing_account_id=request.billing_account_id,
        resource_id=request.run_id,
        kind=ReservationKind.RUN,
        period_start=command.snapshot.period_start,
        reserved_runs=0,
        reserved_scan_bytes=request.estimated_scan_bytes,
        consumed_runs=1,
        consumed_scan_bytes=0,
        status=ReservationStatus.RESERVED,
        created_at=now,
        expires_at=command.expires_at,
    )


def _run_billing_state(
    request: CreateRunRequest, authorization: RunAuthorization, now: datetime
) -> RunBillingState:
    return RunBillingState(
        billing_account_id=request.billing_account_id,
        tenant_id=request.tenant_id,
        run_id=request.run_id,
        authorization=authorization,
        execution_generation=0,
        execution_wave_id=None,
        execution_dispatch_id=None,
        execution_ref=None,
        execution_unit_ids=(),
        execution_status=None,
        execution_terminal_outcome=None,
        fencing_token=0,
        cancel_requested=False,
        updated_at=now,
    )


def _run_plan(command: ReserveRunCommand, now: datetime) -> _RunPlan:
    authorization = _run_authorization(command, now)
    return _RunPlan(
        command,
        authorization,
        _canonical_run(command.request, now),
        _run_reservation(command, now),
        _run_billing_state(command.request, authorization, now),
        now,
    )


def _idempotency_record(
    identity: tuple[str, str, str], request_hash: str, resource_id: str, now: datetime
) -> IdempotencyRecord:
    return IdempotencyRecord(
        tenant_id=identity[0],
        scope=identity[1],
        key=identity[2],
        request_hash=request_hash,
        status="COMPLETED",
        resource_id=resource_id,
        created_at=now,
        expires_at=now + IDEMPOTENCY_TTL,
    )


def _reserved_outbox(
    reservation: QuotaReservation, tenant_id: str, extra: dict[str, str | int], now: datetime
) -> Item:
    payload = {
        "billing_account_id": reservation.billing_account_id,
        "reservation_id": reservation.reservation_id,
        "kind": reservation.kind.value,
        "tenant_id": tenant_id,
        "estimated_scan_bytes": reservation.reserved_scan_bytes,
        **extra,
    }
    return outbox_item(quota_event("quota.reserved", tenant_id, payload, now))


def _run_actions(table: str, plan: _RunPlan) -> tuple[Action, ...]:
    command, run, reservation = plan.command, plan.run, plan.reservation
    request, snapshot = command.request, command.snapshot
    account, now = request.billing_account_id, plan.now
    max_runs = snapshot.quotas.max_runs_per_period
    scan = scan_attributes(ReservationKind.RUN)
    estimate = request.estimated_scan_bytes
    guard = None if max_runs is None else UsageGuard(CONSUMED_RUNS, max_runs - 1)
    identity = (request.tenant_id, RUN_SCOPE, request.idempotency_key)
    record = _idempotency_record(identity, request.request_hash, run.run_id, now)
    extra = {"run_id": run.run_id, "entitlement_version": snapshot.entitlement_version}
    dependencies = run_dependency_actions(table, run, RUN_FIXED_ACTIONS)
    return (
        snapshot_check(
            table,
            SnapshotExpectation(
                account, snapshot.entitlement_version, snapshot.subscription_status
            ),
            now,
        ),
        usage_update(
            table,
            usage_key(account, snapshot.period_start),
            {CONSUMED_RUNS: 1, scan.reserved: estimate, scan.committed: estimate},
            guard,
        ),
        put_new(table, encode_reservation(reservation, request.tenant_id)),
        put_new(table, run_item(run)),
        *dependencies,
        put_new(table, encode_run_billing_state(plan.state)),
        put_new(table, encode_run_lookup(plan.state, reservation)),
        idempotency_put(table, record, plan.authorization),
        put_new(table, _reserved_outbox(reservation, request.tenant_id, extra, now)),
    )


def _analytics_actions(
    table: str, command: ReserveAnalyticsCommand, reservation: QuotaReservation, now: datetime
) -> tuple[Action, ...]:
    request, snapshot = command.request, command.snapshot
    account, estimate = request.billing_account_id, request.estimated_scan_bytes
    budget = snapshot.quotas.athena_scan_budget_bytes
    scan = scan_attributes(ReservationKind.ANALYTICS)
    guard = None if budget is None else UsageGuard(scan.committed, budget - estimate)
    identity = (request.tenant_id, ANALYTICS_SCOPE, request.idempotency_key)
    record = _idempotency_record(identity, request.request_hash, command.reservation_id, now)
    extra = {"query_id": request.query_id}
    expected = SnapshotExpectation(
        account, snapshot.entitlement_version, snapshot.subscription_status
    )
    outbox = _reserved_outbox(reservation, request.tenant_id, extra, now)
    return (
        snapshot_check(table, expected, now),
        usage_update(
            table,
            usage_key(account, snapshot.period_start),
            {scan.reserved: estimate, scan.committed: estimate},
            guard,
        ),
        put_new(table, encode_reservation(reservation, request.tenant_id)),
        idempotency_put(table, record, _analytics_authorization(command, now)),
        put_new(table, outbox),
    )


def _analytics_authorization(
    command: ReserveAnalyticsCommand, now: datetime
) -> AnalyticsAuthorization:
    return AnalyticsAuthorization(
        billing_account_id=command.request.billing_account_id,
        entitlement_version=command.snapshot.entitlement_version,
        budget_reservation_id=command.reservation_id,
        max_scan_bytes=command.request.estimated_scan_bytes,
        authorized_at=now,
    )


def _analytics_reservation(command: ReserveAnalyticsCommand, now: datetime) -> QuotaReservation:
    request = command.request
    return QuotaReservation(
        reservation_id=command.reservation_id,
        billing_account_id=request.billing_account_id,
        resource_id=request.query_id,
        kind=ReservationKind.ANALYTICS,
        period_start=command.snapshot.period_start,
        reserved_runs=0,
        reserved_scan_bytes=request.estimated_scan_bytes,
        consumed_runs=0,
        consumed_scan_bytes=0,
        status=ReservationStatus.RESERVED,
        created_at=now,
        expires_at=command.expires_at,
    )


def _require_future(expires_at: datetime, now: datetime) -> None:
    if expires_at <= now:
        raise PermanentBillingError(EXPIRY_CODE)


def _runs_message(limit: int) -> str:
    return f"reason=max_runs_per_period_exceeded limit={limit}"


def _budget_message(limit: int) -> str:
    return f"reason=athena_scan_budget_exceeded limit={limit}"


def _runs_exceeded(snapshot: EntitlementSnapshot, usage: Item | None) -> str | None:
    limit = snapshot.quotas.max_runs_per_period
    if limit is not None and usage_counter(usage, CONSUMED_RUNS) >= limit:
        return _runs_message(limit)
    return None


def _budget_exceeded(
    estimate: int,
) -> Callable[[EntitlementSnapshot, Item | None], str | None]:
    committed = scan_attributes(ReservationKind.ANALYTICS).committed

    def check(snapshot: EntitlementSnapshot, usage: Item | None) -> str | None:
        budget = snapshot.quotas.athena_scan_budget_bytes
        if budget is not None and usage_counter(usage, committed) + estimate > budget:
            return _budget_message(budget)
        return None

    return check


def _snapshot_drifted(
    stored: EntitlementSnapshot | None, expected: EntitlementSnapshot, now: datetime
) -> bool:
    return (
        stored is None
        or stored.entitlement_version != expected.entitlement_version
        or stored.subscription_status is not expected.subscription_status
        or stored.valid_until <= now
    )


class DynamoQuotaReservations(
    DynamoQuotaCapacityMixin, DynamoQuotaSettlementMixin, DynamoQuotaRecoveryMixin
):
    """Reservas de quota e budget em transações DynamoDB únicas."""

    def __init__(self, client: Any, table_name: str, clock: ClockPort) -> None:
        self._client = client
        self._table = table_name
        self._clock = clock

    def reserve_and_create_run(self, command: ReserveRunCommand) -> RunAuthorization:
        """Reserva o run, cria o Run canônico e o companion em uma transação.
        Args: command: pedido, snapshot, teto do deploy e reserva.
        Returns: Autorização de execução (a mesma em replays).
        Raises: QuotaExceeded, EntitlementDenied, IdempotencyConflict, PermanentBillingError;
            Conflict(TRANSACTION_LIMIT) com mais de 92 dependências.
        """
        request, snapshot = command.request, command.snapshot
        identity = (request.tenant_id, RUN_SCOPE, request.idempotency_key)
        stored = read_replay(self._client, self._table, identity, request.request_hash)
        if stored is not None:
            return decode_run_result(stored)
        now = self._clock()
        _require_future(command.expires_at, now)
        max_runs = snapshot.quotas.max_runs_per_period
        if max_runs is not None and max_runs < 1:
            raise QuotaExceeded(_runs_message(max_runs))
        plan = _run_plan(command, now)
        if transact(self._client, _run_actions(self._table, plan)):
            return plan.authorization
        failed = self._resolve_failure(identity, request.request_hash, snapshot, _runs_exceeded)
        return decode_run_result(failed)

    def reserve_analytics(self, command: ReserveAnalyticsCommand) -> AnalyticsAuthorization:
        """Reserva o budget de scan analítico em uma transação.

        Args: command: pedido, snapshot e reserva.
        Returns: Autorização analítica (a mesma em replays).
        Raises: QuotaExceeded, EntitlementDenied, IdempotencyConflict, PermanentBillingError.
        """
        request, snapshot = command.request, command.snapshot
        identity = (request.tenant_id, ANALYTICS_SCOPE, request.idempotency_key)
        stored = read_replay(self._client, self._table, identity, request.request_hash)
        if stored is not None:
            return decode_analytics_result(stored)
        now = self._clock()
        _require_future(command.expires_at, now)
        budget = snapshot.quotas.athena_scan_budget_bytes
        estimate = request.estimated_scan_bytes
        if budget is not None and budget - estimate < 0:
            raise QuotaExceeded(_budget_message(budget))
        reservation = _analytics_reservation(command, now)
        if transact(self._client, _analytics_actions(self._table, command, reservation, now)):
            return _analytics_authorization(command, now)
        check = _budget_exceeded(estimate)
        failed = self._resolve_failure(identity, request.request_hash, snapshot, check)
        return decode_analytics_result(failed)

    def _resolve_failure(
        self,
        identity: tuple[str, str, str],
        request_hash: str,
        snapshot: EntitlementSnapshot,
        exceeded: Callable[[EntitlementSnapshot, Item | None], str | None],
    ) -> Item:
        stored = read_replay(self._client, self._table, identity, request_hash)
        if stored is not None:
            return stored
        account = snapshot.billing_account_id
        item = get_item(self._client, self._table, entitlement_snapshot_key(account), True)
        current = None if item is None else decode_snapshot(item, account)
        if _snapshot_drifted(current, snapshot, self._clock()):
            raise EntitlementDenied("reason=snapshot_changed")
        usage = get_item(self._client, self._table, usage_key(account, snapshot.period_start), True)
        message = exceeded(snapshot, usage)
        if message is not None:
            raise QuotaExceeded(message)
        raise PermanentBillingError(CONFLICT_CODE)
