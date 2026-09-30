"""Quota reservation capacity mixin."""

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any

from cnes_domain.billing.commands import (
    CapacityReservationCommand,
    ConsumeCapacityCommand,
    ReleaseCapacityCommand,
)
from cnes_domain.billing.errors import (
    EntitlementDenied,
    PermanentBillingError,
    QuotaExceeded,
    RetryableBillingError,
)
from cnes_domain.billing.models import CapacityReservation, ReservationStatus
from cnes_domain.billing.ports import ClockPort
from cnes_domain.control_plane.entities import IdempotencyRecord
from cnes_infra.billing.dynamodb_items import (
    decode_snapshot,
    deterministic_id,
    get_item,
    outbox_item,
    put_new,
    transact,
)
from cnes_infra.billing.dynamodb_quota_items import (
    CAPACITY_COUNTERS,
    CAPACITY_SCOPE,
    IDEMPOTENCY_TTL,
    SnapshotExpectation,
    UsageGuard,
    decode_capacity_reservation,
    decode_capacity_result,
    encode_capacity_reservation,
    idempotency_put,
    quota_event,
    read_replay,
    settle_usage_update,
    snapshot_check,
    usage_counter,
    usage_update,
)
from cnes_infra.billing.keys import (
    capacity_reservation_key,
    capacity_usage_key,
    entitlement_snapshot_key,
)
from cnes_infra.control_plane.dynamodb_codec import Action, Item, payload, put_action

CAPACITY_RESERVATION_TTL = timedelta(minutes=15)
_MAX_ATTEMPTS = 3
_EVENT_TYPES = {
    ReservationStatus.CONSUMED: "quota.consumed",
    ReservationStatus.RELEASED: "quota.released",
}


@dataclass(frozen=True, slots=True)
class CapacityTransition:
    target: ReservationStatus
    at: datetime
    reason_code: str | None = None
    guards: tuple[Action, ...] = ()


def _event_payload(
    reservation: CapacityReservation, reason_code: str | None
) -> dict[str, str | int]:
    data: dict[str, str | int] = {
        "billing_account_id": reservation.billing_account_id,
        "reservation_id": reservation.reservation_id,
        "kind": reservation.kind.value,
        "resource_id": reservation.resource_id,
    }
    if reason_code is not None:
        data["reason_code"] = reason_code
    return data


def _new_reservation(command: CapacityReservationCommand, now: datetime) -> CapacityReservation:
    account = command.billing_account_id
    return CapacityReservation(
        reservation_id=deterministic_id(
            CAPACITY_SCOPE, account, command.tenant_id, command.idempotency_key
        ),
        billing_account_id=account,
        resource_id=command.resource_id,
        kind=command.kind,
        status=ReservationStatus.RESERVED,
        created_at=now,
        expires_at=now + CAPACITY_RESERVATION_TTL,
    )


def _exceeded(command: CapacityReservationCommand) -> QuotaExceeded:
    return QuotaExceeded(f"reason=max_{command.kind.value}s_exceeded limit={command.limit}")


def _identity(command: CapacityReservationCommand) -> tuple[str, str, str]:
    return command.tenant_id, CAPACITY_SCOPE, command.idempotency_key


def _decided(current: CapacityReservation, target: ReservationStatus) -> bool:
    if current.status is ReservationStatus.RELEASED and target is ReservationStatus.CONSUMED:
        raise PermanentBillingError("capacity_reservation_released")
    return current.status is target


class DynamoQuotaCapacityMixin:
    _client: Any
    _table: str
    _clock: ClockPort

    def reserve_capacity(self, command: CapacityReservationCommand) -> CapacityReservation:
        """Reserva um slot de tenant ou agente de forma idempotente.

        Args: command: reserva com limite e versão do entitlement.
        Returns: Reserva de capacidade gravada.
        Raises: QuotaExceeded, EntitlementDenied, IdempotencyConflict.
        """
        now = self._clock()
        stored = read_replay(self._client, self._table, _identity(command), command.request_hash)
        if stored is not None:
            return decode_capacity_result(stored)
        if command.limit is not None and command.limit < 1:
            raise _exceeded(command)
        reservation = _new_reservation(command, now)
        if transact(self._client, self._reserve_actions(command, reservation, now)):
            return reservation
        return self._classify_reserve_failure(command, now)

    def consume_capacity(self, command: ConsumeCapacityCommand) -> CapacityReservation:
        """Confirma o uso da reserva de capacidade sem alterar o contador.

        Args: command: conta, reserva e instante do consumo.
        Returns: Reserva consumida.
        Raises: PermanentBillingError, RetryableBillingError.
        """
        change = CapacityTransition(ReservationStatus.CONSUMED, command.consumed_at)
        return self._settle_capacity(command.billing_account_id, command.reservation_id, change)

    def release_capacity(self, command: ReleaseCapacityCommand) -> CapacityReservation:
        """Libera a reserva de capacidade devolvendo o slot uma única vez.

        Args: command: conta, reserva, instante e motivo.
        Returns: Reserva liberada.
        Raises: PermanentBillingError, RetryableBillingError.
        """
        change = CapacityTransition(
            ReservationStatus.RELEASED, command.released_at, command.reason_code
        )
        return self._settle_capacity(command.billing_account_id, command.reservation_id, change)

    def _reserve_actions(
        self, command: CapacityReservationCommand, reservation: CapacityReservation, now: datetime
    ) -> tuple[Action, ...]:
        account, tenant = command.billing_account_id, command.tenant_id
        counter = CAPACITY_COUNTERS[command.kind]
        limit = command.limit
        guard = None if limit is None else UsageGuard(counter, limit - 1)
        expected = SnapshotExpectation(account, command.entitlement_version, None)
        record = IdempotencyRecord(
            tenant_id=tenant,
            scope=CAPACITY_SCOPE,
            key=command.idempotency_key,
            request_hash=command.request_hash,
            status="COMPLETED",
            resource_id=reservation.reservation_id,
            created_at=now,
            expires_at=now + IDEMPOTENCY_TTL,
        )
        event = quota_event(
            "quota.reserved", tenant, _event_payload(reservation, None), now
        )
        return (
            snapshot_check(self._table, expected, now),
            usage_update(self._table, capacity_usage_key(account), {counter: 1}, guard),
            put_new(self._table, encode_capacity_reservation(reservation, tenant)),
            idempotency_put(self._table, record, reservation),
            put_new(self._table, outbox_item(event)),
        )

    def _classify_reserve_failure(
        self, command: CapacityReservationCommand, now: datetime
    ) -> CapacityReservation:
        stored = read_replay(self._client, self._table, _identity(command), command.request_hash)
        if stored is not None:
            return decode_capacity_result(stored)
        account = command.billing_account_id
        snapshot_item = get_item(self._client, self._table, entitlement_snapshot_key(account), True)
        if snapshot_item is None:
            raise EntitlementDenied("reason=snapshot_changed")
        snapshot = decode_snapshot(snapshot_item, account)
        if (
            snapshot.entitlement_version != command.entitlement_version
            or snapshot.valid_until <= now
        ):
            raise EntitlementDenied("reason=snapshot_changed")
        usage = get_item(self._client, self._table, capacity_usage_key(account), True)
        counter = usage_counter(usage, CAPACITY_COUNTERS[command.kind])
        if command.limit is not None and counter >= command.limit:
            raise _exceeded(command)
        raise PermanentBillingError("capacity_reservation_conflict")

    def _settle_capacity(
        self, account: str, reservation_id: str, change: CapacityTransition
    ) -> CapacityReservation:
        key = capacity_reservation_key(account, reservation_id)
        for _ in range(_MAX_ATTEMPTS):
            item = get_item(self._client, self._table, key, True)
            if item is None:
                raise PermanentBillingError("capacity_reservation_missing")
            current, _tenant = decode_capacity_reservation(item)
            if _decided(current, change.target):
                return current
            if self._transition_capacity(item, change):
                return replace(current, status=change.target)
        raise RetryableBillingError("capacity_reservation_contended")

    def _transition_capacity(self, item: Item, change: CapacityTransition) -> bool:
        current, tenant = decode_capacity_reservation(item)
        updated = replace(current, status=change.target)
        account = current.billing_account_id
        actions = [
            put_action(
                self._table, encode_capacity_reservation(updated, tenant), payload(item)
            )
        ]
        if change.target is ReservationStatus.RELEASED:
            counter = CAPACITY_COUNTERS[current.kind]
            actions.append(
                settle_usage_update(self._table, capacity_usage_key(account), {counter: -1})
            )
        event = quota_event(
            _EVENT_TYPES[change.target],
            tenant,
            _event_payload(current, change.reason_code),
            change.at,
        )
        actions.append(put_new(self._table, outbox_item(event)))
        return transact(self._client, (*actions, *change.guards))
