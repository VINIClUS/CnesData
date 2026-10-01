"""Quota reservation settlement mixin."""

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from cnes_domain.billing.commands import ConsumeReservationCommand, ReleaseReservationCommand
from cnes_domain.billing.errors import (
    BillingDependencyError,
    RetryableBillingError,
)
from cnes_domain.billing.models import QuotaReservation, ReservationStatus
from cnes_domain.billing.ports import ClockPort
from cnes_infra.billing.dynamodb_items import (
    UNAVAILABLE_CODE,
    get_item,
    outbox_item,
    put_new,
    transact,
)
from cnes_infra.billing.dynamodb_quota_items import (
    CONSUMED_RUNS,
    decode_reservation,
    encode_reservation,
    quota_event,
    scan_attributes,
    settle_usage_update,
)
from cnes_infra.billing.keys import QUOTA_RESERVATION_LOCATOR_INDEX as LOCATOR_INDEX
from cnes_infra.billing.keys import quota_reservation_locator, usage_key
from cnes_infra.control_plane.dynamodb_codec import Action, Item, payload, put_action

_MAX_ATTEMPTS = 3
_NOT_FOUND = "quota_reservation_not_found"
_CONTENDED = "quota_reservation_contended"


@dataclass(frozen=True, slots=True)
class ReservationTransition:
    target: ReservationStatus
    at: datetime
    actual_scan_bytes: int = 0
    release_run: bool = False
    reason_code: str | None = None
    guards: tuple[Action, ...] = ()


def _settled(reservation: QuotaReservation, change: ReservationTransition) -> QuotaReservation:
    if change.target is ReservationStatus.CONSUMED:
        return replace(
            reservation,
            reserved_scan_bytes=0,
            consumed_scan_bytes=change.actual_scan_bytes,
            status=ReservationStatus.CONSUMED,
        )
    runs = 0 if change.release_run else reservation.consumed_runs
    return replace(
        reservation,
        reserved_scan_bytes=0,
        consumed_runs=runs,
        status=ReservationStatus.RELEASED,
    )


def _usage_deltas(reservation: QuotaReservation, change: ReservationTransition) -> dict[str, int]:
    scan = scan_attributes(reservation.kind)
    reserved = reservation.reserved_scan_bytes
    if change.target is ReservationStatus.CONSUMED:
        actual = change.actual_scan_bytes
        return {scan.reserved: -reserved, scan.consumed: actual, scan.committed: actual - reserved}
    deltas = {scan.reserved: -reserved, scan.committed: -reserved}
    if change.release_run:
        deltas[CONSUMED_RUNS] = -reservation.consumed_runs
    return deltas


def _event(
    reservation: QuotaReservation, tenant_id: str, change: ReservationTransition
) -> Item:
    data: dict[str, str | int] = {
        "billing_account_id": reservation.billing_account_id,
        "reservation_id": reservation.reservation_id,
        "kind": reservation.kind.value,
    }
    if change.target is ReservationStatus.CONSUMED:
        data["actual_scan_bytes"] = change.actual_scan_bytes
        return outbox_item(quota_event("quota.consumed", tenant_id, data, change.at))
    if change.reason_code is not None:
        data["reason_code"] = change.reason_code
    return outbox_item(quota_event("quota.released", tenant_id, data, change.at))


class DynamoQuotaSettlementMixin:
    _client: Any
    _table: str
    _clock: ClockPort

    def consume(self, command: ConsumeReservationCommand) -> QuotaReservation:
        """Liquida o scan real; uma reserva já liberada ainda contabiliza o medido.

        Args: command: conta, reserva, bytes reais e instante.
        Returns: Reserva armazenada após a liquidação.
        Raises: RetryableBillingError.
        """

        def decide(reservation: QuotaReservation) -> ReservationTransition | None:
            if reservation.status is ReservationStatus.CONSUMED:
                return None
            return ReservationTransition(
                ReservationStatus.CONSUMED,
                command.consumed_at,
                actual_scan_bytes=command.actual_scan_bytes,
            )

        return self._settle(command.billing_account_id, command.reservation_id, decide)

    def release(self, command: ReleaseReservationCommand) -> QuotaReservation:
        """Libera a reserva devolvendo o scan reservado; o Run segue consumido.

        Args: command: conta, reserva, instante e código do motivo.
        Returns: Reserva armazenada após a liberação.
        Raises: RetryableBillingError.
        """

        def decide(reservation: QuotaReservation) -> ReservationTransition | None:
            if reservation.status is not ReservationStatus.RESERVED:
                return None
            return ReservationTransition(
                ReservationStatus.RELEASED, command.released_at, reason_code=command.reason_code
            )

        return self._settle(command.billing_account_id, command.reservation_id, decide)

    def _settle(
        self,
        billing_account_id: str,
        reservation_id: str,
        decide: Callable[[QuotaReservation], ReservationTransition | None],
    ) -> QuotaReservation:
        for _ in range(_MAX_ATTEMPTS):
            item = self._locate_reservation(billing_account_id, reservation_id)
            if item is None:
                raise RetryableBillingError(_NOT_FOUND)
            reservation, _tenant = decode_reservation(item)
            change = decide(reservation)
            if change is None:
                return reservation
            if self._transition_reservation(item, change):
                return _settled(reservation, change)
        raise RetryableBillingError(_CONTENDED)

    def _locate_reservation(self, billing_account_id: str, reservation_id: str) -> Item | None:
        locator = quota_reservation_locator(billing_account_id, reservation_id)
        try:
            response = self._client.query(
                TableName=self._table,
                IndexName=LOCATOR_INDEX,
                KeyConditionExpression="gsi2pk = :locator",
                ExpressionAttributeValues={":locator": {"S": locator}},
                Limit=2,
            )
        except (ClientError, BotoCoreError) as error:
            raise BillingDependencyError(UNAVAILABLE_CODE) from error
        candidates = response.get("Items", ())
        if not candidates:
            return None
        key = (candidates[0]["pk"]["S"], candidates[0]["sk"]["S"])
        item = get_item(self._client, self._table, key, True)
        if item is None:
            return None
        reservation, _tenant = decode_reservation(item)
        found = (reservation.billing_account_id, reservation.reservation_id)
        return item if found == (billing_account_id, reservation_id) else None

    def consume_reserved_actions(
        self, billing_account_id: str, reservation_id: str, at: datetime
    ) -> tuple[Action, ...]:
        """Ações que consomem o scan reservado na transação do chamador.

        Returns: Ações de liquidação; vazio fora de RESERVED.
        Raises: RetryableBillingError: quota_reservation_not_found.
        """
        item = self._locate_reservation(billing_account_id, reservation_id)
        if item is None:
            raise RetryableBillingError(_NOT_FOUND)
        reservation, _tenant = decode_reservation(item)
        if reservation.status is not ReservationStatus.RESERVED:
            return ()
        change = ReservationTransition(
            ReservationStatus.CONSUMED, at, actual_scan_bytes=reservation.reserved_scan_bytes
        )
        return self._transition_actions(item, change)

    def _transition_actions(self, item: Item, change: ReservationTransition) -> tuple[Action, ...]:
        reservation, tenant_id = decode_reservation(item)
        updated = _settled(reservation, change)
        usage = usage_key(reservation.billing_account_id, reservation.period_start)
        return (
            put_action(self._table, encode_reservation(updated, tenant_id), payload(item)),
            settle_usage_update(self._table, usage, _usage_deltas(reservation, change)),
            put_new(self._table, _event(reservation, tenant_id, change)),
            *change.guards,
        )

    def _transition_reservation(self, item: Item, change: ReservationTransition) -> bool:
        return transact(self._client, self._transition_actions(item, change))
