"""Quota reservation recovery mixin."""

import base64
import json
from dataclasses import replace
from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError

from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
from cnes_domain.billing.inbox import ReservationRecoveryRequest, ReservationRecoveryResult
from cnes_domain.billing.models import (
    CapacityKind,
    CapacityReservation,
    QuotaReservation,
    ReservationKind,
    ReservationStatus,
)
from cnes_domain.billing.ports import ClockPort
from cnes_domain.control_plane.entities import Run
from cnes_domain.control_plane.enums import RunState
from cnes_infra.billing.dynamodb_items import (
    UNAVAILABLE_CODE,
    canonical_json,
    get_item,
    transact,
    utc_attribute,
)
from cnes_infra.billing.dynamodb_quota_capacity import CapacityTransition
from cnes_infra.billing.dynamodb_quota_items import (
    CAPACITY_RESERVATION_ENTITY,
    RESERVATION_ENTITY,
    decode_capacity_reservation,
    decode_reservation,
    encode_reservation,
)
from cnes_infra.billing.dynamodb_quota_settlement import ReservationTransition
from cnes_infra.billing.keys import (
    QUOTA_RESERVATION_DUE_INDEX,
    QUOTA_RESERVATION_DUE_PARTITION,
    Key,
    tenant_entity_key,
)
from cnes_infra.control_plane.dynamodb_codec import (
    Item,
    absent_check_action,
    decode_model,
    payload,
    put_action,
)
from cnes_infra.control_plane.dynamodb_keys import entity_key, run_entity_key

_CURSOR_CODE = "invalid_recovery_cursor"
_CURSOR_ATTRIBUTES = frozenset({"pk", "sk", "gsi1pk", "gsi1sk"})
_TERMINAL_RUN_STATES = frozenset(
    {RunState.PUBLISHED, RunState.PUBLISHED_DEGRADED, RunState.FAILED, RunState.CANCELED}
)


def _encode_cursor(last_key: dict[str, Any] | None) -> str | None:
    if last_key is None:
        return None
    text = canonical_json({name: value["S"] for name, value in last_key.items()})
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def _is_due_key(values: Any) -> bool:
    if not isinstance(values, dict) or set(values) != _CURSOR_ATTRIBUTES:
        return False
    if not all(isinstance(value, str) for value in values.values()):
        return False
    return values["gsi1pk"] == QUOTA_RESERVATION_DUE_PARTITION


def _decode_cursor(cursor: str) -> dict[str, dict[str, str]]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        values = json.loads(raw)
        if not _is_due_key(values):
            raise ValueError(_CURSOR_CODE)
    except ValueError as error:
        raise PermanentBillingError(_CURSOR_CODE) from error
    return {name: {"S": value} for name, value in values.items()}


def _is_due(reservation: QuotaReservation | CapacityReservation, now: datetime) -> bool:
    return reservation.status is ReservationStatus.RESERVED and reservation.expires_at <= now


def _capacity_resource_key(reservation: CapacityReservation, tenant_id: str) -> Key:
    if reservation.kind is CapacityKind.TENANT:
        return tenant_entity_key(reservation.resource_id)
    return entity_key(tenant_id, "AGENT", reservation.resource_id)


class DynamoQuotaRecoveryMixin:
    _client: Any
    _table: str
    _clock: ClockPort

    def reconcile_expired_reservations(
        self, request: ReservationRecoveryRequest
    ) -> ReservationRecoveryResult:
        """Reconcilia reservas vencidas sem nunca perder o consumo de um Run existente.

        Args: request: instante, limite da página e cursor opcional.
        Returns: Candidatos examinados, reservas liberadas e próximo cursor.
        Raises: PermanentBillingError, BillingDependencyError.
        """
        keys, cursor = self._due_candidates(request)
        released = sum(self._recover(key, request.now) for key in keys)
        return ReservationRecoveryResult(len(keys), released, cursor)

    def _due_candidates(
        self, request: ReservationRecoveryRequest
    ) -> tuple[list[Key], str | None]:
        query: dict[str, Any] = {
            "TableName": self._table,
            "IndexName": QUOTA_RESERVATION_DUE_INDEX,
            "KeyConditionExpression": "gsi1pk = :partition AND gsi1sk < :bound",
            "ExpressionAttributeValues": {
                ":partition": {"S": QUOTA_RESERVATION_DUE_PARTITION},
                ":bound": {"S": utc_attribute(request.now) + "$"},
            },
            "Limit": request.limit,
        }
        if request.cursor is not None:
            query["ExclusiveStartKey"] = _decode_cursor(request.cursor)
        try:
            response = self._client.query(**query)
        except ClientError as error:
            raise BillingDependencyError(UNAVAILABLE_CODE) from error
        keys = [(row["pk"]["S"], row["sk"]["S"]) for row in response.get("Items", ())]
        return keys, _encode_cursor(response.get("LastEvaluatedKey"))

    def _recover(self, key: Key, now: datetime) -> bool:
        item = get_item(self._client, self._table, key, True)
        if item is None:
            return False
        entity = item["entity"]["S"]
        if entity == RESERVATION_ENTITY:
            reservation, tenant_id = decode_reservation(item)
            return _is_due(reservation, now) and self._recover_quota(
                item, reservation, (tenant_id, now)
            )
        if entity == CAPACITY_RESERVATION_ENTITY:
            capacity, tenant_id = decode_capacity_reservation(item)
            return _is_due(capacity, now) and self._recover_capacity(
                item, capacity, (tenant_id, now)
            )
        return False

    def _recover_quota(
        self, item: Item, reservation: QuotaReservation, context: tuple[str, datetime]
    ) -> bool:
        if reservation.kind is ReservationKind.RUN:
            return self._recover_run(item, reservation, context)
        change = ReservationTransition(
            ReservationStatus.RELEASED, context[1], reason_code="reservation_expired"
        )
        return self._transition_reservation(item, change)

    def _recover_run(
        self, item: Item, reservation: QuotaReservation, context: tuple[str, datetime]
    ) -> bool:
        tenant_id, now = context
        run_key = run_entity_key(tenant_id, reservation.resource_id)
        stored = get_item(self._client, self._table, run_key, True)
        if stored is None:
            change = ReservationTransition(
                ReservationStatus.RELEASED,
                now,
                release_run=True,
                reason_code="run_absent",
                guards=(absent_check_action(self._table, run_key),),
            )
            return self._transition_reservation(item, change)
        if decode_model(stored, Run).state in _TERMINAL_RUN_STATES:
            change = ReservationTransition(
                ReservationStatus.CONSUMED, now, actual_scan_bytes=reservation.reserved_scan_bytes
            )
            self._transition_reservation(item, change)
            return False
        self._renew_lease(item, reservation, context)
        return False

    def _renew_lease(
        self, item: Item, reservation: QuotaReservation, context: tuple[str, datetime]
    ) -> None:
        tenant_id, now = context
        lease = reservation.expires_at - reservation.created_at
        renewed = replace(reservation, expires_at=now + lease)
        action = put_action(self._table, encode_reservation(renewed, tenant_id), payload(item))
        transact(self._client, (action,))

    def _recover_capacity(
        self, item: Item, reservation: CapacityReservation, context: tuple[str, datetime]
    ) -> bool:
        tenant_id, now = context
        resource_key = _capacity_resource_key(reservation, tenant_id)
        if get_item(self._client, self._table, resource_key, True) is not None:
            self._transition_capacity(item, CapacityTransition(ReservationStatus.CONSUMED, now))
            return False
        guards = (absent_check_action(self._table, resource_key),)
        change = CapacityTransition(ReservationStatus.RELEASED, now, "resource_absent", guards)
        return self._transition_capacity(item, change)
