"""Criação transacional de tenant faturado no plano de controle DynamoDB."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

from cnes_domain.billing.errors import (
    BillingTenantConflict,
    EntitlementDenied,
    IdempotencyConflict,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.models import (
    BillingAccountStatus,
    CapacityKind,
    EntitlementAction,
    ReservationStatus,
)
from cnes_domain.billing.policy import EntitlementPolicy, require_allowed
from cnes_domain.control_plane.entities import Tenant
from cnes_domain.profiles import BillingMode
from cnes_infra.control_plane.billed_tenant import (
    TENANT_SCOPE,
    billed_tenant_digest,
    completed_record,
    creator_membership,
    require_creatable_tenant_id,
    tenant_created_event,
)
from cnes_infra.control_plane.dynamodb_codec import (
    Action,
    Item,
    decode_model,
    encode_model,
    payload,
    put_action,
)
from cnes_infra.control_plane.dynamodb_keys import (
    entity_key,
    idempotency_key,
    item_key,
    key_component,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.billing.commands import CreateBilledTenantCommand
    from cnes_domain.billing.models import CapacityReservation, EntitlementSnapshot
    from cnes_domain.control_plane.entities import IdempotencyRecord, Membership
    from cnes_infra.billing.settings import BillingSettings

type _Prior = tuple[Item | None, IdempotencyRecord | None]
_NO_VERSION = 0


def encode_membership(membership: Membership) -> Item:
    """Codifica a membership com a projeção esparsa do gsi1 (usuário → tenants)."""
    key = entity_key(membership.tenant_id, "MEMBERSHIP", membership.user_id)
    attributes = {
        "gsi1pk": f"USER#{key_component(membership.user_id)}",
        "gsi1sk": f"TENANT#{key_component(membership.tenant_id)}",
    }
    return encode_model(membership, "MEMBERSHIP", key, attributes)


def _account_active_check(table: str, billing_account_id: str) -> Action:
    from cnes_infra.billing.keys import billing_account_key

    return {
        "ConditionCheck": {
            "TableName": table,
            "Key": item_key(*billing_account_key(billing_account_id)),
            "ConditionExpression": "attribute_exists(pk) AND #status = :active",
            "ExpressionAttributeNames": {"#status": "status"},
            "ExpressionAttributeValues": {":active": {"S": BillingAccountStatus.ACTIVE.value}},
        }
    }


def _consumed_event_payload(reservation: CapacityReservation) -> dict[str, str | int]:
    return {
        "billing_account_id": reservation.billing_account_id,
        "reservation_id": reservation.reservation_id,
        "kind": reservation.kind.value,
        "resource_id": reservation.resource_id,
    }


def _snapshot_unchanged(snapshot: EntitlementSnapshot, version: int, now: datetime) -> bool:
    return snapshot.entitlement_version == version and snapshot.valid_until > now


def _reservation_usable(
    reservation: CapacityReservation, command: CreateBilledTenantCommand, now: datetime
) -> bool:
    return all(
        (
            reservation.status is ReservationStatus.RESERVED,
            reservation.kind is CapacityKind.TENANT,
            reservation.resource_id == command.tenant.tenant_id,
            reservation.billing_account_id == command.link.billing_account_id,
            reservation.expires_at > now,
        )
    )


def _usable_item(item: Item, command: CreateBilledTenantCommand, now: datetime) -> bool:
    from cnes_infra.billing.dynamodb_quota_items import decode_capacity_reservation

    return _reservation_usable(decode_capacity_reservation(item)[0], command, now)


class DynamoBilledTenantMixin:
    _client: Any
    _table_name: str
    _clock: Callable[[], datetime]
    _billing: BillingSettings

    def create_billed_tenant(self, command: CreateBilledTenantCommand) -> Tenant:
        """Cria tenant, links, consumo da reserva, idempotência e outbox em uma transação.

        Args: command: Tenant, link, reserva de capacidade e chave de idempotência.
        Returns: O tenant criado ou o tenant de um replay idêntico.
        Raises: BillingTenantConflict, IdempotencyConflict, EntitlementDenied, erros de billing.
        """
        from cnes_infra.billing.dynamodb_items import transact

        require_creatable_tenant_id(command.tenant.tenant_id)
        now = self._clock()
        prior, live = self._billed_prior(command, now)
        if live is not None:
            return self._replayed_tenant(live)
        version = self._billed_entitlement_version(command, now)
        if transact(self._client, self._billed_actions(command, now, prior, version)):
            return command.tenant
        return self._classify_billed_failure(command, version)

    def _billed_prior(self, command: CreateBilledTenantCommand, now: datetime) -> _Prior:
        from cnes_infra.billing.dynamodb_items import decode_idempotency_record, get_item

        identity = (command.tenant.tenant_id, TENANT_SCOPE, command.idempotency_key)
        item = get_item(self._client, self._table_name, idempotency_key(*identity), True)
        if item is None:
            return None, None
        record = decode_idempotency_record(item, identity)
        if record.expires_at <= now:
            return item, None
        if record.request_hash != billed_tenant_digest(command):
            raise IdempotencyConflict(f"key={command.idempotency_key}")
        return item, record

    def _replayed_tenant(self, record: IdempotencyRecord) -> Tenant:
        from cnes_infra.billing.dynamodb_items import get_item
        from cnes_infra.billing.keys import tenant_entity_key

        key = tenant_entity_key(record.resource_id)
        item = get_item(self._client, self._table_name, key, True)
        if item is None:
            raise RetryableBillingError("billing_idempotency_incomplete")
        return decode_model(item, Tenant)

    def _billed_entitlement_version(self, command: CreateBilledTenantCommand, now: datetime) -> int:
        from cnes_infra.billing.dynamodb_items import decode_snapshot, get_item
        from cnes_infra.billing.keys import entitlement_snapshot_key

        if not self._billing.enforced:
            return _NO_VERSION
        account = command.link.billing_account_id
        item = get_item(self._client, self._table_name, entitlement_snapshot_key(account), True)
        if item is None:
            raise EntitlementDenied("reason=snapshot_missing")
        snapshot = decode_snapshot(item, account)
        policy = EntitlementPolicy(BillingMode.STRIPE)
        require_allowed(policy.evaluate(snapshot, EntitlementAction.TENANT_CREATION, now))
        return snapshot.entitlement_version

    def _billed_actions(
        self, command: CreateBilledTenantCommand, now: datetime, prior: Item | None, version: int
    ) -> tuple[Action, ...]:
        from cnes_infra.billing.dynamodb_items import (
            audit_outbox_event,
            idempotency_item,
            outbox_item,
            put_new,
        )
        from cnes_infra.billing.keys import tenant_entity_key

        table, tenant = self._table_name, command.tenant
        record = idempotency_item(completed_record(command, now))
        event = audit_outbox_event(tenant_created_event(command))
        actions = [
            put_new(table, encode_model(tenant, "TENANT", tenant_entity_key(tenant.tenant_id))),
            put_action(table, record, None if prior is None else payload(prior)),
            put_new(table, outbox_item(event)),
            {"Put": {"TableName": table, "Item": encode_membership(creator_membership(command))}},
        ]
        if self._billing.mode is BillingMode.STRIPE:
            actions.extend(self._billed_link_actions(command))
        if self._billing.enforced:
            actions.extend(self._billed_capacity_actions(command, now, version))
        return tuple(actions)

    def _billed_link_actions(self, command: CreateBilledTenantCommand) -> tuple[Action, ...]:
        from cnes_infra.billing.dynamodb_items import encode_link, encode_tenant_account, put_new

        table, link = self._table_name, command.link
        return (
            _account_active_check(table, link.billing_account_id),
            put_new(table, encode_link(link)),
            put_new(table, encode_tenant_account(link)),
        )

    def _billed_capacity_actions(
        self, command: CreateBilledTenantCommand, now: datetime, version: int
    ) -> tuple[Action, ...]:
        from cnes_infra.billing.dynamodb_items import outbox_item, put_new
        from cnes_infra.billing.dynamodb_quota_items import (
            SnapshotExpectation,
            decode_capacity_reservation,
            encode_capacity_reservation,
            quota_event,
            snapshot_check,
        )

        table, account = self._table_name, command.link.billing_account_id
        item = self._billed_reservation_item(command, now)
        current, owner = decode_capacity_reservation(item)
        consumed = replace(current, status=ReservationStatus.CONSUMED)
        event = quota_event("quota.consumed", owner, _consumed_event_payload(current), now)
        return (
            snapshot_check(table, SnapshotExpectation(account, version, None), now),
            put_action(table, encode_capacity_reservation(consumed, owner), payload(item)),
            put_new(table, outbox_item(event)),
        )

    def _billed_reservation_item(self, command: CreateBilledTenantCommand, now: datetime) -> Item:
        from cnes_infra.billing.dynamodb_items import get_item
        from cnes_infra.billing.keys import capacity_reservation_key

        key = capacity_reservation_key(command.link.billing_account_id, command.reservation_id)
        item = get_item(self._client, self._table_name, key, True)
        if item is None:
            raise PermanentBillingError("capacity_reservation_missing")
        if not _usable_item(item, command, now):
            raise PermanentBillingError("capacity_reservation_invalid")
        return item

    def _classify_billed_failure(self, command: CreateBilledTenantCommand, version: int) -> Tenant:
        now = self._clock()
        _, live = self._billed_prior(command, now)
        if live is not None:
            return self._replayed_tenant(live)
        self._raise_billed_conflict(command)
        if self._billing.mode is BillingMode.STRIPE:
            self._raise_billed_account_failure(command.link.billing_account_id)
        if self._billing.enforced:
            self._raise_billed_capacity_failure(command, version, now)
        raise RetryableBillingError("billing_transaction_conflict")

    def _raise_billed_conflict(self, command: CreateBilledTenantCommand) -> None:
        from cnes_infra.billing.dynamodb_items import get_item
        from cnes_infra.billing.keys import tenant_account_key, tenant_entity_key

        tenant_id = command.tenant.tenant_id
        keys = [tenant_entity_key(tenant_id)]
        if self._billing.mode is BillingMode.STRIPE:
            keys.append(tenant_account_key(tenant_id))
        for key in keys:
            if get_item(self._client, self._table_name, key, True) is not None:
                raise BillingTenantConflict(f"tenant_id={tenant_id}")

    def _raise_billed_account_failure(self, billing_account_id: str) -> None:
        from cnes_infra.billing.dynamodb_items import decode_account, get_item
        from cnes_infra.billing.keys import billing_account_key

        key = billing_account_key(billing_account_id)
        item = get_item(self._client, self._table_name, key, True)
        if item is None:
            raise PermanentBillingError("billing_account_missing")
        if decode_account(item, billing_account_id).status is not BillingAccountStatus.ACTIVE:
            raise PermanentBillingError("billing_account_inactive")

    def _raise_billed_capacity_failure(
        self, command: CreateBilledTenantCommand, version: int, now: datetime
    ) -> None:
        from cnes_infra.billing.dynamodb_items import decode_snapshot, get_item
        from cnes_infra.billing.keys import capacity_reservation_key, entitlement_snapshot_key

        account = command.link.billing_account_id
        snapshot_key = entitlement_snapshot_key(account)
        snapshot_item = get_item(self._client, self._table_name, snapshot_key, True)
        snapshot = None if snapshot_item is None else decode_snapshot(snapshot_item, account)
        if snapshot is None or not _snapshot_unchanged(snapshot, version, now):
            raise EntitlementDenied("reason=snapshot_changed")
        key = capacity_reservation_key(account, command.reservation_id)
        item = get_item(self._client, self._table_name, key, True)
        if item is None or not _usable_item(item, command, now):
            raise PermanentBillingError("capacity_reservation_invalid")
