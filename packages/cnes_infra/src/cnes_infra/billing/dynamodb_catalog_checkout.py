"""Account-scoped pending checkout reservation in the DynamoDB billing catalog."""

import math
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError

from cnes_domain.billing.commands import (
    PendingCheckout,
    ReleasePendingCheckoutCommand,
    ReservePendingCheckoutCommand,
)
from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.ports import ClockPort
from cnes_infra.billing.dynamodb_items import (
    UNAVAILABLE_CODE,
    corrupt_item,
    get_item,
    utc_attribute,
)
from cnes_infra.billing.keys import pending_checkout_key
from cnes_infra.control_plane.dynamodb_codec import Item
from cnes_infra.control_plane.dynamodb_keys import item_key

PENDING_CHECKOUT_ENTITY = "PENDINGCHECKOUT"
IN_PROGRESS_CODE = "checkout_in_progress"
CONFLICT_CODE = "billing_transaction_conflict"
_CONDITIONAL_FAILED = "ConditionalCheckFailedException"
_ENTITY = "pending_checkout"
_RESERVE_CONDITION = (
    "attribute_not_exists(pk) OR reservation_expires_at <= :now OR request_key = :key"
)
_EXTEND_CONDITION = (
    "request_key = :key AND reserved_at = :reserved_at AND reservation_expires_at > :now"
)
_EXTEND_UPDATE = "SET reservation_expires_at = :reservation_expires_at, expires_at = :expires_at"


def _text(value: str) -> dict[str, str]:
    return {"S": value}


def _encode(pending: PendingCheckout) -> Item:
    key = pending_checkout_key(pending.billing_account_id)
    return {
        **item_key(*key),
        "entity": _text(PENDING_CHECKOUT_ENTITY),
        "billing_account_id": _text(pending.billing_account_id),
        "request_key": _text(pending.request_key),
        "reserved_at": _text(utc_attribute(pending.reserved_at)),
        "reservation_expires_at": _text(utc_attribute(pending.expires_at)),
        "expires_at": {"N": str(math.ceil(pending.expires_at.timestamp()))},
    }


def _decode(item: Item, billing_account_id: str) -> PendingCheckout:
    key = item_key(*pending_checkout_key(billing_account_id))
    try:
        if item["entity"] != _text(PENDING_CHECKOUT_ENTITY) or (
            item["pk"],
            item["sk"],
            item["billing_account_id"],
        ) != (key["pk"], key["sk"], _text(billing_account_id)):
            raise corrupt_item(_ENTITY)
        return PendingCheckout(
            billing_account_id,
            item["request_key"]["S"],
            datetime.fromisoformat(item["reserved_at"]["S"]),
            datetime.fromisoformat(item["reservation_expires_at"]["S"]),
        )
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise corrupt_item(_ENTITY) from error


def _live_match(
    stored: PendingCheckout | None, request_key: str, now: datetime
) -> PendingCheckout | None:
    if stored is None or stored.expires_at <= now:
        return None
    if stored.request_key != request_key:
        raise PermanentBillingError(IN_PROGRESS_CODE)
    return stored


def _is_conditional_failure(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") == _CONDITIONAL_FAILED


def _conditional_write(operation: Callable[..., Any], **request: Any) -> bool:
    try:
        operation(**request)
    except ClientError as error:
        if _is_conditional_failure(error):
            return False
        raise BillingDependencyError(UNAVAILABLE_CODE) from error
    return True


class DynamoPendingCheckoutMixin:
    _client: Any
    _table: str
    _clock: ClockPort

    def reserve_pending_checkout(self, command: ReservePendingCheckoutCommand) -> PendingCheckout:
        """Reserva atomicamente o checkout pendente da conta.

        Args: Conta, chave da requisição e expiração UTC da reserva.
        Returns: Reserva gravada, ou a existente viva da mesma chave, estendida.
        Raises: ValueError, PermanentBillingError, RetryableBillingError,
            BillingDependencyError.
        """
        now = self._clock()
        if command.expires_at <= now:
            raise ValueError("reason=pending_checkout_expired")
        account = command.billing_account_id
        live = _live_match(self._stored_pending(account), command.request_key, now)
        if live is not None:
            return self._extend_pending(live, command.expires_at, now)
        pending = PendingCheckout(account, command.request_key, now, command.expires_at)
        if self._put_pending(pending):
            return pending
        raced = _live_match(self._stored_pending(account), command.request_key, now)
        if raced is None:
            raise RetryableBillingError(CONFLICT_CODE)
        return raced

    def release_pending_checkout(self, command: ReleasePendingCheckoutCommand) -> bool:
        """Libera a reserva somente se pertencer à chave informada.

        Args: Conta e chave da requisição dona da reserva.
        Returns: True se removeu; False se ausente ou de outra chave.
        Raises: BillingDependencyError.
        """
        key = item_key(*pending_checkout_key(command.billing_account_id))
        return _conditional_write(
            self._client.delete_item,
            TableName=self._table,
            Key=key,
            ConditionExpression="request_key = :key",
            ExpressionAttributeValues={":key": _text(command.request_key)},
        )

    def _stored_pending(self, billing_account_id: str) -> PendingCheckout | None:
        key = pending_checkout_key(billing_account_id)
        item = get_item(self._client, self._table, key, True)
        return None if item is None else _decode(item, billing_account_id)

    def _put_pending(self, pending: PendingCheckout) -> bool:
        return _conditional_write(
            self._client.put_item,
            TableName=self._table,
            Item=_encode(pending),
            ConditionExpression=_RESERVE_CONDITION,
            ExpressionAttributeValues={
                ":now": _text(utc_attribute(pending.reserved_at)),
                ":key": _text(pending.request_key),
            },
        )

    def _extend_pending(
        self, stored: PendingCheckout, requested: datetime, now: datetime
    ) -> PendingCheckout:
        expires_at = max(stored.expires_at, requested)
        if expires_at == stored.expires_at:
            return stored
        written = _conditional_write(
            self._client.update_item,
            TableName=self._table,
            Key=item_key(*pending_checkout_key(stored.billing_account_id)),
            UpdateExpression=_EXTEND_UPDATE,
            ConditionExpression=_EXTEND_CONDITION,
            ExpressionAttributeValues={
                ":key": _text(stored.request_key),
                ":reserved_at": _text(utc_attribute(stored.reserved_at)),
                ":now": _text(utc_attribute(now)),
                ":reservation_expires_at": _text(utc_attribute(expires_at)),
                ":expires_at": {"N": str(math.ceil(expires_at.timestamp()))},
            },
        )
        if not written:
            raise RetryableBillingError(CONFLICT_CODE)
        return replace(stored, expires_at=expires_at)
