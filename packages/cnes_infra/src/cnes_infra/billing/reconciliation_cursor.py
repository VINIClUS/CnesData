"""Cursor retomável da reconciliação Stripe sobre DynamoDB com CAS por versão."""

from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from cnes_domain.billing.errors import BillingDependencyError
from cnes_domain.billing.ports import ClockPort
from cnes_domain.billing.validation import optional_id, optional_utc, require_non_negative
from cnes_infra.billing.dynamodb_items import (
    UNAVAILABLE_CODE,
    corrupt_item,
    get_item,
    utc_attribute,
)
from cnes_infra.billing.keys import Key, stripe_reconciliation_cursor_key
from cnes_infra.control_plane.dynamodb_keys import item_key

RECONCILIATION_CURSOR_ENTITY = "STRIPERECONCILIATIONCURSOR"
_CONDITION_LOST = "ConditionalCheckFailedException"
_NAMES = {
    "#entity": "entity",
    "#version": "version",
    "#position": "position",
    "#updated": "updated_at",
    "#done": "last_completed_at",
}


@dataclass(frozen=True, slots=True)
class ReconciliationCursor:
    """Posição retomável da reconciliação Stripe."""

    position: str | None
    version: int
    updated_at: datetime | None
    last_completed_at: datetime | None

    def __post_init__(self) -> None:
        optional_id(self.position, "position")
        require_non_negative(self.version, "version")
        optional_utc(self.updated_at, "updated_at")
        optional_utc(self.last_completed_at, "last_completed_at")


EMPTY_RECONCILIATION_CURSOR = ReconciliationCursor(None, 0, None, None)


def _text(value: str) -> dict[str, str]:
    return {"S": value}


def _optional_time(item: dict[str, Any], name: str) -> datetime | None:
    if name not in item:
        return None
    return datetime.fromisoformat(item[name]["S"])


def _decode(item: dict[str, Any]) -> ReconciliationCursor:
    if item.get("entity") != _text(RECONCILIATION_CURSOR_ENTITY):
        raise corrupt_item(RECONCILIATION_CURSOR_ENTITY)
    try:
        return ReconciliationCursor(
            position=item.get("position", {}).get("S"),
            version=int(item["version"]["N"]),
            updated_at=datetime.fromisoformat(item["updated_at"]["S"]),
            last_completed_at=_optional_time(item, "last_completed_at"),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise corrupt_item(RECONCILIATION_CURSOR_ENTITY) from error


def _condition(expected: ReconciliationCursor) -> tuple[str, dict[str, Any]]:
    if expected.version == 0:
        return "attribute_not_exists(pk)", {}
    return "#version = :expected", {":expected": {"N": str(expected.version)}}


def _changes(position: str | None, now: str) -> tuple[str, dict[str, Any]]:
    values = {":entity": _text(RECONCILIATION_CURSOR_ENTITY), ":updated": _text(now)}
    sets = ["#entity = :entity", "#version = :next", "#updated = :updated"]
    if position is None:
        values[":done"] = _text(now)
        return "SET " + ", ".join([*sets, "#done = :done"]) + " REMOVE #position", values
    values[":position"] = _text(position)
    return "SET " + ", ".join([*sets, "#position = :position"]), values


class DynamoReconciliationCursor:
    """Cursor da reconciliação Stripe sobre DynamoDB single-table."""

    def __init__(
        self, client: Any, table_name: str, clock: ClockPort, key: Key | None = None,
    ) -> None:
        self._client = client
        self._table_name = table_name
        self._clock = clock
        self._key = key or stripe_reconciliation_cursor_key()

    def load(self) -> ReconciliationCursor:
        """Lê o cursor com consistência forte.

        Returns: Cursor armazenado ou EMPTY_RECONCILIATION_CURSOR.
        Raises: PermanentBillingError, BillingDependencyError.
        """
        item = get_item(self._client, self._table_name, self._key, True)
        return EMPTY_RECONCILIATION_CURSOR if item is None else _decode(item)

    def save(
        self, expected: ReconciliationCursor, position: str | None,
    ) -> ReconciliationCursor | None:
        """Persiste a posição por CAS de versão; position None conclui o ciclo.

        Args: expected: Cursor lido; position: Novo último id confirmado.
        Returns: Novo cursor, ou None se a versão divergiu.
        Raises: BillingDependencyError.
        """
        now = self._clock()
        condition, expected_values = _condition(expected)
        expression, values = _changes(position, utc_attribute(now))
        values[":next"] = {"N": str(expected.version + 1)}
        used = f"{expression} {condition}"
        if not self._update(expression, condition, {**values, **expected_values}, used):
            return None
        done = now if position is None else expected.last_completed_at
        return replace(expected, position=position, version=expected.version + 1,
                       updated_at=now, last_completed_at=done)

    def _update(self, expression: str, condition: str, values: dict[str, Any], used: str) -> bool:
        try:
            self._client.update_item(
                TableName=self._table_name,
                Key=item_key(*self._key),
                UpdateExpression=expression,
                ConditionExpression=condition,
                ExpressionAttributeNames={k: v for k, v in _NAMES.items() if k in used},
                ExpressionAttributeValues=values,
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == _CONDITION_LOST:
                return False
            raise BillingDependencyError(UNAVAILABLE_CODE) from error
        except BotoCoreError as error:
            raise BillingDependencyError(UNAVAILABLE_CODE) from error
        return True
