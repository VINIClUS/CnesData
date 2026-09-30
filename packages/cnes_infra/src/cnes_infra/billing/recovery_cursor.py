"""Cursor de recovery Stripe sobre DynamoDB com CAS por ciclo e versão."""

from datetime import datetime
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from cnes_domain.billing.errors import BillingDependencyError
from cnes_domain.billing.inbox import StripeRecoveryCursor, require_cursor_successor
from cnes_domain.billing.models import ReadConsistency
from cnes_domain.billing.ports import ClockPort
from cnes_infra.billing.dynamodb_items import (
    UNAVAILABLE_CODE,
    corrupt_item,
    get_item,
    utc_attribute,
)
from cnes_infra.billing.keys import stripe_recovery_cursor_key
from cnes_infra.control_plane.dynamodb_keys import item_key

RECOVERY_CURSOR_ENTITY = "STRIPERECOVERYCURSOR"
_CONDITION_LOST = "ConditionalCheckFailedException"
_ATTRIBUTES = {
    "#entity": "entity",
    "#cycle": "active_cycle_id",
    "#created": "created_gte",
    "#after": "starting_after",
    "#version": "version",
    "#updated": "updated_at",
    "#done_cycle": "last_completed_cycle_id",
    "#done_version": "last_completed_version",
    "#done_at": "last_success_at",
}
_ACTIVE_REMOVALS = ("#cycle", "#created", "#after", "#version")


def _text(value: str) -> dict[str, str]:
    return {"S": value}


def _number(value: int) -> dict[str, str]:
    return {"N": str(value)}


def _decode(item: dict[str, Any]) -> StripeRecoveryCursor | None:
    if item.get("entity") != _text(RECOVERY_CURSOR_ENTITY):
        raise corrupt_item(RECOVERY_CURSOR_ENTITY)
    if "active_cycle_id" not in item:
        return None
    try:
        return StripeRecoveryCursor(
            cycle_id=item["active_cycle_id"]["S"],
            created_gte=datetime.fromisoformat(item["created_gte"]["S"]),
            starting_after=item.get("starting_after", {}).get("S"),
            version=int(item["version"]["N"]),
        )
    except (KeyError, TypeError, ValueError) as error:
        raise corrupt_item(RECOVERY_CURSOR_ENTITY) from error


def _expected_condition(cursor: StripeRecoveryCursor) -> tuple[str, dict[str, Any]]:
    values = {
        ":e_cycle": _text(cursor.cycle_id),
        ":e_created": _text(utc_attribute(cursor.created_gte)),
        ":e_version": _number(cursor.version),
    }
    condition = "#cycle = :e_cycle AND #created = :e_created AND #version = :e_version AND "
    if cursor.starting_after is None:
        return condition + "attribute_not_exists(#after)", values
    values[":e_after"] = _text(cursor.starting_after)
    return condition + "#after = :e_after", values


def _progress(
    cursor: StripeRecoveryCursor, sets: list[str], removes: list[str], values: dict[str, Any],
) -> None:
    sets.append("#version = :n_version")
    values[":n_version"] = _number(cursor.version)
    if cursor.starting_after is None:
        removes.append("#after")
        return
    sets.append("#after = :n_after")
    values[":n_after"] = _text(cursor.starting_after)


def _expression(sets: list[str], removes: list[str]) -> str:
    parts = ["SET " + ", ".join(sets)]
    if removes:
        parts.append("REMOVE " + ", ".join(removes))
    return " ".join(parts)


class DynamoRecoveryCursor:
    """Cursor de recovery Stripe sobre DynamoDB single-table."""

    def __init__(self, client: Any, table_name: str, clock: ClockPort) -> None:
        self._client = client
        self._table_name = table_name
        self._clock = clock

    def load(self, consistency: ReadConsistency) -> StripeRecoveryCursor | None:
        """Lê o cursor ativo.

        Args: consistency: Consistência da leitura.
        Returns: Cursor ativo ou None.
        Raises: PermanentBillingError, BillingDependencyError.
        """
        strong = consistency is ReadConsistency.STRONG
        item = get_item(self._client, self._table_name, stripe_recovery_cursor_key(), strong)
        return None if item is None else _decode(item)

    def start(self, cursor: StripeRecoveryCursor) -> bool:
        """Inicia um ciclo quando não há ciclo ativo.

        Args: cursor: Cursor inicial.
        Returns: False se já existe ciclo ativo.
        Raises: BillingDependencyError.
        """
        sets = ["#entity = :entity", "#cycle = :n_cycle", "#created = :n_created"]
        values = {
            ":entity": _text(RECOVERY_CURSOR_ENTITY),
            ":n_cycle": _text(cursor.cycle_id),
            ":n_created": _text(utc_attribute(cursor.created_gte)),
        }
        removes: list[str] = []
        _progress(cursor, sets, removes, values)
        return self._update(sets, removes, values, "attribute_not_exists(#cycle)")

    def advance(self, expected: StripeRecoveryCursor, replacement: StripeRecoveryCursor) -> bool:
        """Avança o cursor por CAS exato.

        Args: expected: Cursor armazenado; replacement: Sucessor imediato.
        Returns: False se o cursor armazenado divergiu.
        Raises: ValueError, BillingDependencyError.
        """
        require_cursor_successor(expected, replacement)
        sets: list[str] = []
        removes: list[str] = []
        values: dict[str, Any] = {}
        _progress(replacement, sets, removes, values)
        condition, expected_values = _expected_condition(expected)
        return self._update(sets, removes, {**values, **expected_values}, condition)

    def complete(self, expected: StripeRecoveryCursor, completed_at: datetime) -> bool:
        """Conclui o ciclo, preservando metadados de conclusão.

        Args: expected: Cursor armazenado; completed_at: Instante UTC de conclusão.
        Returns: False se o cursor armazenado divergiu.
        Raises: ValueError, BillingDependencyError.
        """
        sets = ["#done_cycle = :d_cycle", "#done_version = :d_version", "#done_at = :d_at"]
        condition, expected_values = _expected_condition(expected)
        values = {
            ":d_cycle": _text(expected.cycle_id),
            ":d_version": _number(expected.version),
            ":d_at": _text(utc_attribute(completed_at)),
            **expected_values,
        }
        return self._update(sets, list(_ACTIVE_REMOVALS), values, condition)

    def _update(
        self, sets: list[str], removes: list[str], values: dict[str, Any], condition: str,
    ) -> bool:
        sets = [*sets, "#updated = :updated"]
        values = {**values, ":updated": _text(utc_attribute(self._clock()))}
        expression = _expression(sets, removes)
        used = f"{expression} {condition}"
        try:
            self._client.update_item(
                TableName=self._table_name,
                Key=item_key(*stripe_recovery_cursor_key()),
                UpdateExpression=expression,
                ConditionExpression=condition,
                ExpressionAttributeNames={k: v for k, v in _ATTRIBUTES.items() if k in used},
                ExpressionAttributeValues=values,
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") == _CONDITION_LOST:
                return False
            raise BillingDependencyError(UNAVAILABLE_CODE) from error
        except BotoCoreError as error:
            raise BillingDependencyError(UNAVAILABLE_CODE) from error
        return True
