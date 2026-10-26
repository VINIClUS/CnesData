"""Cursor DynamoDB da outbox com CAS condicional por geração."""

from typing import Any

from botocore.exceptions import ClientError

from cnes_domain.control_plane.outbox import OutboxCursor, validate_outbox_advance
from cnes_infra.control_plane.dynamodb_keys import item_key

_ENTITY = {"S": "OUTBOXCURSOR"}
_KEY = item_key("OUTBOX#CURSOR", "CURRENT")


def _position(cursor: OutboxCursor) -> dict[str, Any]:
    return {"NULL": True} if cursor.position is None else {"S": cursor.position}


def _decode(item: dict[str, Any]) -> OutboxCursor:
    if item.get("entity") != _ENTITY:
        raise ValueError("outbox_cursor_entity=invalid")
    try:
        position = item["position"]
        if position != {"NULL": True} and (set(position) != {"S"}):
            raise ValueError("outbox_cursor_position=invalid")
        return OutboxCursor(int(item["version"]["N"]), position.get("S"))
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("outbox_cursor=corrupt") from error


class DynamoOutboxCursor:
    """Lê fortemente e persiste o token global sem apagar voltas."""

    def __init__(self, client: Any, table_name: str) -> None:
        self._client = client
        self._table_name = table_name

    def load(self) -> OutboxCursor | None:
        """Args: Nenhum.
        Returns: Cursor durável lido fortemente ou None se ausente.
        Raises: ValueError para item inválido ou erros do SDK.
        """
        item = self._client.get_item(
            TableName=self._table_name, Key=_KEY, ConsistentRead=True,
        ).get("Item")
        return None if item is None else _decode(item)

    def advance(self, expected: OutboxCursor | None, next_cursor: OutboxCursor | None) -> bool:
        """Args: Token esperado e próxima geração ou volta ao início.
        Returns: True para avanço; False somente para perda do CAS.
        Raises: ValueError para item ou transição inválida; erros do SDK.
        """
        following = validate_outbox_advance(expected, next_cursor)
        self.load()
        values: dict[str, Any] = {
            ":entity": _ENTITY, ":version": {"N": str(following.version)},
            ":position": _position(following),
        }
        condition = "attribute_not_exists(pk)"
        if expected is not None:
            values |= {":expected": {"N": str(expected.version)}, ":old": _position(expected)}
            condition = "#entity = :entity AND #version = :expected AND #position = :old"
        try:
            self._client.update_item(
                TableName=self._table_name, Key=_KEY,
                UpdateExpression=(
                    "SET #entity = :entity, #version = :version, #position = :position"
                ),
                ConditionExpression=condition,
                ExpressionAttributeNames={
                    "#entity": "entity", "#version": "version", "#position": "position",
                },
                ExpressionAttributeValues=values,
            )
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                raise
            return False
        return True
