"""DynamoDB item codec and transaction construction."""

from collections.abc import Iterable, Mapping
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel

from cnes_domain.control_plane.errors import Conflict

type Item = dict[str, dict[str, Any]]
type Action = dict[str, dict[str, Any]]


def encode_model(
    model: BaseModel,
    entity: str,
    key: tuple[str, str],
    attributes: Mapping[str, str] | None = None,
) -> Item:
    """Codifica um modelo sem introduzir Decimal."""
    item: Item = {
        "pk": {"S": key[0]},
        "sk": {"S": key[1]},
        "entity": {"S": entity},
        "payload": {"S": model.model_dump_json()},
    }
    for name, value in (attributes or {}).items():
        item[name] = {"S": value}
    return item


def encode_marker(
    entity: str,
    key: tuple[str, str],
    base_key: tuple[str, str],
    attributes: Mapping[str, str],
) -> Item:
    """Codifica um marcador eventual apontando à base."""
    item: Item = {
        "pk": {"S": key[0]},
        "sk": {"S": key[1]},
        "entity": {"S": entity},
        "base_pk": {"S": base_key[0]},
        "base_sk": {"S": base_key[1]},
    }
    for name, value in attributes.items():
        item[name] = {"S": value}
    return item


def decode_model[T: BaseModel](item: Item, model_type: type[T]) -> T:
    """Decodifica e valida o payload de um modelo."""
    return model_type.model_validate_json(item["payload"]["S"])


def payload(item: Item) -> str:
    """Retorna o payload usado pelo compare-and-set."""
    return str(item["payload"]["S"])


def query_all(client: Any, request: dict[str, Any]) -> tuple[Item, ...]:
    """Percorre todas as páginas de uma Query."""
    items: list[Item] = []
    page_request = dict(request)
    while True:
        response = client.query(**page_request)
        items.extend(response.get("Items", ()))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return tuple(items)
        page_request["ExclusiveStartKey"] = last_key


def query_partition(client: Any, table_name: str, partition: str, prefix: str) -> tuple[Item, ...]:
    """Consulta uma partição base com leitura forte."""
    return query_all(
        client,
        {
            "TableName": table_name,
            "KeyConditionExpression": "pk = :partition AND begins_with(sk, :prefix)",
            "ExpressionAttributeValues": {
                ":partition": {"S": partition},
                ":prefix": {"S": prefix},
            },
            "ConsistentRead": True,
        },
    )


def put_action(table_name: str, item: Item, expected_payload: str | None) -> Action:
    """Cria uma ação Put condicional."""
    request: dict[str, Any] = {"TableName": table_name, "Item": item}
    if expected_payload is None:
        request["ConditionExpression"] = "attribute_not_exists(pk)"
    else:
        request["ConditionExpression"] = "payload = :expected"
        request["ExpressionAttributeValues"] = {":expected": {"S": expected_payload}}
    return {"Put": request}


def check_action(table_name: str, item: Item) -> Action:
    """Cria uma ação ConditionCheck pelo payload."""
    return {
        "ConditionCheck": {
            "TableName": table_name,
            "Key": {"pk": item["pk"], "sk": item["sk"]},
            "ConditionExpression": "payload = :expected",
            "ExpressionAttributeValues": {":expected": item["payload"]},
        }
    }


def _action_key(action: Action) -> tuple[str, str]:
    request = next(iter(action.values()))
    values = request.get("Item", request.get("Key"))
    return str(values["pk"]["S"]), str(values["sk"]["S"])


def _validate_actions(actions: tuple[Action, ...]) -> None:
    if len(actions) > 100:
        raise Conflict("transaction_limit")
    keys = tuple(_action_key(action) for action in actions)
    if len(set(keys)) != len(keys):
        raise Conflict("duplicate_transaction_key")


def execute_transaction(client: Any, actions: Iterable[Action]) -> None:
    """Valida e envia uma transação de chaves únicas."""
    normalized = tuple(actions)
    _validate_actions(normalized)
    try:
        client.transact_write_items(TransactItems=list(normalized))
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        reasons = error.response.get("CancellationReasons") or ()
        reason_codes = {reason.get("Code") for reason in reasons} - {None, "None"}
        conditional = code == "TransactionCanceledException" and reason_codes == {
            "ConditionalCheckFailed"
        }
        if conditional:
            raise Conflict("transaction_conflict") from error
        raise
