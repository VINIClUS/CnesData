"""DynamoDB item codec and transaction construction."""

from collections.abc import Iterable, Iterator, Mapping
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel

from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.dynamodb_keys import item_key

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
    for page in query_pages(client, request):
        items.extend(page)
    return tuple(items)


def query_pages(client: Any, request: dict[str, Any]) -> Iterator[tuple[Item, ...]]:
    """Percorre páginas de uma Query sob demanda."""
    page_request = dict(request)
    while True:
        response = client.query(**page_request)
        yield tuple(response.get("Items", ()))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return
        page_request["ExclusiveStartKey"] = last_key


def bounded_candidates[T: BaseModel](
    client: Any,
    request: dict[str, Any],
    *args: Any,
) -> tuple[T, ...]:
    """Relê candidatos válidos até preencher o limite solicitado."""
    model_type, predicate, limit = args
    if limit <= 0:
        return ()
    page_request = dict(request)
    index = request["IndexName"]
    seen = set()
    base_items = {}
    models = []
    while True:
        page_request["Limit"] = limit - len(models)
        response = client.query(**page_request)
        for candidate in response.get("Items", ()):
            key = (candidate.get("base_pk", candidate["pk"])["S"],
                   candidate.get("base_sk", candidate["sk"])["S"])
            signature = (*key, candidate[f"{index}pk"]["S"], candidate[f"{index}sk"]["S"])
            if signature in seen:
                continue
            seen.add(signature)
            if key not in base_items:
                base_items[key] = client.get_item(
                    TableName=request["TableName"], Key=item_key(*key), ConsistentRead=True
                ).get("Item")
            item = base_items[key]
            if not _candidate_matches(item, candidate, index, model_type):
                continue
            model = decode_model(item, model_type)
            if predicate(model):
                models.append(model)
                if len(models) == limit:
                    return tuple(models)
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return tuple(models)
        page_request["ExclusiveStartKey"] = last_key


def _candidate_matches(
    item: Item | None, candidate: Item, index: str, model_type: type[BaseModel]
) -> bool:
    if item is None or item.get("entity", {}).get("S") != model_type.__name__.upper():
        return False
    names = (f"{index}pk", f"{index}sk")
    return not all(name in item for name in names) or all(
        item[name] == candidate.get(name) for name in names
    )


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


def aggregate_items(
    client: Any, table_name: str, base_key: tuple[str, str], child_key: tuple[str, str]
) -> tuple[Item | None, tuple[Item, ...]]:
    """Relê fortemente um agregado e todos os filhos do prefixo."""
    response = client.get_item(
        TableName=table_name, Key=item_key(*base_key), ConsistentRead=True)
    return response.get("Item"), query_partition(client, table_name, *child_key)


def aggregate_replay(
    client: Any, table_name: str, expected: tuple[Item, tuple[str, str], tuple[Item, ...]]
) -> bool:
    """Reconhece replay exato de um agregado base e seus filhos."""
    base, child_key, children = expected
    base_key = base["pk"]["S"], base["sk"]["S"]
    current, current_children = aggregate_items(client, table_name, base_key, child_key)
    if current is None:
        if current_children:
            raise Conflict("run_dependency_conflict")
        return False
    if current != base:
        raise Conflict("run_conflict")
    def keyed(items: tuple[Item, ...]) -> dict[tuple[str, str], Item]:
        return {(item["pk"]["S"], item["sk"]["S"]): item for item in items}
    if keyed(current_children) != keyed(children):
        raise Conflict("run_dependency_conflict")
    return True


def bounded_partition(
    client: Any, table_name: str, key: tuple[str, str], max_items: int
) -> tuple[Item, ...] | None:
    """Consulta uma partição forte dentro de orçamento fechado."""
    items: list[Item] = []
    request: dict[str, Any] = {
        "TableName": table_name,
        "KeyConditionExpression": "pk = :partition AND begins_with(sk, :prefix)",
        "ExpressionAttributeValues": {
            ":partition": {"S": key[0]}, ":prefix": {"S": key[1]}},
        "ConsistentRead": True,
    }
    for _ in range(max_items + 1):
        request["Limit"] = max_items + 1 - len(items)
        response = client.query(**request)
        items.extend(response.get("Items", ()))
        last_key = response.get("LastEvaluatedKey")
        if len(items) > max_items:
            return None
        if not last_key:
            return tuple(items)
        request["ExclusiveStartKey"] = last_key
    return None


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
