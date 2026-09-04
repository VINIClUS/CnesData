"""DynamoDB item codec and transaction construction."""

import json
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any

from botocore.exceptions import ClientError
from pydantic import BaseModel

from cnes_domain.control_plane.entities import ManifestRef, RawManifestRecord
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.dynamodb_keys import item_key, key_component, raw_partition, timestamp

type Item = dict[str, dict[str, Any]]
type Action = dict[str, dict[str, Any]]


@dataclass(frozen=True)
class CandidateQuery[T: BaseModel]:
    model_type: type[T]
    predicate: Callable[[T], bool]
    limit: int


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
    query: CandidateQuery[T],
) -> tuple[T, ...]:
    """Relê candidatos válidos até preencher o limite solicitado."""
    if query.limit <= 0:
        return ()
    page_request = dict(request)
    index = request["IndexName"]
    seen = set()
    base_items = {}
    models = []
    while True:
        page_request["Limit"] = query.limit - len(models)
        response = client.query(**page_request)
        for candidate in response.get("Items", ()):
            key = (
                candidate.get("base_pk", candidate["pk"])["S"],
                candidate.get("base_sk", candidate["sk"])["S"],
            )
            signature = (*key, candidate[f"{index}pk"]["S"], candidate[f"{index}sk"]["S"])
            if signature in seen:
                continue
            seen.add(signature)
            if key not in base_items:
                base_items[key] = client.get_item(
                    TableName=request["TableName"], Key=item_key(*key), ConsistentRead=True
                ).get("Item")
            item = base_items[key]
            if not _candidate_matches(item, candidate, index, query.model_type):
                continue
            model = decode_model(item, query.model_type)
            if query.predicate(model):
                models.append(model)
                if len(models) == query.limit:
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
    response = client.get_item(TableName=table_name, Key=item_key(*base_key), ConsistentRead=True)
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


def _ancestry_prefix(record: RawManifestRecord, sequence: int, digest: str) -> str:
    base = record.snapshot_id if record.sequence == 1 else record.base_snapshot_id
    values = (record.agent_id, str(base), f"{sequence:020d}", digest)
    return "ANCESTRY#" + "#".join(map(key_component, values)) + "#"


def _manifest_chain(
    client: Any, table_name: str, record: RawManifestRecord, partition: str
) -> tuple[dict[str, str], ...] | None:
    ref = {"manifest_id": record.manifest_id, "manifest_key": record.manifest_key}
    if record.sequence == 1:
        return (ref,)
    prefix = _ancestry_prefix(record, record.sequence - 1, str(record.previous_manifest_sha256))
    predecessor = unique_partition_item(client, table_name, partition, prefix)
    if predecessor is None or "chain" not in predecessor:
        return None
    chain = tuple(json.loads(predecessor["chain"]["S"]))
    return (*chain, ref)


def _ancestry_item(
    record: RawManifestRecord, raw_item: Item, chain: tuple[dict[str, str], ...] | None
) -> Item:
    prefix = _ancestry_prefix(record, record.sequence, record.manifest_sha256)
    item = {
        "pk": raw_item["pk"],
        "sk": {"S": prefix + key_component(record.manifest_id)},
        "entity": {"S": "RAWANCESTRY"},
        "base_pk": raw_item["pk"],
        "base_sk": raw_item["sk"],
    }
    if chain is not None:
        item["chain"] = {"S": json.dumps(chain, separators=(",", ":"))}
    return item


def _waiting_item(record: RawManifestRecord, ancestry: Item) -> Item:
    prefix = _ancestry_prefix(
        record, record.sequence - 1, str(record.previous_manifest_sha256)
    ).replace("ANCESTRY#", "WAITING#", 1)
    return {
        "pk": ancestry["pk"],
        "sk": {"S": prefix + key_component(record.manifest_id)},
        "entity": {"S": "RAWWAITING"},
        "payload": {"S": record.model_dump_json()},
        "base_pk": ancestry["pk"],
        "base_sk": ancestry["sk"],
    }


def _raw_version_action(client: Any, table_name: str, partition: str) -> Action:
    key = partition, "RAWVERSION#CURRENT"
    current = client.get_item(TableName=table_name, Key=item_key(*key), ConsistentRead=True).get(
        "Item"
    )
    generation = 1 if current is None else int(payload(current)) + 1
    item = {
        "pk": {"S": partition},
        "sk": {"S": key[1]},
        "entity": {"S": "RAWVERSION"},
        "payload": {"S": str(generation)},
    }
    expected = None if current is None else payload(current)
    return put_action(table_name, item, expected)


def _waiting_children(
    client: Any, table_name: str, record: RawManifestRecord, remaining: int
) -> tuple[Item, ...]:
    partition = raw_partition(
        record.tenant_id, record.source_type, record.file_subtype, record.competencia
    )
    prefix = _ancestry_prefix(record, record.sequence, record.manifest_sha256).replace(
        "ANCESTRY#", "WAITING#", 1
    )
    response = client.query(
        TableName=table_name,
        KeyConditionExpression="pk = :partition AND begins_with(sk, :prefix)",
        ExpressionAttributeValues={":partition": {"S": partition}, ":prefix": {"S": prefix}},
        ConsistentRead=True,
        Limit=remaining + 1,
    )
    children = tuple(response.get("Items", ()))
    if len(children) > remaining:
        raise Conflict("transaction_limit")
    return children


def _chain_action(table_name: str, ancestry: Item, chain: tuple[dict[str, str], ...]) -> Action:
    updated = dict(ancestry)
    updated["chain"] = {"S": json.dumps(chain, separators=(",", ":"))}
    return {
        "Put": {
            "TableName": table_name,
            "Item": updated,
            "ConditionExpression": "attribute_exists(pk) AND attribute_not_exists(chain)",
        }
    }


def _repair_descendants(
    client: Any, table_name: str, root: RawManifestRecord, root_chain: tuple[dict[str, str], ...]
) -> tuple[tuple[Action, ...], tuple[tuple[RawManifestRecord, tuple[dict[str, str], ...]], ...]]:
    actions: list[Action] = []
    endpoints = []
    pending = [(root, root_chain)]
    visited = 0
    while pending:
        parent, chain = pending.pop()
        children = _waiting_children(client, table_name, parent, 92 - visited)
        visited += len(children)
        if not children:
            endpoints.append((parent, chain))
            continue
        for marker in children:
            child = decode_model(marker, RawManifestRecord)
            key = marker["base_pk"]["S"], marker["base_sk"]["S"]
            ancestry = client.get_item(
                TableName=table_name, Key=item_key(*key), ConsistentRead=True
            ).get("Item")
            if ancestry is None:
                raise Conflict("raw_ancestry_conflict")
            ref = {"manifest_id": child.manifest_id, "manifest_key": child.manifest_key}
            child_chain = (*chain, ref)
            if "chain" not in ancestry:
                actions.append(_chain_action(table_name, ancestry, child_chain))
            elif tuple(json.loads(ancestry["chain"]["S"])) != child_chain:
                raise Conflict("raw_ancestry_conflict")
            pending.append((child, child_chain))
    return tuple(actions), tuple(endpoints)


def _head_action(
    client: Any, table_name: str, record: RawManifestRecord, chain: tuple[dict[str, str], ...]
) -> Action:
    partition = raw_partition(
        record.tenant_id, record.source_type, record.file_subtype, record.competencia
    )
    key = partition, "HEAD#CURRENT"
    current = client.get_item(TableName=table_name, Key=item_key(*key), ConsistentRead=True).get(
        "Item"
    )
    ordering = (
        f"{timestamp(record.created_at)}#{key_component(record.agent_id)}#"
        f"{key_component(record.snapshot_id)}"
    )
    data = {"ordering": ordering, "chain": chain}
    head = {
        "pk": {"S": partition},
        "sk": {"S": key[1]},
        "entity": {"S": "RAWHEAD"},
        "payload": {"S": json.dumps(data, separators=(",", ":"))},
    }
    if current is None:
        return put_action(table_name, head, None)
    previous = json.loads(current["payload"]["S"])
    descendant = previous["chain"][-1]["manifest_id"] in {
        item["manifest_id"] for item in chain[:-1]
    }
    if descendant or ordering > previous["ordering"]:
        return put_action(table_name, head, payload(current))
    return check_action(table_name, current)


def raw_manifest_actions(
    client: Any, table_name: str, record: RawManifestRecord, raw_item: Item
) -> tuple[Action, ...]:
    """Cria manifesto, ancestry e HEAD atomicamente."""
    partition = raw_item["pk"]["S"]
    version = _raw_version_action(client, table_name, partition)
    chain = _manifest_chain(client, table_name, record, partition)
    ancestry = _ancestry_item(record, raw_item, chain)
    actions = [put_action(table_name, raw_item, None), put_action(table_name, ancestry, None)]
    waiting = _waiting_item(record, ancestry) if chain is None else None
    if waiting is not None:
        actions.append(put_action(table_name, waiting, None))
    if chain is not None:
        repairs, endpoints = _repair_descendants(client, table_name, record, chain)
        actions.extend(repairs)
        head_record, head_chain = max(
            endpoints, key=lambda item: (item[0].created_at, item[0].agent_id, item[0].snapshot_id)
        )
        actions.append(_head_action(client, table_name, head_record, head_chain))
    actions.append(version)
    return tuple(actions)


def raw_head_chain(
    client: Any, table_name: str, partition: str, limit: int
) -> tuple[ManifestRef, ...]:
    """Lê fortemente a cadeia HEAD materializada."""
    if limit < 1:
        return ()
    response = client.get_item(
        TableName=table_name, Key=item_key(partition, "HEAD#CURRENT"), ConsistentRead=True
    )
    head = response.get("Item")
    if head is None:
        return ()
    chain = json.loads(head["payload"]["S"])["chain"]
    if len(chain) > limit:
        return ()
    return tuple(ManifestRef.model_validate(item) for item in chain)


def unique_partition_item(client: Any, table_name: str, partition: str, prefix: str) -> Item | None:
    """Retorna o único item forte de um prefixo, limitado a dois."""
    response = client.query(
        TableName=table_name,
        KeyConditionExpression="pk = :partition AND begins_with(sk, :prefix)",
        ExpressionAttributeValues={":partition": {"S": partition}, ":prefix": {"S": prefix}},
        ConsistentRead=True,
        Limit=2,
    )
    items = response.get("Items", ())
    return items[0] if len(items) == 1 else None


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
