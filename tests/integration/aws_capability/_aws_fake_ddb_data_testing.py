"""Plano de dados DynamoDB da fronteira: itens, índices e LeadingKeys por principal."""

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass, field

from tests.integration.aws_capability._aws_fake_types_testing import denied, good
from tests.integration.aws_capability._b1_testing import ACCOUNT
from tests.integration.aws_capability._service_contracts import Reply

LEADING_KEYS = "dynamodb:LeadingKeys"
Row = dict[str, dict[str, str]]


@dataclass
class FakeTable:
    tags: dict[str, str]
    status: str = "ACTIVE"
    deleting_describes: int = 0
    protection: bool = False
    throughput: Mapping[str, object] | None = None
    index_throughput: dict[str, Mapping[str, object] | None] = field(default_factory=dict)
    ready_in: int = 0
    items: set[tuple[str, str]] = field(default_factory=set)
    rows: dict[tuple[str, str], Row] = field(default_factory=dict)


class DdbDataHandlers:
    tables: dict[str, FakeTable]
    ddb_errors: dict[str, str]
    permits: Callable[..., bool]

    def _arn(self, name: str) -> str:
        return f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/{name}"

    def _writable(self, name: str, operation: str) -> FakeTable | Reply:
        table = self.tables.get(name)
        assert table is None or table.status == "ACTIVE", f"write_before_active op={operation}"
        if table is None:
            return denied("ResourceNotFoundException")
        if operation in self.ddb_errors:
            return denied(self.ddb_errors[operation])
        return table

    @staticmethod
    def _key(item: Mapping[str, Mapping[str, str]]) -> tuple[str, str]:
        return item["pk"]["S"], item["sk"]["S"]

    def _guard(
        self, who: str, action: str, resource: str, keys: Sequence[str] | None = None
    ) -> Reply | None:
        context = None if keys is None else {LEADING_KEYS: list(keys)}
        if self.permits(who, action, resource, context):
            return None
        message = f"User is not authorized to perform: {action} on resource"
        return denied("AccessDeniedException", message)

    def _dynamodb_put_item(self, payload: dict, who: str) -> Reply:
        table = self._writable(payload["TableName"], "put-item")
        if isinstance(table, Reply):
            return table
        item, key = payload["Item"], self._key(payload["Item"])
        denial = self._guard(who, "dynamodb:PutItem", self._arn(payload["TableName"]), [key[0]])
        if denial is not None:
            return denial
        table.rows[key] = deepcopy(item)
        table.items.add(key)
        return good()

    def _dynamodb_get_item(self, payload: dict, who: str) -> Reply:
        table = self._writable(payload["TableName"], "get-item")
        if isinstance(table, Reply):
            return table
        key = self._key(payload["Key"])
        resource = self._arn(payload["TableName"])
        denial = self._guard(who, "dynamodb:GetItem", resource, [key[0]])
        if denial is not None:
            return denial
        return good({"Item": table.rows[key]}) if key in table.rows else good()

    def _dynamodb_update_item(self, payload: dict, who: str) -> Reply:
        table = self._writable(payload["TableName"], "update-item")
        if isinstance(table, Reply):
            return table
        assert payload["UpdateExpression"] == "SET touched = :v"
        assert payload.get("ReturnValues", "NONE") == "NONE"
        key = self._key(payload["Key"])
        resource = self._arn(payload["TableName"])
        denial = self._guard(who, "dynamodb:UpdateItem", resource, [key[0]])
        if denial is not None:
            return denial
        if key in table.rows:
            table.rows[key]["touched"] = payload["ExpressionAttributeValues"][":v"]
        return good()

    def _dynamodb_delete_item(self, payload: dict, who: str) -> Reply:
        table = self._writable(payload["TableName"], "delete-item")
        if isinstance(table, Reply):
            return table
        key = self._key(payload["Key"])
        resource = self._arn(payload["TableName"])
        denial = self._guard(who, "dynamodb:DeleteItem", resource, [key[0]])
        if denial is not None:
            return denial
        table.rows.pop(key, None)
        table.items.discard(key)
        return good()

    def _dynamodb_query(self, payload: dict, who: str) -> Reply:
        assert payload["Select"] == "COUNT"
        index = payload.get("IndexName")
        attribute = f"{index}pk" if index else "pk"
        assert payload["KeyConditionExpression"] == f"{attribute} = :pk"
        value = payload["ExpressionAttributeValues"][":pk"]["S"]
        table = self._writable(payload["TableName"], "query")
        if isinstance(table, Reply):
            return table
        resource = self._arn(payload["TableName"]) + (f"/index/{index}" if index else "")
        denial = self._guard(who, "dynamodb:Query", resource, [value])
        if denial is not None:
            return denial
        count = sum(
            1
            for row in table.rows.values()
            if row.get(attribute, {}).get("S") == value and (not index or f"{index}sk" in row)
        )
        return good({"Count": count, "ScannedCount": count})

    def _transact_updates(self, actions: list[dict], who: str) -> Reply:
        name = actions[0]["Update"]["TableName"]
        table = self._writable(name, "transact-write-items")
        if isinstance(table, Reply):
            return table
        for action in actions:
            key = self._key(action["Update"]["Key"])
            denial = self._guard(who, "dynamodb:UpdateItem", self._arn(name), [key[0]])
            if denial is not None:
                return denial
        for action in actions:
            key = self._key(action["Update"]["Key"])
            if key in table.rows:
                table.rows[key]["touched"] = action["Update"]["ExpressionAttributeValues"][":v"]
        return good()
