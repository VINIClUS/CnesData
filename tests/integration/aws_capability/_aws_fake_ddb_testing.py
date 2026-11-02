"""Operações DynamoDB da fronteira com estado: criação, ACTIVE, escritas, contagem e DELETING."""

from collections.abc import Mapping
from dataclasses import dataclass, field

from tests.integration.aws_capability._aws_fake_types_testing import denied, good
from tests.integration.aws_capability._b1_testing import ACCOUNT
from tests.integration.aws_capability._service_contracts import ApiError, Reply

TRANSACTION_LIMIT = 100


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


class DdbHandlers:
    tables: dict[str, FakeTable]
    ddb_ready_describes: int
    ddb_deleting_describes: int
    ddb_tx_reasons: dict[str, int] | None
    ddb_errors: dict[str, str]
    batch_unprocessed: dict[int, int]
    batch_errors: dict[int, str]
    batch_calls: int
    scan_page_size: int | None

    def _arn(self, name: str) -> str:
        return f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/{name}"

    def _description(self, name: str) -> dict:
        table = self.tables[name]
        description: dict = {
            "TableName": name,
            "TableArn": self._arn(name),
            "TableStatus": table.status,
            "DeletionProtectionEnabled": table.protection,
        }
        if table.throughput is not None:
            index_status = "ACTIVE" if table.status == "ACTIVE" else "CREATING"
            description["OnDemandThroughput"] = table.throughput
            description["GlobalSecondaryIndexes"] = [
                {
                    "IndexName": index,
                    "IndexStatus": index_status,
                    **({} if value is None else {"OnDemandThroughput": value}),
                }
                for index, value in table.index_throughput.items()
            ]
        return description

    def _dynamodb_create_table(self, payload: dict, who: str) -> Reply:
        name = payload["TableName"]
        if name in self.tables:
            return denied("ResourceInUseException")
        assert payload["BillingMode"] == "PAY_PER_REQUEST"
        indexes = payload.get("GlobalSecondaryIndexes", [])
        self.tables[name] = FakeTable(
            {item["Key"]: item["Value"] for item in payload["Tags"]},
            "CREATING",
            protection=payload["DeletionProtectionEnabled"],
            throughput=payload.get("OnDemandThroughput"),
            index_throughput={i["IndexName"]: i.get("OnDemandThroughput") for i in indexes},
            ready_in=self.ddb_ready_describes,
            deleting_describes=self.ddb_deleting_describes,
        )
        return good({"TableDescription": self._description(name)})

    def _dynamodb_update_table(self, payload: dict, who: str) -> Reply:
        table = self.tables.get(payload["TableName"])
        if table is None:
            return denied("ResourceNotFoundException")
        table.protection = payload["DeletionProtectionEnabled"]
        return good({"TableDescription": self._description(payload["TableName"])})

    def _dynamodb_list_tables(self, payload: dict, who: str) -> Reply:
        return good({"TableNames": list(self.tables)})

    def _dynamodb_describe_table(self, payload: dict, who: str) -> Reply:
        name = payload["TableName"]
        table = self.tables.get(name)
        if table is None:
            return denied("ResourceNotFoundException")
        if table.status == "DELETING":
            if table.deleting_describes == 0:
                del self.tables[name]
                return denied("ResourceNotFoundException")
            table.deleting_describes -= 1
        if table.status == "CREATING":
            if table.ready_in > 0:
                table.ready_in -= 1
            else:
                table.status = "ACTIVE"
        return good({"Table": self._description(name)})

    def _dynamodb_list_tags_of_resource(self, payload: dict, who: str) -> Reply:
        name = str(payload["ResourceArn"]).rsplit("/", 1)[1]
        table = self.tables.get(name)
        if table is None or table.status == "DELETING":
            return denied("ResourceNotFoundException")
        return good({"Tags": [{"Key": k, "Value": v} for k, v in table.tags.items()]})

    def _dynamodb_delete_table(self, payload: dict, who: str) -> Reply:
        name = payload["TableName"]
        if name not in self.tables:
            return denied("ResourceNotFoundException")
        self.tables[name].status = "DELETING"
        return good({"TableDescription": self._description(name)})

    def _writable(self, name: str, operation: str) -> FakeTable | Reply:
        table = self.tables.get(name)
        assert table is None or table.status == "ACTIVE", f"write_before_active op={operation}"
        if table is None:
            return denied("ResourceNotFoundException")
        if operation in self.ddb_errors:
            return denied(self.ddb_errors[operation])
        return table

    @staticmethod
    def _key(item: dict) -> tuple[str, str]:
        return item["pk"]["S"], item["sk"]["S"]

    def _dynamodb_transact_write_items(self, payload: dict, who: str) -> Reply:
        actions = payload["TransactItems"]
        table = self._writable(actions[0]["Put"]["TableName"], "transact-write-items")
        if isinstance(table, Reply):
            return table
        if len(actions) > TRANSACTION_LIMIT:
            return denied("ValidationException")
        if self.ddb_tx_reasons is not None:
            reasons = [{"Code": c} for c, n in self.ddb_tx_reasons.items() for _ in range(n)]
            details = {"Code": "TransactionCanceledException", "CancellationReasons": reasons}
            return Reply(False, {}, ApiError("TransactionCanceledException", details, "service"))
        table.items.update(self._key(action["Put"]["Item"]) for action in actions)
        return good()

    def _dynamodb_batch_write_item(self, payload: dict, who: str) -> Reply:
        self.batch_calls += 1
        (name, requests), = payload["RequestItems"].items()
        table = self._writable(name, "batch-write-item")
        if isinstance(table, Reply):
            return table
        if self.batch_calls in self.batch_errors:
            return denied(self.batch_errors[self.batch_calls])
        left = self.batch_unprocessed.get(self.batch_calls, 0)
        accepted, rejected = requests[: len(requests) - left], requests[len(requests) - left :]
        table.items.update(self._key(request["PutRequest"]["Item"]) for request in accepted)
        return good({"UnprocessedItems": {name: rejected} if rejected else {}})

    def _dynamodb_scan(self, payload: dict, who: str) -> Reply:
        assert payload["Select"] == "COUNT"
        assert payload["ConsistentRead"] is True
        table = self._writable(payload["TableName"], "scan")
        if isinstance(table, Reply):
            return table
        total = len(table.items)
        size = self.scan_page_size or max(total, 1)
        start = int(payload.get("ExclusiveStartKey", {}).get("offset", {}).get("N", "0"))
        count = min(size, total - start)
        reply: dict = {"Count": count, "ScannedCount": count}
        if start + count < total:
            reply["LastEvaluatedKey"] = {"offset": {"N": str(start + count)}}
        return good(reply)
