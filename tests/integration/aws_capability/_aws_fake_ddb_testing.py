"""Operações DynamoDB da fronteira com estado, incluindo a janela DELETING."""

from dataclasses import dataclass

from tests.integration.aws_capability._aws_fake_types_testing import denied, good
from tests.integration.aws_capability._b1_testing import ACCOUNT
from tests.integration.aws_capability._service_contracts import Reply


@dataclass
class FakeTable:
    tags: dict[str, str]
    status: str = "ACTIVE"
    deleting_describes: int = 0


class DdbHandlers:
    tables: dict[str, FakeTable]

    def _arn(self, name: str) -> str:
        return f"arn:aws:dynamodb:us-east-2:{ACCOUNT}:table/{name}"

    def _description(self, name: str) -> dict:
        table = self.tables[name]
        return {
            "TableName": name,
            "TableArn": self._arn(name),
            "TableStatus": table.status,
            "DeletionProtectionEnabled": False,
        }

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
