"""Inputs e observação pública dos testes de preparo DynamoDB."""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from tests.integration.aws_capability._service_read_assertions import assert_private
from tests.integration.aws_capability._service_read_testing import CANARY, ReadLab


@dataclass(frozen=True)
class Privacy:
    caplog: pytest.LogCaptureFixture
    capsys: pytest.CaptureFixture[str]

    def check(self, error: BaseException, lab: ReadLab) -> None:
        captured = self.capsys.readouterr()
        assert_private(error, lab, self.caplog.text + captured.out + captured.err)


def _data() -> Any:
    path = Path(__file__).parent / "fixtures" / "cli" / "b3" / "ddb_prepare.json"
    return json.loads(path.read_text())


def _invalid_table(case: str) -> Any:
    data = _data()["active"]
    table = data["Table"]
    if case.startswith("arn_"):
        replacements = {
            "arn_conta": ("111111111111", "222222222222"),
            "arn_regiao": ("us-east-2", "us-west-1"),
            "arn_nome": ("a1b2c3d4", "ffffffff"),
        }
        table["TableArn"] = table["TableArn"].replace(*replacements[case])
    elif case == "gsi_ausente":
        table["GlobalSecondaryIndexes"].pop()
    elif case == "gsi_duplicado":
        table["GlobalSecondaryIndexes"].append(table["GlobalSecondaryIndexes"][0])
    elif case == "gsi_chave":
        table["GlobalSecondaryIndexes"][0]["KeySchema"][0]["AttributeName"] = "gsi2pk"
    elif case == "projecao":
        table["GlobalSecondaryIndexes"][0]["Projection"]["ProjectionType"] = "KEYS_ONLY"
    elif case == "atributo":
        table["AttributeDefinitions"][0]["AttributeType"] = "N"
    elif case == "atributo_duplicado":
        table["AttributeDefinitions"].append(table["AttributeDefinitions"][0])
    elif case == "chave":
        table["KeySchema"][0]["KeyType"] = "RANGE"
    else:
        values = {
            "protection": ("DeletionProtectionEnabled", False),
            "protection_int": ("DeletionProtectionEnabled", 1),
            "billing": ("BillingModeSummary", {"BillingMode": "PROVISIONED"}),
            "terminal": ("TableStatus", "DELETING"),
            "status_tipo": ("TableStatus", []),
            "nome": ("TableName", CANARY),
        }
        key, value = values[case]
        table[key] = value
    return data
