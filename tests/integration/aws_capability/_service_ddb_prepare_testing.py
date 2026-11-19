"""Inputs e observação pública dos testes de preparo DynamoDB."""

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pytest

from tests.integration.aws_capability._service_read_assertions import ExpectedCall, assert_private
from tests.integration.aws_capability._service_read_testing import CANARY, ReadLab, respond


@dataclass(frozen=True)
class Privacy:
    caplog: pytest.LogCaptureFixture
    capsys: pytest.CaptureFixture[str]

    def check(self, error: BaseException, lab: ReadLab) -> None:
        captured = self.capsys.readouterr()
        assert_private(error, lab, self.caplog.text + captured.out + captured.err)

    def check_value(self, value: object, lab: ReadLab) -> None:
        self.check(ValueError(repr(value)), lab)


CONDITIONS_EVIDENCE: dict[str, Any] = {
    "cas_write_versions": [1, 2],
    "cas_stale_error_code": "ConditionalCheckFailedException",
    "cas_final_version": 2,
    "cancel_error_code": "TransactionCanceledException",
    "cancel_reason_codes": ["None", "ConditionalCheckFailed", "None"],
    "cancel_readback_count": 3,
    "token_replay_accepted": True,
    "token_mismatch_error_code": "IdempotentParameterMismatchException",
    "token_item_absent_after_replay": True,
}


def transactions_evidence(
    code: str = "TransactionCanceledException",
    reasons: tuple[str, ...] = ("None", "ValidationError", "None"),
) -> dict[str, Any]:
    return {
        "tx_100_accepted": True,
        "tx_100_readback_count": 100,
        "tx_101_error_code": "ValidationException",
        "tx_duplicate_error_code": code,
        "tx_duplicate_reason_codes": list(reasons),
        "tx_duplicate_readback_count": 0,
    }


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


Step = tuple[str, Any]


def _queue(lab: ReadLab, steps: Sequence[Step]) -> list[ExpectedCall]:
    expected = []
    for operation, outcome in steps:
        expected.append(tuple(_data()["requests"][operation]))
        if isinstance(outcome, BaseException | tuple):
            lab.process.outcomes.append(outcome)
        else:
            respond(lab, outcome)
    return cast("list[ExpectedCall]", expected)


def _ready() -> list[Step]:
    data = _data()
    return [("describe", data["active"]), ("tags", data["tags"])]


def _disabled() -> list[Step]:
    return [*_ready(), ("ttl", _data()["ttl"]["disabled"]), *_ready()]
