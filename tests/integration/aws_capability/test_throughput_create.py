"""Criação do perfil ddb-throughput e ausência de retry nas operações de carga."""

import json
import logging
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from tests.integration.aws_capability._b1_testing import b1_lab, creation_for, ok
from tests.integration.aws_capability._service_read_assertions import (
    assert_environment,
    assert_process,
)
from tests.integration.aws_capability._service_read_testing import ReadLab, make_read_lab, respond
from tests.integration.aws_capability.cli import Request

THROUGHPUT = {"MaxReadRequestUnits": 100, "MaxWriteRequestUnits": 25}


@pytest.fixture
def lab(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> ReadLab:
    caplog.set_level(logging.DEBUG)
    return make_read_lab(monkeypatch, tmp_path)


def fixture() -> Any:
    path = Path(__file__).parent / "fixtures" / "cli" / "b3" / "ddb_create.json"
    return json.loads(path.read_text())


def sent(lab: ReadLab, probe: str) -> dict:
    lab = b1_lab(lab, probe)
    respond(lab, fixture()["creating"])
    lab.services.create("dynamodb", creation_for(lab), lab.context)
    return json.loads(lab.process.calls[0][1]["input"])


def test_ddb_throughput_espelha_o_main_tf_com_teto_na_tabela_e_nos_seis_gsis(
    lab: ReadLab,
) -> None:
    expected = deepcopy(fixture()["request"])
    expected["OnDemandThroughput"] = THROUGHPUT
    for index in expected["GlobalSecondaryIndexes"]:
        index["OnDemandThroughput"] = THROUGHPUT
    payload = sent(lab, "ddb-throughput")
    assert payload == expected
    assert [i["IndexName"] for i in payload["GlobalSecondaryIndexes"]] == [
        f"gsi{n}" for n in range(1, 7)
    ]
    assert all(i["OnDemandThroughput"] == THROUGHPUT for i in payload["GlobalSecondaryIndexes"])
    assert payload["OnDemandThroughput"] == {"MaxReadRequestUnits": 100, "MaxWriteRequestUnits": 25}
    assert payload["BillingMode"] == "PAY_PER_REQUEST"
    assert payload["DeletionProtectionEnabled"] is True


@pytest.mark.parametrize("probe", ["all", "ddb-transactions", "ddb-conditions", "ddb-ttl-gsi"])
def test_outros_perfis_ddb_mantem_o_payload_sem_teto(lab: ReadLab, probe: str) -> None:
    payload = sent(lab, probe)
    assert payload == fixture()["request"]
    assert "OnDemandThroughput" not in json.dumps(payload)


@pytest.mark.parametrize(
    ("operation", "body", "reply"),
    [
        ("transact-write-items", {"TransactItems": []}, None),
        ("batch-write-item", {"RequestItems": {}}, {"UnprocessedItems": {}}),
        ("scan", {"TableName": "t", "Select": "COUNT"}, {"Count": 0, "ScannedCount": 0}),
    ],
)
def test_operacoes_de_carga_nao_repetem_para_nao_esconder_throttle(
    lab: ReadLab, operation: str, body: dict, reply: dict | None
) -> None:
    lab = replace(lab)
    ok(lab, reply)
    result = lab.services.ports.commands.call(Request("dynamodb", operation, body), lab.context)
    assert result.ok
    assert_process(lab, [("dynamodb", operation, "us-east-2", body)])
    assert_environment(lab.process.calls[0][1]["env"], "us-east-2")
    assert lab.process.calls[0][1]["env"]["AWS_MAX_ATTEMPTS"] == "1"
