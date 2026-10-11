"""DynamoDB no Floci com o schema canônico: condições, transações, TTL, GSI e proteção."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import pytest
from botocore.exceptions import ClientError

from tests.integration.floci._dynamodb import TTL_ATTRIBUTE, create_canonical_table, delete_table

if TYPE_CHECKING:
    from contextlib import ExitStack

    from tests.integration.floci._floci import FlociContext

pytestmark = [pytest.mark.floci]

_ABSENT = "attribute_not_exists(pk)"


@dataclass(frozen=True, slots=True)
class _Table:
    client: Any
    name: str

    def put(self, item: dict[str, Any], **options: Any) -> Any:
        return self.client.put_item(TableName=self.name, Item=item, **options)

    def get(self, pk: str, sk: str) -> dict[str, Any] | None:
        response = self.client.get_item(TableName=self.name, Key=_key(pk, sk), ConsistentRead=True)
        return response.get("Item")

    def query(self, value: str, index: str | None = None) -> list[str]:
        attribute = f"{index}pk" if index else "pk"
        options: dict[str, Any] = {"IndexName": index} if index else {"ConsistentRead": True}
        response = self.client.query(
            TableName=self.name,
            KeyConditionExpression=f"{attribute} = :value",
            ExpressionAttributeValues={":value": {"S": value}},
            **options,
        )
        return sorted(item["sk"]["S"] for item in response["Items"])

    def transact(self, actions: list[dict[str, Any]], **options: Any) -> Any:
        return self.client.transact_write_items(TransactItems=actions, **options)


@pytest.fixture(scope="module")
def dynamodb(floci: FlociContext) -> Any:
    return floci.client("dynamodb")


@pytest.fixture
def table(dynamodb: Any, resource_name: str, cleanup: ExitStack) -> _Table:
    create_canonical_table(dynamodb, resource_name)
    cleanup.callback(delete_table, dynamodb, resource_name)
    return _Table(dynamodb, resource_name)


def _key(pk: str, sk: str) -> dict[str, dict[str, str]]:
    return {"pk": {"S": pk}, "sk": {"S": sk}}


def _put(table: _Table, pk: str, sk: str) -> dict[str, Any]:
    return {"Put": {"TableName": table.name, "Item": _key(pk, sk)}}


def _bump(table: _Table, expected: int) -> dict[str, Any]:
    return {
        "TableName": table.name,
        "Key": _key("TENANT#a", "RUN#1"),
        "UpdateExpression": "SET version = :next",
        "ConditionExpression": "version = :expected",
        "ExpressionAttributeValues": {
            ":next": {"N": str(expected + 1)},
            ":expected": {"N": str(expected)},
        },
    }


def _expiring(pk: str, sk: str, expires_at: dict[str, str]) -> dict[str, Any]:
    return {**_key(pk, sk), TTL_ATTRIBUTE: expires_at, "gsi1pk": {"S": pk}, "gsi1sk": {"S": sk}}


def _rejected(call: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
    with pytest.raises(ClientError) as error:
        call(*args, **kwargs)
    return cast("dict[str, Any]", error.value.response)


def test_put_condicional_rejeita_item_existente_e_devolve_item_atual(table: _Table) -> None:
    item = {**_key("TENANT#a", "RUN#1"), "version": {"N": "1"}}
    table.put(item, ConditionExpression=_ABSENT)
    replacement = {**item, "version": {"N": "9"}}

    response = _rejected(
        table.put, replacement, ConditionExpression=_ABSENT,
        ReturnValuesOnConditionCheckFailure="ALL_OLD",
    )

    assert response["Error"]["Code"] == "ConditionalCheckFailedException"
    assert response["Item"] == item
    assert table.get("TENANT#a", "RUN#1") == item


def test_put_condicional_sem_return_values_nao_expoe_o_item(table: _Table) -> None:
    item = _key("TENANT#a", "RUN#1")
    table.put(item)

    response = _rejected(table.put, item, ConditionExpression=_ABSENT)

    assert response["Error"]["Code"] == "ConditionalCheckFailedException"
    assert "Item" not in response


def test_cas_por_versao_aplica_uma_vez_e_rejeita_versao_obsoleta(table: _Table) -> None:
    table.put({**_key("TENANT#a", "RUN#1"), "version": {"N": "1"}})
    table.client.update_item(**_bump(table, 1))

    response = _rejected(
        table.client.update_item, **_bump(table, 1), ReturnValuesOnConditionCheckFailure="ALL_OLD",
    )

    assert response["Error"]["Code"] == "ConditionalCheckFailedException"
    assert response["Item"]["version"] == {"N": "2"}
    assert (table.get("TENANT#a", "RUN#1") or {})["version"] == {"N": "2"}


def test_transacao_com_100_acoes_e_aceita(table: _Table) -> None:
    table.transact([_put(table, "TX", f"I#{index:03d}") for index in range(100)])

    assert len(table.query("TX")) == 100


def test_transacao_com_101_acoes_e_rejeitada(table: _Table) -> None:
    actions = [_put(table, "TX", f"I#{index:03d}") for index in range(101)]

    response = _rejected(table.transact, actions)

    assert response["Error"]["Code"] == "ValidationException"
    assert response["Error"]["Message"].endswith("length less than or equal to 100")
    assert table.query("TX") == []


def test_transacao_com_chave_duplicada_e_rejeitada(table: _Table) -> None:
    check = {"TableName": table.name, "Key": _key("DUP", "1"), "ConditionExpression": _ABSENT}
    actions = [_put(table, "DUP", "1"), {"ConditionCheck": check}]

    response = _rejected(table.transact, actions)

    assert response["Error"]["Code"] == "ValidationException"
    assert "multiple operations on one item" in response["Error"]["Message"]
    assert table.query("DUP") == []


def test_conflito_de_condicao_devolve_cancellation_reasons_por_item(table: _Table) -> None:
    current = {**_key("TENANT#a", "RUN#1"), "version": {"N": "2"}}
    table.put(current)
    stale = {"Update": {**_bump(table, 1), "ReturnValuesOnConditionCheckFailure": "ALL_OLD"}}
    actions = [_put(table, "CONF", "1"), stale, _put(table, "CONF", "2")]

    response = _rejected(table.transact, actions)
    reasons = response["CancellationReasons"]

    assert response["Error"]["Code"] == "TransactionCanceledException"
    assert [reason["Code"] for reason in reasons] == ["None", "ConditionalCheckFailed", "None"]
    assert reasons[1]["Item"] == current
    assert table.query("CONF") == []


def test_client_request_token_repetido_e_idempotente(table: _Table) -> None:
    token = str(uuid4())
    table.transact([_put(table, "IDEM", "1")], ClientRequestToken=token)
    table.client.delete_item(TableName=table.name, Key=_key("IDEM", "1"))

    table.transact([_put(table, "IDEM", "1")], ClientRequestToken=token)

    assert table.get("IDEM", "1") is None


def test_client_request_token_com_outra_requisicao_e_rejeitado(table: _Table) -> None:
    token = str(uuid4())
    table.transact([_put(table, "IDEM", "1")], ClientRequestToken=token)

    response = _rejected(table.transact, [_put(table, "IDEM", "2")], ClientRequestToken=token)

    assert response["Error"]["Code"] == "IdempotentParameterMismatchException"
    assert table.query("IDEM") == ["1"]


def test_ttl_esta_habilitado_no_atributo_expires_at(table: _Table) -> None:
    description = table.client.describe_time_to_live(TableName=table.name)

    assert description["TimeToLiveDescription"] == {
        "TimeToLiveStatus": "ENABLED",
        "AttributeName": TTL_ATTRIBUTE,
    }


def test_floci_esconde_item_expirado_de_todas_as_leituras_antes_da_varredura(
    table: _Table,
) -> None:
    now = int(time.time())
    table.put(_expiring("TTL", "expirado", {"N": str(now - 3600)}))
    table.put(_expiring("TTL", "vigente", {"N": str(now + 3600)}))
    scanned = table.client.scan(TableName=table.name, ConsistentRead=True)["Items"]

    assert table.get("TTL", "expirado") is None
    assert table.query("TTL") == ["vigente"]
    assert table.query("TTL", index="gsi1") == ["vigente"]
    assert [item["sk"]["S"] for item in scanned] == ["vigente"]


def test_ttl_ignora_atributo_que_nao_e_numero(table: _Table) -> None:
    table.put(_expiring("TTL", "texto", {"S": str(int(time.time()) - 3600)}))

    assert table.get("TTL", "texto") is not None


def test_floci_ainda_avalia_condicoes_contra_item_expirado(table: _Table) -> None:
    table.put(_expiring("TTL", "expirado", {"N": str(int(time.time()) - 3600)}))

    response = _rejected(table.put, _key("TTL", "expirado"), ConditionExpression=_ABSENT)
    table.client.update_item(
        TableName=table.name, Key=_key("TTL", "expirado"), UpdateExpression="SET marca = :m",
        ConditionExpression="attribute_exists(pk)", ExpressionAttributeValues={":m": {"S": "x"}},
    )

    assert response["Error"]["Code"] == "ConditionalCheckFailedException"
    assert table.get("TTL", "expirado") is None


def test_gsi_esparso_indexa_somente_itens_com_a_chave_do_indice(table: _Table) -> None:
    pending = {"gsi6pk": {"S": "OUTBOX#PENDING"}, "gsi6sk": {"S": "2026-01-01T00:00:00"}}
    table.put({**_key("TENANT#_GLOBAL", "OUTBOX#a"), **pending})
    table.put(_key("TENANT#_GLOBAL", "OUTBOX#b"))
    indexed = table.query("OUTBOX#PENDING", index="gsi6")
    table.client.update_item(
        TableName=table.name, Key=_key("TENANT#_GLOBAL", "OUTBOX#a"),
        UpdateExpression="REMOVE gsi6pk, gsi6sk",
    )

    assert indexed == ["OUTBOX#a"]
    assert table.query("OUTBOX#PENDING", index="gsi6") == []


def test_floci_reflete_escrita_no_gsi_imediatamente(table: _Table) -> None:
    for index in range(20):
        table.put({**_key("G", f"{index:02d}"), "gsi2pk": {"S": "G"}, "gsi2sk": {"S": "x"}})
        assert len(table.query("G", index="gsi2")) == index + 1


def test_gsi_rejeita_leitura_fortemente_consistente(table: _Table) -> None:
    response = _rejected(
        table.client.query, TableName=table.name, IndexName="gsi6", ConsistentRead=True,
        KeyConditionExpression="gsi6pk = :p", ExpressionAttributeValues={":p": {"S": "X"}},
    )

    assert response["Error"]["Code"] == "ValidationException"


def test_pitr_habilita_e_descreve_a_janela_de_recuperacao(table: _Table) -> None:
    before = table.client.describe_continuous_backups(TableName=table.name)
    table.client.update_continuous_backups(
        TableName=table.name,
        PointInTimeRecoverySpecification={
            "PointInTimeRecoveryEnabled": True,
            "RecoveryPeriodInDays": 35,
        },
    )
    after = table.client.describe_continuous_backups(TableName=table.name)
    pitr = after["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]

    disabled = before["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
    assert disabled["PointInTimeRecoveryStatus"] == "DISABLED"
    assert (pitr["PointInTimeRecoveryStatus"], pitr["RecoveryPeriodInDays"]) == ("ENABLED", 35)


def test_deletion_protection_bloqueia_delete_ate_ser_desligada(
    dynamodb: Any, resource_name: str, cleanup: ExitStack,
) -> None:
    create_canonical_table(dynamodb, resource_name, {"DeletionProtectionEnabled": True})
    cleanup.callback(delete_table, dynamodb, resource_name)

    response = _rejected(dynamodb.delete_table, TableName=resource_name)
    described = dynamodb.describe_table(TableName=resource_name)["Table"]

    assert response["Error"]["Code"] == "ValidationException"
    assert described["DeletionProtectionEnabled"] is True


def test_floci_aceita_e_descreve_on_demand_throughput_sem_simular_limite(
    dynamodb: Any, resource_name: str, cleanup: ExitStack,
) -> None:
    ceiling = {"MaxReadRequestUnits": 5, "MaxWriteRequestUnits": 5}
    create_canonical_table(dynamodb, resource_name, {"OnDemandThroughput": ceiling})
    cleanup.callback(delete_table, dynamodb, resource_name)
    table = _Table(dynamodb, resource_name)

    table.transact([_put(table, "BURST", f"{index:03d}") for index in range(100)])
    described = dynamodb.describe_table(TableName=resource_name)["Table"]

    assert described["OnDemandThroughput"] == ceiling
    assert described["BillingModeSummary"]["BillingMode"] == "PAY_PER_REQUEST"
    assert len(table.query("BURST")) == 100


def test_item_acima_de_400_kb_e_rejeitado(table: _Table) -> None:
    oversized = {**_key("BIG", "1"), "blob": {"S": "x" * (400 * 1024 + 1)}}

    response = _rejected(table.put, oversized)

    assert response["Error"]["Code"] == "ValidationException"
    assert table.get("BIG", "1") is None
