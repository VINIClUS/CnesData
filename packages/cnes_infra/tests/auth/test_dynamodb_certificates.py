"""Condições, leitura forte e falhas do registro DynamoDB com Moto."""

from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

import boto3
import pytest
from botocore.exceptions import ClientError, ReadTimeoutError
from moto import mock_aws

from cnes_domain.auth.certificates import CertificateStoreUnavailable
from cnes_infra.auth.dynamodb_certificates import DynamoDBCertificateRegister
from packages.cnes_infra.tests.auth.test_certificate_register_contract import (
    _FINGERPRINT,
    _NOW,
    _TABLE,
    _certificate,
    _key,
)

if TYPE_CHECKING:
    from botocore.client import BaseClient

_ITEM_KEY = {
    "pk": {"S": "TENANT#74656e616e742d61"},
    "sk": {"S": "CERT#73657269616c2d6f6c64"},
}


class _ClientProbe:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.reads: list[dict[str, Any]] = []
        self.writes: list[dict[str, Any]] = []
        self.on_transaction: Callable[[], object] | None = None
        self.read_error: Exception | None = None

    def get_item(self, **request: Any) -> dict[str, Any]:
        self.reads.append(request)
        if self.read_error is not None:
            raise self.read_error
        return self.client.get_item(**request)

    def transact_write_items(self, **request: Any) -> dict[str, Any]:
        self.writes.append(request)
        callback = self.on_transaction
        self.on_transaction = None
        if callback is not None:
            callback()
        return self.client.transact_write_items(**request)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.client, name)


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
    with mock_aws():
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName=_TABLE,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        yield dynamodb


def test_record_usa_put_condicional_e_payload_tipado(client: Any) -> None:
    probe = _ClientProbe(client)
    register = DynamoDBCertificateRegister(cast("BaseClient", probe), _TABLE)
    register.record(_certificate())

    assert len(probe.writes) == 1
    action = probe.writes[0]["TransactItems"][0]["Put"]
    assert action["TableName"] == _TABLE
    assert action["ConditionExpression"] == "attribute_not_exists(pk)"
    assert {name: action["Item"][name] for name in ("pk", "sk")} == _ITEM_KEY
    assert action["Item"]["entity"] == {"S": "CERTIFICATE"}
    assert register.get(_key()) == _certificate()


def test_get_e_is_active_exigem_leitura_forte_por_chave(client: Any) -> None:
    probe = _ClientProbe(client)
    register = DynamoDBCertificateRegister(cast("BaseClient", probe), _TABLE)
    register.record(_certificate())

    assert register.get(_key()) == _certificate()
    assert register.is_active(_key(), _FINGERPRINT, _NOW) is True
    assert probe.reads == [
        {"TableName": _TABLE, "Key": _ITEM_KEY, "ConsistentRead": True},
        {"TableName": _TABLE, "Key": _ITEM_KEY, "ConsistentRead": True},
    ]


def test_revoke_usa_cas_do_payload_anterior(client: Any) -> None:
    register = DynamoDBCertificateRegister(client, _TABLE)
    register.record(_certificate())
    old_item = client.get_item(TableName=_TABLE, Key=_ITEM_KEY, ConsistentRead=True)["Item"]
    probe = _ClientProbe(client)
    instrumented = DynamoDBCertificateRegister(cast("BaseClient", probe), _TABLE)

    assert instrumented.revoke(_key(), _NOW) is True

    action = probe.writes[0]["TransactItems"][0]["Put"]
    assert action["ConditionExpression"] == "payload = :expected"
    assert action["ExpressionAttributeValues"] == {":expected": old_item["payload"]}
    assert action["Item"]["pk"] == _ITEM_KEY["pk"]
    assert action["Item"]["sk"] == _ITEM_KEY["sk"]
    assert instrumented.get(_key()) == _certificate(revoked_at=_NOW)


def test_corrida_de_revogacao_rele_e_retorna_falso(client: Any) -> None:
    winner = DynamoDBCertificateRegister(client, _TABLE)
    winner.record(_certificate())
    probe = _ClientProbe(client)
    loser = DynamoDBCertificateRegister(cast("BaseClient", probe), _TABLE)
    probe.on_transaction = lambda: winner.revoke(_key(), _NOW)

    assert loser.revoke(_key(), _NOW + timedelta(minutes=1)) is False
    assert len(probe.reads) == 2
    assert all(request["ConsistentRead"] is True for request in probe.reads)
    assert loser.get(_key()) == _certificate(revoked_at=_NOW)


def test_condicao_falha_sem_revogacao_vira_indisponivel(client: Any) -> None:
    register = DynamoDBCertificateRegister(client, _TABLE)
    register.record(_certificate())
    probe = _ClientProbe(client)
    instrumented = DynamoDBCertificateRegister(cast("BaseClient", probe), _TABLE)

    def reject() -> None:
        raise ClientError(
            {
                "Error": {"Code": "TransactionCanceledException"},
                "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
            },
            "TransactWriteItems",
        )

    probe.on_transaction = reject
    with pytest.raises(CertificateStoreUnavailable) as failure:
        instrumented.revoke(_key(), _NOW)
    assert failure.value.code == "certificate_store_unavailable"
    assert str(failure.value) == "code=certificate_store_unavailable"
    assert register.get(_key()) == _certificate()


def test_falha_de_transporte_na_leitura_nao_vira_inativo(client: Any) -> None:
    register = DynamoDBCertificateRegister(client, _TABLE)
    register.record(_certificate())
    probe = _ClientProbe(client)
    probe.read_error = ReadTimeoutError(endpoint_url="http://127.0.0.1")
    instrumented = DynamoDBCertificateRegister(cast("BaseClient", probe), _TABLE)

    with pytest.raises(CertificateStoreUnavailable) as failure:
        instrumented.is_active(_key(), _FINGERPRINT, _NOW)
    assert failure.value.code == "certificate_store_unavailable"
    assert str(failure.value) == "code=certificate_store_unavailable"


@pytest.mark.parametrize(
    "error",
    [
        ReadTimeoutError(endpoint_url="http://127.0.0.1"),
        ClientError(
            {"Error": {"Code": "InternalServerError", "Message": "synthetic-private"}},
            "TransactWriteItems",
        ),
    ],
)
def test_falha_na_escrita_de_revogacao_preserva_registro(
    client: Any,
    error: Exception,
) -> None:
    register = DynamoDBCertificateRegister(client, _TABLE)
    original = _certificate()
    register.record(original)
    probe = _ClientProbe(client)
    instrumented = DynamoDBCertificateRegister(cast("BaseClient", probe), _TABLE)

    def fail_transaction() -> None:
        raise error

    probe.on_transaction = fail_transaction
    with pytest.raises(CertificateStoreUnavailable) as failure:
        instrumented.revoke(_key(), _NOW)
    assert failure.value.code == "certificate_store_unavailable"
    assert str(failure.value) == "code=certificate_store_unavailable"
    assert len(probe.reads) >= 1
    assert all(request["ConsistentRead"] is True for request in probe.reads)
    assert register.get(_key()) == original
    assert register.is_active(_key(), _FINGERPRINT, _NOW) is True


def test_falha_de_transporte_na_escrita_levanta_erro_publico(client: Any) -> None:
    probe = _ClientProbe(client)
    register = DynamoDBCertificateRegister(cast("BaseClient", probe), _TABLE)

    def timeout() -> None:
        raise ReadTimeoutError(endpoint_url="http://127.0.0.1")

    probe.on_transaction = timeout
    with pytest.raises(CertificateStoreUnavailable) as failure:
        register.record(_certificate())
    assert failure.value.code == "certificate_store_unavailable"
    assert str(failure.value) == "code=certificate_store_unavailable"
    assert DynamoDBCertificateRegister(client, _TABLE).get(_key()) is None
