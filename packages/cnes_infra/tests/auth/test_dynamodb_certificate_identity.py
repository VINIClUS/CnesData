"""Rejeição de identidade divergente no payload de certificados DynamoDB."""

import json
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.auth.certificates import (
    CertificateKey,
    CertificateRecord,
    CertificateRegisterPort,
)
from cnes_infra.auth.dynamodb_certificates import DynamoDBCertificateRegister

_TABLE = "certificate-identity"
_NOW = datetime(2026, 1, 2, 4, tzinfo=UTC)
_PAYLOAD = {
    "tenant_id": "tenant-a",
    "agent_id": "agent-a",
    "serial": "serial-a",
    "certificate_fingerprint_sha256": "a" * 64,
    "subject_cn": "agent-a",
    "issued_at": "2026-01-02T03:00:00Z",
    "expires_at": "2026-01-03T03:00:00Z",
    "revoked_at": None,
}
_PHYSICAL = ("TENANT#74656e616e742d61", "CERT#73657269616c2d61")
_TARGETS = {
    "tenant_id": ("TENANT#74656e616e742d62", _PHYSICAL[1]),
    "agent_id": _PHYSICAL,
    "serial": (_PHYSICAL[0], "CERT#73657269616c2d62"),
}
_DIVERGENT = {"tenant_id": "tenant-b", "agent_id": "agent-b", "serial": "serial-b"}


@pytest.fixture
def client() -> Iterator[Any]:
    with mock_aws():
        dynamodb = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",  # noqa: S106
        )
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
            BillingMode="PAY_PER_REQUEST",
        )
        yield dynamodb


def _native_item(key: tuple[str, str], payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "pk": {"S": key[0]},
        "sk": {"S": key[1]},
        "entity": {"S": "CERTIFICATE"},
        "payload": {"S": json.dumps(payload, separators=(",", ":"))},
    }


def _snapshot(client: Any) -> list[dict[str, Any]]:
    return sorted(
        client.scan(TableName=_TABLE, ConsistentRead=True)["Items"],
        key=lambda item: (item["pk"]["S"], item["sk"]["S"]),
    )


def _matched_control(register: CertificateRegisterPort, key: CertificateKey) -> None:
    certificate = CertificateRecord.model_validate(_PAYLOAD)
    register.record(certificate)
    register.record(certificate.model_copy(update={"serial": "serial-sentinel"}))
    assert register.get(key) == certificate
    assert register.is_active(key, "a" * 64, _NOW) is True
    assert register.revoke(key, _NOW) is True
    assert register.get(key) == certificate.model_copy(update={"revoked_at": _NOW})


def _seed_mismatch(client: Any, field: str) -> None:
    payload = {**_PAYLOAD, field: _DIVERGENT[field]}
    CertificateRecord.model_validate(payload)
    assert {name for name in _PAYLOAD if payload[name] != _PAYLOAD[name]} == {field}
    client.put_item(TableName=_TABLE, Item=_native_item(_PHYSICAL, payload))
    if field != "agent_id":
        client.put_item(TableName=_TABLE, Item=_native_item(_TARGETS[field], payload))


def _invoke(register: CertificateRegisterPort, key: CertificateKey, operation: str) -> object:
    if operation == "get":
        return register.get(key)
    if operation == "is_active":
        return register.is_active(key, "a" * 64, _NOW)
    return register.revoke(key, _NOW)


@pytest.mark.parametrize("field", ["tenant_id", "agent_id", "serial"])
@pytest.mark.parametrize("operation", ["get", "is_active", "revoke"])
def test_rejeita_identidade_divergente_sem_alterar_registros(
    client: Any,
    field: str,
    operation: str,
    record_property: Callable[[str, object], None],
) -> None:
    register: CertificateRegisterPort = DynamoDBCertificateRegister(client, _TABLE)
    key = CertificateKey(tenant_id="tenant-a", agent_id="agent-a", serial="serial-a")
    _matched_control(register, key)
    _seed_mismatch(client, field)
    before = _snapshot(client)
    result = _invoke(register, key, operation)
    after = _snapshot(client)
    record_property("native_before", json.dumps(before, sort_keys=True))
    record_property("native_after", json.dumps(after, sort_keys=True))
    record_property("public_result", repr(result))
    assert after == before
    expected = None if operation == "get" else False
    assert result is expected
