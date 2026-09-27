"""Testes do DynamoDBMembershipCandidates — paginação e parsing estrito do gsi1sk."""
from typing import Any

import pytest

from cnes_infra.auth.dynamodb_memberships import DynamoDBMembershipCandidates
from cnes_infra.control_plane.dynamodb_keys import key_component

_TABLE = "cnesdata-raw-dev"


class _FakeClient:
    def __init__(self, *pages: list[dict[str, Any]]) -> None:
        self._pages = list(pages)
        self.requests: list[dict[str, Any]] = []

    def query(self, **request: Any) -> dict[str, Any]:
        self.requests.append(request)
        index = len(self.requests) - 1
        response: dict[str, Any] = {"Items": self._pages[index]}
        if index + 1 < len(self._pages):
            response["LastEvaluatedKey"] = {"page": {"S": str(index + 1)}}
        return response


def _sk(value: str, index: str = "gsi1") -> dict[str, Any]:
    return {f"{index}sk": {"S": value}}


def _tenant(tenant_id: str) -> dict[str, Any]:
    return _sk(f"TENANT#{key_component(tenant_id)}")


def test_consulta_gsi1_por_igualdade_do_usuario() -> None:
    client = _FakeClient([])
    DynamoDBMembershipCandidates(client, _TABLE).list_candidates("user-1")
    assert client.requests == [{
        "TableName": _TABLE,
        "IndexName": "gsi1",
        "KeyConditionExpression": "gsi1pk = :user",
        "ExpressionAttributeValues": {":user": {"S": f"USER#{key_component('user-1')}"}},
        "ProjectionExpression": "gsi1sk",
    }]


def test_pagina_query_e_preserva_ordem() -> None:
    client = _FakeClient([_tenant("tenant-a")], [_tenant("tenant-b")])
    result = DynamoDBMembershipCandidates(client, _TABLE).list_candidates("user-1")
    assert result == ("tenant-a", "tenant-b")
    assert "ExclusiveStartKey" not in client.requests[0]
    assert client.requests[1]["ExclusiveStartKey"] == {"page": {"S": "1"}}


def test_deduplica_candidatos_repetidos() -> None:
    client = _FakeClient([_tenant("tenant-a"), _tenant("tenant-a")], [_tenant("tenant-a")])
    result = DynamoDBMembershipCandidates(client, _TABLE).list_candidates("user-1")
    assert result == ("tenant-a",)


@pytest.mark.parametrize(
    "item",
    [
        {},
        _sk("TENANT#"),
        _sk(key_component("tenant-a")),
        _sk(f"JOB#{key_component('tenant-a')}"),
        _sk("TENANT#zz"),
        _sk("TENANT#abc"),
        _sk(f"TENANT#{key_component('tenant-a').upper()}"),
        _sk(f"TENANT# {key_component('tenant-a')}"),
        _sk("TENANT#ff"),
    ],
    ids=[
        "sem_sk", "vazio", "sem_prefixo", "outro_prefixo", "hex_invalido", "hex_impar",
        "hex_maiusculo", "com_espaco", "utf8_invalido",
    ],
)
def test_descarta_gsi1sk_malformado(item: dict[str, Any]) -> None:
    client = _FakeClient([item, _tenant("tenant-a")])
    result = DynamoDBMembershipCandidates(client, _TABLE).list_candidates("user-1")
    assert result == ("tenant-a",)


def test_usa_atributos_do_indice_configurado() -> None:
    client = _FakeClient([_sk(f"TENANT#{key_component('tenant-a')}", index="gsi9")])
    result = DynamoDBMembershipCandidates(client, _TABLE, index_name="gsi9").list_candidates("u")
    assert result == ("tenant-a",)
    assert client.requests[0]["KeyConditionExpression"] == "gsi9pk = :user"
    assert client.requests[0]["ProjectionExpression"] == "gsi9sk"
