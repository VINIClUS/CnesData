"""Persistência DynamoDB real via Moto do contrato de leads públicos."""

import traceback
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from moto import mock_aws

from cnes_domain import public_leads as domain
from cnes_infra.public_leads import dynamodb as dynamodb_module

PublicLead = domain.PublicLead
PublicLeadInput = domain.PublicLeadInput
PublicLeadConflict = domain.PublicLeadConflict
PublicLeadUnavailable = domain.PublicLeadUnavailable
DynamoDBPublicLeadStore = dynamodb_module.DynamoDBPublicLeadStore

_TABLE_NAME = "public-leads-dynamodb"
_FIELDS = {
    "name": "Pessoa Sintética",
    "email": "Pessoa.Sintetica+Leads@EXAMPLE.ORG",
    "interest": "contact",
    "organization": "Org UTF-8 — saúde",
    "municipality": "Cidade",
    "role": "Gestão",
    "message": "Linha 1\nLinha 2",
    "source_path": "/contato",
    "source_cta": "fale conosco",
    "privacy_notice_version": "1",
    "newsletter_opt_in": True,
}
_NOW = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
_IDS = (
    UUID("11111111-1111-4111-8111-111111111111"),
    UUID("22222222-2222-4222-8222-222222222222"),
)


def _input(**changes: object) -> PublicLeadInput:
    return PublicLeadInput(**{**_FIELDS, **changes})


def _record(**changes: object) -> PublicLead:
    return PublicLead(
        lead_id=_IDS[0],
        created_at=_NOW,
        **{**_FIELDS, **changes},
    )


def _create_table(client: Any, table_name: str = _TABLE_NAME) -> None:
    client.create_table(
        TableName=table_name,
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


def _store(
    client: Any,
    calls: list[str] | None = None,
    table_name: str = _TABLE_NAME,
    uuid_values: tuple[UUID, ...] = _IDS,
) -> Any:
    events = calls if calls is not None else []
    ids = iter(uuid_values)

    def clock() -> datetime:
        events.append("clock")
        return _NOW

    def uuid_factory() -> UUID:
        events.append("uuid")
        return next(ids)

    return DynamoDBPublicLeadStore(client, table_name, clock, uuid_factory)


@pytest.fixture
def dynamodb_store() -> Iterator[tuple[Any, Any, list[str]]]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        calls: list[str] = []
        yield client, _store(client, calls), calls


def test_constructor_nao_cria_tabela_ou_recurso() -> None:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _store(client, table_name="tabela-ausente")
        assert client.list_tables()["TableNames"] == []


def test_create_persiste_item_global_com_campos_literais(dynamodb_store: Any) -> None:
    client, store, calls = dynamodb_store
    created = store.create(_input())
    assert created.lead_id == _IDS[0]
    assert created.created_at == _NOW
    assert calls.count("clock") == 1
    assert calls.count("uuid") == 1
    item = client.get_item(
        TableName=_TABLE_NAME,
        Key={"pk": {"S": "PUBLIC_LEADS"}, "sk": {"S": f"LEAD#{_IDS[0].hex}"}},
        ConsistentRead=True,
    )["Item"]
    assert item == {
        "pk": {"S": "PUBLIC_LEADS"},
        "sk": {"S": f"LEAD#{created.lead_id.hex}"},
        "entity_type": {"S": "PUBLIC_LEAD"},
        "id": {"S": str(created.lead_id)},
        "created_at": {"S": _NOW.isoformat()},
        "name": {"S": _FIELDS["name"]},
        "email": {"S": _FIELDS["email"]},
        "interest": {"S": _FIELDS["interest"]},
        "organization": {"S": _FIELDS["organization"]},
        "municipality": {"S": _FIELDS["municipality"]},
        "role": {"S": _FIELDS["role"]},
        "message": {"S": _FIELDS["message"]},
        "source_path": {"S": _FIELDS["source_path"]},
        "source_cta": {"S": _FIELDS["source_cta"]},
        "privacy_notice_version": {"S": _FIELDS["privacy_notice_version"]},
        "newsletter_opt_in": {"BOOL": True},
    }


def test_colisao_de_uuid_no_create_nao_sobrescreve(dynamodb_store: Any) -> None:
    client, _, calls = dynamodb_store
    collision_store = _store(client, calls, uuid_values=(_IDS[0], _IDS[0]))
    first = collision_store.create(_input())
    key = {"pk": {"S": "PUBLIC_LEADS"}, "sk": {"S": f"LEAD#{_IDS[0].hex}"}}
    before = client.get_item(TableName=_TABLE_NAME, Key=key, ConsistentRead=True)["Item"]
    with pytest.raises(PublicLeadUnavailable, match="code=leads_unavailable"):
        collision_store.create(_input(name="Segundo registro"))
    after = client.get_item(TableName=_TABLE_NAME, Key=key, ConsistentRead=True)["Item"]
    assert calls.count("clock") == 2
    assert calls.count("uuid") == 2
    assert before == after
    assert collision_store.readback(first.lead_id) == first


def test_import_invalido_nao_acessa_dynamodb_antes_da_validacao(
    dynamodb_store: Any,
) -> None:
    client, _, calls = dynamodb_store

    class ClientRecorder:
        def __init__(self, wrapped: Any) -> None:
            self.wrapped = wrapped
            self.events: list[str] = []

        def __getattr__(self, name: str) -> Any:
            method = getattr(self.wrapped, name)

            def record_call(*args: object, **kwargs: object) -> Any:
                self.events.append(name)
                return method(*args, **kwargs)

            return record_call

    recorder = ClientRecorder(client)
    guarded_store = _store(recorder, calls)
    calls.clear()
    with pytest.raises(PublicLeadUnavailable, match="code=leads_unavailable"):
        guarded_store.import_record(_record(name=""))
    assert calls == []
    assert recorder.events == []
    assert client.scan(TableName=_TABLE_NAME)["Items"] == []


def test_readback_dynamodb_exige_consistent_read(dynamodb_store: Any) -> None:
    client, store, _ = dynamodb_store
    created = store.create(_input())
    calls: list[dict[str, object]] = []

    class Spy:
        def __init__(self, wrapped: Any) -> None:
            self.wrapped = wrapped

        def __getattr__(self, name: str) -> Any:
            return getattr(self.wrapped, name)

        def get_item(self, **kwargs: object) -> dict[str, object]:
            calls.append(kwargs)
            return self.wrapped.get_item(**kwargs)

    store = _store(Spy(client))
    assert store.readback(created.lead_id) == created
    assert len(calls) == 1
    assert calls[0]["ConsistentRead"] is True


def test_readback_rele_item_fresco_alterado(dynamodb_store: Any) -> None:
    client, store, _ = dynamodb_store
    created = store.create(_input())
    client.update_item(
        TableName=_TABLE_NAME,
        Key={"pk": {"S": "PUBLIC_LEADS"}, "sk": {"S": f"LEAD#{created.lead_id.hex}"}},
        UpdateExpression="SET #name = :name",
        ExpressionAttributeNames={"#name": "name"},
        ExpressionAttributeValues={":name": {"S": "alterado no banco"}},
    )
    assert store.readback(created.lead_id) == replace(created, name="alterado no banco")


def test_schema_e_item_nao_tem_tenant_ttl_gsi_ou_pii_index(dynamodb_store: Any) -> None:
    client, store, _ = dynamodb_store
    store.create(_input())
    description = client.describe_table(TableName=_TABLE_NAME)["Table"]
    item = client.scan(TableName=_TABLE_NAME)["Items"][0]
    assert description.get("GlobalSecondaryIndexes", []) == []
    assert "tenant_id" not in item
    assert "expires_at" not in item
    assert "ttl" not in item


def test_import_conflito_e_indisponibilidade_expoem_erros_neutros(dynamodb_store: Any) -> None:
    client, store, _ = dynamodb_store
    record = _record()
    store.import_record(record)
    with pytest.raises(PublicLeadConflict, match="code=leads_conflict"):
        store.import_record(replace(record, message="outro"))
    client.delete_table(TableName=_TABLE_NAME)
    with pytest.raises(PublicLeadUnavailable) as caught:
        store.readback(record.lead_id)
    assert str(caught.value) == "code=leads_unavailable"
    assert _TABLE_NAME not in str(caught.value)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("id", {"S": "22222222-2222-4222-8222-222222222222"}, id="uuid-divergente"),
        pytest.param("entity_type", {"S": "SYNTHETIC_OTHER"}, id="discriminator"),
        pytest.param("created_at", {"S": "not-a-time"}, id="time-invalid"),
        pytest.param("created_at", {"S": "2026-01-02T03:04:05.123456"}, id="time-naive"),
        pytest.param("created_at", {"S": "2026-01-02T03:04:05.123456+01:00"}, id="time-offset"),
        pytest.param("newsletter_opt_in", {"N": "1"}, id="bool-invalid"),
        pytest.param("email", None, id="missing-field"),
        pytest.param("name", {"S": "N" * 121}, id="length-invalid"),
    ],
)
def test_readback_rejeita_item_corrompido_por_escrita_moto_real(
    dynamodb_store: Any,
    field: str,
    value: Any,
) -> None:
    client, store, _ = dynamodb_store
    store.create(_input())
    key = {"pk": {"S": "PUBLIC_LEADS"}, "sk": {"S": f"LEAD#{_IDS[0].hex}"}}
    item = client.get_item(TableName=_TABLE_NAME, Key=key, ConsistentRead=True)["Item"]
    if value is None:
        del item[field]
    else:
        item[field] = value
    client.put_item(TableName=_TABLE_NAME, Item=item)
    with pytest.raises(PublicLeadUnavailable, match=r"^code=leads_unavailable$"):
        store.readback(_IDS[0])


@pytest.mark.parametrize("field", ["pk", "sk"])
def test_readback_rejeita_chave_divergente_retornada_por_leitura_moto_real(
    dynamodb_store: Any,
    field: str,
) -> None:
    client, store, _ = dynamodb_store
    store.create(_input())
    key = {"pk": {"S": "PUBLIC_LEADS"}, "sk": {"S": f"LEAD#{_IDS[0].hex}"}}
    item = client.get_item(TableName=_TABLE_NAME, Key=key, ConsistentRead=True)["Item"]
    item[field] = {"S": "SYNTHETIC_OTHER_KEY"}
    client.put_item(TableName=_TABLE_NAME, Item=item)

    class MisdirectedClient:
        def __getattr__(self, name: str) -> Any:
            return getattr(client, name)

        def get_item(self, **kwargs: Any) -> Any:
            kwargs["Key"] = {"pk": item["pk"], "sk": item["sk"]}
            return client.get_item(**kwargs)

    misdirected = _store(MisdirectedClient())
    with pytest.raises(PublicLeadUnavailable, match=r"^code=leads_unavailable$"):
        misdirected.readback(_IDS[0])


@pytest.mark.parametrize(
    ("method", "failure"),
    [
        pytest.param("put_item", "transport", id="put-transport"),
        pytest.param("get_item", "transport", id="get-transport"),
        pytest.param("put_item", "remote", id="put-remote"),
        pytest.param("get_item", "remote", id="get-remote"),
    ],
)
def test_falha_de_transporte_ou_remota_e_neutra_sem_cadeia_publica(
    dynamodb_store: Any,
    method: str,
    failure: str,
) -> None:
    client, _, calls = dynamodb_store
    events: list[str] = []

    class UnavailableClient:
        def __getattr__(self, name: str) -> Any:
            if name != method:
                return getattr(client, name)

            def unavailable(**kwargs: object) -> Any:
                events.append(name)
                if failure == "transport":
                    raise EndpointConnectionError(endpoint_url="https://private.example/synthetic")
                raise ClientError(
                    {
                        "Error": {
                            "Code": "InternalServerError",
                            "Message": "synthetic-private.example",
                        }
                    },
                    "PutItem" if method == "put_item" else "GetItem",
                )

            return unavailable

    store = _store(UnavailableClient(), calls)
    with pytest.raises(PublicLeadUnavailable) as caught:
        store.create(_input()) if method == "put_item" else store.readback(_IDS[0])
    assert events == [method]
    assert str(caught.value) == "code=leads_unavailable"
    assert caught.value.code == "leads_unavailable"
    formatted = "".join(traceback.format_exception(caught.value))
    assert "private.example" not in formatted
    assert "EndpointConnectionError" not in formatted
    assert "InternalServerError" not in formatted
    assert client.scan(TableName=_TABLE_NAME)["Items"] == []
