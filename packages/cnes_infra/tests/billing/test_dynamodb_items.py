"""Testes do codec de itens DynamoDB de billing."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from enum import StrEnum
from typing import Any
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
from cnes_domain.billing.validation import FrozenMapping
from cnes_domain.control_plane.entities import IdempotencyRecord
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.billing import dynamodb_items as items
from cnes_infra.billing.keys import (
    BILLING_AUDIT_TENANT_ID,
    billing_account_key,
    entitlement_snapshot_key,
    plan_version_key,
)
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import timestamp
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    make_account,
    make_audit,
    make_link,
    make_plan,
    make_snapshot,
)

HASH = "a" * 64


class _Color(StrEnum):
    RED = "red"


@dataclass(frozen=True)
class _Inner:
    when: datetime


@dataclass(frozen=True)
class _Outer:
    inner: _Inner
    color: _Color


def _client_error(code: str, reasons: list[dict[str, str]] | None = None) -> ClientError:
    response: dict[str, Any] = {"Error": {"Code": code, "Message": "x"}}
    if reasons is not None:
        response["CancellationReasons"] = reasons
    return ClientError(response, "TransactWriteItems")


def _assert_corrupt(error: pytest.ExceptionInfo[PermanentBillingError], entity: str) -> None:
    assert error.value.code == "billing_item_corrupt"
    assert f"entity={entity}" in str(error.value)


def _with(item: dict[str, Any], **changes: Any) -> dict[str, Any]:
    return {**item, **changes}


def test_snapshot_faz_roundtrip_com_grace_nulo() -> None:
    snapshot = make_snapshot()
    assert items.decode_snapshot(items.encode_snapshot(snapshot), "ba_01") == snapshot


def test_snapshot_faz_roundtrip_com_grace_definido() -> None:
    snapshot = make_snapshot(grace_until=NOW + timedelta(days=7))
    assert items.decode_snapshot(items.encode_snapshot(snapshot), "ba_01") == snapshot


def test_snapshot_faz_roundtrip_com_valid_until_maximo() -> None:
    snapshot = make_snapshot(valid_until=datetime.max.replace(tzinfo=UTC))
    assert items.decode_snapshot(items.encode_snapshot(snapshot), "ba_01") == snapshot


def test_encode_snapshot_grava_versao_como_atributo_numerico() -> None:
    item = items.encode_snapshot(make_snapshot(version=7))
    assert item["entitlement_version"] == {"N": "7"}
    assert (item["pk"]["S"], item["sk"]["S"]) == entitlement_snapshot_key("ba_01")


def test_decode_snapshot_rejeita_versao_divergente_do_atributo() -> None:
    item = _with(items.encode_snapshot(make_snapshot(version=2)), entitlement_version={"N": "3"})
    with pytest.raises(PermanentBillingError) as error:
        items.decode_snapshot(item, "ba_01")
    _assert_corrupt(error, items.SNAPSHOT_ENTITY)


def test_decode_snapshot_rejeita_conta_solicitada_divergente() -> None:
    item = items.encode_snapshot(make_snapshot("ba_01"))
    with pytest.raises(PermanentBillingError) as error:
        items.decode_snapshot(item, "ba_02")
    _assert_corrupt(error, items.SNAPSHOT_ENTITY)


def test_account_faz_roundtrip_sem_customer() -> None:
    account = make_account()
    item = items.encode_account(account)
    assert "stripe_customer_id" not in item
    assert items.decode_account(item, "ba_01") == account


def test_account_faz_roundtrip_com_customer_e_atributos() -> None:
    account = make_account(stripe_customer_id="cus_1")
    item = items.encode_account(account)
    assert item["stripe_customer_id"] == {"S": "cus_1"}
    assert item["status"] == {"S": account.status.value}
    assert item["owner_user_id"] == {"S": "user-owner"}
    assert item["updated_at"] == {"S": items.utc_attribute(account.updated_at)}
    assert items.decode_account(item, "ba_01") == account


def test_decode_account_rejeita_id_solicitado_divergente() -> None:
    with pytest.raises(PermanentBillingError) as error:
        items.decode_account(items.encode_account(make_account("ba_01")), "ba_02")
    _assert_corrupt(error, items.ACCOUNT_ENTITY)


def test_linha_da_lista_faz_roundtrip_com_e_sem_customer() -> None:
    plain = items.encode_account_list_row(make_account())
    with_customer = items.encode_account_list_row(make_account(stripe_customer_id="cus_1"))
    assert "stripe_customer_id" not in plain
    assert with_customer["stripe_customer_id"] == {"S": "cus_1"}
    assert items.decode_account_list_row(plain) == "ba_01"
    assert items.decode_account_list_row(with_customer) == "ba_01"


def test_linha_da_lista_rejeita_prefixo_invalido() -> None:
    item = _with(items.encode_account_list_row(make_account()), sk={"S": "OTHER#6261"})
    with pytest.raises(PermanentBillingError) as error:
        items.decode_account_list_row(item)
    _assert_corrupt(error, items.ACCOUNT_LIST_ENTITY)


def test_linha_da_lista_rejeita_sufixo_nao_hexadecimal() -> None:
    item = _with(items.encode_account_list_row(make_account()), sk={"S": "ACCOUNT#zz"})
    with pytest.raises(PermanentBillingError) as error:
        items.decode_account_list_row(item)
    _assert_corrupt(error, items.ACCOUNT_LIST_ENTITY)


def test_linha_da_lista_rejeita_item_sem_sort_key() -> None:
    with pytest.raises(PermanentBillingError) as error:
        items.decode_account_list_row({})
    _assert_corrupt(error, items.ACCOUNT_LIST_ENTITY)


def test_linha_da_lista_rejeita_id_do_payload_divergente() -> None:
    item = items.encode_account_list_row(make_account("ba_01"))
    other = items.encode_account_list_row(make_account("ba_02"))
    with pytest.raises(PermanentBillingError) as error:
        items.decode_account_list_row(_with(item, payload=other["payload"]))
    _assert_corrupt(error, items.ACCOUNT_LIST_ENTITY)


def test_link_faz_roundtrip() -> None:
    link = make_link()
    assert items.decode_link(items.encode_link(link), "ba_01", "tenant-a") == link


def test_decode_link_rejeita_tenant_solicitado_divergente() -> None:
    item = items.encode_link(make_link(tenant_id="tenant-a"))
    with pytest.raises(PermanentBillingError) as error:
        items.decode_link(item, "ba_01", "tenant-b")
    _assert_corrupt(error, items.ACCOUNT_TENANT_ENTITY)


def test_tenant_account_faz_roundtrip() -> None:
    item = items.encode_tenant_account(make_link())
    assert items.decode_tenant_account(item, "tenant-a") == "ba_01"


def test_decode_tenant_account_rejeita_tenant_divergente() -> None:
    item = items.encode_tenant_account(make_link(tenant_id="tenant-a"))
    with pytest.raises(PermanentBillingError) as error:
        items.decode_tenant_account(item, "tenant-b")
    _assert_corrupt(error, items.TENANT_ACCOUNT_ENTITY)


def test_customer_map_faz_roundtrip() -> None:
    item = items.encode_customer_map("ba_01", "cus_1")
    assert items.decode_customer_map(item, "cus_1") == "ba_01"


def test_decode_customer_map_rejeita_customer_divergente() -> None:
    item = items.encode_customer_map("ba_01", "cus_1")
    with pytest.raises(PermanentBillingError) as error:
        items.decode_customer_map(item, "cus_2")
    _assert_corrupt(error, items.CUSTOMER_MAP_ENTITY)


def test_plan_faz_roundtrip_preservando_tupla_de_precos() -> None:
    plan = make_plan()
    decoded = items.decode_plan(items.encode_plan(plan), "plan_v1")
    assert decoded == plan
    assert decoded.stripe_price_ids == ("price_monthly", "price_yearly")


def test_decode_plan_rejeita_id_solicitado_divergente() -> None:
    with pytest.raises(PermanentBillingError) as error:
        items.decode_plan(items.encode_plan(make_plan("plan_v1")), "plan_v2")
    _assert_corrupt(error, items.PLAN_ENTITY)


def test_price_map_faz_roundtrip() -> None:
    item = items.encode_price_map("price_1", "plan_v1")
    assert items.decode_price_map(item, "price_1") == "plan_v1"


def test_decode_price_map_rejeita_price_divergente() -> None:
    item = items.encode_price_map("price_1", "plan_v1")
    with pytest.raises(PermanentBillingError) as error:
        items.decode_price_map(item, "price_2")
    _assert_corrupt(error, items.PRICE_MAP_ENTITY)


def _decoders() -> list[tuple[str, dict[str, Any], Any]]:
    return [
        (
            items.SNAPSHOT_ENTITY,
            items.encode_snapshot(make_snapshot()),
            lambda item: items.decode_snapshot(item, "ba_01"),
        ),
        (
            items.ACCOUNT_ENTITY,
            items.encode_account(make_account()),
            lambda item: items.decode_account(item, "ba_01"),
        ),
        (
            items.ACCOUNT_LIST_ENTITY,
            items.encode_account_list_row(make_account()),
            items.decode_account_list_row,
        ),
        (
            items.ACCOUNT_TENANT_ENTITY,
            items.encode_link(make_link()),
            lambda item: items.decode_link(item, "ba_01", "tenant-a"),
        ),
        (
            items.TENANT_ACCOUNT_ENTITY,
            items.encode_tenant_account(make_link()),
            lambda item: items.decode_tenant_account(item, "tenant-a"),
        ),
        (
            items.CUSTOMER_MAP_ENTITY,
            items.encode_customer_map("ba_01", "cus_1"),
            lambda item: items.decode_customer_map(item, "cus_1"),
        ),
        (
            items.PLAN_ENTITY,
            items.encode_plan(make_plan()),
            lambda item: items.decode_plan(item, "plan_v1"),
        ),
        (
            items.PRICE_MAP_ENTITY,
            items.encode_price_map("price_1", "plan_v1"),
            lambda item: items.decode_price_map(item, "price_1"),
        ),
    ]


def _corruptions() -> list[tuple[str, dict[str, Any] | None]]:
    return [
        ("entity_errada", {"entity": {"S": "OUTRA"}}),
        ("pk_errada", {"pk": {"S": "X#1"}}),
        ("sk_errada", {"sk": {"S": "X#1"}}),
        ("payload_json_invalido", {"payload": {"S": "{not-json"}}),
        ("payload_ausente", None),
    ]


@pytest.mark.parametrize(("name", "change"), _corruptions())
@pytest.mark.parametrize("decoder", _decoders(), ids=lambda v: str(v[0]))
def test_decoders_rejeitam_itens_corrompidos(
    decoder: tuple[str, dict[str, Any], Any], name: str, change: dict[str, Any] | None
) -> None:
    entity, valid, decode = decoder
    corrupted = (
        {key: value for key, value in valid.items() if key != "payload"}
        if change is None
        else _with(valid, **change)
    )
    with pytest.raises(PermanentBillingError) as error:
        decode(corrupted)
    _assert_corrupt(error, entity)


def test_canonical_json_independe_da_ordem_das_chaves() -> None:
    assert items.canonical_json({"b": 1, "a": 2}) == items.canonical_json({"a": 2, "b": 1})
    assert items.canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'


def test_canonical_json_serializa_tipos_de_billing() -> None:
    moment = datetime(2026, 9, 30, tzinfo=UTC)
    value = {
        "frozen": FrozenMapping({"z": 1, "a": frozenset({"y", "x"})}),
        "tuple": (moment, _Color.RED),
        "nested": _Outer(_Inner(moment), _Color.RED),
    }
    assert items.canonical_json(value) == (
        '{"frozen":{"a":["x","y"],"z":1},'
        '"nested":{"color":"red","inner":{"when":"2026-09-30T00:00:00+00:00"}},'
        '"tuple":["2026-09-30T00:00:00+00:00","red"]}'
    )


def test_request_hash_e_sha256_estavel() -> None:
    first = items.request_hash({"a": 1, "b": (1, 2)})
    assert re.fullmatch(r"[0-9a-f]{64}", first)
    assert first == items.request_hash({"b": (1, 2), "a": 1})
    assert first != items.request_hash({"a": 2, "b": (1, 2)})


def test_deterministic_id_e_estavel_e_sensivel_as_partes() -> None:
    first = items.deterministic_id("a", "b")
    assert re.fullmatch(r"[0-9a-f]{32}", first)
    assert first == items.deterministic_id("a", "b")
    assert first != items.deterministic_id("a", "c")
    assert items.deterministic_id("a", "bc") != items.deterministic_id("ab", "c")


def test_utc_attribute_normaliza_para_timestamp_de_largura_fixa() -> None:
    value = datetime(2026, 9, 30, 12, tzinfo=timezone(timedelta(0)))
    assert items.utc_attribute(value) == timestamp(value.astimezone(UTC))
    assert items.utc_attribute(value).endswith("000000+00:00")


def test_audit_outbox_event_mapeia_campos_do_audit() -> None:
    audit = make_audit()
    event = items.audit_outbox_event(audit)
    assert event.tenant_id == BILLING_AUDIT_TENANT_ID
    assert (event.event_id, event.event_type) == (audit.event_id, audit.event_type)
    assert event.aggregate_id == audit.aggregate_id
    assert event.payload == {
        "actor_id": "stripe",
        "reason_code": "webhook_projection",
        "attributes": {"entitlement_version": 1},
    }
    assert event.created_at == audit.occurred_at
    assert event.delivered_at is None


def _control_plane() -> DynamoDBControlPlane:
    return DynamoDBControlPlane(None, TABLE_NAME, lambda: NOW)


def test_outbox_item_equivale_ao_encoder_do_control_plane() -> None:
    event = items.audit_outbox_event(make_audit())
    assert items.outbox_item(event) == _control_plane()._outbox_item(event)


def test_idempotency_item_equivale_ao_encoder_do_control_plane() -> None:
    record = IdempotencyRecord(
        tenant_id="tenant-a",
        scope="create",
        key="k1",
        request_hash=HASH,
        status="completed",
        resource_id="res-1",
        created_at=NOW,
        expires_at=NOW + timedelta(days=1),
    )
    assert items.idempotency_item(record) == _control_plane()._idempotency_item(record)


def test_put_new_exige_ausencia_da_chave() -> None:
    item = items.encode_plan(make_plan())
    action = items.put_new(TABLE_NAME, item)
    assert action == {
        "Put": {
            "TableName": TABLE_NAME,
            "Item": item,
            "ConditionExpression": "attribute_not_exists(pk)",
        }
    }


@pytest.mark.parametrize("strong", [True, False])
def test_get_item_repassa_consistencia_solicitada(strong: bool) -> None:
    client = Mock()
    client.get_item.return_value = {"Item": {"pk": {"S": "p"}}}
    result = items.get_item(client, TABLE_NAME, plan_version_key("pv"), strong)
    assert result == {"pk": {"S": "p"}}
    assert client.get_item.call_args.kwargs["ConsistentRead"] is strong


def test_get_item_retorna_none_quando_ausente() -> None:
    client = Mock()
    client.get_item.return_value = {}
    assert items.get_item(client, TABLE_NAME, billing_account_key("x"), True) is None


def test_get_item_converte_falha_de_storage_em_dependencia_indisponivel() -> None:
    client = Mock()
    client.get_item.side_effect = _client_error("InternalServerError")
    with pytest.raises(BillingDependencyError) as error:
        items.get_item(client, TABLE_NAME, billing_account_key("x"), True)
    assert error.value.code == "dynamodb_unavailable"


def _plan_key(plan: Any) -> dict[str, Any]:
    pk, sk = plan_version_key(plan.plan_version_id)
    return {"pk": {"S": pk}, "sk": {"S": sk}}


@pytest.fixture
def client():
    with mock_aws():
        dynamo = boto3.client("dynamodb", region_name="us-east-1")
        create_table(dynamo)
        yield dynamo


def test_transact_retorna_true_em_sucesso(client: Any) -> None:
    action = items.put_new(TABLE_NAME, items.encode_plan(make_plan()))
    assert items.transact(client, (action,)) is True
    assert client.get_item(TableName=TABLE_NAME, Key=_plan_key(make_plan()))["Item"]


def test_transact_retorna_false_em_cancelamento_condicional(client: Any) -> None:
    action = items.put_new(TABLE_NAME, items.encode_plan(make_plan()))
    assert items.transact(client, (action,)) is True
    assert items.transact(client, (action,)) is False


def test_transact_repropaga_conflito_nao_condicional(client: Any) -> None:
    action = items.put_new(TABLE_NAME, items.encode_plan(make_plan()))
    with pytest.raises(Conflict) as error:
        items.transact(client, (action, action))
    assert error.value.code.name == "DUPLICATE_TRANSACTION_KEY"


def test_transact_converte_erro_nao_condicional_em_dependencia() -> None:
    client = Mock()
    client.transact_write_items.side_effect = _client_error("ProvisionedThroughputExceeded")
    action = items.put_new(TABLE_NAME, items.encode_plan(make_plan()))
    with pytest.raises(BillingDependencyError) as error:
        items.transact(client, (action,))
    assert error.value.code == "dynamodb_unavailable"
