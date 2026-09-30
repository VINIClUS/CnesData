"""Testes do catálogo DynamoDB de planos e preços."""

from dataclasses import replace
from typing import Any
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.billing.errors import (
    BillingDependencyError,
    ImmutablePlanConflict,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_items import encode_price_map
from cnes_infra.billing.keys import plan_version_key, stripe_price_key
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    make_plan,
    table_items,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import ClientSpy


def _raise_conflict(_: list[dict[str, Any]]) -> None:
    raise ClientError(
        {
            "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
            "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
        },
        "TransactWriteItems",
    )


@pytest.fixture
def env() -> Any:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        yield client, DynamoBillingCatalog(client, TABLE_NAME, MutableClock(NOW).now)


def test_plan_version_publicada_nao_pode_ser_sobrescrita(env: Any) -> None:
    _, catalog = env
    plan = make_plan("plan_v1")

    assert catalog.publish_plan(plan) == plan
    with pytest.raises(ImmutablePlanConflict, match="plan_version_id=plan_v1"):
        catalog.publish_plan(make_plan("plan_v1", max_agents=99))
    assert catalog.publish_plan(plan) == plan
    assert catalog.get_plan("plan_v1") == plan


def test_publica_plano_grava_plano_e_um_mapa_por_preco(env: Any) -> None:
    client, catalog = env
    catalog.publish_plan(make_plan("plan_v1"))

    keys = {(item["pk"]["S"], item["sk"]["S"]) for item in table_items(client)}

    assert keys == {
        plan_version_key("plan_v1"),
        stripe_price_key("price_monthly"),
        stripe_price_key("price_yearly"),
    }


def test_preco_ja_mapeado_para_outro_plano_e_rejeitado(env: Any) -> None:
    client, catalog = env
    catalog.publish_plan(make_plan("plan_v1"))
    before = table_items(client)

    with pytest.raises(ImmutablePlanConflict, match="stripe_price_id=price_monthly"):
        catalog.publish_plan(make_plan("plan_v2", stripe_price_ids=("price_monthly",)))

    assert table_items(client) == before
    assert catalog.get_plan("plan_v2") is None


def test_republicacao_com_mapa_de_preco_ausente_e_retryable(env: Any) -> None:
    client, catalog = env
    catalog.publish_plan(make_plan("plan_v1"))
    client.delete_item(TableName=TABLE_NAME, Key=item_key(*stripe_price_key("price_yearly")))

    with pytest.raises(RetryableBillingError, match="billing_transaction_conflict"):
        catalog.publish_plan(make_plan("plan_v1"))


def test_cancelamento_inesperado_de_publicacao_e_retryable(env: Any) -> None:
    client, _ = env
    catalog = DynamoBillingCatalog(
        ClientSpy(client, before_transaction=_raise_conflict), TABLE_NAME, MutableClock(NOW).now
    )

    with pytest.raises(RetryableBillingError, match="billing_transaction_conflict"):
        catalog.publish_plan(make_plan("plan_v1"))


def test_get_plan_inexistente_retorna_none(env: Any) -> None:
    _, catalog = env

    assert catalog.get_plan("plan_x") is None


def test_get_plan_by_price_retorna_plano_mapeado(env: Any) -> None:
    _, catalog = env
    plan = make_plan("plan_v1")
    catalog.publish_plan(plan)

    assert catalog.get_plan_by_price("price_yearly") == plan


def test_get_plan_by_price_inexistente_retorna_none(env: Any) -> None:
    _, catalog = env

    assert catalog.get_plan_by_price("price_x") is None


def test_get_plan_by_price_rejeita_mapa_sem_plano(env: Any) -> None:
    client, catalog = env
    client.put_item(TableName=TABLE_NAME, Item=encode_price_map("price_x", "plan_ghost"))

    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        catalog.get_plan_by_price("price_x")


def test_get_plan_by_price_rejeita_plano_sem_o_preco(env: Any) -> None:
    client, catalog = env
    catalog.publish_plan(replace(make_plan("plan_v1"), stripe_price_ids=("price_monthly",)))
    client.put_item(TableName=TABLE_NAME, Item=encode_price_map("price_other", "plan_v1"))

    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        catalog.get_plan_by_price("price_other")


def test_leituras_de_plano_usam_chave_base_e_leitura_forte(env: Any) -> None:
    client, _ = env
    spy = ClientSpy(client)
    catalog = DynamoBillingCatalog(spy, TABLE_NAME, MutableClock(NOW).now)

    catalog.get_plan("plan_v1")

    assert spy.requests == [
        (
            "get_item",
            {
                "TableName": TABLE_NAME,
                "Key": item_key(*plan_version_key("plan_v1")),
                "ConsistentRead": True,
            },
        )
    ]


def test_erro_de_storage_na_leitura_de_plano_vira_dependencia() -> None:
    client = Mock()
    client.get_item.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "boom"}}, "GetItem"
    )
    catalog = DynamoBillingCatalog(client, TABLE_NAME, MutableClock(NOW).now)

    with pytest.raises(BillingDependencyError):
        catalog.get_plan("plan_v1")
