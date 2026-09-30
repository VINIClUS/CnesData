"""Testes das guardas do codec de billing: limites de transação e instantes."""

from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.billing.errors import PermanentBillingError
from cnes_infra.billing import dynamodb_items as items
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    make_snapshot,
)


@pytest.fixture
def client() -> Any:
    with mock_aws():
        value = boto3.client("dynamodb", region_name="us-east-1")
        create_table(value)
        yield value


def _put(pk: str) -> dict[str, Any]:
    return items.put_new(TABLE_NAME, {"pk": {"S": pk}, "sk": {"S": "X"}})


def test_transacao_acima_de_cem_acoes_e_erro_permanente_de_billing(client: Any) -> None:
    actions = tuple(_put(f"P#{index}") for index in range(101))

    with pytest.raises(PermanentBillingError, match="billing_transaction_too_large"):
        items.transact(client, actions)


def test_transacao_com_chave_duplicada_e_erro_permanente_de_billing(client: Any) -> None:
    with pytest.raises(PermanentBillingError, match="billing_duplicate_action"):
        items.transact(client, (_put("P#1"), _put("P#1")))


def test_utc_attribute_rejeita_datetime_ingenuo() -> None:
    with pytest.raises(ValueError, match="datetime_not_utc"):
        items.utc_attribute(NOW.replace(tzinfo=None))


def test_utc_attribute_rejeita_fuso_nao_utc() -> None:
    with pytest.raises(ValueError, match="datetime_not_utc"):
        items.utc_attribute(datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=-3))))


def test_snapshot_expoe_status_e_validade_para_condicoes() -> None:
    item = items.encode_snapshot(make_snapshot(valid_until=NOW + timedelta(days=1)))

    assert item["subscription_status"] == {"S": "active"}
    assert item["valid_until"] == {"S": items.utc_attribute(NOW + timedelta(days=1))}
