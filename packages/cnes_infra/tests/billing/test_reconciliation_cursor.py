"""Testes do DynamoReconciliationCursor sobre moto e cliente low-level."""

from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from moto import mock_aws

from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
from cnes_infra.billing.dynamodb_items import utc_attribute
from cnes_infra.billing.keys import stripe_reconciliation_cursor_key
from cnes_infra.billing.reconciliation_cursor import (
    EMPTY_RECONCILIATION_CURSOR,
    RECONCILIATION_CURSOR_ENTITY,
    DynamoReconciliationCursor,
    ReconciliationCursor,
)
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME, create_table
from packages.cnes_infra.tests.contracts.clock import MutableClock

CURSOR_KEY = item_key(*stripe_reconciliation_cursor_key())


class _FailingClient:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def update_item(self, **_: Any) -> Any:
        raise self._error


@pytest.fixture
def context():
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        clock = MutableClock(NOW)
        yield client, clock, DynamoReconciliationCursor(client, TABLE_NAME, clock.now)


def _stored(client: Any) -> dict[str, Any]:
    return client.get_item(TableName=TABLE_NAME, Key=CURSOR_KEY, ConsistentRead=True)["Item"]


def _put_raw(client: Any, entity: str = RECONCILIATION_CURSOR_ENTITY, **attrs: Any) -> None:
    item = {**CURSOR_KEY, "entity": {"S": entity}, **attrs}
    client.put_item(TableName=TABLE_NAME, Item=item)


def test_chave_do_cursor_usa_particao_do_sistema() -> None:
    assert stripe_reconciliation_cursor_key() == ("BILLING#SYSTEM", "RECONCILIATION#STRIPE")


def test_cursor_ausente_carrega_vazio(context):
    assert context[2].load() == EMPTY_RECONCILIATION_CURSOR


def test_primeiro_save_cria_cursor_versao_um(context):
    client, _, cursors = context
    saved = cursors.save(EMPTY_RECONCILIATION_CURSOR, "ba_01")
    assert saved == ReconciliationCursor("ba_01", 1, NOW, None)
    assert cursors.load() == saved
    item = _stored(client)
    assert item["entity"] == {"S": RECONCILIATION_CURSOR_ENTITY}
    assert item["version"] == {"N": "1"}
    assert item["updated_at"] == {"S": utc_attribute(NOW)}
    assert "last_completed_at" not in item


def test_save_avanca_posicao_e_versao(context):
    _, clock, cursors = context
    first = cursors.save(EMPTY_RECONCILIATION_CURSOR, "ba_01")
    clock.advance(timedelta(minutes=3))
    second = cursors.save(first, "ba_02")
    assert second == ReconciliationCursor("ba_02", 2, clock.now(), None)
    assert cursors.load() == second


def test_save_com_versao_obsoleta_retorna_none(context):
    _, _, cursors = context
    first = cursors.save(EMPTY_RECONCILIATION_CURSOR, "ba_01")
    assert cursors.save(first, "ba_02") is not None
    assert cursors.save(first, "ba_03") is None
    assert cursors.load().position == "ba_02"


def test_primeiro_save_concorrente_perde_quando_ja_existe(context):
    _, _, cursors = context
    cursors.save(EMPTY_RECONCILIATION_CURSOR, "ba_01")
    assert cursors.save(EMPTY_RECONCILIATION_CURSOR, "ba_09") is None


def test_conclusao_do_ciclo_remove_posicao_e_registra_last_completed_at(context):
    client, clock, cursors = context
    first = cursors.save(EMPTY_RECONCILIATION_CURSOR, "ba_01")
    clock.advance(timedelta(minutes=5))
    done = cursors.save(first, None)
    assert done == ReconciliationCursor(None, 2, clock.now(), clock.now())
    assert cursors.load() == done
    assert "position" not in _stored(client)


def test_avanco_preserva_last_completed_at(context):
    _, clock, cursors = context
    done = cursors.save(cursors.save(EMPTY_RECONCILIATION_CURSOR, "ba_01"), None)
    clock.advance(timedelta(hours=1))
    next_cycle = cursors.save(done, "ba_01")
    assert next_cycle.last_completed_at == done.last_completed_at
    assert cursors.load() == next_cycle


@pytest.mark.parametrize(
    "attributes",
    [
        {"version": {"S": "abc"}},
        {"version": {"N": "-1"}},
        {},
        {"version": {"N": "1"}, "updated_at": {"S": "2026-09-29T12:00:00"}},
        {"version": {"N": "1"}, "updated_at": {"S": utc_attribute(NOW)}, "position": {"S": " "}},
    ],
    ids=["versao_invalida", "versao_negativa", "sem_campos", "updated_naive", "posicao_vazia"],
)
def test_cursor_corrompido_vira_permanent_error(context, attributes):
    client, _, cursors = context
    _put_raw(client, **attributes)
    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        cursors.load()


def test_cursor_de_outra_entidade_vira_permanent_error(context):
    client, _, cursors = context
    _put_raw(client, entity="OTHER", version={"N": "1"}, updated_at={"S": utc_attribute(NOW)})
    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        cursors.load()


@pytest.mark.parametrize(
    "error",
    [
        ClientError({"Error": {"Code": "ThrottlingException"}}, "UpdateItem"),
        EndpointConnectionError(endpoint_url="http://x.invalid"),
    ],
    ids=["client_error", "botocore_error"],
)
def test_falha_dynamodb_vira_dependency_error(error):
    cursors = DynamoReconciliationCursor(_FailingClient(error), TABLE_NAME, lambda: NOW)
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        cursors.save(EMPTY_RECONCILIATION_CURSOR, "ba_01")


@pytest.mark.parametrize("version", [-1, True, 1.5])
def test_rejeita_versao_invalida(version):
    with pytest.raises(ValueError, match="field=version"):
        ReconciliationCursor(None, version, None, None)


@pytest.mark.parametrize("field", ["updated_at", "last_completed_at"])
def test_rejeita_datetime_naive(field):
    naive = datetime.fromisoformat("2026-09-29T12:00:00")
    kwargs = {"position": None, "version": 1, "updated_at": None, "last_completed_at": None}
    with pytest.raises(ValueError, match=f"field={field}"):
        ReconciliationCursor(**{**kwargs, field: naive})


def test_rejeita_posicao_vazia():
    with pytest.raises(ValueError, match="field=position"):
        ReconciliationCursor(" ", 1, None, None)


def test_aceita_datetimes_utc():
    cursor = ReconciliationCursor("ba_01", 1, NOW.astimezone(UTC), None)
    assert cursor.position == "ba_01"
