"""Testes do DynamoRecoveryCursor (BIL-021) sobre moto e cliente low-level."""

from dataclasses import replace
from datetime import timedelta
from typing import Any
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from moto import mock_aws

from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
from cnes_domain.billing.inbox import StripeRecoveryCursor
from cnes_domain.billing.models import ReadConsistency
from cnes_domain.billing.ports import RecoveryCursorPort
from cnes_infra.billing.dynamodb_items import utc_attribute
from cnes_infra.billing.keys import stripe_recovery_cursor_key
from cnes_infra.billing.recovery_cursor import RECOVERY_CURSOR_ENTITY, DynamoRecoveryCursor
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    table_items,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

CURSOR_KEY = item_key(*stripe_recovery_cursor_key())
STRONG = ReadConsistency.STRONG
CREATED_GTE = NOW - timedelta(hours=24)


def _cursor(cycle_id: str = "cycle-01", after: str | None = None, version: int = 1):
    return StripeRecoveryCursor(cycle_id, CREATED_GTE, after, version)


class _FailingClient:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def update_item(self, **_: Any) -> Any:
        raise ClientError({"Error": {"Code": "ThrottlingException"}}, "UpdateItem")

    def get_item(self, **_: Any) -> Any:
        raise ClientError({"Error": {"Code": "ThrottlingException"}}, "GetItem")


@pytest.fixture
def context():
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        clock = MutableClock(NOW)
        yield client, clock, DynamoRecoveryCursor(client, TABLE_NAME, clock.now)


def _stored(client: Any) -> dict[str, Any]:
    return client.get_item(TableName=TABLE_NAME, Key=CURSOR_KEY, ConsistentRead=True)["Item"]


def _put_raw(client: Any, **attributes: dict[str, str]) -> None:
    item = {**CURSOR_KEY, "entity": {"S": RECOVERY_CURSOR_ENTITY}, **attributes}
    client.put_item(TableName=TABLE_NAME, Item=item)


def _valid_attributes() -> dict[str, dict[str, str]]:
    return {
        "active_cycle_id": {"S": "cycle-01"},
        "created_gte": {"S": utc_attribute(CREATED_GTE)},
        "version": {"N": "1"},
    }


def test_satisfaz_a_porta_de_cursor(context):
    assert isinstance(context[2], RecoveryCursorPort)


def test_cas_atrasado_de_ciclo_antigo_nao_avanca_novo(context):
    _, _, cursors = context
    old = _cursor("cycle-old", None, 1)
    assert cursors.start(old) is True
    assert cursors.complete(old, NOW) is True
    current = _cursor("cycle-new", None, 1)
    assert cursors.start(current) is True
    stale = replace(old, starting_after="evt_old", version=2)
    assert cursors.advance(old, stale) is False
    assert cursors.complete(old, NOW + timedelta(seconds=1)) is False
    assert cursors.load(STRONG) == current


def test_load_ausente_retorna_none(context):
    assert context[2].load(STRONG) is None


def test_start_grava_cursor_ativo_e_load_devolve_igual(context):
    client, _, cursors = context
    cursor = _cursor(after="evt_106")
    assert cursors.start(cursor) is True
    assert cursors.load(STRONG) == cursor
    item = _stored(client)
    assert item["entity"] == {"S": RECOVERY_CURSOR_ENTITY}
    assert item["version"] == {"N": "1"}


def test_start_sem_starting_after_nao_grava_o_atributo(context):
    client, _, cursors = context
    assert cursors.start(_cursor()) is True
    assert "starting_after" not in _stored(client)


def test_start_com_ciclo_ativo_retorna_falso_sem_alterar_item(context):
    client, _, cursors = context
    cursors.start(_cursor())
    before = _stored(client)
    assert cursors.start(_cursor("cycle-other")) is False
    assert _stored(client) == before


def test_complete_remove_apenas_campos_ativos_e_grava_metadados(context):
    client, clock, cursors = context
    cursor = _cursor(after="evt_106", version=3)
    cursors.start(cursor)
    clock.advance(timedelta(minutes=5))
    done_at = NOW + timedelta(minutes=4)
    assert cursors.complete(cursor, done_at) is True
    assert cursors.load(STRONG) is None
    item = _stored(client)
    assert set(item) == {
        "pk", "sk", "entity", "last_completed_cycle_id", "last_completed_version",
        "last_success_at", "updated_at",
    }
    assert item["last_completed_cycle_id"] == {"S": "cycle-01"}
    assert item["last_completed_version"] == {"N": "3"}
    assert item["last_success_at"] == {"S": utc_attribute(done_at)}


def test_start_apos_conclusao_preserva_metadados_de_conclusao(context):
    client, _, cursors = context
    first = _cursor("cycle-old")
    cursors.start(first)
    cursors.complete(first, NOW)
    completed = _stored(client)
    assert cursors.start(_cursor("cycle-new")) is True
    item = _stored(client)
    for name in ("last_completed_cycle_id", "last_completed_version", "last_success_at"):
        assert item[name] == completed[name]
    assert item["active_cycle_id"] == {"S": "cycle-new"}


def test_advance_incrementa_versao_e_trata_starting_after_none(context):
    client, _, cursors = context
    first = _cursor()
    cursors.start(first)
    second = first.advance("evt_106")
    assert cursors.advance(first, second) is True
    assert cursors.load(STRONG) == second
    third = second.advance("evt_006")
    assert cursors.advance(second, third) is True
    assert cursors.load(STRONG) == third
    fourth = third.advance(None)
    assert cursors.advance(third, fourth) is True
    assert cursors.load(STRONG) == fourth
    assert "starting_after" not in _stored(client)


@pytest.mark.parametrize(
    "expected",
    [
        _cursor(after="evt_106", version=2),
        _cursor(after="evt_other"),
        replace(_cursor(after="evt_106"), created_gte=CREATED_GTE - timedelta(hours=1)),
        _cursor("cycle-other", "evt_106"),
    ],
    ids=["versao", "starting_after", "created_gte", "ciclo"],
)
def test_advance_com_esperado_divergente_retorna_falso_sem_alterar_item(context, expected):
    client, _, cursors = context
    cursors.start(_cursor(after="evt_106"))
    before = _stored(client)
    assert cursors.advance(expected, expected.advance("evt_next")) is False
    assert _stored(client) == before


def test_advance_esperando_starting_after_ausente_falha_quando_existe(context):
    client, _, cursors = context
    cursors.start(_cursor(after="evt_106"))
    before = _stored(client)
    expected = _cursor()
    assert cursors.advance(expected, expected.advance("evt_next")) is False
    assert _stored(client) == before


def test_advance_rejeita_substituto_nao_sucessor_sem_io():
    client = Mock()
    cursors = DynamoRecoveryCursor(client, TABLE_NAME, lambda: NOW)
    expected = _cursor()
    with pytest.raises(ValueError, match="cursor_not_successor"):
        cursors.advance(expected, replace(expected, version=3))
    assert client.mock_calls == []


def test_advance_grava_updated_at_do_relogio_injetado(context):
    client, clock, cursors = context
    first = _cursor()
    cursors.start(first)
    assert _stored(client)["updated_at"] == {"S": utc_attribute(NOW)}
    clock.advance(timedelta(minutes=7))
    cursors.advance(first, first.advance("evt_1"))
    assert _stored(client)["updated_at"] == {"S": utc_attribute(clock.now())}


@pytest.mark.parametrize(
    ("consistency", "expected"),
    [(ReadConsistency.STRONG, True), (ReadConsistency.EVENTUAL, False)],
)
def test_load_envia_consistent_read_conforme_consistencia(consistency, expected):
    client = Mock()
    client.get_item.return_value = {}
    DynamoRecoveryCursor(client, TABLE_NAME, lambda: NOW).load(consistency)
    client.get_item.assert_called_once_with(
        TableName=TABLE_NAME, Key=CURSOR_KEY, ConsistentRead=expected
    )


def test_load_ignora_item_apenas_com_metadados_de_conclusao(context):
    client, _, cursors = context
    _put_raw(client, last_completed_cycle_id={"S": "cycle-01"})
    assert cursors.load(STRONG) is None


@pytest.mark.parametrize(
    "mutation",
    [
        {"version": {"S": "abc"}},
        {"version": {"N": "0"}},
        {"created_gte": None},
        {"created_gte": {"S": "2026-09-29T12:00:00"}},
        {"version": None},
        {"active_cycle_id": {"S": " "}},
    ],
    ids=["versao_invalida", "versao_zero", "sem_created", "created_naive", "sem_versao", "ciclo"],
)
def test_load_rejeita_item_corrompido(context, mutation):
    client, _, cursors = context
    attributes = {**_valid_attributes(), **mutation}
    _put_raw(client, **{k: v for k, v in attributes.items() if v is not None})
    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        cursors.load(STRONG)


def test_load_rejeita_item_de_outra_entidade(context):
    client, _, cursors = context
    client.put_item(
        TableName=TABLE_NAME,
        Item={**CURSOR_KEY, "entity": {"S": "OTHER"}, **_valid_attributes()},
    )
    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        cursors.load(STRONG)


def test_falha_de_storage_vira_dependencia_indisponivel_em_toda_operacao(context):
    client, _, _ = context
    cursors = DynamoRecoveryCursor(_FailingClient(client), TABLE_NAME, lambda: NOW)
    cursor = _cursor()
    operations = (
        lambda: cursors.load(STRONG),
        lambda: cursors.start(cursor),
        lambda: cursors.advance(cursor, cursor.advance("evt_1")),
        lambda: cursors.complete(cursor, NOW),
    )
    for operation in operations:
        with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
            operation()
    assert table_items(client) == []


def test_falha_de_conexao_vira_dependencia_indisponivel_no_cursor() -> None:
    client = Mock()
    client.get_item.side_effect = EndpointConnectionError(endpoint_url="http://x.invalid")
    client.update_item.side_effect = EndpointConnectionError(endpoint_url="http://x.invalid")
    cursors = DynamoRecoveryCursor(client, TABLE_NAME, lambda: NOW)
    cursor = _cursor()
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        cursors.load(STRONG)
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        cursors.start(cursor)
