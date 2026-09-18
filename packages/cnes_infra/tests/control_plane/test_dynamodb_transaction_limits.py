from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.commands import FinalizeRunCancellation, ReserveRunDispatch
from cnes_domain.control_plane.enums import RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import execute_transaction
from cnes_infra.control_plane.dynamodb_keys import (
    dependency_marker_key,
    item_key,
    key_component,
    outbox_key,
    run_entity_key,
)
from packages.cnes_infra.tests.contracts.clock import _NOW, _TENANT, MutableClock, _event, _run
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    ClientSpy,
    _create_table,
    _dependency_run,
    _DynamoContext,
    _expected_item,
    _expected_put,
    _expected_unit_action,
    _hide_unit_from_gsi,
    _lose_transaction_response,
    _many_units,
    _put_many_units,
    _raise_transaction_canceled,
    _store_waiting,
    _transaction_action,
)


@pytest.fixture
def ctx() -> Iterator[_DynamoContext]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        clock = MutableClock(_NOW)
        yield DynamoDBControlPlane(client, _TABLE_NAME, clock.now), clock


def test_grava_99_unidades_em_ate_100_acoes_unicas(ctx: _DynamoContext) -> None:
    adapter, _ = ctx
    adapter.put_run(_run("run-a"))
    adapter._client = spy = ClientSpy(adapter._client)
    _put_many_units(adapter, 99)
    assert _put_many_units(adapter, 99) == _many_units(99)
    actions = spy.transactions[-1]
    run = _run("run-a")
    expected_check = {
        "ConditionCheck": {
            "TableName": _TABLE_NAME,
            "Key": item_key(*run_entity_key(_TENANT, "run-a")),
            "ConditionExpression": "payload = :expected",
            "ExpressionAttributeValues": {":expected": {"S": run.model_dump_json()}},
        }
    }
    assert actions == [
        expected_check,
        *(_expected_unit_action(index, RunUnitState.PENDING) for index in range(99)),
    ]
    with pytest.raises(Conflict, match="run_units_conflict"):
        _put_many_units(adapter, 98)
    assert adapter.list_run_units(_TENANT, "run-a") == _many_units(99)


def test_rejeita_100_unidades_antes_de_chamar_boto3(ctx: _DynamoContext) -> None:
    adapter, _ = ctx
    adapter._client = spy = ClientSpy(adapter._client)
    with pytest.raises(Conflict, match="transaction_limit"):
        _put_many_units(adapter, 100)
    assert spy.calls == []


def test_rejeita_uniao_de_99_unidades_antes_de_transacao(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    adapter.put_run(_run("run-a"))
    units = _put_many_units(adapter, 99)
    first = ReserveRunDispatch(
        tenant_id=_TENANT,
        run_id="run-a",
        wave_id="a" * 16,
        unit_ids=tuple(unit.unit_id for unit in units[:98]),
        now=clock.now(),
        lease_seconds=30,
    )
    adapter._client = original = ClientSpy(adapter._client)
    dispatch = adapter.reserve_run_dispatch(first)
    assert len(original.transactions) == 1
    actions = original.transactions[0]
    assert len(actions) == 100
    assert len([action for action in actions if "ConditionCheck" in action]) == 99
    assert list(actions[-1]) == ["Put"]
    assert adapter.get_active_run_dispatch(_TENANT, "run-a") == dispatch
    clock.advance(timedelta(seconds=31))
    adapter._client = spy = ClientSpy(adapter._client)
    replacement = first.model_copy(
        update={"wave_id": "b" * 16, "unit_ids": ("unit-098",), "now": clock.now()}
    )
    with pytest.raises(Conflict, match="transaction_limit"):
        adapter.reserve_run_dispatch(replacement)
    assert spy.transactions == []


def _prepare_cancellation(adapter: DynamoDBControlPlane, amount: int) -> None:
    adapter.put_run(_run("run-a"))
    _put_many_units(adapter, amount)
    adapter.put_run(_run("run-a", RunState.CANCEL_REQUESTED))


def _finalize(adapter: DynamoDBControlPlane, clock: MutableClock) -> Any:
    command = FinalizeRunCancellation(
        tenant_id=_TENANT,
        run_id="run-a",
        expected_state=RunState.CANCEL_REQUESTED,
        canceled_at=clock.now(),
    )
    return adapter.finalize_run_cancellation(command, _event("run-canceled"))


def test_cancela_98_unidades_em_100_acoes_unicas(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    _prepare_cancellation(adapter, 98)
    _hide_unit_from_gsi(adapter, "unit-097")
    adapter._client = spy = ClientSpy(adapter._client)
    assert _finalize(adapter, clock).state is RunState.CANCELED
    actions = spy.transactions[-1]
    canceled = _run("run-a", RunState.CANCELED)
    run_item = _expected_item(canceled, "RUN", run_entity_key(_TENANT, "run-a"), {})
    original_payload = _run("run-a", RunState.CANCEL_REQUESTED).model_dump_json()
    event = _event("run-canceled")
    indexes = {
        "gsi6pk": "OUTBOX#PENDING",
        "gsi6sk": f"{_NOW.isoformat(timespec='microseconds')}#{key_component('run-canceled')}",
    }
    event_item = _expected_item(event, "OUTBOXEVENT", outbox_key("run-canceled"), indexes)
    assert actions == [
        _expected_put(run_item, original_payload),
        *(_expected_unit_action(index, RunUnitState.CANCELED) for index in range(98)),
        _expected_put(event_item, None),
    ]


def test_rejeita_cancelamento_de_99_unidades_sem_transacao(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    _prepare_cancellation(adapter, 99)
    _hide_unit_from_gsi(adapter, "unit-098")
    adapter._client = spy = ClientSpy(adapter._client)
    with pytest.raises(Conflict, match="transaction_limit"):
        _finalize(adapter, clock)
    assert spy.transactions == []
    assert adapter.get_run(_TENANT, "run-a").state is RunState.CANCEL_REQUESTED
    spy.calls.clear()
    spy.query_limit = 1
    assert adapter.list_run_units(_TENANT, "run-a") == _many_units(99)
    assert spy.calls.count("query") == 99
    assert adapter.get_outbox_event("run-canceled") is None


@pytest.mark.parametrize(
    ("state", "count"), [(RunState.WAITING_INPUTS, 99), (RunState.PLANNED, 98)]
)
def test_waiting_limita_acoes_e_chaves(state: RunState, count: int, ctx: _DynamoContext) -> None:
    adapter, _ = ctx
    run = _dependency_run(count, state, "run-limit")
    if state is RunState.PLANNED:
        adapter.put_run(run)
    adapter._client = spy = ClientSpy(adapter._client)
    overflow = _dependency_run(count + 1, state, "run-overflow")
    if state is RunState.PLANNED:
        adapter.put_run(overflow)
    mutations = (len(spy.transactions), spy.calls.count("put_item"))
    with pytest.raises(Conflict, match="transaction_limit"):
        _store_waiting(adapter, overflow, "waiting-overflow")
    assert (len(spy.transactions), spy.calls.count("put_item")) == mutations
    _store_waiting(adapter, run, "waiting-limit")
    actions = spy.transactions[-1]
    keys = {
        (action["Put"]["Item"]["pk"]["S"], action["Put"]["Item"]["sk"]["S"]) for action in actions
    }
    assert len(actions) == len(keys) == 100
    action = _transaction_action("same")
    with pytest.raises(Conflict, match="duplicate_transaction_key"):
        execute_transaction(spy, (action, action))


@pytest.mark.parametrize("state", [RunState.WAITING_INPUTS, RunState.PLANNED])
def test_waiting_reverte_run_quando_marcador_falha(state: RunState, ctx: _DynamoContext) -> None:
    adapter, _ = ctx
    run = _dependency_run(1, state, "run-a")
    if state is RunState.PLANNED:
        adapter.put_run(run)
    identity = "RUN_DEP#" + "#".join(map(key_component, (_TENANT, "SOURCE-0", "ST", "2026-07")))
    marker_key = dependency_marker_key(_TENANT, "run-a", identity)
    adapter._client.put_item(TableName=_TABLE_NAME, Item=item_key(*marker_key))
    error = "transaction_conflict" if state is RunState.PLANNED else "run_dependency_conflict"
    with pytest.raises(Conflict, match=error):
        _store_waiting(adapter, run, "waiting-rollback")
    expected = run if state is RunState.PLANNED else None
    assert adapter.get_run(_TENANT, "run-a") == expected
    assert adapter.get_outbox_event("waiting-rollback") is None


def test_put_waiting_recupera_resposta_perdida_e_rejeita_divergencias(
    ctx: _DynamoContext,
) -> None:
    adapter, _ = ctx
    run = _dependency_run(2, RunState.WAITING_INPUTS, "run-replay")
    adapter._client = ClientSpy(adapter._client, after_transaction=_lose_transaction_response)
    adapter.put_run(run)
    adapter._client = spy = ClientSpy(adapter._client.client)
    adapter.put_run(run)
    assert any(request.get("ConsistentRead") is True for request in spy.query_requests)
    before = spy.scan(TableName=_TABLE_NAME)["Items"]
    divergent = run.model_copy(update={"created_at": _NOW + timedelta(seconds=1)})
    with pytest.raises(Conflict, match="run_conflict"):
        adapter.put_run(divergent)
    assert spy.scan(TableName=_TABLE_NAME)["Items"] == before
    marker = next(item for item in before if item.get("entity", {}).get("S") == "RUN_DEP")
    marker["gsi3sk"] = {"S": "divergent"}
    spy.put_item(TableName=_TABLE_NAME, Item=marker)
    changed = spy.scan(TableName=_TABLE_NAME)["Items"]
    with pytest.raises(Conflict, match="run_dependency_conflict"):
        adapter.put_run(run)
    assert spy.scan(TableName=_TABLE_NAME)["Items"] == changed
    fresh = _dependency_run(1, RunState.WAITING_INPUTS, "run-loser")
    adapter._client = ClientSpy(spy.client, before_transaction=_raise_transaction_canceled)
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.put_run(fresh)
    assert adapter.get_run(_TENANT, "run-loser") is None
