from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    ClaimRunUnit,
    FinalizeRunCancellation,
    FinishRunDispatch,
    PutRunUnits,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.enums import DispatchOutcome, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost, NotFound
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_claims import DynamoDBClaims
from cnes_infra.control_plane.dynamodb_codec import execute_transaction, payload, put_action
from cnes_infra.control_plane.dynamodb_keys import (
    item_key,
    key_component,
    outbox_key,
    run_entity_key,
    unit_key,
)
from packages.cnes_infra.tests.contracts.clock import (
    _NOW,
    _TENANT,
    MutableClock,
    _agent,
    _claim_job,
    _claim_unit,
    _commit_command,
    _event,
    _job,
    _prepare_unit,
    _run,
    _unit,
)
from packages.cnes_infra.tests.contracts.control_plane_contract import (
    ControlPlaneCase,
    control_plane_cases,
)

_TABLE_NAME = "cnesdata-control-plane"
_INDEXES = tuple(f"gsi{number}" for number in range(1, 7))
type _DynamoContext = tuple[DynamoDBControlPlane, MutableClock]
@pytest.fixture
def ctx(dynamodb_adapter: _DynamoContext) -> _DynamoContext:
    return dynamodb_adapter
class ClientSpy:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.transactions: list[list[dict[str, Any]]] = []
        self.calls: list[str] = []
        self.query_limits: list[int | None] = []
    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("transact_write_items")
        self.transactions.append(kwargs["TransactItems"])
        return self.client.transact_write_items(**kwargs)
    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("query")
        self.query_limits.append(kwargs.get("Limit"))
        kwargs.setdefault("Limit", getattr(self, "query_limit", 100))
        return self.client.query(**kwargs)
    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self.client, name)
        if not callable(attribute):
            return attribute
        def tracked(**kwargs: Any) -> Any:
            self.calls.append(name)
            return attribute(**kwargs)
        return tracked
class FailingTransactionClient(ClientSpy):
    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("transact_write_items")
        self.transactions.append(kwargs["TransactItems"])
        raise ClientError(
            {"Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
             "CancellationReasons": [{"Code": "ConditionalCheckFailed"}]},
            "TransactWriteItems",
        )
def _create_table(client: object) -> None:
    index_attributes = tuple(f"{index}{suffix}" for index in _INDEXES for suffix in ("pk", "sk"))
    attribute_definitions = [
        {"AttributeName": name, "AttributeType": "S"} for name in ("pk", "sk", *index_attributes)
    ]
    global_secondary_indexes = [
        {
            "IndexName": index,
            "KeySchema": [
                {"AttributeName": f"{index}pk", "KeyType": "HASH"},
                {"AttributeName": f"{index}sk", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
            "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        }
        for index in _INDEXES
    ]
    client.create_table(
        TableName=_TABLE_NAME,
        KeySchema=[
            {"AttributeName": "pk", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=attribute_definitions,
        GlobalSecondaryIndexes=global_secondary_indexes,
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    client.update_time_to_live(
        TableName=_TABLE_NAME,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"},
    )
@pytest.fixture
def dynamodb_adapter() -> Iterator[tuple[DynamoDBControlPlane, MutableClock]]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        clock = MutableClock(_NOW)
        yield DynamoDBControlPlane(client, _TABLE_NAME, clock.now), clock
@pytest.mark.parametrize("case", control_plane_cases(), ids=lambda case: case.name)
def test_cumpre_contrato_compartilhado(
    case: ControlPlaneCase,
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
    case.run(adapter, clock)
def _many_units(amount: int) -> tuple[Any, ...]:
    return tuple(_unit(f"unit-{index:03d}") for index in range(amount))
def _put_many_units(adapter: DynamoDBControlPlane, amount: int) -> tuple[Any, ...]:
    units = tuple(reversed(_many_units(amount)))
    return adapter.put_run_units(
        PutRunUnits(
            tenant_id=_TENANT,
            run_id="run-a",
            expected_run_state=RunState.PROCESSING,
            units=units,
        )
    )
def _expected_item(
    model: Any, entity: str, key: tuple[str, str], indexes: dict[str, str]
) -> dict[str, Any]:
    item = {
        "pk": {"S": key[0]},
        "sk": {"S": key[1]},
        "entity": {"S": entity},
        "payload": {"S": model.model_dump_json()},
    }
    return item | {name: {"S": value} for name, value in indexes.items()}
def _expected_put(item: dict[str, Any], expected: str | None) -> dict[str, Any]:
    request = {"TableName": _TABLE_NAME, "Item": item}
    request["ConditionExpression"] = (
        "attribute_not_exists(pk)" if expected is None else "payload = :expected"
    )
    if expected is not None:
        request["ExpressionAttributeValues"] = {":expected": {"S": expected}}
    return {"Put": request}
def _transaction_action(identifier: str) -> dict[str, Any]:
    item = {
        "pk": {"S": "TENANT#354130"},
        "sk": {"S": f"TEST#{identifier}"},
        "payload": {"S": "{}"},
    }
    return put_action(_TABLE_NAME, item, None)
def _expected_unit_action(index: int, state: RunUnitState) -> dict[str, Any]:
    unit = _unit(f"unit-{index:03d}").model_copy(update={"state": state})
    item = _expected_item(
        unit,
        "RUNUNIT",
        unit_key(_TENANT, "run-a", f"unit-{index:03d}"),
        {
            "gsi5pk": f"RUN_ITEMS#{key_component(_TENANT)}#{key_component('run-a')}",
            "gsi5sk": f"UNIT#{key_component(f'unit-{index:03d}')}",
        },
    )
    expected = None if state is RunUnitState.PENDING else _unit(f"unit-{index:03d}")
    return _expected_put(item, expected.model_dump_json() if expected is not None else None)
def _hide_unit_from_gsi(adapter: DynamoDBControlPlane, unit_id: str) -> None:
    item = adapter._client.get_item(
        TableName=_TABLE_NAME,
        Key=item_key(*unit_key(_TENANT, "run-a", unit_id)),
        ConsistentRead=True,
    )["Item"]
    item.pop("gsi5pk")
    item.pop("gsi5sk")
    adapter._client.put_item(TableName=_TABLE_NAME, Item=item)
def test_grava_99_unidades_em_ate_100_acoes_unicas(ctx: _DynamoContext) -> None:
    adapter, _ = ctx
    adapter.put_run(_run("run-a"))
    spy = ClientSpy(adapter._client)
    adapter._client = spy
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
    spy = ClientSpy(adapter._client)
    adapter._client = spy
    with pytest.raises(Conflict, match="transaction_limit"):
        _put_many_units(adapter, 100)
    assert spy.calls == []
def test_rejeita_uniao_de_99_unidades_antes_de_transacao(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    adapter.put_run(_run("run-a"))
    units = _put_many_units(adapter, 99)
    first = ReserveRunDispatch(
        tenant_id=_TENANT, run_id="run-a", wave_id="a" * 16,
        unit_ids=tuple(unit.unit_id for unit in units[:98]), now=clock.now(), lease_seconds=30,
    )
    adapter.reserve_run_dispatch(first)
    clock.advance(timedelta(seconds=31))
    spy = ClientSpy(adapter._client)
    adapter._client = spy
    replacement = first.model_copy(update={
        "wave_id": "b" * 16, "unit_ids": ("unit-098",), "now": clock.now(),
    })
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
    spy = ClientSpy(adapter._client)
    adapter._client = spy
    assert _finalize(adapter, clock).state is RunState.CANCELED
    actions = spy.transactions[-1]
    canceled = _run("run-a", RunState.CANCELED)
    run_item = _expected_item(canceled, "RUN", run_entity_key(_TENANT, "run-a"), {})
    original_payload = _run("run-a", RunState.CANCEL_REQUESTED).model_dump_json()
    event = _event("run-canceled")
    event_item = _expected_item(
        event,
        "OUTBOXEVENT",
        outbox_key("run-canceled"),
        {
            "gsi6pk": "OUTBOX#PENDING",
            "gsi6sk": f"{_NOW.isoformat(timespec='microseconds')}#"
                       f"{key_component('run-canceled')}",
        },
    )
    assert actions == [
        _expected_put(run_item, original_payload),
        *(_expected_unit_action(index, RunUnitState.CANCELED) for index in range(98)),
        _expected_put(event_item, None),
    ]
def test_rejeita_cancelamento_de_99_unidades_sem_transacao(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    _prepare_cancellation(adapter, 99)
    _hide_unit_from_gsi(adapter, "unit-098")
    spy = ClientSpy(adapter._client)
    adapter._client = spy
    with pytest.raises(Conflict, match="transaction_limit"):
        _finalize(adapter, clock)
    assert spy.transactions == []
    assert adapter.get_run(_TENANT, "run-a").state is RunState.CANCEL_REQUESTED
    spy.calls.clear()
    spy.query_limit = 1
    assert adapter.list_run_units(_TENANT, "run-a") == _many_units(99)
    assert spy.calls.count("query") == 99
    assert adapter.get_outbox_event("run-canceled") is None
def test_rejeita_transacao_com_chaves_duplicadas_ou_mais_de_100_acoes(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, _ = dynamodb_adapter
    spy = ClientSpy(adapter._client)
    action = _transaction_action("same")
    with pytest.raises(Conflict, match="duplicate_transaction_key"):
        execute_transaction(spy, (action, action))
    actions = tuple(_transaction_action(str(index)) for index in range(101))
    with pytest.raises(Conflict, match="transaction_limit"):
        execute_transaction(spy, actions)
    assert spy.calls == []

def test_moto_cancela_transacao_inteira_quando_condicao_falha(ctx: _DynamoContext) -> None:
    adapter, _ = ctx
    original = _run("run-a")
    adapter.put_run(original)
    existing_event = _event("existing")
    adapter._put_direct(adapter._outbox_item(existing_event))
    original_item = adapter._get_item(run_entity_key(_TENANT, "run-a"))
    updated_item = _expected_item(
        _run("run-a", RunState.FAILED), "RUN", run_entity_key(_TENANT, "run-a"), {}
    )
    actions = (
        put_action(_TABLE_NAME, updated_item, payload(original_item)),
        put_action(_TABLE_NAME, adapter._outbox_item(_event("existing")), None),
    )
    with pytest.raises(Conflict, match="transaction_conflict"):
        execute_transaction(adapter._client, actions)
    assert adapter.get_run(_TENANT, "run-a") == original
    assert adapter.get_outbox_event("existing") == existing_event

def test_claims_retornam_none_quando_cas_perde_corrida(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    adapter._client = FailingTransactionClient(adapter._client)
    assert adapter.claim_job(_claim_job("job-a", "worker-a", clock)) is None
    adapter._client = adapter._client.client
    dispatch = _prepare_unit(adapter, clock)
    adapter._client = FailingTransactionClient(adapter._client)
    command = ClaimRunUnit(
        tenant_id=_TENANT,
        run_id="run-a",
        unit_id="unit-a",
        dispatch_id=dispatch.dispatch_id,
        owner="worker-a",
        now=clock.now(),
        lease_seconds=30,
    )
    assert adapter.claim_run_unit(command) is None

def test_unit_ausente_nao_e_reivindicada_nem_finalizada(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
    claim = ClaimRunUnit(
        tenant_id=_TENANT,
        run_id="run-a",
        unit_id="unit-a",
        dispatch_id="a" * 16,
        owner="worker-a",
        now=clock.now(),
        lease_seconds=30,
    )
    assert adapter.claim_run_unit(claim) is None
    with pytest.raises(NotFound, match="unit_context_missing"):
        adapter.commit_run_unit(_commit_command("a" * 16, "worker-a", 1), _event("missing-unit"))

def test_dispatch_terminal_invalida_commit_de_unidade(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    adapter.finish_run_dispatch(
        FinishRunDispatch(
            tenant_id=_TENANT,
            run_id="run-a",
            dispatch_id=dispatch.dispatch_id,
            outcome=DispatchOutcome.CANCELED,
            finished_at=clock.now(),
        )
    )
    with pytest.raises(LeaseLost, match="dispatch_terminal"):
        adapter.commit_run_unit(
            _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token),
            _event("terminal-dispatch"),
        )

def test_dispatch_expirado_rejeita_corrida_que_aluga_unidade_omitida(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
    reserve = ReserveRunDispatch(
        tenant_id=_TENANT, run_id="run-a", wave_id="a" * 16,
        unit_ids=("unit-000",), now=clock.now(), lease_seconds=30)
    with pytest.raises(Conflict, match="run_not_processing"):
        adapter.reserve_run_dispatch(reserve)
    assert adapter.get_active_run_dispatch(_TENANT, "run-a") is None
    bind = BindRunDispatch(
        tenant_id=_TENANT, run_id="run-a", dispatch_id="a" * 16,
        execution_ref="missing", now=clock.now(), lease_seconds=30)
    with pytest.raises(Conflict, match="run_not_processing"):
        adapter.bind_run_dispatch(bind)
    adapter.put_run(_run("run-a"))
    with pytest.raises(NotFound, match="dispatch_missing"):
        adapter.bind_run_dispatch(bind)
    with pytest.raises(Conflict, match="dispatch_unit_missing"):
        adapter.reserve_run_dispatch(reserve)
    _put_many_units(adapter, 2)
    both = reserve.model_copy(update={"unit_ids": ("unit-000", "unit-001")})
    dispatch = adapter.reserve_run_dispatch(both)
    clock.advance(timedelta(seconds=31))
    replacement = reserve.model_copy(update={"wave_id": "b" * 16, "now": clock.now()})
    contender = DynamoDBControlPlane(adapter._client, _TABLE_NAME, clock.now)
    transact = adapter._transact
    def lease_omitted_then_replace(actions: Any) -> None:
        claim = ClaimRunUnit(
            tenant_id=_TENANT, run_id="run-a", unit_id="unit-001",
            dispatch_id=dispatch.dispatch_id, owner="worker-a",
            now=_NOW + timedelta(seconds=29), lease_seconds=60,
        )
        assert contender.claim_run_unit(claim) is not None
        transact(actions)
    adapter._transact = lease_omitted_then_replace
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.reserve_run_dispatch(replacement)
    adapter._transact = transact
    assert adapter._required_dispatch(_TENANT, "run-a")[1] == dispatch
    assert adapter.list_run_units(_TENANT, "run-a")[1].state is RunUnitState.LEASED
    with pytest.raises(Conflict, match="dispatch_unit_unavailable"):
        adapter.reserve_run_dispatch(replacement)
    adapter._client.delete_item(
        TableName=_TABLE_NAME,
        Key=item_key(*unit_key(_TENANT, "run-a", "unit-001")),
    )
    with pytest.raises(Conflict, match="dispatch_unit_missing"):
        adapter.reserve_run_dispatch(replacement)
    terminal = _unit("unit-001").model_copy(update={"state": RunUnitState.SUCCEEDED})
    adapter._put_direct(adapter._unit_item(terminal))
    with pytest.raises(Conflict, match="dispatch_unit_unavailable"):
        adapter.reserve_run_dispatch(replacement.model_copy(update={"unit_ids": ("unit-001",)}))

def test_dispatch_started_expirado_e_recuperavel_sem_lease_vivo(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
    dispatch = _prepare_unit(adapter, clock)
    bind = BindRunDispatch(
        tenant_id=_TENANT, run_id="run-a", dispatch_id=dispatch.dispatch_id,
        execution_ref="exec-b", now=clock.now(), lease_seconds=1,
    )
    contender = DynamoDBControlPlane(adapter._client, _TABLE_NAME, clock.now)
    transact = adapter._transact
    def cancel_then_bind(actions: Any) -> None:
        contender.transition_run(
            TransitionRun(tenant_id=_TENANT, run_id="run-a", expected_state=RunState.PROCESSING,
                          new_state=RunState.CANCEL_REQUESTED), _event("cancel-before-bind"))
        transact(actions)
    adapter._transact = cancel_then_bind
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.bind_run_dispatch(bind)
    adapter._transact = transact
    assert adapter.get_active_run_dispatch(_TENANT, "run-a") == dispatch
    adapter.put_run(_run("run-a"))
    adapter.bind_run_dispatch(bind)
    clock.advance(timedelta(seconds=2))
    recovered = adapter.reserve_run_dispatch(
        ReserveRunDispatch(
            tenant_id=_TENANT, run_id="run-a", wave_id="b" * 16,
            unit_ids=("unit-a",), now=clock.now(), lease_seconds=30,
        )
    )
    assert recovered.generation == 2

def test_commit_rejeita_dispatch_da_unidade_obsoleto(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    stale = claimed.model_copy(update={"dispatch_id": "b" * 16})
    adapter._put_direct(adapter._unit_item(stale))
    with pytest.raises(FenceRejected, match="unit_dispatch_rejected"):
        adapter.commit_run_unit(
            _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token),
            _event("stale-unit-dispatch"),
        )

def test_validador_rejeita_lease_corrompido_sem_prazo() -> None:
    unit = _unit("unit-a").model_copy(
        update={
            "state": "LEASED",
            "fencing_token": 1,
            "dispatch_id": "a" * 16,
            "lease_owner": "worker-a",
            "lease_until": None,
        }
    )
    command = _commit_command("a" * 16, "worker-a", 1)
    with pytest.raises(LeaseLost, match="unit_lease_expired"):
        DynamoDBClaims._validate_unit_fence(unit, command, _NOW)
