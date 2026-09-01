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
    CompleteJob,
    FinalizeRunCancellation,
    FinishRunDispatch,
    PutRunUnits,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState, RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost, NotFound
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_claims import DynamoDBClaims
from cnes_infra.control_plane.dynamodb_codec import execute_transaction, put_action
from cnes_infra.control_plane.dynamodb_keys import (
    dependency_marker_key,
    item_key,
    key_component,
    outbox_key,
    run_entity_key,
    unit_key,
)
from packages.cnes_infra.tests.contracts import control_plane_contract
from packages.cnes_infra.tests.contracts.clock import (
    _NOW,
    _TENANT,
    MutableClock,
    _agent,
    _claim_job,
    _claim_unit,
    _claim_unit_command,
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
        self.query_requests: list[dict[str, Any]] = []
        self.requests: list[tuple[str, dict[str, Any]]] = []
    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("transact_write_items")
        self.transactions.append(kwargs["TransactItems"])
        return self.client.transact_write_items(**kwargs)
    def query(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("query")
        self.query_requests.append(dict(kwargs))
        kwargs.setdefault("Limit", getattr(self, "query_limit", 100))
        return self.client.query(**kwargs)
    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self.client, name)
        if not callable(attribute):
            return attribute
        def tracked(**kwargs: Any) -> Any:
            self.calls.append(name)
            self.requests.append((name, dict(kwargs)))
            return attribute(**kwargs)
        return tracked
class FailingTransactionClient(ClientSpy):
    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("transact_write_items")
        self.transactions.append(kwargs["TransactItems"])
        response = {"Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
                    "CancellationReasons": [{"Code": "ConditionalCheckFailed"}]}
        raise ClientError(response, "TransactWriteItems")
def _create_table(client: object) -> None:
    index_attributes = tuple(f"{index}{suffix}" for index in _INDEXES for suffix in ("pk", "sk"))
    attribute_definitions = [{"AttributeName": name, "AttributeType": "S"}
                             for name in ("pk", "sk", *index_attributes)]
    global_secondary_indexes = [
        {
            "IndexName": index,
            "KeySchema": [{"AttributeName": f"{index}pk", "KeyType": "HASH"},
                          {"AttributeName": f"{index}sk", "KeyType": "RANGE"}],
            "Projection": {"ProjectionType": "ALL"},
            "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        }
        for index in _INDEXES
    ]
    client.create_table(
        TableName=_TABLE_NAME,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                   {"AttributeName": "sk", "KeyType": "RANGE"}],
        AttributeDefinitions=attribute_definitions,
        GlobalSecondaryIndexes=global_secondary_indexes,
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    client.update_time_to_live(TableName=_TABLE_NAME,
        TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"})
@pytest.fixture
def dynamodb_adapter() -> Iterator[tuple[DynamoDBControlPlane, MutableClock]]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        clock = MutableClock(_NOW)
        yield DynamoDBControlPlane(client, _TABLE_NAME, clock.now), clock
@pytest.mark.parametrize("case", control_plane_cases(), ids=lambda case: case.name)
def test_cumpre_contrato(case: ControlPlaneCase, ctx: _DynamoContext, monkeypatch: Any) -> None:
    adapter, clock = ctx
    if case.name == "raw_chains":
        monkeypatch.setattr(control_plane_contract, "_store_record", _store_record_matching_mode)
    case.run(adapter, clock)
def _store_record_matching_mode(adapter: Any, record: Any, clock: MutableClock) -> None:
    job_id = f"job-{record.agent_id}-{record.snapshot_id}"
    job = _job(job_id, record.agent_id, record.tenant_id).model_copy(update={
        "source_type": record.source_type, "file_subtype": record.file_subtype,
        "competencia": record.competencia, "requested_snapshot_mode": record.snapshot_mode,
        "created_at": record.created_at})
    adapter.put_agent(_agent(record.agent_id, tenant_id=record.tenant_id))
    adapter.create_job(job, _event(f"created-{job_id}", tenant_id=record.tenant_id))
    claimed = adapter.claim_job(_claim_job(job_id, "raw-worker", clock, record.tenant_id))
    assert claimed is not None
    command = CompleteJob(
        tenant_id=record.tenant_id, job_id=job_id, owner="raw-worker",
        fencing_token=claimed.fencing_token, manifest=record)
    adapter.complete_job(command, _event(f"completed-{job_id}", tenant_id=record.tenant_id))
def _many_units(amount: int) -> tuple[Any, ...]:
    return tuple(_unit(f"unit-{index:03d}") for index in range(amount))
def _put_many_units(adapter: DynamoDBControlPlane, amount: int) -> tuple[Any, ...]:
    units = tuple(reversed(_many_units(amount)))
    command = PutRunUnits(
        tenant_id=_TENANT, run_id="run-a", expected_run_state=RunState.PROCESSING, units=units)
    return adapter.put_run_units(command)
def _expected_item(model: Any, entity: str, key: tuple[str, str],
    indexes: dict[str, str]) -> dict[str, Any]:
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
def _dependency_run(count: int, state: RunState, run_id: str) -> Any:
    return _run(run_id, state, tuple(
        RunDependency(source_type=f"SOURCE-{index}", file_subtype="ST", required=True)
        for index in range(count)))
def _store_waiting(adapter: DynamoDBControlPlane, run: Any, event_id: str) -> Any:
    if run.state is RunState.PLANNED:
        command = TransitionRun(tenant_id=_TENANT, run_id=run.run_id, expected_state=run.state,
            new_state=RunState.WAITING_INPUTS)
        return adapter.transition_run(command, _event(event_id))
    adapter.put_run(run)
    return run
def _expected_unit_action(index: int, state: RunUnitState) -> dict[str, Any]:
    unit = _unit(f"unit-{index:03d}").model_copy(update={"state": state})
    indexes = {
        "gsi5pk": f"RUN_ITEMS#{key_component(_TENANT)}#{key_component('run-a')}",
        "gsi5sk": f"UNIT#{key_component(f'unit-{index:03d}')}"}
    item = _expected_item(unit, "RUNUNIT",
        unit_key(_TENANT, "run-a", f"unit-{index:03d}"), indexes)
    expected = None if state is RunUnitState.PENDING else _unit(f"unit-{index:03d}")
    return _expected_put(item, expected.model_dump_json() if expected is not None else None)
def _hide_unit_from_gsi(adapter: DynamoDBControlPlane, unit_id: str) -> None:
    item = adapter._client.get_item(TableName=_TABLE_NAME,
        Key=item_key(*unit_key(_TENANT, "run-a", unit_id)), ConsistentRead=True)["Item"]
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
            "TableName": _TABLE_NAME, "Key": item_key(*run_entity_key(_TENANT, "run-a")),
            "ConditionExpression": "payload = :expected",
            "ExpressionAttributeValues": {":expected": {"S": run.model_dump_json()}}}}
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
        unit_ids=tuple(unit.unit_id for unit in units[:98]), now=clock.now(), lease_seconds=30)
    adapter.reserve_run_dispatch(first)
    clock.advance(timedelta(seconds=31))
    spy = ClientSpy(adapter._client)
    adapter._client = spy
    replacement = first.model_copy(update={"wave_id": "b" * 16,
        "unit_ids": ("unit-098",), "now": clock.now()})
    with pytest.raises(Conflict, match="transaction_limit"):
        adapter.reserve_run_dispatch(replacement)
    assert spy.transactions == []
def _prepare_cancellation(adapter: DynamoDBControlPlane, amount: int) -> None:
    adapter.put_run(_run("run-a"))
    _put_many_units(adapter, amount)
    adapter.put_run(_run("run-a", RunState.CANCEL_REQUESTED))
def _finalize(adapter: DynamoDBControlPlane, clock: MutableClock) -> Any:
    command = FinalizeRunCancellation(
        tenant_id=_TENANT, run_id="run-a", expected_state=RunState.CANCEL_REQUESTED,
        canceled_at=clock.now())
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
    indexes = {"gsi6pk": "OUTBOX#PENDING",
        "gsi6sk": f"{_NOW.isoformat(timespec='microseconds')}#"
                  f"{key_component('run-canceled')}"}
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
@pytest.mark.parametrize(("state","count"),[(RunState.WAITING_INPUTS,99),(RunState.PLANNED,98)])
def test_waiting_limita_acoes_e_chaves(state: RunState, count: int, ctx: _DynamoContext) -> None:
    adapter, _ = ctx
    run = _dependency_run(count, state, "run-limit")
    if state is RunState.PLANNED:
        adapter.put_run(run)
    spy = ClientSpy(adapter._client)
    adapter._client = spy
    overflow = _dependency_run(count + 1, state, "run-overflow")
    if state is RunState.PLANNED:
        adapter.put_run(overflow)
    mutations = (len(spy.transactions), spy.calls.count("put_item"))
    with pytest.raises(Conflict, match="transaction_limit"):
        _store_waiting(adapter, overflow, "waiting-overflow")
    assert (len(spy.transactions), spy.calls.count("put_item")) == mutations
    _store_waiting(adapter, run, "waiting-limit")
    actions = spy.transactions[-1]
    keys = {(action["Put"]["Item"]["pk"]["S"], action["Put"]["Item"]["sk"]["S"])
            for action in actions}
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
    transact = adapter._transact
    def commit_then_lose_response(actions: Any) -> None:
        transact(actions)
        raise Conflict("transaction_conflict")
    adapter._transact = commit_then_lose_response
    adapter.put_run(run)
    adapter._transact = transact
    adapter._client = spy = ClientSpy(adapter._client)
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
    adapter._client = FailingTransactionClient(spy.client)
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.put_run(fresh)
    assert adapter.get_run(_TENANT, "run-loser") is None
def test_claims_retornam_none_quando_cas_perde_corrida(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    adapter._client = FailingTransactionClient(adapter._client)
    assert adapter.claim_job(_claim_job("job-a", "worker-a", clock)) is None
    adapter._client = adapter._client.client
    dispatch = _prepare_unit(adapter, clock)
    adapter._client = FailingTransactionClient(adapter._client)
    command = ClaimRunUnit(tenant_id=_TENANT, run_id="run-a", unit_id="unit-a",
        dispatch_id=dispatch.dispatch_id, owner="worker-a", now=clock.now(), lease_seconds=30)
    assert adapter.claim_run_unit(command) is None
def test_unit_ausente_nao_e_reivindicada_nem_finalizada(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    claim = ClaimRunUnit(tenant_id=_TENANT, run_id="run-a", unit_id="unit-a",
        dispatch_id="a" * 16, owner="worker-a", now=clock.now(), lease_seconds=30)
    assert adapter.claim_run_unit(claim) is None
    with pytest.raises(NotFound, match="unit_context_missing"):
        adapter.commit_run_unit(_commit_command("a" * 16, "worker-a", 1), _event("missing-unit"))
def test_dispatch_terminal_invalida_commit_de_unidade(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
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
def test_dispatch_expirado_rejeita_unidade_omitida(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
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
    claim_clock = MutableClock(_NOW + timedelta(seconds=29))
    claim = _claim_unit_command(dispatch.dispatch_id, "worker-a", claim_clock, "unit-001")
    assert contender.claim_run_unit(claim) is not None
    terminal_dispatch = dispatch.model_copy(update={
        "state": DispatchState.TERMINAL, "terminal_outcome": DispatchOutcome.CANCELED})
    adapter._put_direct(adapter._dispatch_item(terminal_dispatch))
    with pytest.raises(Conflict, match="dispatch_unit_unavailable"):
        adapter.reserve_run_dispatch(replacement)
    adapter._put_direct(adapter._unit_item(_unit("unit-001")))
    transact = adapter._transact
    def mutate_prior_then_replace(actions: Any) -> None:
        stale = _unit("unit-001").model_copy(update={"state": RunUnitState.SUCCEEDED})
        contender._put_direct(contender._unit_item(stale))
        transact(actions)
    adapter._transact = mutate_prior_then_replace
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.reserve_run_dispatch(replacement)
    adapter._transact = transact
    assert adapter._required_dispatch(_TENANT, "run-a")[1] == terminal_dispatch
    adapter._client.delete_item(
        TableName=_TABLE_NAME, Key=item_key(*unit_key(_TENANT, "run-a", "unit-001")))
    with pytest.raises(Conflict, match="dispatch_unit_missing"):
        adapter.reserve_run_dispatch(replacement)
    terminal = _unit("unit-001").model_copy(update={"state": RunUnitState.SUCCEEDED})
    adapter._put_direct(adapter._unit_item(terminal))
    with pytest.raises(Conflict, match="dispatch_unit_unavailable"):
        adapter.reserve_run_dispatch(replacement.model_copy(update={"unit_ids": ("unit-001",)}))
def test_dispatch_started_expirado_e_recuperavel(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
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
def test_commit_rejeita_dispatch_obsoleto(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
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
