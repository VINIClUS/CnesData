from collections.abc import Callable, Iterator
from datetime import timedelta
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError, ConnectionClosedError, ReadTimeoutError
from moto import mock_aws

from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    ClaimRunUnit,
    FailRunUnit,
    FinishRunDispatch,
    PutRunUnits,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import AccessRequest, RunDependency
from cnes_domain.control_plane.enums import (
    AccessRequestState,
    DispatchOutcome,
    DispatchState,
    RunState,
    RunUnitState,
)
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost, NotFound
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_claims import DynamoDBClaims
from cnes_infra.control_plane.dynamodb_codec import put_action
from cnes_infra.control_plane.dynamodb_keys import (
    item_key,
    key_component,
    unit_key,
)
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
    _renew_job,
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
type TransactionCallback = Callable[[list[dict[str, Any]]], None]


@pytest.fixture
def ctx(dynamodb_adapter: _DynamoContext) -> _DynamoContext:
    return dynamodb_adapter


class ClientSpy:
    def __init__(
        self,
        client: Any,
        before_transaction: TransactionCallback | None = None,
        after_transaction: TransactionCallback | None = None,
    ) -> None:
        self.client = client
        self.before_transaction = before_transaction
        self.after_transaction = after_transaction
        self.transactions: list[list[dict[str, Any]]] = []
        self.transaction_requests: list[dict[str, Any]] = []
        self.calls: list[str] = []
        self.query_requests: list[dict[str, Any]] = []
        self.requests: list[tuple[str, dict[str, Any]]] = []

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        actions: list[dict[str, Any]] = kwargs["TransactItems"]
        self.calls.append("transact_write_items")
        self.transactions.append(actions)
        self.transaction_requests.append(dict(kwargs))
        if self.before_transaction is not None:
            self.before_transaction(actions)
        response = self.client.transact_write_items(**kwargs)
        if self.after_transaction is not None:
            self.after_transaction(actions)
        return response

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


def _raise_transaction_canceled(_: list[dict[str, Any]]) -> None:
    raise ClientError(
        {
            "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
            "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
        },
        "TransactWriteItems",
    )


def _lose_transaction_response(_: list[dict[str, Any]]) -> None:
    raise Conflict("transaction_conflict")


def _dynamodb_client_error(status: int) -> ClientError:
    return ClientError(
        {
            "Error": {"Code": "InternalServerError"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "TransactWriteItems",
    )


def _create_table(client: object) -> None:
    index_attributes = tuple(f"{index}{suffix}" for index in _INDEXES for suffix in ("pk", "sk"))
    attribute_definitions = [
        {"AttributeName": name, "AttributeType": "S"} for name in ("pk", "sk", *index_attributes)
    ]
    throughput = {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    global_secondary_indexes = [
        {
            "IndexName": index,
            "KeySchema": [
                {"AttributeName": f"{index}pk", "KeyType": "HASH"},
                {"AttributeName": f"{index}sk", "KeyType": "RANGE"},
            ],
            "Projection": {"ProjectionType": "ALL"},
            "ProvisionedThroughput": throughput,
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
        ProvisionedThroughput=throughput,
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
def test_cumpre_contrato(case: ControlPlaneCase, ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    case.run(adapter, clock)


def _many_units(amount: int) -> tuple[Any, ...]:
    return tuple(_unit(f"unit-{index:03d}") for index in range(amount))


def _put_many_units(adapter: DynamoDBControlPlane, amount: int) -> tuple[Any, ...]:
    units = tuple(reversed(_many_units(amount)))
    command = PutRunUnits(
        tenant_id=_TENANT, run_id="run-a", expected_run_state=RunState.PROCESSING, units=units
    )
    return adapter.put_run_units(command)


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


def _dependency_run(count: int, state: RunState, run_id: str) -> Any:
    return _run(
        run_id,
        state,
        tuple(
            RunDependency(source_type=f"SOURCE-{index}", file_subtype="ST", required=True)
            for index in range(count)
        ),
    )


def _store_waiting(adapter: DynamoDBControlPlane, run: Any, event_id: str) -> Any:
    if run.state is RunState.PLANNED:
        command = TransitionRun(
            tenant_id=_TENANT,
            run_id=run.run_id,
            expected_state=run.state,
            new_state=RunState.WAITING_INPUTS,
        )
        return adapter.transition_run(command, _event(event_id))
    adapter.put_run(run)
    return run


def _expected_unit_action(index: int, state: RunUnitState) -> dict[str, Any]:
    unit = _unit(f"unit-{index:03d}").model_copy(update={"state": state})
    indexes = {
        "gsi5pk": f"RUN_ITEMS#{key_component(_TENANT)}#{key_component('run-a')}",
        "gsi5sk": f"UNIT#{key_component(f'unit-{index:03d}')}",
    }
    item = _expected_item(unit, "RUNUNIT", unit_key(_TENANT, "run-a", f"unit-{index:03d}"), indexes)
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
def test_claims_retornam_none_quando_cas_perde_corrida(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    adapter._client = ClientSpy(adapter._client, before_transaction=_raise_transaction_canceled)
    assert adapter.claim_job(_claim_job("job-a", "worker-a", clock)) is None
    adapter._client = adapter._client.client
    dispatch = _prepare_unit(adapter, clock)
    adapter._client = ClientSpy(adapter._client, before_transaction=_raise_transaction_canceled)
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


def test_claim_job_com_relogio_futuro_nao_toma_lease_vivo(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    leased = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    assert leased is not None
    future_claim = _claim_job("job-a", "worker-b", clock).model_copy(
        update={"now": clock.now() + timedelta(seconds=31)}
    )

    assert adapter.claim_job(future_claim) is None
    assert adapter.get_job(_TENANT, "job-a") == leased


@pytest.mark.parametrize(
    "reported_now", [(_NOW - timedelta(hours=1),), (_NOW + timedelta(hours=1),)]
)
def test_renovacao_de_job_usa_relogio_autoritativo(
    ctx: _DynamoContext, reported_now: Any
) -> None:
    adapter, clock = ctx
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    claimed = adapter.claim_job(_claim_job("job-a", "worker-a", clock))
    assert claimed is not None
    renewal = _renew_job(clock).model_copy(update={"now": reported_now})

    renewed = adapter.renew_job_lease(renewal)

    assert renewed.lease_until == clock.now() + timedelta(seconds=renewal.lease_seconds)
    assert adapter.get_job(_TENANT, "job-a") == renewed


def test_commit_retorna_sucesso_quando_resposta_da_transacao_confirmada_se_perde(
    ctx: _DynamoContext,
) -> None:
    adapter, clock = ctx
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    event = _event("unit-completed")
    adapter._client = ClientSpy(adapter._client, after_transaction=_lose_transaction_response)

    completed = adapter.commit_run_unit(command, event)

    assert completed.state is RunUnitState.SUCCEEDED
    assert adapter.list_run_units(_TENANT, "run-a")[0] == completed
    assert adapter.get_outbox_event(event.event_id) == event


@pytest.mark.parametrize("error_type", [ReadTimeoutError, ConnectionClosedError])
def test_commit_reconhece_resposta_de_transporte_perdida_apos_transacao_confirmada(
    ctx: _DynamoContext, error_type: type[Exception]
) -> None:
    adapter, clock = ctx
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    event = _event("unit-completed")

    def lose_response(_: list[dict[str, Any]]) -> None:
        raise error_type(endpoint_url="https://dynamodb.us-east-1.amazonaws.com")

    adapter._client = ClientSpy(adapter._client, after_transaction=lose_response)

    completed = adapter.commit_run_unit(command, event)

    assert adapter.list_run_units(_TENANT, "run-a")[0] == completed
    assert adapter.get_outbox_event(event.event_id) == event


def test_commit_repete_transacao_com_token_estavel_apos_timeout_antes_da_resposta(
    ctx: _DynamoContext,
) -> None:
    adapter, clock = ctx
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    event = _event("unit-completed")
    attempts = 0

    def lose_first_response(_: list[dict[str, Any]]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ReadTimeoutError(endpoint_url="https://dynamodb.us-east-1.amazonaws.com")

    spy = ClientSpy(adapter._client, before_transaction=lose_first_response)
    adapter._client = spy

    completed = adapter.commit_run_unit(command, event)

    tokens = [request["ClientRequestToken"] for request in spy.transaction_requests]
    assert tokens[0] == tokens[1]
    assert adapter.get_outbox_event(event.event_id) == event
    assert adapter.list_run_units(_TENANT, "run-a")[0] == completed


@pytest.mark.parametrize("status", [500, 503])
def test_commit_repete_transacao_com_token_estavel_apos_erro_dynamodb_5xx(
    ctx: _DynamoContext, status: int
) -> None:
    adapter, clock = ctx
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    event = _event("unit-completed")
    attempts = 0

    def fail_first_response(_: list[dict[str, Any]]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _dynamodb_client_error(status)

    spy = ClientSpy(adapter._client, before_transaction=fail_first_response)
    adapter._client = spy

    completed = adapter.commit_run_unit(command, event)

    tokens = [request["ClientRequestToken"] for request in spy.transaction_requests]
    assert tokens[0] == tokens[1]
    assert adapter.get_outbox_event(event.event_id) == event
    assert adapter.list_run_units(_TENANT, "run-a")[0] == completed


def test_commit_propaga_erro_dynamodb_4xx(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)

    def fail_request(_: list[dict[str, Any]]) -> None:
        raise _dynamodb_client_error(400)

    adapter._client = ClientSpy(adapter._client, before_transaction=fail_request)

    with pytest.raises(ClientError, match="InternalServerError"):
        adapter.commit_run_unit(command, _event("unit-completed"))


@pytest.mark.parametrize("status", [400, 500])
def test_commit_propaga_segundo_erro_dynamodb_apos_timeout(
    ctx: _DynamoContext, status: int
) -> None:
    adapter, clock = ctx
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    attempts = 0

    def timeout_then_fail(_: list[dict[str, Any]]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ReadTimeoutError(endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
        raise _dynamodb_client_error(status)

    adapter._client = ClientSpy(adapter._client, before_transaction=timeout_then_fail)
    with pytest.raises(ClientError, match="InternalServerError"):
        adapter.commit_run_unit(command, _event("unit-completed"))


def test_commit_reconhece_erro_dynamodb_5xx_apos_repeticao_confirmada(
    ctx: _DynamoContext,
) -> None:
    adapter, clock = ctx
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    event = _event("unit-completed")
    attempts = 0

    def fail_first_request(_: list[dict[str, Any]]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ReadTimeoutError(endpoint_url="https://dynamodb.us-east-1.amazonaws.com")

    def fail_second_response(_: list[dict[str, Any]]) -> None:
        raise _dynamodb_client_error(500)

    adapter._client = ClientSpy(
        adapter._client,
        before_transaction=fail_first_request,
        after_transaction=fail_second_response,
    )

    completed = adapter.commit_run_unit(command, event)

    assert adapter.get_outbox_event(event.event_id) == event
    assert adapter.list_run_units(_TENANT, "run-a")[0] == completed


def test_commit_propaga_conflito_se_repeticao_apos_timeout_nao_encontra_replay(
    ctx: _DynamoContext,
) -> None:
    adapter, clock = ctx
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    event = _event("unit-completed")
    attempts = 0

    def timeout_then_persist_incompatible(_: list[dict[str, Any]]) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ReadTimeoutError(endpoint_url="https://dynamodb.us-east-1.amazonaws.com")
        winner = claimed.model_copy(
            update={
                "state": RunUnitState.SUCCEEDED,
                "lease_owner": None,
                "lease_until": None,
                "output_manifests": command.output_manifests,
            }
        )
        adapter._put_direct(adapter._unit_item(winner))

    adapter._client = ClientSpy(
        adapter._client, before_transaction=timeout_then_persist_incompatible
    )
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.commit_run_unit(command, event)


def test_commit_propaga_conflito_quando_evento_persistido_nao_corresponde(
    ctx: _DynamoContext,
) -> None:
    adapter, clock = ctx
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    event = _event("unit-completed")

    def persist_winner_incompativel(_: list[dict[str, Any]]) -> None:
        winner = claimed.model_copy(
            update={
                "state": RunUnitState.SUCCEEDED,
                "lease_owner": None,
                "lease_until": None,
                "output_manifests": command.output_manifests,
            }
        )
        different_event = event.model_copy(update={"payload": {"event_id": "different"}})
        adapter._put_direct(adapter._unit_item(winner))
        adapter._put_direct(adapter._outbox_item(different_event))

    adapter._client = ClientSpy(adapter._client, before_transaction=persist_winner_incompativel)
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.commit_run_unit(command, event)


def test_falha_opcional_rele_parent_apos_cas_concorrente(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
    dispatch = _prepare_unit(adapter, clock)
    dependency = RunDependency(source_type="CNES", file_subtype="ST", required=False)
    adapter.put_run(_run("run-a", dependencies=(dependency,)))
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    command = FailRunUnit(
        tenant_id=_TENANT,
        run_id="run-a",
        unit_id="unit-a",
        dispatch_id=dispatch.dispatch_id,
        owner="worker-a",
        fencing_token=claimed.fencing_token,
        error_code="optional_failed",
        retryable=False,
    )

    def parent_wins(_: list[dict[str, Any]]) -> None:
        current = adapter.get_run(_TENANT, "run-a")
        adapter.put_run(current.model_copy(update={"missing_sources": ("SIHD/AIH",)}))

    adapter._client = ClientSpy(adapter._client, before_transaction=parent_wins)
    failed = adapter.fail_run_unit(command, _event("unit-degraded"))
    assert (failed.state, adapter.get_run(_TENANT, "run-a").missing_sources) == (
        RunUnitState.SUCCEEDED_DEGRADED,
        ("CNES/ST", "SIHD/AIH"),
    )


def test_access_request_propaga_conflito_sem_winner(ctx: _DynamoContext) -> None:
    adapter, _ = ctx
    request = AccessRequest(
        tenant_id=_TENANT,
        request_id="missing-winner",
        user_id="user-a",
        state=AccessRequestState.PENDING,
        decided_by=None,
        decided_at=None,
    )
    adapter._client = ClientSpy(adapter._client, before_transaction=_raise_transaction_canceled)
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.put_access_request(request, _event("missing-winner"))
    assert adapter.get_access_request(_TENANT, "missing-winner") is None


def test_unit_ausente_nao_e_reivindicada_nem_finalizada(ctx: _DynamoContext) -> None:
    adapter, clock = ctx
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
        tenant_id=_TENANT,
        run_id="run-a",
        wave_id="a" * 16,
        unit_ids=("unit-000",),
        now=clock.now(),
        lease_seconds=30,
    )
    with pytest.raises(Conflict, match="run_not_processing"):
        adapter.reserve_run_dispatch(reserve)
    assert adapter.get_active_run_dispatch(_TENANT, "run-a") is None
    bind = BindRunDispatch(
        tenant_id=_TENANT,
        run_id="run-a",
        dispatch_id="a" * 16,
        execution_ref="missing",
        now=clock.now(),
        lease_seconds=30,
    )
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
    terminal_dispatch = dispatch.model_copy(
        update={"state": DispatchState.TERMINAL, "terminal_outcome": DispatchOutcome.CANCELED}
    )
    adapter._put_direct(adapter._dispatch_item(terminal_dispatch))
    with pytest.raises(Conflict, match="dispatch_unit_unavailable"):
        adapter.reserve_run_dispatch(replacement)
    adapter._put_direct(adapter._unit_item(_unit("unit-001")))

    def mutate_prior_then_replace(_: list[dict[str, Any]]) -> None:
        stale = _unit("unit-001").model_copy(update={"state": RunUnitState.SUCCEEDED})
        contender._put_direct(contender._unit_item(stale))

    adapter._client = ClientSpy(adapter._client, before_transaction=mutate_prior_then_replace)
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.reserve_run_dispatch(replacement)
    adapter._client = adapter._client.client
    assert adapter._required_dispatch(_TENANT, "run-a")[1] == terminal_dispatch
    adapter._client.delete_item(
        TableName=_TABLE_NAME, Key=item_key(*unit_key(_TENANT, "run-a", "unit-001"))
    )
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
        tenant_id=_TENANT,
        run_id="run-a",
        dispatch_id=dispatch.dispatch_id,
        execution_ref="exec-b",
        now=clock.now(),
        lease_seconds=1,
    )
    contender = DynamoDBControlPlane(adapter._client, _TABLE_NAME, clock.now)

    def cancel_then_bind(_: list[dict[str, Any]]) -> None:
        contender.transition_run(
            TransitionRun(
                tenant_id=_TENANT,
                run_id="run-a",
                expected_state=RunState.PROCESSING,
                new_state=RunState.CANCEL_REQUESTED,
            ),
            _event("cancel-before-bind"),
        )

    adapter._client = ClientSpy(adapter._client, before_transaction=cancel_then_bind)
    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.bind_run_dispatch(bind)
    adapter._client = adapter._client.client
    assert adapter.get_active_run_dispatch(_TENANT, "run-a") == dispatch
    adapter.put_run(_run("run-a"))
    adapter.bind_run_dispatch(bind)
    clock.advance(timedelta(seconds=2))
    recovered = adapter.reserve_run_dispatch(
        ReserveRunDispatch(
            tenant_id=_TENANT,
            run_id="run-a",
            wave_id="b" * 16,
            unit_ids=("unit-a",),
            now=clock.now(),
            lease_seconds=30,
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
