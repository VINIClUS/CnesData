from collections.abc import Iterator
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
from cnes_domain.control_plane.enums import DispatchOutcome, RunState
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost, NotFound
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_claims import DynamoDBClaims
from cnes_infra.control_plane.dynamodb_codec import execute_transaction, put_action
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


class ClientSpy:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.transactions: list[list[dict[str, Any]]] = []
        self.calls: list[str] = []

    def transact_write_items(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("transact_write_items")
        self.transactions.append(kwargs["TransactItems"])
        return self.client.transact_write_items(**kwargs)

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
            {"Error": {"Code": "TransactionCanceledException", "Message": "cancelled"}},
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
    units = _many_units(amount)
    return adapter.put_run_units(
        PutRunUnits(
            tenant_id=_TENANT,
            run_id="run-a",
            expected_run_state=RunState.PROCESSING,
            units=units,
        )
    )


def _action_key(action: dict[str, Any]) -> tuple[str, str]:
    request = next(iter(action.values()))
    values = request.get("Item", request.get("Key"))
    return values["pk"]["S"], values["sk"]["S"]


def test_grava_99_unidades_em_ate_100_acoes_unicas(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, _ = dynamodb_adapter
    adapter.put_run(_run("run-a"))
    spy = ClientSpy(adapter._client)
    adapter._client = spy

    assert _put_many_units(adapter, 99) == _many_units(99)

    actions = spy.transactions[-1]
    keys = [_action_key(action) for action in actions]
    assert len(actions) == 100
    assert tuple(next(iter(action)) for action in actions) == (
        "ConditionCheck",
        *("Put" for _ in range(99)),
    )
    assert len(keys) == len(set(keys))


def test_rejeita_100_unidades_antes_de_chamar_boto3(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, _ = dynamodb_adapter
    spy = ClientSpy(adapter._client)
    adapter._client = spy

    with pytest.raises(Conflict, match="transaction_limit"):
        _put_many_units(adapter, 100)

    assert spy.calls == []


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


def test_cancela_98_unidades_em_100_acoes_unicas(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
    _prepare_cancellation(adapter, 98)
    spy = ClientSpy(adapter._client)
    adapter._client = spy

    assert _finalize(adapter, clock).state is RunState.CANCELED

    actions = spy.transactions[-1]
    keys = [_action_key(action) for action in actions]
    assert len(actions) == 100
    assert all(tuple(action) == ("Put",) for action in actions)
    assert len(keys) == len(set(keys))


def test_rejeita_cancelamento_de_99_unidades_sem_transacao(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
    _prepare_cancellation(adapter, 99)
    spy = ClientSpy(adapter._client)
    adapter._client = spy

    with pytest.raises(Conflict, match="transaction_limit"):
        _finalize(adapter, clock)

    assert spy.transactions == []
    assert adapter.get_run(_TENANT, "run-a").state is RunState.CANCEL_REQUESTED
    assert all(unit.state.value == "PENDING" for unit in adapter.list_run_units(_TENANT, "run-a"))
    assert adapter.get_outbox_event("run-canceled") is None


def test_rejeita_chaves_duplicadas_antes_de_enviar_transacao(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, _ = dynamodb_adapter
    spy = ClientSpy(adapter._client)
    item = {
        "pk": {"S": "TENANT#354130"},
        "sk": {"S": "TEST#same"},
        "payload": {"S": "{}"},
    }
    action = put_action(_TABLE_NAME, item, None)

    with pytest.raises(Conflict, match="duplicate_transaction_key"):
        execute_transaction(spy, (action, action))

    assert spy.transactions == []


def test_transacao_falha_sem_mutar_run_ou_outbox(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, _ = dynamodb_adapter
    original = _run("run-a")
    adapter.put_run(original)
    adapter._client = FailingTransactionClient(adapter._client)
    command = TransitionRun(
        tenant_id=_TENANT,
        run_id="run-a",
        expected_state=RunState.PROCESSING,
        new_state=RunState.FAILED,
    )

    with pytest.raises(Conflict, match="transaction_conflict"):
        adapter.transition_run(command, _event("run-failed"))

    assert adapter.get_run(_TENANT, "run-a") == original
    assert adapter.get_outbox_event("run-failed") is None


def test_rejeita_mais_de_100_acoes_antes_do_cliente(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, _ = dynamodb_adapter
    spy = ClientSpy(adapter._client)
    actions = tuple(
        put_action(
            _TABLE_NAME,
            {
                "pk": {"S": "TENANT#354130"},
                "sk": {"S": f"TEST#{index}"},
                "payload": {"S": "{}"},
            },
            None,
        )
        for index in range(101)
    )
    with pytest.raises(Conflict, match="transaction_limit"):
        execute_transaction(spy, actions)
    assert spy.calls == []


def test_claim_job_retorna_none_quando_cas_perde_corrida(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
    adapter.put_agent(_agent("agent-a"))
    adapter.create_job(_job("job-a"), _event("job-created"))
    adapter._client = FailingTransactionClient(adapter._client)

    assert adapter.claim_job(_claim_job("job-a", "worker-a", clock)) is None


def test_claim_unit_retorna_none_quando_cas_perde_corrida(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
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


def test_dispatch_ausente_ou_sem_run_e_rejeitado(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
    reserve = ReserveRunDispatch(
        tenant_id=_TENANT,
        run_id="run-a",
        wave_id="a" * 16,
        unit_ids=("unit-a",),
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
        execution_ref="exec-a",
        now=clock.now(),
        lease_seconds=30,
    )
    with pytest.raises(NotFound, match="dispatch_missing"):
        adapter.bind_run_dispatch(bind)


def test_dispatch_rejeita_unidade_ausente_ou_terminal(
    dynamodb_adapter: tuple[DynamoDBControlPlane, MutableClock],
) -> None:
    adapter, clock = dynamodb_adapter
    adapter.put_run(_run("run-a"))
    reserve = ReserveRunDispatch(
        tenant_id=_TENANT,
        run_id="run-a",
        wave_id="a" * 16,
        unit_ids=("unit-000",),
        now=clock.now(),
        lease_seconds=30,
    )
    with pytest.raises(Conflict, match="dispatch_unit_missing"):
        adapter.reserve_run_dispatch(reserve)
    _put_many_units(adapter, 1)
    dispatch = adapter.reserve_run_dispatch(reserve)
    claimed = adapter.claim_run_unit(
        ClaimRunUnit(
            tenant_id=_TENANT,
            run_id="run-a",
            unit_id="unit-000",
            dispatch_id=dispatch.dispatch_id,
            owner="worker-a",
            now=clock.now(),
            lease_seconds=30,
        )
    )
    adapter.commit_run_unit(
        _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token).model_copy(
            update={"unit_id": "unit-000"}
        ),
        _event("unit-complete"),
    )
    adapter.finish_run_dispatch(
        FinishRunDispatch(
            tenant_id=_TENANT,
            run_id="run-a",
            dispatch_id=dispatch.dispatch_id,
            outcome=DispatchOutcome.SUCCEEDED,
            finished_at=clock.now(),
        )
    )
    with pytest.raises(Conflict, match="dispatch_unit_unavailable"):
        adapter.reserve_run_dispatch(reserve.model_copy(update={"wave_id": "b" * 16}))


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
