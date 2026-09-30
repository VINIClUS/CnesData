"""Extensões de billing do control plane DynamoDB: run sem medição e vinculação."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any
from unittest.mock import patch

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.billing.commands import (
    AuthorizedRunCommand,
    ConsumeReservationCommand,
    ReleaseReservationCommand,
)
from cnes_domain.billing.errors import (
    IdempotencyConflict,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.execution import RunBillingState, RunExecutionBindingCommand
from cnes_domain.billing.models import RunAuthorization
from cnes_domain.control_plane.entities import Run
from cnes_domain.control_plane.enums import RunState
from cnes_domain.ports.control_plane import ControlPlanePort
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.dynamodb_quota_items import (
    decode_run_billing_state,
    encode_run_billing_state,
)
from cnes_infra.billing.keys import run_billing_key
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import idempotency_key, item_key, run_entity_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME, create_table
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    DEPENDENCIES,
    HASH_B,
    TENANT,
    make_quota_snapshot,
    make_reserve_command,
    make_run_request,
    seed_snapshot,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

WAVE = "a" * 16
DISPATCH = "b" * 16
OTHER_DISPATCH = "c" * 16


def cancellation() -> ClientError:
    response = {
        "Error": {"Code": "TransactionCanceledException", "Message": "canceled"},
        "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
    }
    return ClientError(response, "TransactWriteItems")


class SpyClient:
    def __init__(self, client: Any) -> None:
        self.inner = client
        self.transactions: list[list[dict[str, Any]]] = []
        self.before_transact: Callable[[], None] | None = None
        self.fail_transact = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def transact_write_items(self, **request: Any) -> Any:
        self.transactions.append(request["TransactItems"])
        if self.before_transact is not None:
            self.before_transact()
        if self.fail_transact:
            raise cancellation()
        return self.inner.transact_write_items(**request)


@dataclass(frozen=True, slots=True)
class Env:
    client: Any
    spy: SpyClient
    clock: MutableClock
    plane: DynamoDBControlPlane

    def other_plane(self) -> DynamoDBControlPlane:
        return DynamoDBControlPlane(self.client, TABLE_NAME, self.clock.now)


@contextmanager
def open_env() -> Iterator[Env]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        clock = MutableClock(NOW)
        spy = SpyClient(client)
        yield Env(client, spy, clock, DynamoDBControlPlane(spy, TABLE_NAME, clock.now))


@pytest.fixture
def env() -> Iterator[Env]:
    with open_env() as opened:
        yield opened


def authorized(**changes: Any) -> AuthorizedRunCommand:
    authorization = RunAuthorization(ACCOUNT, "plan_v1", 1, 4, None, NOW)
    return AuthorizedRunCommand(make_run_request(**changes), authorization)


def binding(**changes: Any) -> RunExecutionBindingCommand:
    command = RunExecutionBindingCommand(
        tenant_id=TENANT, run_id="run-01", wave_id=WAVE, dispatch_id=DISPATCH, generation=1,
        execution_ref="exec-1", unit_ids=("unit-001",), expected_previous_dispatch_id=None,
        expected_previous_execution_ref=None, expected_entitlement_version=1,
        expected_fencing_token=0, bound_at=NOW,
    )
    return replace(command, **changes)


def read_item(env: Env, key: tuple[str, str]) -> dict[str, Any] | None:
    response = env.client.get_item(TableName=TABLE_NAME, Key=item_key(*key), ConsistentRead=True)
    return response.get("Item")


def overwrite_companion(env: Env, **changes: Any) -> None:
    state = env.plane.get_run_billing_state(TENANT, "run-01")
    env.client.put_item(
        TableName=TABLE_NAME, Item=encode_run_billing_state(replace(state, **changes))
    )


def test_cria_run_sem_medicao_em_uma_unica_transacao(env: Env) -> None:
    run = env.plane.create_unmetered_run(authorized())

    assert len(env.spy.transactions) == 1
    assert len(env.spy.transactions[0]) == 6
    assert run.state is RunState.WAITING_INPUTS
    assert run.missing_sources == ("CNES/LFCES",)
    assert env.plane.get_run(TENANT, "run-01") == run


def test_grava_companion_nao_vinculado_com_autorizacao(env: Env) -> None:
    command = authorized()

    env.plane.create_unmetered_run(command)

    state = env.plane.get_run_billing_state(TENANT, "run-01")
    assert state.authorization == command.authorization
    assert (state.execution_generation, state.fencing_token) == (0, 0)
    assert state.execution_dispatch_id is None
    assert state.cancel_requested is False


def test_grava_idempotencia_e_evento_de_run_autorizado(env: Env) -> None:
    env.plane.create_unmetered_run(authorized())

    key = idempotency_key(TENANT, "billing.quota.run", "req-01")
    assert read_item(env, key) is not None
    (event,) = env.plane.pending_outbox(10)
    assert event.event_type == "run.authorized"
    assert event.aggregate_id == "run-01"
    assert event.payload["plan_version_id"] == "plan_v1"
    assert event.payload["dataset_name"] == "cnes_vinculos"


def test_replay_com_mesmo_hash_devolve_o_mesmo_run_sem_gravar(env: Env) -> None:
    first = env.plane.create_unmetered_run(authorized())
    env.spy.transactions.clear()

    second = env.plane.create_unmetered_run(authorized())

    assert second == first
    assert env.spy.transactions == []


def test_rejeita_replay_com_outro_hash(env: Env) -> None:
    env.plane.create_unmetered_run(authorized())

    with pytest.raises(IdempotencyConflict, match="key=req-01"):
        env.plane.create_unmetered_run(authorized(request_hash=HASH_B))


def test_idempotencia_expirada_e_sobrescrita(env: Env) -> None:
    env.plane.create_unmetered_run(authorized())
    env.clock.advance(timedelta(days=2))

    run = env.plane.create_unmetered_run(authorized(run_id="run-02", request_hash=HASH_B))

    assert run.run_id == "run-02"
    assert env.plane.get_run(TENANT, "run-01") is not None


def test_run_existente_sob_outra_chave_e_conflito_sem_gravar_nada(env: Env) -> None:
    env.plane.put_run(
        Run(
            tenant_id=TENANT, run_id="run-01", competencia="2026-08", dataset_name="cnes_vinculos",
            state=RunState.WAITING_INPUTS, dependencies=DEPENDENCIES, missing_sources=(),
            created_at=NOW,
        )
    )

    with pytest.raises(PermanentBillingError) as error:
        env.plane.create_unmetered_run(authorized())

    assert error.value.code == "run_conflict"
    assert env.plane.get_run_billing_state(TENANT, "run-01") is None
    assert read_item(env, idempotency_key(TENANT, "billing.quota.run", "req-01")) is None
    assert env.plane.pending_outbox(10) == ()


def test_perda_de_condicao_sem_colisao_e_contencao_retentavel(env: Env) -> None:
    env.spy.fail_transact = True

    with pytest.raises(RetryableBillingError) as error:
        env.plane.create_unmetered_run(authorized())

    assert error.value.code == "run_creation_contended"
    assert env.plane.get_run(TENANT, "run-01") is None


def test_replay_sem_run_persistido_falha_com_run_ausente(env: Env) -> None:
    env.plane.create_unmetered_run(authorized())
    env.client.delete_item(TableName=TABLE_NAME, Key=item_key(*run_entity_key(TENANT, "run-01")))

    with pytest.raises(PermanentBillingError) as error:
        env.plane.create_unmetered_run(authorized())

    assert error.value.code == "run_missing_after_replay"


def test_devolve_run_do_vencedor_concorrente_apos_perda_da_transacao(env: Env) -> None:
    winner: list[Any] = []
    env.spy.before_transact = lambda: winner.append(
        env.other_plane().create_unmetered_run(authorized())
    )
    env.spy.fail_transact = True

    run = env.plane.create_unmetered_run(authorized())

    assert run == winner[0]


def test_estado_de_billing_ausente_devolve_none(env: Env) -> None:
    assert env.plane.get_run_billing_state(TENANT, "run-01") is None


def test_estado_de_billing_decodifica_o_companion(env: Env) -> None:
    env.plane.create_unmetered_run(authorized())

    item = read_item(env, run_billing_key(TENANT, "run-01"))

    assert env.plane.get_run_billing_state(TENANT, "run-01") == decode_run_billing_state(item)


def test_primeira_vinculacao_persiste_a_execucao(env: Env) -> None:
    env.plane.create_unmetered_run(authorized())

    state = env.plane.bind_run_execution(binding())

    assert (state.execution_dispatch_id, state.execution_ref) == (DISPATCH, "exec-1")
    assert env.plane.get_run_billing_state(TENANT, "run-01") == state


def test_vinculacao_idempotente_nao_abre_transacao(env: Env) -> None:
    env.plane.create_unmetered_run(authorized())
    first = env.plane.bind_run_execution(binding())
    env.spy.transactions.clear()

    assert env.plane.bind_run_execution(binding()) == first
    assert env.spy.transactions == []


def test_vinculacao_com_referencia_diferente_e_conflito(env: Env) -> None:
    env.plane.create_unmetered_run(authorized())
    env.plane.bind_run_execution(binding())

    with pytest.raises(PermanentBillingError) as error:
        env.plane.bind_run_execution(binding(execution_ref="exec-2"))

    assert error.value.code == "run_execution_conflict"


def test_vinculacao_com_anterior_obsoleto_e_rejeitada(env: Env) -> None:
    env.plane.create_unmetered_run(authorized())
    env.plane.bind_run_execution(binding())

    with pytest.raises(PermanentBillingError) as error:
        env.plane.bind_run_execution(binding(dispatch_id=OTHER_DISPATCH, generation=2))

    assert error.value.code == "run_execution_stale"


def test_vinculacao_sem_companion_falha(env: Env) -> None:
    with pytest.raises(PermanentBillingError) as error:
        env.plane.bind_run_execution(binding())

    assert error.value.code == "run_billing_state_missing"


def test_perda_do_cas_com_vencedor_identico_devolve_estado_persistido(env: Env) -> None:
    env.plane.create_unmetered_run(authorized())
    env.spy.before_transact = lambda: env.other_plane().bind_run_execution(binding())
    env.spy.fail_transact = True

    state = env.plane.bind_run_execution(binding())

    assert state == env.plane.get_run_billing_state(TENANT, "run-01")
    assert state.execution_ref == "exec-1"


def test_perda_do_cas_com_estado_diferente_e_contencao(env: Env) -> None:
    env.plane.create_unmetered_run(authorized())
    env.spy.before_transact = lambda: overwrite_companion(
        env, updated_at=NOW + timedelta(seconds=5)
    )
    env.spy.fail_transact = True

    with pytest.raises(RetryableBillingError) as error:
        env.plane.bind_run_execution(binding())

    assert error.value.code == "run_execution_contended"


def test_perda_do_cas_por_atualizacao_alheia_reaplica_o_vinculo(env: Env) -> None:
    env.plane.create_unmetered_run(authorized())
    calls: list[int] = []

    def interfere() -> None:
        calls.append(1)
        if len(calls) == 1:
            overwrite_companion(env, updated_at=NOW + timedelta(seconds=5))
        else:
            env.spy.fail_transact = False

    env.spy.before_transact = interfere
    env.spy.fail_transact = True

    state = env.plane.bind_run_execution(binding())

    assert len(calls) == 2
    assert state == env.plane.get_run_billing_state(TENANT, "run-01")
    assert state.execution_ref == "exec-1"


def test_replay_resolve_o_run_pelo_recurso_gravado(env: Env) -> None:
    first = env.plane.create_unmetered_run(authorized())

    assert env.plane.create_unmetered_run(authorized(run_id="run-02")) == first
    assert env.plane.get_run(TENANT, "run-02") is None


@pytest.mark.parametrize("operation", ["consume", "release"])
def test_consumo_e_liberacao_delegam_as_reservas_de_quota(env: Env, operation: str) -> None:
    consume = ConsumeReservationCommand(ACCOUNT, "res-1", 0, NOW)
    release = ReleaseReservationCommand(ACCOUNT, "res-1", NOW, "run_failed")
    with patch("cnes_infra.billing.dynamodb_quota.DynamoQuotaReservations") as quota:
        if operation == "consume":
            result = env.plane.consume_reservation(consume)
            expected = quota.return_value.consume
        else:
            result = env.plane.release_reservation(release)
            expected = quota.return_value.release

    quota.assert_called_once_with(env.spy, TABLE_NAME, env.clock.now)
    expected.assert_called_once()
    assert result is expected.return_value


def test_reserva_e_criacao_do_run_delegam_a_quota_dynamodb(env: Env) -> None:
    seed_snapshot(env.client, make_quota_snapshot())

    authorization = env.plane.reserve_and_create_run(make_reserve_command())

    assert authorization.billing_account_id == ACCOUNT
    assert env.plane.get_run(TENANT, "run-01").state is RunState.WAITING_INPUTS
    assert isinstance(env.plane.get_run_billing_state(TENANT, "run-01"), RunBillingState)


def test_dynamodb_control_plane_cumpre_a_porta(env: Env) -> None:
    assert isinstance(env.plane, ControlPlanePort)


def test_modo_de_billing_padrao_e_desabilitado(env: Env) -> None:
    assert env.plane._billing_mode is BillingMode.DISABLED
