"""Cancelamento de unidades, liquidação e cenários de revogação do DynamoRevocationStore."""

import os
from collections.abc import Iterator
from dataclasses import replace
from datetime import timedelta
from typing import Any, cast
from uuid import uuid4

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.models import BillingEnforcementMode, ReservationStatus
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState, RunState, RunUnitState
from cnes_domain.profiles import BillingMode
from cnes_infra.billing import dynamodb_revocation_units as revocation_units
from cnes_infra.billing.dynamodb_quota_items import encode_run_billing_state
from cnes_infra.billing.keys import run_billing_key
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.control_plane import dynamodb_adapter
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import dispatch_key, item_key, run_entity_key
from packages.cnes_infra.tests.billing.quota_support import TENANT
from packages.cnes_infra.tests.billing.revocation_support import (
    RUN_ID,
    RevEnv,
    before_transaction,
    bind_command,
    build_env,
    cancel_command,
    cancel_until_done,
    claim_unit,
    create_named_table,
    create_run,
    fence,
    finish_wave,
    get_raw,
    make_unit,
    move_to_processing,
    open_env,
    put_run_state,
    put_units,
    reject_transaction,
    reserve_wave,
    seed_units,
    start_wave,
    stored_dispatch,
    stored_reservation,
    stored_run,
    three_wave_revocation,
    units_by_id,
)
from packages.cnes_infra.tests.billing.test_control_plane_extensions import authorized

ENDPOINT = os.getenv("DYNAMODB_ENDPOINT_URL", "http://127.0.0.1:18000")
STRIPE_ENFORCED = BillingSettings(BillingMode.STRIPE, BillingEnforcementMode.ENFORCE, 60)
LIMIT_PER_TRANSACTION = 100


@pytest.fixture
def env() -> Iterator[RevEnv]:
    with open_env() as opened:
        yield opened


def local_client() -> Any:
    client = boto3.client(
        "dynamodb",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",  # noqa: S106
        config=Config(retries={"max_attempts": 1}, connect_timeout=2, read_timeout=10),
    )
    try:
        client.list_tables(Limit=1)
    except (BotoCoreError, ClientError, OSError):
        pytest.skip("reason=dynamodb_local_unreachable")
    return client


def test_tres_waves_revogacao_no_materialize_cancela_so_dispatch_mais_recente(
    env: RevEnv,
) -> None:
    three_wave_revocation(env)


@pytest.mark.dynamodb_local
def test_tres_waves_revogacao_no_dynamodb_local() -> None:
    client = local_client()
    table = f"revocation-{uuid4().hex[:12]}"
    create_named_table(client, table)
    try:
        three_wave_revocation(build_env(client, table))
    finally:
        client.delete_table(TableName=table)


def stripe_plane(env: RevEnv) -> DynamoDBControlPlane:
    return DynamoDBControlPlane(env.client, env.table, env.clock.now, billing=STRIPE_ENFORCED)


def test_fence_invalida_claims_do_dispatch_antigo(
    env: RevEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("cnes_infra.control_plane.dynamodb_billing.sleep", lambda _: None)
    create_run(env)
    put_units(env, (make_unit("unit-a"), make_unit("unit-b")))
    dispatch = start_wave(env, ("unit-a", "unit-b"), None)
    plane = stripe_plane(env)
    assert claim_unit(env, dispatch, "unit-a", plane) is not None

    fence(env)
    put_run_state(env, RunState.PROCESSING)

    assert claim_unit(env, dispatch, "unit-b", plane) is None
    assert units_by_id(env)["unit-b"].state is RunUnitState.PENDING


def test_fence_torna_inutilizavel_o_permit_da_proxima_geracao(env: RevEnv) -> None:
    create_run(env)
    put_units(env, (make_unit("unit-a"), make_unit("unit-b")))
    first = start_wave(env, ("unit-a",), None)
    finish_wave(env, first)
    second = reserve_wave(env, ("unit-b",), first)
    stale_bind = bind_command(env, second, "exec-2", first)

    fence(env)

    with pytest.raises(PermanentBillingError, match="run_execution_canceled"):
        env.plane.bind_run_execution(stale_bind)
    state = env.store.get_run_billing_state(TENANT, RUN_ID)
    assert state is not None
    assert state.execution_dispatch_id == first.dispatch_id


def test_run_com_99_ou_mais_unidades_cancela_em_lotes_e_finaliza(env: RevEnv) -> None:
    create_run(env)
    ids = seed_units(env, 200)
    fenced = fence(env)
    env.spy.transactions.clear()

    results = cancel_until_done(env, fenced)

    assert [len(result.canceled_unit_ids) for result in results] == [98, 98, 4]
    assert [result.run_canceled for result in results] == [False, False, True]
    assert results[0].next_cursor == ids[97]
    assert results[1].next_cursor == ids[195]
    assert results[2].next_cursor is None
    assert sorted(unit for result in results for unit in result.canceled_unit_ids) == list(ids)
    assert all(len(items) <= LIMIT_PER_TRANSACTION for items in env.spy.transactions)
    assert len(env.spy.transactions) == 4
    assert stored_run(env).state is RunState.CANCELED
    assert {unit.state for unit in units_by_id(env).values()} == {RunUnitState.CANCELED}


def test_retomada_por_cursor_continua_depois_do_ultimo_lote(env: RevEnv) -> None:
    create_run(env)
    seed_units(env, 200)
    fenced = fence(env)
    first = env.store.cancel_run_units(cancel_command(env, fenced))

    second = env.store.cancel_run_units(cancel_command(env, fenced, cursor=first.next_cursor))

    assert min(second.canceled_unit_ids) > cast("str", first.next_cursor)
    assert not set(first.canceled_unit_ids) & set(second.canceled_unit_ids)
    assert second.run_canceled is False


def test_cursor_nulo_apos_queda_converge_sem_repetir_unidades(env: RevEnv) -> None:
    create_run(env)
    seed_units(env, 200)
    fenced = fence(env)
    env.store.cancel_run_units(cancel_command(env, fenced))

    results = []
    while not results or not results[-1].run_canceled:
        results.append(env.store.cancel_run_units(cancel_command(env, fenced)))

    assert sum(len(result.canceled_unit_ids) for result in results) == 102
    assert stored_run(env).state is RunState.CANCELED


def test_cursor_obsoleto_alem_do_ultimo_id_recomeca_do_inicio(env: RevEnv) -> None:
    create_run(env)
    ids = seed_units(env, 200)
    fenced = fence(env)

    result = env.store.cancel_run_units(cancel_command(env, fenced, cursor="unit-zzzz"))

    assert result.canceled_unit_ids == ids[:98]


def test_limite_menor_que_o_lote_maximo_e_respeitado(env: RevEnv) -> None:
    create_run(env)
    ids = seed_units(env, 120)
    fenced = fence(env)

    result = env.store.cancel_run_units(cancel_command(env, fenced, limit=5))

    assert result.canceled_unit_ids == ids[:5]
    assert result.next_cursor == ids[4]


def test_perda_de_cas_no_lote_e_retentavel_e_nao_cancela_nada(env: RevEnv) -> None:
    create_run(env)
    seed_units(env, 200)
    fenced = fence(env)

    def mutate() -> None:
        state = env.store.get_run_billing_state(TENANT, RUN_ID)
        assert state is not None
        changed = replace(state, updated_at=state.updated_at + timedelta(seconds=1))
        env.client.put_item(TableName=env.table, Item=encode_run_billing_state(changed))

    before_transaction(env, "RUNUNIT", mutate)

    with pytest.raises(RetryableBillingError, match="run_cancellation_contended"):
        env.store.cancel_run_units(cancel_command(env, fenced))

    assert stored_run(env).state is RunState.CANCEL_REQUESTED
    assert {unit.state for unit in units_by_id(env).values()} == {RunUnitState.PENDING}


def test_fence_alterado_ou_ausente_e_retentavel(env: RevEnv) -> None:
    create_run(env)
    unfenced = env.store.get_run_billing_state(TENANT, RUN_ID)
    assert unfenced is not None
    with pytest.raises(RetryableBillingError, match="run_fence_changed"):
        env.store.cancel_run_units(cancel_command(env, unfenced))

    fenced = fence(env)
    stale = replace(fenced, fencing_token=99)
    with pytest.raises(RetryableBillingError, match="run_fence_changed"):
        env.store.cancel_run_units(cancel_command(env, stale))


def test_cancelamento_sem_companion_e_permanente(env: RevEnv) -> None:
    create_run(env)
    fenced = fence(env)
    key = run_billing_key(TENANT, RUN_ID)
    env.client.delete_item(TableName=env.table, Key=item_key(*key))

    with pytest.raises(PermanentBillingError, match="run_billing_state_missing"):
        env.store.cancel_run_units(cancel_command(env, fenced))


def test_cancelamento_sem_run_e_permanente(env: RevEnv) -> None:
    create_run(env)
    fenced = fence(env)
    env.client.delete_item(TableName=env.table, Key=item_key(*run_entity_key(TENANT, RUN_ID)))

    with pytest.raises(PermanentBillingError, match="run_missing"):
        env.store.cancel_run_units(cancel_command(env, fenced))


def test_run_fora_de_cancel_requested_e_permanente(env: RevEnv) -> None:
    create_run(env)
    state = env.store.get_run_billing_state(TENANT, RUN_ID)
    assert state is not None
    fenced = replace(state, cancel_requested=True, fencing_token=1)
    env.client.put_item(TableName=env.table, Item=encode_run_billing_state(fenced))

    with pytest.raises(PermanentBillingError, match="run_not_cancel_requested"):
        env.store.cancel_run_units(cancel_command(env, fenced))


def settled_run(env: RevEnv) -> Any:
    create_run(env)
    put_units(env, (make_unit("unit-a"),))
    start_wave(env, ("unit-a",), None)
    return fence(env)


def test_run_ja_cancelado_retoma_a_liquidacao_e_depois_nao_regrava(env: RevEnv) -> None:
    fenced = settled_run(env)
    put_run_state(env, RunState.CANCELED)

    first = env.store.cancel_run_units(cancel_command(env, fenced))

    assert (first.canceled_unit_ids, first.next_cursor, first.run_canceled) == ((), None, True)
    assert stored_reservation(env).status is ReservationStatus.RELEASED
    assert cast("Any", stored_dispatch(env)).terminal_outcome is DispatchOutcome.CANCELED
    env.spy.transactions.clear()
    second = env.store.cancel_run_units(cancel_command(env, fenced))
    assert second.run_canceled is True
    assert env.spy.transactions == []


def test_evento_de_finalizacao_da_revogacao_nao_colide_com_o_do_coordenador(
    env: RevEnv,
) -> None:
    create_run(env)
    fenced = fence(env)

    cancel_until_done(env, fenced)

    ids = {event.event_id for event in env.plane.pending_outbox(100)}
    assert f"run.canceled.revoked:{TENANT}:{RUN_ID}" in ids
    assert f"run.canceled:{TENANT}:{RUN_ID}" not in ids


def test_estados_nao_terminais_de_unidade_espelham_o_adapter_canonico() -> None:
    assert revocation_units._NONTERMINAL_UNITS == dynamodb_adapter._NONTERMINAL_UNITS


def test_run_sem_reserva_nem_dispatch_cancela_sem_liquidar(env: RevEnv) -> None:
    env.plane.create_unmetered_run(authorized())
    move_to_processing(env)
    put_units(env, (make_unit("unit-a"),))
    fenced = fence(env)

    results = cancel_until_done(env, fenced)

    assert results[-1].canceled_unit_ids == ("unit-a",)
    assert stored_run(env).state is RunState.CANCELED
    entities = {item["entity"]["S"] for item in env.client.scan(TableName=env.table)["Items"]}
    assert "QUOTARESERVATION" not in entities
    assert stored_dispatch(env) is None


def test_dispatch_com_lease_expirado_e_ignorado_na_liquidacao(env: RevEnv) -> None:
    fenced = settled_run(env)
    env.clock.advance(timedelta(seconds=301))

    results = cancel_until_done(env, fenced)

    assert results[-1].run_canceled
    assert stored_run(env).state is RunState.CANCELED
    assert cast("Any", stored_dispatch(env)).state is DispatchState.STARTED
    assert stored_reservation(env).status is ReservationStatus.RELEASED
    state = env.store.get_run_billing_state(TENANT, RUN_ID)
    assert state is not None
    assert state.execution_status is DispatchState.STARTED


def test_conflito_do_dispatch_diferente_de_expirado_e_retentavel(env: RevEnv) -> None:
    fenced = settled_run(env)

    def mutate() -> None:
        raw = get_raw(env, dispatch_key(TENANT, RUN_ID))
        dispatch = cast("Any", stored_dispatch(env))
        changed = cast("Any", stored_dispatch(env)).model_copy(
            update={"lease_until": dispatch.lease_until + timedelta(seconds=1)}
        )
        env.client.put_item(
            TableName=env.table,
            Item={**cast("dict[str, Any]", raw), "payload": {"S": changed.model_dump_json()}},
        )

    before_transaction(env, "RUNDISPATCH", mutate)

    with pytest.raises(RetryableBillingError, match="run_cancellation_contended"):
        env.store.cancel_run_units(cancel_command(env, fenced))

    assert stored_run(env).state is RunState.CANCEL_REQUESTED


def test_perda_de_cas_no_espelho_do_companion_e_retentavel(env: RevEnv) -> None:
    fenced = settled_run(env)

    def mutate() -> None:
        state = env.store.get_run_billing_state(TENANT, RUN_ID)
        assert state is not None
        changed = replace(state, updated_at=state.updated_at + timedelta(seconds=1))
        env.client.put_item(TableName=env.table, Item=encode_run_billing_state(changed))

    before_transaction(env, "RUNBILLINGSTATE", mutate)

    with pytest.raises(RetryableBillingError, match="run_cancellation_contended"):
        env.store.cancel_run_units(cancel_command(env, fenced))

    assert stored_run(env).state is RunState.CANCEL_REQUESTED


def test_falha_na_finalizacao_deixa_liquidacao_feita_e_retentativa_converge(
    env: RevEnv,
) -> None:
    fenced = settled_run(env)
    before_transaction(env, "RUN", reject_transaction)

    with pytest.raises(RetryableBillingError, match="run_cancellation_contended"):
        env.store.cancel_run_units(cancel_command(env, fenced))

    assert stored_run(env).state is RunState.CANCEL_REQUESTED
    assert stored_reservation(env).status is ReservationStatus.RELEASED
    dispatch = stored_dispatch(env)
    assert dispatch is not None
    assert (dispatch.state, dispatch.terminal_outcome) == (
        DispatchState.TERMINAL, DispatchOutcome.CANCELED
    )
    state = env.store.get_run_billing_state(TENANT, RUN_ID)
    assert state is not None
    assert state.execution_status is DispatchState.TERMINAL
    assert units_by_id(env)["unit-a"].state is RunUnitState.PENDING

    retry = env.store.cancel_run_units(cancel_command(env, fenced))

    assert (retry.canceled_unit_ids, retry.run_canceled) == (("unit-a",), True)
    assert stored_run(env).state is RunState.CANCELED
    assert units_by_id(env)["unit-a"].state is RunUnitState.CANCELED
