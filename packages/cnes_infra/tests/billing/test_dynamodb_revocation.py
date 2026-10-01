"""Listagem, fence de revogação e progresso durável do DynamoRevocationStore."""

from collections.abc import Iterator
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError

from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.revocation import (
    RevocationPhase,
    RevocationProgress,
    RevocationStorePort,
)
from cnes_domain.control_plane.enums import RunState
from cnes_infra.billing.dynamodb_items import canonical_json
from cnes_infra.billing.dynamodb_quota_items import encode_run_billing_state
from cnes_infra.billing.dynamodb_revocation import DynamoRevocationStore
from cnes_infra.billing.keys import (
    revocation_progress_key,
    run_billing_key,
    run_lookup_key,
)
from cnes_infra.control_plane.dynamodb_keys import item_key, run_entity_key
from cnes_infra.control_plane.dynamodb_run_codec import run_item
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, NOW, TENANT
from packages.cnes_infra.tests.billing.revocation_support import (
    RUN_ID,
    RevEnv,
    before_transaction,
    create_run,
    event_of,
    fence,
    get_raw,
    make_unit,
    open_env,
    put_run_state,
    put_units,
    revocation_event,
    revoke_command,
    start_wave,
)


@pytest.fixture
def env() -> Iterator[RevEnv]:
    with open_env() as opened:
        yield opened


def query_error() -> ClientError:
    return ClientError({"Error": {"Code": "ThrottlingException"}}, "Query")


class FailingQueryClient:
    def __init__(self, inner: Any) -> None:
        self.inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def query(self, **request: Any) -> Any:
        raise query_error()


def failing_store(env: RevEnv) -> DynamoRevocationStore:
    return DynamoRevocationStore(FailingQueryClient(env.client), env.table, env.clock.now)


def listed_ids(env: RevEnv, limit: int = 10, cursor: str | None = None) -> list[str]:
    page = env.store.list_revocable_runs(ACCOUNT, limit, cursor)
    return [state.run_id for state in page.runs]


def test_store_satisfaz_a_porta_de_revogacao(env: RevEnv) -> None:
    assert isinstance(env.store, RevocationStorePort)


def test_delega_leituras_ao_control_plane_canonico(env: RevEnv) -> None:
    create_run(env)
    put_units(env, (make_unit("unit-a"),))
    dispatch = start_wave(env, ("unit-a",), None)

    assert env.store.get_run(TENANT, RUN_ID) == env.plane.get_run(TENANT, RUN_ID)
    assert env.store.get_run_billing_state(TENANT, RUN_ID) == env.plane.get_run_billing_state(
        TENANT, RUN_ID
    )
    active = env.store.get_active_run_dispatch(TENANT, RUN_ID)
    assert active.dispatch_id == dispatch.dispatch_id


def test_lista_runs_da_conta_em_estados_revogaveis(env: RevEnv) -> None:
    create_run(env, "run-01")
    create_run(env, "run-02", to_processing=False)

    page = env.store.list_revocable_runs(ACCOUNT, 10, None)

    assert [state.run_id for state in page.runs] == ["run-01", "run-02"]
    assert page.next_cursor is None
    assert page.runs[0] == env.plane.get_run_billing_state(TENANT, "run-01")


def test_lista_em_paginas_de_um_run_com_cursor(env: RevEnv) -> None:
    for run_id in ("run-01", "run-02"):
        create_run(env, run_id)

    first = env.store.list_revocable_runs(ACCOUNT, 1, None)
    assert [state.run_id for state in first.runs] == ["run-01"]
    assert first.next_cursor == run_lookup_key(ACCOUNT, TENANT, "run-01")[1]
    collected, cursor = ["run-01"], first.next_cursor
    while cursor is not None:
        page = env.store.list_revocable_runs(ACCOUNT, 1, cursor)
        assert len(page.runs) <= 1
        collected.extend(state.run_id for state in page.runs)
        cursor = page.next_cursor
    assert collected == ["run-01", "run-02"]


@pytest.mark.parametrize(
    "state",
    [
        RunState.PUBLISHING,
        RunState.PUBLISHED,
        RunState.PUBLISHED_DEGRADED,
        RunState.FAILED,
        RunState.CANCELED,
    ],
)
def test_nao_lista_run_em_estado_nao_revogavel(env: RevEnv, state: RunState) -> None:
    create_run(env)
    put_run_state(env, state)

    assert listed_ids(env) == []


def test_lista_run_cancelado_com_fence_para_liquidacao_idempotente(env: RevEnv) -> None:
    create_run(env)
    fence(env)
    put_run_state(env, RunState.CANCELED)

    assert listed_ids(env) == [RUN_ID]


@pytest.mark.parametrize(
    "state", [RunState.PUBLISHING, RunState.PUBLISHED, RunState.PUBLISHED_DEGRADED, RunState.FAILED]
)
def test_nao_lista_run_nao_revogavel_mesmo_com_fence(env: RevEnv, state: RunState) -> None:
    create_run(env)
    fence(env)
    put_run_state(env, state)

    assert listed_ids(env) == []


def test_lista_run_com_cancelamento_ja_solicitado(env: RevEnv) -> None:
    create_run(env)
    put_run_state(env, RunState.CANCEL_REQUESTED)

    assert listed_ids(env) == [RUN_ID]


def test_rejeita_cursor_de_listagem_invalido(env: RevEnv) -> None:
    with pytest.raises(ValueError, match="reason=invalid_revocation_cursor"):
        env.store.list_revocable_runs(ACCOUNT, 10, "OUTRO#cursor")


def corrupt_lookup(env: RevEnv, change: Any) -> None:
    key = run_lookup_key(ACCOUNT, TENANT, RUN_ID)
    item = get_raw(env, key)
    env.client.put_item(TableName=env.table, Item=change(item))


@pytest.mark.parametrize(
    "change",
    [
        lambda item: {**item, "entity": {"S": "OUTRA"}},
        lambda item: {**item, "payload": {"S": "{nao-json"}},
        lambda item: {**item, "payload": {"S": '{"tenant_id": "354130"}'}},
        lambda item: {**item, "payload": {"S": '{"billing_account_id": "ba_99"}'}},
        lambda item: {
            **item,
            "payload": {
                "S": '{"billing_account_id": "ba_01", "tenant_id": "354130", "run_id": "run-99"}'
            },
        },
    ],
)
def test_lookup_corrompido_vira_item_corrompido(env: RevEnv, change: Any) -> None:
    create_run(env)
    corrupt_lookup(env, change)

    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        env.store.list_revocable_runs(ACCOUNT, 10, None)


@pytest.mark.parametrize("missing", [run_billing_key, run_entity_key])
def test_lookup_orfao_e_ignorado_com_aviso_sem_bloquear_outros_runs(
    env: RevEnv, missing: Any, caplog: pytest.LogCaptureFixture
) -> None:
    create_run(env, "run-01")
    create_run(env, "run-02")
    env.client.delete_item(TableName=env.table, Key=item_key(*missing(TENANT, "run-01")))

    with caplog.at_level("WARNING"):
        assert listed_ids(env) == ["run-02"]

    assert f"revocation_lookup_orphan tenant_id={TENANT} run_id=run-01" in caplog.text


def test_falha_de_query_na_listagem_vira_dependencia_indisponivel(env: RevEnv) -> None:
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        failing_store(env).list_revocable_runs(ACCOUNT, 10, None)


def stored_event_ids(env: RevEnv) -> list[str]:
    return [event.event_id for event in env.plane.pending_outbox(100)]


def test_fence_marca_run_e_companion_e_grava_evento_em_uma_transacao(env: RevEnv) -> None:
    create_run(env)
    put_units(env, (make_unit("unit-a"),))
    dispatch = start_wave(env, ("unit-a",), None)
    before = env.store.get_run_billing_state(TENANT, RUN_ID)
    env.spy.transactions.clear()

    fenced = fence(env)

    assert (fenced.cancel_requested, fenced.fencing_token) == (True, before.fencing_token + 1)
    assert fenced == env.store.get_run_billing_state(TENANT, RUN_ID)
    assert (fenced.execution_dispatch_id, fenced.execution_ref) == (
        dispatch.dispatch_id, "exec-1"
    )
    assert fenced.execution_status == before.execution_status
    assert env.store.get_run(TENANT, RUN_ID).state is RunState.CANCEL_REQUESTED
    assert len(env.spy.transactions) == 1
    assert len(env.spy.transactions[0]) == 3
    assert "run.revocation_requested:run-01" in stored_event_ids(env)


def test_retentativa_do_fence_e_idempotente_sem_nova_escrita(env: RevEnv) -> None:
    create_run(env)
    first = fence(env)
    env.spy.transactions.clear()

    second = env.store.request_run_revocation(
        replace(revoke_command(env), expected_fencing_token=0), revocation_event()
    )

    assert second == first
    assert second.fencing_token == 1
    assert env.spy.transactions == []


@pytest.mark.parametrize(
    "changes",
    [{"expected_fencing_token": 5}, {"expected_state": RunState.WAITING_INPUTS}],
)
def test_expectativa_obsoleta_do_fence_e_retentavel(env: RevEnv, changes: Any) -> None:
    create_run(env)

    with pytest.raises(RetryableBillingError, match="run_revocation_stale"):
        env.store.request_run_revocation(revoke_command(env, **changes), revocation_event())

    assert env.store.get_run(TENANT, RUN_ID).state is RunState.PROCESSING
    assert env.store.get_run_billing_state(TENANT, RUN_ID).cancel_requested is False


def test_mudanca_concorrente_antes_da_transacao_e_retentavel(env: RevEnv) -> None:
    create_run(env)
    command = revoke_command(env)

    def mutate() -> None:
        state = env.store.get_run_billing_state(TENANT, RUN_ID)
        changed = replace(state, updated_at=state.updated_at + timedelta(seconds=1))
        env.client.put_item(TableName=env.table, Item=encode_run_billing_state(changed))

    before_transaction(env, "OUTBOXEVENT", mutate)

    with pytest.raises(RetryableBillingError, match="run_revocation_stale"):
        env.store.request_run_revocation(command, revocation_event())

    assert env.store.get_run(TENANT, RUN_ID).state is RunState.PROCESSING
    assert "run.revocation_requested:run-01" not in stored_event_ids(env)


def test_run_ja_em_cancel_requested_e_fenceado_com_condition_check(env: RevEnv) -> None:
    create_run(env)
    put_run_state(env, RunState.CANCEL_REQUESTED)
    env.spy.transactions.clear()

    fenced = env.store.request_run_revocation(
        revoke_command(env, expected_state=RunState.CANCEL_REQUESTED), revocation_event()
    )

    kinds = sorted(next(iter(action)) for action in env.spy.transactions[0])
    assert kinds == ["ConditionCheck", "Put", "Put"]
    assert (fenced.cancel_requested, fenced.fencing_token) == (True, 1)
    assert env.store.get_run(TENANT, RUN_ID).state is RunState.CANCEL_REQUESTED


def test_run_waiting_inputs_e_fenceado_para_cancel_requested(env: RevEnv) -> None:
    create_run(env, to_processing=False)

    fence(env)

    run = env.store.get_run(TENANT, RUN_ID)
    assert run.state is RunState.CANCEL_REQUESTED
    assert get_raw(env, run_entity_key(TENANT, RUN_ID)) == run_item(run)


@pytest.mark.parametrize(
    "changes",
    [
        {"tenant_id": "outro"},
        {"aggregate_id": "outro-run"},
        {"delivered_at": NOW},
    ],
)
def test_rejeita_evento_de_revogacao_inconsistente(env: RevEnv, changes: Any) -> None:
    create_run(env)

    with pytest.raises(ValueError, match="reason=revocation_event_mismatch"):
        env.store.request_run_revocation(
            revoke_command(env), event_of("run.revocation_requested", **changes)
        )


def test_fence_sem_companion_e_permanente(env: RevEnv) -> None:
    create_run(env)
    command = revoke_command(env)
    env.client.delete_item(TableName=env.table, Key=item_key(*run_billing_key(TENANT, RUN_ID)))

    with pytest.raises(PermanentBillingError, match="run_billing_state_missing"):
        env.store.request_run_revocation(command, revocation_event())


def test_fence_sem_run_e_permanente(env: RevEnv) -> None:
    create_run(env)
    command = revoke_command(env)
    env.client.delete_item(TableName=env.table, Key=item_key(*run_entity_key(TENANT, RUN_ID)))

    with pytest.raises(PermanentBillingError, match="run_missing"):
        env.store.request_run_revocation(command, revocation_event())


def progress(**changes: Any) -> RevocationProgress:
    base = RevocationProgress(
        billing_account_id=ACCOUNT, entitlement_version=1, phase=RevocationPhase.FENCING,
        run_cursor=None, updated_at=NOW,
    )
    return replace(base, **changes)


def test_progresso_ausente_retorna_none(env: RevEnv) -> None:
    assert env.store.get_revocation_progress(ACCOUNT) is None


def test_inicia_e_le_o_progresso(env: RevEnv) -> None:
    started = progress()

    assert env.store.save_revocation_progress(None, started) is True
    assert env.store.get_revocation_progress(ACCOUNT) == started


def test_substitui_o_progresso_por_compare_and_set(env: RevEnv) -> None:
    started = progress()
    env.store.save_revocation_progress(None, started)
    advanced = progress(
        phase=RevocationPhase.FINALIZING, run_cursor="RUN#a",
        updated_at=NOW + timedelta(seconds=5),
    )

    assert env.store.save_revocation_progress(started, advanced) is True
    assert env.store.get_revocation_progress(ACCOUNT) == advanced


def test_conflito_de_compare_and_set_do_progresso_retorna_false(env: RevEnv) -> None:
    started = progress()
    env.store.save_revocation_progress(None, started)
    stale = progress(phase=RevocationPhase.CANCELING)
    other = progress(phase=RevocationPhase.COMPLETE, updated_at=NOW + timedelta(seconds=1))

    assert env.store.save_revocation_progress(stale, other) is False
    assert env.store.save_revocation_progress(None, other) is False
    assert env.store.get_revocation_progress(ACCOUNT) == started


def test_retorna_a_versao_mais_recente_do_progresso(env: RevEnv) -> None:
    env.store.save_revocation_progress(None, progress(entitlement_version=1))
    env.store.save_revocation_progress(None, progress(entitlement_version=2))
    env.store.save_revocation_progress(None, progress(entitlement_version=10))

    assert env.store.get_revocation_progress(ACCOUNT).entitlement_version == 10


@pytest.mark.parametrize("changes", [{"entitlement_version": 2}, {"billing_account_id": "ba_99"}])
def test_rejeita_substituicao_de_progresso_de_outra_identidade(
    env: RevEnv, changes: Any
) -> None:
    with pytest.raises(ValueError, match="reason=revocation_progress_mismatch"):
        env.store.save_revocation_progress(progress(), progress(**changes))


def corrupt_progress(env: RevEnv, change: Any) -> None:
    started = progress()
    env.store.save_revocation_progress(None, started)
    key = revocation_progress_key(ACCOUNT, 1)
    env.client.put_item(TableName=env.table, Item=change(get_raw(env, key)))


@pytest.mark.parametrize(
    "change",
    [
        lambda item: {**item, "entity": {"S": "OUTRA"}},
        lambda item: {**item, "payload": {"S": "{nao-json"}},
        lambda item: {**item, "payload": {"S": '{"billing_account_id": "ba_01"}'}},
        lambda item: {
            **item,
            "payload": {"S": canonical_json(progress(entitlement_version=7))},
        },
        lambda item: {
            **item,
            "payload": {"S": canonical_json(progress()).replace("fencing", "desconhecida")},
        },
    ],
)
def test_progresso_corrompido_vira_item_corrompido(env: RevEnv, change: Any) -> None:
    corrupt_progress(env, change)

    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        env.store.get_revocation_progress(ACCOUNT)


def test_falha_de_query_do_progresso_vira_dependencia_indisponivel(env: RevEnv) -> None:
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        failing_store(env).get_revocation_progress(ACCOUNT)
