"""Falha de Run PUBLISHING com publicação negada pela revogação sobre moto."""

from collections.abc import Iterator
from dataclasses import replace
from typing import Any

import pytest

from cnes_domain.billing.errors import RetryableBillingError
from cnes_domain.billing.models import ReservationStatus
from cnes_domain.billing.revocation import FailDeniedPublicationCommand
from cnes_domain.control_plane.enums import RunState
from cnes_infra.billing.dynamodb_quota_items import encode_reservation, encode_run_billing_state
from cnes_infra.control_plane.dynamodb_run_codec import run_item
from packages.cnes_infra.tests.billing.quota_support import NOW, TENANT
from packages.cnes_infra.tests.billing.revocation_support import (
    RUN_ID,
    RevEnv,
    before_transaction,
    create_run,
    event_of,
    fence,
    open_env,
    put_run_state,
    stored_reservation,
    stored_run,
    usage_counters,
)

REASON = "revoked"


@pytest.fixture
def env() -> Iterator[RevEnv]:
    with open_env() as opened:
        yield opened


def publishing(env: RevEnv) -> None:
    create_run(env)
    put_run_state(env, RunState.PUBLISHING)


def command(**changes: Any) -> FailDeniedPublicationCommand:
    base = FailDeniedPublicationCommand(TENANT, RUN_ID, 0, REASON, NOW)
    return replace(base, **changes)


def failed_event(**changes: Any) -> Any:
    return event_of("run.failed", **changes)


def event_ids(env: RevEnv) -> list[str]:
    return [event.event_id for event in env.plane.pending_outbox(500)]


def test_falha_run_publishing_libera_reserva_sem_devolver_run_consumido(env: RevEnv) -> None:
    publishing(env)
    before = usage_counters(env)
    reserved = stored_reservation(env).reserved_scan_bytes
    env.spy.transactions.clear()

    assert env.store.fail_denied_publication(command(), failed_event()) is True

    assert stored_run(env).state is RunState.FAILED
    assert "run.failed:run-01" in event_ids(env)
    assert stored_reservation(env).status is ReservationStatus.RELEASED
    after = usage_counters(env)
    assert after["consumed_runs"] == before["consumed_runs"]
    assert after["run_reserved_scan_bytes"] == before["run_reserved_scan_bytes"] - reserved
    assert len(env.spy.transactions) == 1


def test_segunda_chamada_e_idempotente_sem_nova_escrita(env: RevEnv) -> None:
    publishing(env)
    assert env.store.fail_denied_publication(command(), failed_event())
    counters = usage_counters(env)
    env.spy.transactions.clear()

    assert env.store.fail_denied_publication(command(), failed_event()) is False

    assert env.spy.transactions == []
    assert usage_counters(env) == counters


def test_run_fora_de_publishing_nao_e_falhado(env: RevEnv) -> None:
    create_run(env)

    assert env.store.fail_denied_publication(command(), failed_event()) is False

    assert stored_run(env).state is RunState.PROCESSING
    assert stored_reservation(env).status is ReservationStatus.RESERVED


def test_run_com_fence_de_revogacao_nao_e_falhado(env: RevEnv) -> None:
    create_run(env)
    fence(env)
    put_run_state(env, RunState.PUBLISHING)

    denied = env.store.fail_denied_publication(command(expected_fencing_token=1), failed_event())

    assert denied is False

    assert stored_run(env).state is RunState.PUBLISHING


def test_fence_diferente_do_esperado_nao_falha_o_run(env: RevEnv) -> None:
    publishing(env)

    denied = env.store.fail_denied_publication(command(expected_fencing_token=7), failed_event())

    assert denied is False

    assert stored_run(env).state is RunState.PUBLISHING


def test_sem_reserva_no_companion_a_transacao_nao_tem_acoes_de_liberacao(env: RevEnv) -> None:
    publishing(env)
    state = env.store.get_run_billing_state(TENANT, RUN_ID)
    cleared = replace(state, authorization=replace(state.authorization, budget_reservation_id=None))
    env.client.put_item(TableName=env.table, Item=encode_run_billing_state(cleared))
    env.spy.transactions.clear()

    assert env.store.fail_denied_publication(command(), failed_event()) is True

    assert len(env.spy.transactions[0]) == 3
    assert stored_reservation(env).status is ReservationStatus.RESERVED


@pytest.mark.parametrize("status", [ReservationStatus.CONSUMED, ReservationStatus.RELEASED])
def test_reserva_ja_liquidada_falha_o_run_sem_acoes_de_liberacao(
    env: RevEnv, status: ReservationStatus
) -> None:
    publishing(env)
    settled = replace(stored_reservation(env), status=status)
    env.client.put_item(TableName=env.table, Item=encode_reservation(settled, TENANT))
    counters = usage_counters(env)
    env.spy.transactions.clear()

    assert env.store.fail_denied_publication(command(), failed_event()) is True

    assert len(env.spy.transactions[0]) == 3
    assert stored_run(env).state is RunState.FAILED
    assert usage_counters(env) == counters


@pytest.mark.parametrize(
    "changes", [{"tenant_id": "outro"}, {"aggregate_id": "run-99"}, {"delivered_at": NOW}]
)
def test_rejeita_evento_inconsistente(env: RevEnv, changes: Any) -> None:
    publishing(env)

    with pytest.raises(ValueError, match="publication_event_mismatch"):
        env.store.fail_denied_publication(command(), failed_event(**changes))

    assert stored_run(env).state is RunState.PUBLISHING


def test_publicacao_concorrente_vence_e_o_run_nao_e_falhado(env: RevEnv) -> None:
    publishing(env)
    before_transaction(env, "OUTBOXEVENT", lambda: put_run_state(env, RunState.PUBLISHED))

    assert env.store.fail_denied_publication(command(), failed_event()) is False

    assert stored_run(env).state is RunState.PUBLISHED
    assert "run.failed:run-01" not in event_ids(env)
    assert stored_reservation(env).status is ReservationStatus.RESERVED


def test_falha_concorrente_por_outro_processo_retorna_false_sem_duplicar(env: RevEnv) -> None:
    publishing(env)
    counters = usage_counters(env)

    def other_wins() -> None:
        put_run_state(env, RunState.FAILED)

    before_transaction(env, "OUTBOXEVENT", other_wins)

    assert env.store.fail_denied_publication(command(), failed_event()) is False

    assert usage_counters(env) == counters
    assert "run.failed:run-01" not in event_ids(env)


def test_mudanca_do_payload_do_run_com_publishing_e_retentavel(env: RevEnv) -> None:
    publishing(env)

    def touch_run() -> None:
        run = stored_run(env)
        changed = run.model_copy(update={"missing_sources": ("CNES",)})
        env.client.put_item(TableName=env.table, Item=run_item(changed))

    before_transaction(env, "OUTBOXEVENT", touch_run)

    with pytest.raises(RetryableBillingError, match="run_revocation_stale"):
        env.store.fail_denied_publication(command(), failed_event())

    assert stored_run(env).state is RunState.PUBLISHING
    assert stored_reservation(env).status is ReservationStatus.RESERVED


def test_reserva_inexistente_e_retentavel(env: RevEnv) -> None:
    publishing(env)
    state = env.store.get_run_billing_state(TENANT, RUN_ID)
    unknown = replace(
        state, authorization=replace(state.authorization, budget_reservation_id="res-ghost")
    )
    env.client.put_item(TableName=env.table, Item=encode_run_billing_state(unknown))

    with pytest.raises(RetryableBillingError, match="quota_reservation_not_found"):
        env.store.fail_denied_publication(command(), failed_event())
