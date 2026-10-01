"""Dispatch canônico, listagem de PUBLISHING e revogação ponta a ponta com publicação negada."""

from collections.abc import Iterator
from datetime import timedelta

import pytest

from cnes_domain.billing.models import ReservationStatus
from cnes_domain.control_plane.enums import DispatchState, RunState
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, TENANT
from packages.cnes_infra.tests.billing.revocation_support import (
    LEASE_SECONDS,
    RUN_ID,
    RevEnv,
    create_run,
    finish_wave,
    make_unit,
    open_env,
    put_run_state,
    put_units,
    start_wave,
    stored_reservation,
    stored_run,
    usage_counters,
)
from packages.cnes_infra.tests.billing.test_dynamodb_revocation_service import (
    COMMAND,
    RecordingExecutor,
    build_service,
    seed_simple_run,
)

PUBLISHING_RUN = "run-02"


@pytest.fixture
def env() -> Iterator[RevEnv]:
    with open_env() as opened:
        yield opened


def test_le_dispatch_started_com_lease_expirado(env: RevEnv) -> None:
    create_run(env)
    put_units(env, (make_unit("unit-a"),))
    started = start_wave(env, ("unit-a",), None)
    env.clock.advance(timedelta(seconds=LEASE_SECONDS + 1))

    assert env.store.get_active_run_dispatch(TENANT, RUN_ID) is None
    dispatch = env.store.get_run_dispatch(TENANT, RUN_ID)

    assert dispatch.dispatch_id == started.dispatch_id
    assert dispatch.execution_ref == "exec-1"
    assert dispatch.state is DispatchState.STARTED


def test_le_dispatch_terminal(env: RevEnv) -> None:
    create_run(env)
    put_units(env, (make_unit("unit-a"),))
    finish_wave(env, start_wave(env, ("unit-a",), None))

    assert env.store.get_run_dispatch(TENANT, RUN_ID).state is DispatchState.TERMINAL


def test_dispatch_ausente_retorna_none(env: RevEnv) -> None:
    create_run(env)

    assert env.store.get_run_dispatch(TENANT, RUN_ID) is None


def test_lista_run_publishing_sem_fence(env: RevEnv) -> None:
    create_run(env)
    put_run_state(env, RunState.PUBLISHING)

    page = env.store.list_revocable_runs(ACCOUNT, 10, None)

    assert [state.run_id for state in page.runs] == [RUN_ID]
    assert page.runs[0].cancel_requested is False


def test_revogacao_cancela_processing_e_falha_publishing_liberando_a_reserva(env: RevEnv) -> None:
    seed_simple_run(env, RUN_ID)
    create_run(env, PUBLISHING_RUN)
    put_run_state(env, RunState.PUBLISHING, PUBLISHING_RUN)
    consumed = usage_counters(env)["consumed_runs"]

    result = build_service(env, RecordingExecutor()).revoke(COMMAND)

    assert result.failed_run_ids == (PUBLISHING_RUN,)
    assert result.fenced_run_ids == (RUN_ID,)
    assert stored_run(env, RUN_ID).state is RunState.CANCELED
    assert stored_run(env, PUBLISHING_RUN).state is RunState.FAILED
    assert stored_reservation(env, PUBLISHING_RUN).status is ReservationStatus.RELEASED
    assert stored_reservation(env, RUN_ID).status is ReservationStatus.RELEASED
    assert usage_counters(env)["consumed_runs"] == consumed
    failed = [e for e in env.plane.pending_outbox(500) if e.event_type == "run.failed"]
    assert [event.aggregate_id for event in failed] == [PUBLISHING_RUN]


def test_revogacao_cancela_executor_de_dispatch_started_com_lease_expirado(env: RevEnv) -> None:
    dispatch = seed_simple_run(env, RUN_ID)
    env.clock.advance(timedelta(seconds=LEASE_SECONDS + 1))
    executor = RecordingExecutor()

    build_service(env, executor).revoke(COMMAND)

    assert executor.refs() == {(RUN_ID, dispatch.execution_ref)}
    assert stored_run(env, RUN_ID).state is RunState.CANCELED
