"""Dispatch canônico, listagem de PUBLISHING e revogação ponta a ponta com publicação negada."""

from collections.abc import Iterator
from datetime import timedelta
from typing import Any, cast

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
    assert dispatch is not None

    assert dispatch.dispatch_id == started.dispatch_id
    assert dispatch.execution_ref == "exec-1"
    assert dispatch.state is DispatchState.STARTED


def test_le_dispatch_terminal(env: RevEnv) -> None:
    create_run(env)
    put_units(env, (make_unit("unit-a"),))
    finish_wave(env, start_wave(env, ("unit-a",), None))

    assert cast("Any", env.store.get_run_dispatch(TENANT, RUN_ID)).state is DispatchState.TERMINAL


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
    failed = [
        e for e in env.plane.read_outbox_page(None, 500).events if e.event_type == "run.failed"
    ]
    assert [event.aggregate_id for event in failed] == [PUBLISHING_RUN]


def test_revogacao_cancela_executor_de_dispatch_started_com_lease_expirado(env: RevEnv) -> None:
    dispatch = seed_simple_run(env, RUN_ID)
    env.clock.advance(timedelta(seconds=LEASE_SECONDS + 1))
    executor = RecordingExecutor()

    build_service(env, executor).revoke(COMMAND)

    assert executor.refs() == {(RUN_ID, dispatch.execution_ref)}
    assert stored_run(env, RUN_ID).state is RunState.CANCELED


@pytest.mark.parametrize("bound", [False, True])
def test_revogacao_preserva_claim_e_permit_ate_reconciliacao_nativa(env, bound):
    from cnes_domain.control_plane.commands import BindRunDispatch, ClaimRunDispatchStart
    from cnes_domain.control_plane.dispatch_start import DispatchStartClaim
    from cnes_domain.control_plane.environment_gate import AcquireUnitPermit, PermitHolder
    from packages.cnes_infra.tests.billing.revocation_support import bind_command, reserve_wave

    create_run(env)
    put_units(env, (make_unit("unit-a"),))
    dispatch = reserve_wave(env, ("unit-a",), None)
    holder = PermitHolder(TENANT, RUN_ID, dispatch.dispatch_id, dispatch.generation, "a" * 64)
    permit = env.plane.acquire_unit_permit(
        AcquireUnitPermit("prod", holder, env.clock.now(), 300)
    ).permit
    claim = DispatchStartClaim(
        dispatch_id=dispatch.dispatch_id,
        generation=dispatch.generation,
        permit_token=permit.token,
        fence_version=0,
        request_sha256="a" * 64,
    )
    env.plane.claim_run_dispatch_start(
        ClaimRunDispatchStart(
            tenant_id=TENANT,
            run_id=RUN_ID,
            dispatch_id=dispatch.dispatch_id,
            claim=claim,
            now=env.clock.now(),
            lease_seconds=300,
        )
    )
    reference = f"arn:aws:states:us-east-1:123456789012:execution:processor:{dispatch.dispatch_id}"
    if bound:
        env.plane.bind_run_dispatch(
            BindRunDispatch(
                tenant_id=TENANT,
                run_id=RUN_ID,
                dispatch_id=dispatch.dispatch_id,
                execution_ref=reference,
                now=env.clock.now(),
                lease_seconds=300,
                claim=claim,
            )
        )
        env.plane.bind_run_execution(bind_command(env, dispatch, reference, None))
    executor = RecordingExecutor()
    build_service(env, executor).revoke(COMMAND)
    settled = env.plane.get_run_dispatch(TENANT, RUN_ID)
    assert settled.start_claim == claim
    assert settled.state is DispatchState.TERMINAL
    assert env.plane.observe_environment_gate("prod").permit is not None
    assert env.plane.observe_environment_gate("prod").settlement is None


@pytest.mark.parametrize("bound", [False, True])
def test_billing_seguido_de_ausencia_retem_permit_ate_terminal_real(env, bound):
    from botocore.exceptions import ClientError

    from cnes_domain.control_plane.environment_gate import ClosePromotionFence
    from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
    from cnes_infra.executor.step_functions import StepFunctionsExecutionProbe

    test_revogacao_preserva_claim_e_permit_ate_reconciliacao_nativa(env, bound)
    if bound:
        from cnes_domain.control_plane.environment_gate import BindUnitPermit

        current = env.plane.observe_environment_gate("prod").permit
        dispatch = env.plane.get_run_dispatch(TENANT, RUN_ID)
        env.plane.bind_unit_permit(
            BindUnitPermit("prod", current.token, dispatch.execution_ref, env.clock.now())
        )
    before = env.plane.observe_environment_gate("prod")
    assert env.plane.close_promotion_fence(
        ClosePromotionFence("prod", "release-n1", env.clock.now())
    )

    class NativeClient:
        terminal = False

        def describe_execution(self, **request):
            if not self.terminal:
                raise ClientError({"Error": {"Code": "ExecutionDoesNotExist"}}, "DescribeExecution")
            return {"executionArn": request["executionArn"], "status": "ABORTED"}

    client = NativeClient()
    probe = StepFunctionsExecutionProbe(
        cast("Any", client), "arn:aws:states:us-east-1:123456789012:stateMachine:processor"
    )
    admission = EnvironmentAdmission(env.plane, probe, "prod", 300, env.plane)
    env.clock.advance(timedelta(hours=2))
    admission.reconcile(env.clock.now())
    after = env.plane.observe_environment_gate("prod")
    assert after.permit == before.permit
    assert after.settlement is None
    assert after.fence.closed_membership[0]["dispatch_id"] == before.permit.holder.dispatch_id
    assert env.plane.get_run_dispatch(TENANT, RUN_ID).canceled_before_send is False
    client.terminal = True
    admission.reconcile(env.clock.now())
    terminal = env.plane.observe_environment_gate("prod")
    assert terminal.permit is None
    assert b'"resolution":"TERMINAL"' in terminal.settlement
