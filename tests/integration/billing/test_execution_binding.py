"""Integração G11: execução faturada vincula dispatch, companion e claim de unidades."""

import logging
from collections.abc import Iterator
from pathlib import Path

import pytest

from apps.data_processor.tests.orchestration.test_coordinator import _processor
from cnes_domain.billing.errors import EntitlementDenied, PermanentBillingError
from cnes_domain.control_plane.commands import BindRunDispatch, ReserveRunDispatch
from cnes_domain.control_plane.entities import RunDispatch
from cnes_domain.control_plane.enums import DispatchState
from cnes_domain.control_plane.errors import LeaseLost
from cnes_domain.orchestration.planner import RunPlan, logical_wave_id, ready_units
from cnes_domain.ports.processing import CancelRunExecution
from cnes_infra.control_plane.dynamodb_billing import CLAIM_BIND_BACKOFF_SECONDS
from data_processor.orchestration.unit_worker import UnitWorker, UnitWorkerDependencies
from tests.integration.billing._execution_stack import (
    LEASE_SECONDS,
    RUN_ID,
    TENANT,
    Case,
    Stack,
    active_dispatch,
    billing_state,
    claim_command,
    complete_wave,
    create_processing_run,
    open_stack,
    overwrite_companion,
    resume,
    seed_run_without_companion,
)

SQLITE_DISABLED = Case("sqlite-disabled", dynamo=False, stripe=False)
DYNAMO_DISABLED = Case("dynamodb-disabled", dynamo=True, stripe=False)
DYNAMO_STRIPE = Case("dynamodb-stripe", dynamo=True, stripe=True)
MATRIX = [
    pytest.param(SQLITE_DISABLED, id=SQLITE_DISABLED.name),
    pytest.param(DYNAMO_DISABLED, id=DYNAMO_DISABLED.name),
    pytest.param(DYNAMO_STRIPE, id=DYNAMO_STRIPE.name),
]
DISABLED_ONLY = [param for param in MATRIX if not param.values[0].stripe]
WAVE_COUNT = 3


@pytest.fixture(params=MATRIX)
def stack(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Stack]:
    with open_stack(request.param, tmp_path) as opened:
        yield opened


@pytest.fixture(autouse=True)
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    delays: list[float] = []
    monkeypatch.setattr("cnes_infra.control_plane.dynamodb_billing.sleep", delays.append)
    return delays


def binding_of(stack: Stack) -> tuple[str, str, int, str]:
    state = billing_state(stack)
    return (
        state.execution_wave_id,
        state.execution_dispatch_id,
        state.execution_generation,
        state.execution_ref,
    )


def reserve_canonical(stack: Stack) -> RunDispatch:
    run = stack.plane.get_run(TENANT, RUN_ID)
    units = stack.plane.list_run_units(TENANT, RUN_ID)
    plan = RunPlan(
        run=run, units=units, missing_required=(), missing_optional=(), deployment_limit=2,
    )
    ready = ready_units(plan, stack.clock.now())
    return stack.plane.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=TENANT, run_id=RUN_ID, wave_id=logical_wave_id(ready),
        unit_ids=tuple(sorted(unit.unit_id for unit in ready)), now=stack.clock.now(),
        lease_seconds=LEASE_SECONDS,
    ))


def reserve_and_bind_canonical(stack: Stack) -> RunDispatch:
    dispatch = reserve_canonical(stack)
    return stack.plane.bind_run_dispatch(BindRunDispatch(
        tenant_id=TENANT, run_id=RUN_ID, dispatch_id=dispatch.dispatch_id,
        execution_ref="exec-manual", now=stack.clock.now(), lease_seconds=LEASE_SECONDS,
    ))


def cancel_companion_before_start(stack: Stack) -> None:
    stack.executor.before_start = lambda: overwrite_companion(stack, cancel_requested=True)


def test_tres_ondas_geram_tres_bindings_encadeados_do_companion(stack: Stack) -> None:
    create_processing_run(stack)
    bindings = []

    for _ in range(WAVE_COUNT):
        resume(stack)
        bindings.append(binding_of(stack))
        assert binding_of(stack)[1:] == (
            active_dispatch(stack).dispatch_id,
            active_dispatch(stack).generation,
            active_dispatch(stack).execution_ref,
        )
        complete_wave(stack)

    waves, dispatches, generations, refs = zip(*bindings, strict=True)
    assert len(set(waves)) == len(set(dispatches)) == len(set(refs)) == WAVE_COUNT
    assert list(generations) == sorted(set(generations))
    assert len(stack.executor.started) == WAVE_COUNT


def test_cada_binding_espera_o_anterior_e_o_callback_recebe_o_mesmo_permit(
    stack: Stack,
) -> None:
    create_processing_run(stack)
    previous: tuple[str | None, str | None] = (None, None)

    for index in range(WAVE_COUNT):
        resume(stack)
        context = stack.recorder.returned[index].binding_context
        assert (context.expected_previous_dispatch_id, context.expected_previous_execution_ref) == (
            previous
        )
        assert stack.recorder.seen[index] is stack.recorder.returned[index]
        previous = (binding_of(stack)[1], binding_of(stack)[3])
        complete_wave(stack)

    assert len(stack.recorder.seen) == len(stack.recorder.returned) == WAVE_COUNT


def test_claim_repara_binding_do_companion_apenas_no_modo_stripe(stack: Stack) -> None:
    create_processing_run(stack)
    dispatch = reserve_and_bind_canonical(stack)
    unit_id = dispatch.unit_ids[0]
    command = claim_command(stack, dispatch, unit_id)

    claimed = stack.plane.claim_run_unit(command)

    assert claimed is not None
    assert claimed.lease_owner == "worker-a"
    assert (billing_state(stack).execution_dispatch_id == dispatch.dispatch_id) is (
        stack.case.stripe
    )


@pytest.mark.parametrize("case", [DYNAMO_STRIPE])
def test_unit_worker_perde_lease_apos_retries_com_dispatch_ainda_nao_iniciado(
    case: Case, tmp_path: Path, sleeps: list[float]
) -> None:
    with open_stack(case, tmp_path) as stack:
        create_processing_run(stack)
        dispatch = reserve_canonical(stack)
        dependencies = UnitWorkerDependencies(
            control_plane=stack.plane, store=stack.store, processor=_processor,
            clock=stack.clock.now,
        )

        with pytest.raises(LeaseLost):
            UnitWorker(dependencies).execute(claim_command(stack, dispatch, dispatch.unit_ids[0]))

        assert sleeps == list(CLAIM_BIND_BACKOFF_SECONDS)


def test_falha_no_bind_cancela_execucao_e_finaliza_dispatch(
    stack: Stack, caplog: pytest.LogCaptureFixture
) -> None:
    create_processing_run(stack)
    cancel_companion_before_start(stack)

    with caplog.at_level(logging.WARNING), pytest.raises(PermanentBillingError) as error:
        resume(stack)

    assert error.value.code == "run_execution_canceled"
    (started,) = stack.executor.started
    assert stack.executor.canceled == [CancelRunExecution(
        tenant_id=TENANT, run_id=RUN_ID, execution_ref=f"exec-{started.dispatch_id}",
    )]
    assert active_dispatch(stack) is None
    assert "billing_audit" in caplog.text
    assert "reason_code=bind_failed" in caplog.text
    assert billing_state(stack).execution_generation == 0


def test_retomada_apos_bind_falho_reserva_geracao_seguinte_sem_unidade_reivindicada(
    stack: Stack,
) -> None:
    create_processing_run(stack)
    cancel_companion_before_start(stack)
    with pytest.raises(PermanentBillingError):
        resume(stack)
    first_dispatch_id = stack.executor.started[0].dispatch_id
    overwrite_companion(stack, cancel_requested=False)

    resume(stack)

    retry = active_dispatch(stack)
    assert retry.state is DispatchState.STARTED
    assert retry.dispatch_id != first_dispatch_id
    assert retry.generation == 2
    assert retry.terminal_outcome is None
    assert binding_of(stack)[1:3] == (retry.dispatch_id, 2)


def test_duas_retomadas_com_dispatch_ativo_iniciam_uma_unica_execucao(stack: Stack) -> None:
    create_processing_run(stack)

    first = resume(stack)
    second = resume(stack)

    assert first.execution_ref == second.execution_ref
    assert len(stack.executor.started) == 1
    assert len(stack.recorder.returned) == 1
    assert billing_state(stack).execution_generation == 1


@pytest.mark.parametrize("case", [DYNAMO_STRIPE])
def test_stripe_sem_companion_nega_na_policy_e_nao_inicia_execucao(
    case: Case, tmp_path: Path
) -> None:
    with open_stack(case, tmp_path) as stack:
        seed_run_without_companion(stack)

        with pytest.raises(EntitlementDenied, match="reason=run_billing_state_missing"):
            resume(stack)

        assert stack.executor.started == []
        assert billing_state(stack) is None


@pytest.mark.parametrize("case", [param.values[0] for param in DISABLED_ONLY])
def test_desabilitado_sem_companion_executa_com_permit_sem_medicao(
    case: Case, tmp_path: Path
) -> None:
    with open_stack(case, tmp_path) as stack:
        seed_run_without_companion(stack)

        result = resume(stack)

        (permit,) = stack.recorder.returned
        assert result.execution_ref is not None
        assert len(stack.executor.started) == 1
        assert permit.binding_context.expected_entitlement_version == 1
        assert permit.fencing_token == 0
        assert stack.recorder.seen == [permit]
        assert billing_state(stack) is None
