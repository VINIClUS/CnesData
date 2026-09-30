"""Testes da política de concorrência e do callback de vinculação."""

import logging
from datetime import UTC, datetime
from typing import Any

import pytest

from cnes_domain.billing.errors import EntitlementDenied, PermanentBillingError
from cnes_domain.billing.execution import (
    RunBillingState,
    RunExecutionBindingCommand,
    RunExecutionPermit,
)
from cnes_domain.billing.execution_policy import (
    BillingConcurrencyPolicy,
    BillingExecutionDependencies,
    BillingExecutionStarted,
    ExecutionBindingPort,
    local_billing_account_id,
)
from cnes_domain.billing.models import RunAuthorization
from cnes_domain.control_plane.entities import Run, RunDependency, RunDispatch
from cnes_domain.control_plane.enums import DispatchState, RunState
from cnes_domain.ports.processing import ExecutionPermit, StartRunExecution
from cnes_domain.profiles import BillingMode

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
WAVE = "0123456789abcdef"
DISPATCH = "fedcba9876543210"
PREVIOUS = "aaaaaaaaaaaaaaaa"


class FakeControlPlane:
    def __init__(
        self,
        dispatch: RunDispatch | None = None,
        state: RunBillingState | None = None,
        bind_error: Exception | None = None,
    ) -> None:
        self.dispatch = dispatch
        self.state = state
        self.bind_error = bind_error
        self.bound: list[RunExecutionBindingCommand] = []

    def get_active_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None:
        return self.dispatch

    def get_run_billing_state(self, tenant_id: str, run_id: str) -> RunBillingState | None:
        return self.state

    def bind_run_execution(self, command: RunExecutionBindingCommand) -> RunBillingState:
        if self.bind_error is not None:
            raise self.bind_error
        self.bound.append(command)
        return self.state


def _run() -> Run:
    return Run(
        tenant_id="tenant-1",
        run_id="run-1",
        competencia="2026-09",
        dataset_name="cnes",
        state=RunState.PROCESSING,
        dependencies=(RunDependency(source_type="cnes", file_subtype="pf", required=True),),
        missing_sources=(),
        created_at=NOW,
    )


def _dispatch(**overrides: Any) -> RunDispatch:
    values: dict[str, Any] = {
        "tenant_id": "tenant-1",
        "run_id": "run-1",
        "wave_id": WAVE,
        "dispatch_id": DISPATCH,
        "generation": 2,
        "unit_ids": ("u1", "u2"),
        "state": DispatchState.RESERVED,
        "lease_until": NOW,
    }
    return RunDispatch(**{**values, **overrides})


def _started_dispatch(**overrides: Any) -> RunDispatch:
    values: dict[str, Any] = {"state": DispatchState.STARTED, "execution_ref": "exec-1"}
    return _dispatch(**{**values, **overrides})


def _state(**overrides: Any) -> RunBillingState:
    values: dict[str, Any] = {
        "billing_account_id": "acct-1",
        "tenant_id": "tenant-1",
        "run_id": "run-1",
        "authorization": RunAuthorization("acct-1", "plan-1", 3, 4, "res-1", NOW),
        "execution_generation": 0,
        "execution_wave_id": None,
        "execution_dispatch_id": None,
        "execution_ref": None,
        "execution_unit_ids": (),
        "execution_status": None,
        "execution_terminal_outcome": None,
        "fencing_token": 7,
        "cancel_requested": False,
        "updated_at": NOW,
    }
    return RunBillingState(**{**values, **overrides})


def _bound_state() -> RunBillingState:
    return _state(
        execution_generation=1,
        execution_wave_id=WAVE,
        execution_dispatch_id=PREVIOUS,
        execution_ref="exec-0",
        execution_unit_ids=("u1",),
        execution_status=DispatchState.STARTED,
    )


def _deps(fake: FakeControlPlane, mode: BillingMode) -> BillingExecutionDependencies:
    return BillingExecutionDependencies(control_plane=fake, clock=lambda: NOW, mode=mode)


def _policy(fake: FakeControlPlane, mode: BillingMode = BillingMode.STRIPE):
    return BillingConcurrencyPolicy(_deps(fake, mode))


def _started(fake: FakeControlPlane, mode: BillingMode = BillingMode.STRIPE):
    return BillingExecutionStarted(_deps(fake, mode))


def _context(**overrides: Any) -> RunExecutionPermit:
    values: dict[str, Any] = {
        "billing_account_id": "acct-1",
        "wave_id": WAVE,
        "dispatch_id": DISPATCH,
        "generation": 2,
        "expected_previous_dispatch_id": PREVIOUS,
        "expected_previous_execution_ref": "exec-0",
        "expected_entitlement_version": 3,
        "expected_fencing_token": 7,
        "authorized_at": NOW,
    }
    return RunExecutionPermit(**{**values, **overrides})


def _permit(context: object | None = None, **overrides: Any) -> ExecutionPermit:
    values: dict[str, Any] = {
        "tenant_id": "tenant-1",
        "run_id": "run-1",
        "max_concurrency": 2,
        "policy_version": 3,
        "fencing_token": 7,
        "binding_context": _context() if context is None else context,
    }
    return ExecutionPermit(**{**values, **overrides})


def _request(**overrides: Any) -> StartRunExecution:
    values: dict[str, Any] = {
        "tenant_id": "tenant-1",
        "run_id": "run-1",
        "wave_id": WAVE,
        "dispatch_id": DISPATCH,
        "unit_ids": ("u1", "u2"),
        "max_concurrency": 2,
    }
    return StartRunExecution(**{**values, **overrides})


def _bindable() -> FakeControlPlane:
    return FakeControlPlane(dispatch=_started_dispatch(), state=_bound_state())


def test_porta_de_vinculacao_reconhece_fake_com_tres_metodos() -> None:
    assert isinstance(FakeControlPlane(), ExecutionBindingPort)


def test_conta_local_usa_prefixo_e_tenant() -> None:
    assert local_billing_account_id("tenant-1") == "local-tenant-1"


def test_conta_local_rejeita_tenant_em_branco() -> None:
    with pytest.raises(ValueError, match="blank_value"):
        local_billing_account_id(" ")


def test_politica_desabilitada_sem_companion_devolve_permit_local() -> None:
    permit = _policy(FakeControlPlane(), BillingMode.DISABLED)(_run(), _dispatch(), 5)

    assert (permit.max_concurrency, permit.policy_version, permit.fencing_token) == (5, 1, 0)
    assert permit.binding_context == _context(
        billing_account_id="local-tenant-1",
        expected_previous_dispatch_id=None,
        expected_previous_execution_ref=None,
        expected_entitlement_version=1,
        expected_fencing_token=0,
    )


def test_politica_stripe_sem_companion_nega_entitlement() -> None:
    with pytest.raises(EntitlementDenied, match="reason=run_billing_state_missing"):
        _policy(FakeControlPlane())(_run(), _dispatch(), 5)


@pytest.mark.parametrize("mode", list(BillingMode))
def test_politica_nega_run_com_cancelamento_solicitado(mode: BillingMode) -> None:
    fake = FakeControlPlane(state=_state(cancel_requested=True))

    with pytest.raises(EntitlementDenied, match="reason=run_cancel_requested"):
        _policy(fake, mode)(_run(), _dispatch(), 2)


@pytest.mark.parametrize(("requested", "expected"), [(10, 4), (2, 2)])
@pytest.mark.parametrize("mode", list(BillingMode))
def test_politica_limita_concorrencia_ao_menor_valor(
    mode: BillingMode, requested: int, expected: int
) -> None:
    permit = _policy(FakeControlPlane(state=_state()), mode)(_run(), _dispatch(), requested)

    assert permit.max_concurrency == expected


def test_politica_herda_versao_fence_e_contexto_do_companion() -> None:
    fake = FakeControlPlane(state=_state())

    permit = _policy(fake)(_run(), _dispatch(), 2)

    assert (permit.policy_version, permit.fencing_token) == (3, 7)
    assert permit.binding_context == _context(
        expected_previous_dispatch_id=None, expected_previous_execution_ref=None
    )


def test_politica_usa_vinculo_anterior_quando_companion_ja_vinculado() -> None:
    permit = _policy(FakeControlPlane(state=_bound_state()))(_run(), _dispatch(), 2)

    assert permit.binding_context == _context()


def test_politica_rejeita_dispatch_de_outro_run() -> None:
    with pytest.raises(PermanentBillingError, match="dispatch_identity_mismatch"):
        _policy(FakeControlPlane(state=_state()))(_run(), _dispatch(run_id="run-2"), 2)


def test_politica_nao_escreve_no_plano_de_controle() -> None:
    fake = FakeControlPlane(state=_state())

    _policy(fake)(_run(), _dispatch(), 2)

    assert fake.bound == []


def test_rejeita_contexto_que_nao_e_run_execution_permit() -> None:
    permit = _permit().model_copy(update={"binding_context": None})

    with pytest.raises(PermanentBillingError, match="execution_permit_context_invalid"):
        _started(_bindable())(_run(), _request(), "exec-1", permit)


@pytest.mark.parametrize(
    "override",
    [
        {"permit": {"tenant_id": "tenant-2"}},
        {"permit": {"run_id": "run-2"}},
        {"permit": {"policy_version": 4}},
        {"permit": {"fencing_token": 8}},
        {"request": {"tenant_id": "tenant-2"}},
        {"request": {"run_id": "run-2"}},
        {"request": {"dispatch_id": PREVIOUS}},
        {"request": {"wave_id": PREVIOUS}},
    ],
)
def test_rejeita_identidade_divergente_entre_permit_run_e_request(
    override: dict[str, dict[str, Any]],
) -> None:
    permit = _permit(**override.get("permit", {}))
    request = _request(**override.get("request", {}))

    with pytest.raises(PermanentBillingError, match="execution_permit_mismatch"):
        _started(_bindable())(_run(), request, "exec-1", permit)


@pytest.mark.parametrize(
    "dispatch",
    [
        None,
        _dispatch(),
        _started_dispatch(dispatch_id=PREVIOUS),
        _started_dispatch(execution_ref="exec-2"),
    ],
)
def test_rejeita_dispatch_que_nao_esta_iniciado_com_a_referencia(
    dispatch: RunDispatch | None,
) -> None:
    fake = FakeControlPlane(dispatch=dispatch, state=_bound_state())

    with pytest.raises(PermanentBillingError, match="dispatch_not_started"):
        _started(fake)(_run(), _request(), "exec-1", _permit())

    assert fake.bound == []


def test_modo_desabilitado_sem_companion_nao_vincula() -> None:
    fake = FakeControlPlane(dispatch=_started_dispatch())

    _started(fake, BillingMode.DISABLED)(_run(), _request(), "exec-1", _permit())

    assert fake.bound == []


def test_modo_stripe_sem_companion_falha_no_callback() -> None:
    fake = FakeControlPlane(dispatch=_started_dispatch())

    with pytest.raises(PermanentBillingError, match="run_billing_state_missing"):
        _started(fake)(_run(), _request(), "exec-1", _permit())


def test_vincula_execucao_uma_vez_com_comando_exato() -> None:
    fake = _bindable()

    _started(fake)(_run(), _request(), "exec-1", _permit())

    assert fake.bound == [
        RunExecutionBindingCommand(
            tenant_id="tenant-1",
            run_id="run-1",
            wave_id=WAVE,
            dispatch_id=DISPATCH,
            generation=2,
            execution_ref="exec-1",
            unit_ids=("u1", "u2"),
            expected_previous_dispatch_id=PREVIOUS,
            expected_previous_execution_ref="exec-0",
            expected_entitlement_version=3,
            expected_fencing_token=7,
            bound_at=NOW,
        )
    ]


def test_repropaga_erro_de_vinculacao_e_registra_auditoria_uma_vez(
    caplog: pytest.LogCaptureFixture,
) -> None:
    error = PermanentBillingError("run_execution_stale")
    fake = _bindable()
    fake.bind_error = error

    with caplog.at_level(logging.WARNING), pytest.raises(PermanentBillingError) as raised:
        _started(fake)(_run(), _request(), "exec-1", _permit())

    assert raised.value is error
    messages = [record.getMessage() for record in caplog.records]
    assert messages == [
        "billing_audit event_type=run_execution.bind_failed reason_code=bind_failed "
        f"tenant_id=tenant-1 run_id=run-1 dispatch_id={DISPATCH}"
    ]


def test_registra_auditoria_tambem_em_rejeicao_de_contexto(
    caplog: pytest.LogCaptureFixture,
) -> None:
    permit = _permit().model_copy(update={"binding_context": None})

    with caplog.at_level(logging.WARNING), pytest.raises(PermanentBillingError):
        _started(_bindable())(_run(), _request(), "exec-1", permit)

    assert len(caplog.records) == 1


def test_callback_nunca_chama_bind_run_dispatch() -> None:
    fake = _bindable()

    _started(fake)(_run(), _request(), "exec-1", _permit())

    assert not hasattr(fake, "bind_run_dispatch")
