"""Testes da regra pura de vinculação de execução."""

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest

from cnes_domain.billing.errors import PermanentBillingError
from cnes_domain.billing.execution import RunBillingState, RunExecutionBindingCommand
from cnes_domain.billing.execution_policy import apply_execution_binding
from cnes_domain.billing.models import RunAuthorization
from cnes_domain.control_plane.enums import DispatchState

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
LATER = datetime(2026, 9, 30, 13, tzinfo=UTC)
WAVE = "0123456789abcdef"
DISPATCH = "fedcba9876543210"
PREVIOUS = "aaaaaaaaaaaaaaaa"


def _state(**overrides: Any) -> RunBillingState:
    values: dict[str, Any] = {
        "billing_account_id": "acct-1",
        "tenant_id": "tenant-1",
        "run_id": "run-1",
        "authorization": RunAuthorization("acct-1", "plan-1", 3, 2, "res-1", NOW),
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


def _bound(**overrides: Any) -> RunBillingState:
    values: dict[str, Any] = {
        "execution_generation": 1,
        "execution_wave_id": WAVE,
        "execution_dispatch_id": PREVIOUS,
        "execution_ref": "exec-0",
        "execution_unit_ids": ("u1",),
        "execution_status": DispatchState.STARTED,
    }
    return _state(**{**values, **overrides})


def _command(**overrides: Any) -> RunExecutionBindingCommand:
    values: dict[str, Any] = {
        "tenant_id": "tenant-1",
        "run_id": "run-1",
        "wave_id": WAVE,
        "dispatch_id": DISPATCH,
        "generation": 1,
        "execution_ref": "exec-1",
        "unit_ids": ("u1", "u2"),
        "expected_previous_dispatch_id": None,
        "expected_previous_execution_ref": None,
        "expected_entitlement_version": 3,
        "expected_fencing_token": 7,
        "bound_at": LATER,
    }
    return RunExecutionBindingCommand(**{**values, **overrides})


def _code(state: RunBillingState | None, command: RunExecutionBindingCommand) -> str:
    with pytest.raises(PermanentBillingError) as error:
        apply_execution_binding(state, command)
    return error.value.code


def test_devolve_mesmo_objeto_em_replay_idempotente() -> None:
    state = _bound(execution_dispatch_id=DISPATCH, execution_ref="exec-1")

    assert apply_execution_binding(state, _command(generation=2)) is state


def test_rejeita_mesmo_dispatch_com_outra_referencia() -> None:
    state = _bound(execution_dispatch_id=DISPATCH, execution_ref="exec-9")

    assert _code(state, _command(generation=2)) == "run_execution_conflict"


def test_rejeita_companion_ausente() -> None:
    assert _code(None, _command()) == "run_billing_state_missing"


@pytest.mark.parametrize("override", [{"tenant_id": "tenant-2"}, {"run_id": "run-2"}])
def test_rejeita_identidade_divergente(override: dict[str, str]) -> None:
    assert _code(_state(), _command(**override)) == "run_billing_state_mismatch"


def test_rejeita_run_com_cancelamento_solicitado() -> None:
    assert _code(_state(cancel_requested=True), _command()) == "run_execution_canceled"


def test_rejeita_entitlement_alterado() -> None:
    command = _command(expected_entitlement_version=2)

    assert _code(_state(), command) == "run_entitlement_changed"


def test_rejeita_fence_alterado() -> None:
    assert _code(_state(), _command(expected_fencing_token=6)) == "run_fence_changed"


def test_rejeita_vinculo_anterior_obsoleto() -> None:
    command = _command(
        generation=2,
        expected_previous_dispatch_id=PREVIOUS,
        expected_previous_execution_ref="exec-x",
    )

    assert _code(_bound(), command) == "run_execution_stale"


def test_rejeita_geracao_que_nao_e_maior() -> None:
    command = _command(
        generation=1,
        expected_previous_dispatch_id=PREVIOUS,
        expected_previous_execution_ref="exec-0",
    )

    assert _code(_bound(), command) == "run_execution_stale"


def test_primeiro_vinculo_preenche_execucao_iniciada() -> None:
    state = _state()

    result = apply_execution_binding(state, _command())

    assert result == replace(
        state,
        execution_generation=1,
        execution_wave_id=WAVE,
        execution_dispatch_id=DISPATCH,
        execution_ref="exec-1",
        execution_unit_ids=("u1", "u2"),
        execution_status=DispatchState.STARTED,
        execution_terminal_outcome=None,
        updated_at=LATER,
    )


def test_substitui_vinculo_anterior_com_geracao_seguinte() -> None:
    command = _command(
        generation=2,
        expected_previous_dispatch_id=PREVIOUS,
        expected_previous_execution_ref="exec-0",
    )

    result = apply_execution_binding(_bound(), command)

    assert (result.execution_generation, result.execution_dispatch_id) == (2, DISPATCH)
    assert result.execution_ref == "exec-1"
