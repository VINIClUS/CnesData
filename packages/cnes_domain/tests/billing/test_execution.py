"""Testes dos modelos de execução e publicação de runs faturadas."""

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest

from cnes_domain.billing.execution import (
    PublicationGuard,
    RunBillingState,
    RunExecutionBindingCommand,
    RunExecutionPermit,
)
from cnes_domain.billing.models import RunAuthorization
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState

_NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
_NAIVE = _NOW.replace(tzinfo=None)
_WAVE = "0123456789abcdef"
_DISPATCH = "fedcba9876543210"
_PREVIOUS = "aaaaaaaaaaaaaaaa"
_BINDING_FIELDS = (
    "execution_wave_id",
    "execution_dispatch_id",
    "execution_ref",
    "execution_status",
    "execution_terminal_outcome",
)


def _authorization(**overrides: Any) -> RunAuthorization:
    values: dict[str, Any] = {
        "billing_account_id": "acct-1",
        "plan_version_id": "plan-1",
        "entitlement_version": 3,
        "max_concurrency": 2,
        "budget_reservation_id": "res-1",
        "authorized_at": _NOW,
    }
    return RunAuthorization(**{**values, **overrides})


def _permit(**overrides: Any) -> RunExecutionPermit:
    values: dict[str, Any] = {
        "billing_account_id": "acct-1",
        "wave_id": _WAVE,
        "dispatch_id": _DISPATCH,
        "generation": 2,
        "expected_previous_dispatch_id": _PREVIOUS,
        "expected_previous_execution_ref": "exec-0",
        "expected_entitlement_version": 3,
        "expected_fencing_token": 7,
        "authorized_at": _NOW,
    }
    return RunExecutionPermit(**{**values, **overrides})


def _command(**overrides: Any) -> RunExecutionBindingCommand:
    values: dict[str, Any] = {
        "tenant_id": "tenant-1",
        "run_id": "run-1",
        "wave_id": _WAVE,
        "dispatch_id": _DISPATCH,
        "generation": 2,
        "execution_ref": "exec-1",
        "unit_ids": ("u1", "u2"),
        "expected_previous_dispatch_id": _PREVIOUS,
        "expected_previous_execution_ref": "exec-0",
        "expected_entitlement_version": 3,
        "expected_fencing_token": 7,
        "bound_at": _NOW,
    }
    return RunExecutionBindingCommand(**{**values, **overrides})


def _state(**overrides: Any) -> RunBillingState:
    values: dict[str, Any] = {
        "billing_account_id": "acct-1",
        "tenant_id": "tenant-1",
        "run_id": "run-1",
        "authorization": _authorization(),
        "execution_generation": 2,
        "execution_wave_id": _WAVE,
        "execution_dispatch_id": _DISPATCH,
        "execution_ref": "exec-1",
        "execution_unit_ids": ("u1", "u2"),
        "execution_status": DispatchState.STARTED,
        "execution_terminal_outcome": None,
        "fencing_token": 7,
        "cancel_requested": False,
        "updated_at": _NOW,
    }
    return RunBillingState(**{**values, **overrides})


def _initial_state(**overrides: Any) -> RunBillingState:
    values: dict[str, Any] = {
        "execution_generation": 0,
        "execution_wave_id": None,
        "execution_dispatch_id": None,
        "execution_ref": None,
        "execution_unit_ids": (),
        "execution_status": None,
        "execution_terminal_outcome": None,
    }
    return _state(**{**values, **overrides})


def _guard(**overrides: Any) -> PublicationGuard:
    values: dict[str, Any] = {
        "billing_account_id": "acct-1",
        "expected_entitlement_version": 3,
        "expected_run_fencing_token": 7,
        "checked_at": _NOW,
    }
    return PublicationGuard(**{**values, **overrides})


_BINDING_FACTORIES = [_permit, _command]


def test_permit_instancia_todos_os_campos() -> None:
    permit = _permit()

    assert permit.billing_account_id == "acct-1"
    assert permit.wave_id == _WAVE
    assert permit.dispatch_id == _DISPATCH
    assert permit.generation == 2
    assert permit.expected_previous_dispatch_id == _PREVIOUS
    assert permit.expected_previous_execution_ref == "exec-0"
    assert permit.expected_entitlement_version == 3
    assert permit.expected_fencing_token == 7
    assert permit.authorized_at == _NOW


def test_binding_command_instancia_todos_os_campos() -> None:
    command = _command()

    assert command.tenant_id == "tenant-1"
    assert command.run_id == "run-1"
    assert command.wave_id == _WAVE
    assert command.dispatch_id == _DISPATCH
    assert command.generation == 2
    assert command.execution_ref == "exec-1"
    assert command.unit_ids == ("u1", "u2")
    assert command.expected_previous_dispatch_id == _PREVIOUS
    assert command.expected_previous_execution_ref == "exec-0"
    assert command.expected_entitlement_version == 3
    assert command.expected_fencing_token == 7
    assert command.bound_at == _NOW


def test_run_billing_state_instancia_todos_os_campos() -> None:
    authorization = _authorization()
    state = _state(
        authorization=authorization,
        execution_status=DispatchState.TERMINAL,
        execution_terminal_outcome=DispatchOutcome.SUCCEEDED,
        cancel_requested=True,
    )

    assert state.billing_account_id == "acct-1"
    assert state.tenant_id == "tenant-1"
    assert state.run_id == "run-1"
    assert state.authorization is authorization
    assert state.execution_generation == 2
    assert state.execution_wave_id == _WAVE
    assert state.execution_dispatch_id == _DISPATCH
    assert state.execution_ref == "exec-1"
    assert state.execution_unit_ids == ("u1", "u2")
    assert state.execution_status is DispatchState.TERMINAL
    assert state.execution_terminal_outcome is DispatchOutcome.SUCCEEDED
    assert state.fencing_token == 7
    assert state.cancel_requested is True
    assert state.updated_at == _NOW


def test_publication_guard_instancia_todos_os_campos() -> None:
    guard = _guard()

    assert guard.billing_account_id == "acct-1"
    assert guard.expected_entitlement_version == 3
    assert guard.expected_run_fencing_token == 7
    assert guard.checked_at == _NOW


@pytest.mark.parametrize("factory", _BINDING_FACTORIES)
def test_binding_aceita_primeira_geracao_sem_par_anterior(factory: Any) -> None:
    built = factory(
        generation=1,
        expected_previous_dispatch_id=None,
        expected_previous_execution_ref=None,
    )

    assert built.expected_previous_dispatch_id is None
    assert built.expected_previous_execution_ref is None


@pytest.mark.parametrize("factory", _BINDING_FACTORIES)
@pytest.mark.parametrize("field", ["wave_id", "dispatch_id"])
@pytest.mark.parametrize("value", ["ABCDEF0123456789", "abc", "g" * 16, "", 5])
def test_binding_exige_wave_e_dispatch_hex_minusculo(
    factory: Any, field: str, value: Any
) -> None:
    with pytest.raises(ValueError, match=f"reason=invalid_hex16 field={field}"):
        factory(**{field: value})


@pytest.mark.parametrize("factory", _BINDING_FACTORIES)
@pytest.mark.parametrize("generation", [0, -1, True, "1"])
def test_binding_exige_generation_positiva(factory: Any, generation: Any) -> None:
    with pytest.raises(ValueError, match="reason=positive_value_required field=generation"):
        factory(generation=generation)


@pytest.mark.parametrize("factory", _BINDING_FACTORIES)
@pytest.mark.parametrize(
    ("dispatch", "ref"),
    [(_PREVIOUS, None), (None, "exec-0")],
)
def test_binding_exige_par_anterior_completo_ou_ausente(
    factory: Any, dispatch: str | None, ref: str | None
) -> None:
    with pytest.raises(ValueError, match="reason=previous_binding_incomplete"):
        factory(expected_previous_dispatch_id=dispatch, expected_previous_execution_ref=ref)


@pytest.mark.parametrize("factory", _BINDING_FACTORIES)
@pytest.mark.parametrize(
    ("dispatch", "ref"),
    [(None, None), (_PREVIOUS, "exec-0")],
)
def test_binding_aceita_par_anterior_completo_ou_ausente(
    factory: Any, dispatch: str | None, ref: str | None
) -> None:
    built = factory(
        expected_previous_dispatch_id=dispatch, expected_previous_execution_ref=ref
    )

    assert built.expected_previous_dispatch_id == dispatch
    assert built.expected_previous_execution_ref == ref


@pytest.mark.parametrize("factory", _BINDING_FACTORIES)
@pytest.mark.parametrize("dispatch", ["XYZ", "", "A" * 16])
def test_binding_rejeita_dispatch_anterior_invalido(factory: Any, dispatch: str) -> None:
    with pytest.raises(ValueError, match="field=expected_previous_dispatch_id"):
        factory(expected_previous_dispatch_id=dispatch)


@pytest.mark.parametrize("factory", _BINDING_FACTORIES)
def test_binding_rejeita_ref_anterior_em_branco(factory: Any) -> None:
    with pytest.raises(ValueError, match="field=expected_previous_execution_ref"):
        factory(expected_previous_execution_ref=" ")


@pytest.mark.parametrize("factory", _BINDING_FACTORIES)
@pytest.mark.parametrize("version", [0, -1])
def test_binding_exige_versao_de_entitlement_positiva(factory: Any, version: int) -> None:
    with pytest.raises(ValueError, match="field=expected_entitlement_version"):
        factory(expected_entitlement_version=version)


@pytest.mark.parametrize("factory", _BINDING_FACTORIES)
def test_binding_rejeita_fencing_token_negativo(factory: Any) -> None:
    with pytest.raises(ValueError, match="field=expected_fencing_token"):
        factory(expected_fencing_token=-1)


def test_binding_aceita_fencing_token_zero() -> None:
    assert _permit(expected_fencing_token=0).expected_fencing_token == 0
    assert _command(expected_fencing_token=0).expected_fencing_token == 0


@pytest.mark.parametrize(
    ("factory", "field"),
    [
        (_permit, "billing_account_id"),
        (_command, "tenant_id"),
        (_command, "run_id"),
        (_command, "execution_ref"),
    ],
)
def test_binding_rejeita_identificador_em_branco(factory: Any, field: str) -> None:
    with pytest.raises(ValueError, match=f"reason=blank_value field={field}"):
        factory(**{field: "  "})


@pytest.mark.parametrize(
    ("factory", "field"),
    [(_permit, "authorized_at"), (_command, "bound_at")],
)
@pytest.mark.parametrize("value", [_NAIVE, "2026-09-01"])
def test_binding_exige_instante_utc(factory: Any, field: str, value: Any) -> None:
    with pytest.raises(ValueError, match=f"reason=datetime_not_utc field={field}"):
        factory(**{field: value})


_INVALID_UNIT_IDS = [
    ((), "unit_ids_required"),
    (("u1", "u1"), "duplicate_value"),
    (("u1", " "), "blank_value"),
]


@pytest.mark.parametrize(("unit_ids", "reason"), _INVALID_UNIT_IDS)
def test_binding_exige_unit_ids_unicos_e_nao_vazios(
    unit_ids: tuple[str, ...], reason: str
) -> None:
    with pytest.raises(ValueError, match=f"reason={reason}"):
        _command(unit_ids=unit_ids)


def test_binding_command_rejeita_unit_ids_vazio_com_codigo_proprio() -> None:
    with pytest.raises(ValueError, match="reason=unit_ids_required"):
        _command(unit_ids=())


def test_binding_command_e_imutavel_por_replace_revalidado() -> None:
    with pytest.raises(ValueError, match="reason=unit_ids_required"):
        replace(_command(), unit_ids=())


def test_run_billing_state_inicial_sem_binding() -> None:
    state = _initial_state()

    assert state.execution_generation == 0
    assert state.execution_unit_ids == ()
    assert all(getattr(state, name) is None for name in _BINDING_FIELDS)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("execution_wave_id", _WAVE),
        ("execution_dispatch_id", _DISPATCH),
        ("execution_ref", "exec-1"),
        ("execution_unit_ids", ("u1",)),
        ("execution_status", DispatchState.RESERVED),
        ("execution_terminal_outcome", DispatchOutcome.FAILED),
    ],
)
def test_run_billing_state_inicial_rejeita_qualquer_binding(field: str, value: Any) -> None:
    with pytest.raises(ValueError, match="reason=unbound_execution_has_binding"):
        _initial_state(**{field: value})


def test_run_billing_state_vinculado_exige_status() -> None:
    with pytest.raises(ValueError, match="reason=bound_execution_requires_status"):
        _state(execution_status=None)


def test_run_billing_state_vinculado_aceita_ref_ausente() -> None:
    state = _state(execution_ref=None, execution_status=DispatchState.RESERVED)

    assert state.execution_ref is None


def test_run_billing_state_vinculado_rejeita_ref_em_branco() -> None:
    with pytest.raises(ValueError, match="field=execution_ref"):
        _state(execution_ref=" ")


@pytest.mark.parametrize("field", ["execution_wave_id", "execution_dispatch_id"])
@pytest.mark.parametrize("value", [None, "ZZ", ""])
def test_run_billing_state_vinculado_exige_wave_e_dispatch_hex(field: str, value: Any) -> None:
    with pytest.raises(ValueError, match=f"reason=invalid_hex16 field={field}"):
        _state(**{field: value})


@pytest.mark.parametrize(("unit_ids", "reason"), _INVALID_UNIT_IDS)
def test_run_billing_state_vinculado_exige_unit_ids_unicos_e_nao_vazios(
    unit_ids: tuple[str, ...], reason: str
) -> None:
    with pytest.raises(ValueError, match=f"reason={reason}"):
        _state(execution_unit_ids=unit_ids)


def test_outcome_terminal_so_com_estado_terminal() -> None:
    with pytest.raises(ValueError, match="reason=terminal_outcome_mismatch"):
        _state(execution_status=DispatchState.TERMINAL, execution_terminal_outcome=None)
    with pytest.raises(ValueError, match="reason=terminal_outcome_mismatch"):
        _state(
            execution_status=DispatchState.STARTED,
            execution_terminal_outcome=DispatchOutcome.FAILED,
        )

    terminal = _state(
        execution_status=DispatchState.TERMINAL,
        execution_terminal_outcome=DispatchOutcome.CANCELED,
    )

    assert terminal.execution_terminal_outcome is DispatchOutcome.CANCELED


def test_run_billing_state_rejeita_autorizacao_de_outra_conta() -> None:
    with pytest.raises(ValueError, match="reason=authorization_account_mismatch"):
        _state(authorization=_authorization(billing_account_id="acct-2"))


@pytest.mark.parametrize("field", ["billing_account_id", "tenant_id", "run_id"])
def test_run_billing_state_rejeita_identificador_em_branco(field: str) -> None:
    with pytest.raises(ValueError, match=f"reason=blank_value field={field}"):
        _state(**{field: ""})


@pytest.mark.parametrize("field", ["execution_generation", "fencing_token"])
def test_run_billing_state_rejeita_contador_negativo(field: str) -> None:
    with pytest.raises(ValueError, match=f"reason=negative_value field={field}"):
        _state(**{field: -1})


def test_run_billing_state_rejeita_cancel_requested_nao_booleano() -> None:
    with pytest.raises(ValueError, match="reason=cancel_requested_not_bool"):
        _state(cancel_requested=1)


def test_run_billing_state_exige_updated_at_utc() -> None:
    with pytest.raises(ValueError, match="reason=datetime_not_utc field=updated_at"):
        _state(updated_at=_NAIVE)


def test_publication_guard_rejeita_conta_em_branco() -> None:
    with pytest.raises(ValueError, match="reason=blank_value field=billing_account_id"):
        _guard(billing_account_id="")


@pytest.mark.parametrize("version", [0, -1])
def test_publication_guard_exige_versao_positiva(version: int) -> None:
    with pytest.raises(ValueError, match="field=expected_entitlement_version"):
        _guard(expected_entitlement_version=version)


def test_publication_guard_rejeita_fencing_negativo_e_aceita_zero() -> None:
    with pytest.raises(ValueError, match="field=expected_run_fencing_token"):
        _guard(expected_run_fencing_token=-1)

    assert _guard(expected_run_fencing_token=0).expected_run_fencing_token == 0


def test_publication_guard_exige_instante_utc() -> None:
    with pytest.raises(ValueError, match="reason=datetime_not_utc field=checked_at"):
        _guard(checked_at=_NAIVE)
