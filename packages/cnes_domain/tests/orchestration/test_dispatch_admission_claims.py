"""Claims nativos: reconciliação exige vínculo e prova antes de liberar o permit."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import timedelta
from typing import TYPE_CHECKING, cast

import pytest

from cnes_domain.control_plane.dispatch_start import DispatchStartClaim
from cnes_domain.control_plane.entities import RunDispatch
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState
from cnes_domain.control_plane.execution_quota import execution_period
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission, dispatch_holder
from cnes_domain.ports.environment_gate import (
    DeferralReason,
    DispatchDeferred,
    ExecutionObservation,
    ExecutionPresence,
    UnitAdmission,
)
from packages.cnes_domain.tests.orchestration.test_dispatch_admission import (
    ENV,
    GRACE,
    HOLDER,
    LEASE,
    NOW,
    OTHER,
    SECOND,
    _MemoryGate,
    _ref,
)

if TYPE_CHECKING:
    from cnes_domain.control_plane.commands import (
        BindRunDispatch,
        CancelReservedRunDispatch,
        FinishRunDispatch,
    )
    from cnes_domain.control_plane.environment_gate import AcquireUnitPermit, ReleaseUnitPermit
    from cnes_domain.ports.control_plane import ControlPlanePort

HASH = "a" * 64


def _dispatch() -> RunDispatch:
    return RunDispatch(
        tenant_id=HOLDER.tenant_id,
        run_id=HOLDER.run_id,
        dispatch_id=HOLDER.dispatch_id,
        wave_id="a" * 16,
        generation=1,
        unit_ids=("unit",),
        state=DispatchState.RESERVED,
        lease_until=NOW + GRACE,
    )


def _claim() -> DispatchStartClaim:
    return DispatchStartClaim(
        dispatch_id=HOLDER.dispatch_id,
        generation=1,
        permit_token=1,
        fence_version=0,
        request_sha256=HASH,
    )


class _ControlBoundary:
    def __init__(self) -> None:
        self.dispatch: RunDispatch | None = _dispatch()
        self.writes: list[object] = []
        self.cancel_allowed = True

    def get_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None:
        assert (tenant_id, run_id) == (HOLDER.tenant_id, HOLDER.run_id)
        return self.dispatch

    def cancel_reserved_run_dispatch(
        self, command: CancelReservedRunDispatch
    ) -> RunDispatch | None:
        self.writes.append(command)
        if not self.cancel_allowed:
            return None
        assert self.dispatch is not None
        self.dispatch = self.dispatch.model_copy(
            update={
                "state": DispatchState.TERMINAL,
                "terminal_outcome": DispatchOutcome.CANCELED,
                "start_claim": command.claim,
                "canceled_before_send": True,
            }
        )
        return self.dispatch

    def bind_run_dispatch(self, command: BindRunDispatch) -> RunDispatch:
        self.writes.append(command)
        assert self.dispatch is not None
        self.dispatch = self.dispatch.model_copy(
            update={
                "state": DispatchState.STARTED,
                "execution_ref": command.execution_ref,
            }
        )
        return self.dispatch

    def finish_run_dispatch(self, command: FinishRunDispatch) -> RunDispatch:
        self.writes.append(command)
        assert self.dispatch is not None
        self.dispatch = self.dispatch.model_copy(
            update={
                "state": DispatchState.TERMINAL,
                "terminal_outcome": command.outcome,
            }
        )
        return self.dispatch


class _NativeGate(_MemoryGate):
    def __init__(self) -> None:
        super().__init__()
        self.releases: list[ReleaseUnitPermit] = []

    def acquire_unit_permit(self, command: AcquireUnitPermit):
        result = super().acquire_unit_permit(command)
        assert result.permit is not None
        self.permit = replace(
            result.permit,
            generation=1,
            request_sha256=HASH,
            start_state="RESERVED",
            fence_version=0,
        )
        return replace(result, permit=self.permit)

    def release_unit_permit(self, command: ReleaseUnitPermit) -> bool:
        self.releases.append(command)
        return super().release_unit_permit(command)


class _NativeProbe:
    def __init__(self) -> None:
        self.observation = ExecutionObservation(ExecutionPresence.ABSENT, None)
        self.calls: list[str] = []

    def probe(self, dispatch_id: str) -> ExecutionObservation:
        self.calls.append(dispatch_id)
        return self.observation


@pytest.fixture
def native():
    gate, probe, control = _NativeGate(), _NativeProbe(), _ControlBoundary()
    gate.permit = replace(
        gate.hold(HOLDER),
        generation=1,
        request_sha256=HASH,
        start_state="RESERVED",
        fence_version=0,
    )
    service = EnvironmentAdmission(gate, probe, ENV, LEASE, cast("ControlPlanePort", control))
    return gate, probe, control, service


@pytest.mark.parametrize("cancel_allowed", [False, True])
def test_ausencia_so_libera_apos_cancelamento_duravel(native, cancel_allowed: bool) -> None:
    gate, probe, control, service = native
    original = gate.permit
    control.cancel_allowed = cancel_allowed
    service.reconcile(NOW + SECOND)
    assert probe.calls == [HOLDER.dispatch_id]
    assert len(control.writes) == 1
    command = control.writes[0]
    assert command.claim == _claim()
    assert command.canceled_at == NOW + SECOND
    if cancel_allowed:
        assert gate.permit is None
        assert control.dispatch.canceled_before_send
        proof = json.loads(gate.releases[0].proof)
        assert proof["resolution"] == "CANCEL_BEFORE_SEND"
        assert proof["cancellation"]["canceled_before_send"] is True
        assert proof["cancellation"]["start_claim"]["permit_token"] == original.token
    else:
        assert gate.permit == original
        assert gate.releases == []
        assert control.dispatch == _dispatch()


@pytest.mark.parametrize("mismatch", ["missing", "identity", "generation", "digest", "starting"])
def test_ausencia_sem_identidade_reservada_nao_cancela_nem_libera(native, mismatch: str) -> None:
    gate, _probe, control, service = native
    if mismatch == "missing":
        control.dispatch = None
    elif mismatch == "identity":
        control.dispatch = _dispatch().model_copy(update={"dispatch_id": OTHER.dispatch_id})
    elif mismatch == "generation":
        control.dispatch = _dispatch().model_copy(update={"generation": 2})
    elif mismatch == "digest":
        gate.permit = replace(gate.permit, request_sha256=None)
    else:
        gate.permit = replace(gate.permit, start_state="STARTING")
    original = gate.permit
    service.reconcile(NOW + GRACE * 3)
    assert gate.permit == original
    assert gate.releases == control.writes == []


@pytest.mark.parametrize("valid", [False, True])
def test_replay_do_cancelamento_exige_claim_e_marca_duravel(native, valid: bool) -> None:
    gate, _probe, control, service = native
    control.dispatch = _dispatch().model_copy(
        update={
            "state": DispatchState.TERMINAL,
            "terminal_outcome": DispatchOutcome.CANCELED,
            "start_claim": _claim(),
            "canceled_before_send": valid,
        }
    )
    service.reconcile(NOW)
    assert (gate.permit is None) is valid
    assert control.writes == []


@pytest.mark.parametrize("cancel_allowed", [False, True])
def test_quota_esgotada_so_libera_permit_nativo_depois_de_cancelar(native, cancel_allowed) -> None:
    gate, probe, control, service = native
    gate.permit = None
    gate.tokens = 0
    gate.counted[execution_period(NOW)] = {f"{index:016x}" for index in range(200)}
    control.cancel_allowed = cancel_allowed
    assert service.admit(HOLDER, NOW) == DispatchDeferred(DeferralReason.MONTHLY_ATTEMPTS_EXHAUSTED)
    assert (gate.permit is None) is cancel_allowed
    assert len(control.writes) == 1
    assert probe.calls == []
    assert len(gate.counted[execution_period(NOW)]) == 200


def test_admissao_nativa_carrega_versao_do_fence(native) -> None:
    gate, probe, control, service = native
    gate.permit = None
    gate.tokens = 0
    gate.fence = replace(gate.fence, version=9)
    assert service.admit(HOLDER, NOW) == UnitAdmission(ENV, 1, 9)
    assert control.writes == []
    assert probe.calls == []


@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED", "UNKNOWN", None])
def test_terminal_vinculado_conclui_dispatch_e_libera_com_prova(native, status: str | None) -> None:
    gate, probe, control, service = native
    control.dispatch = _dispatch().model_copy(
        update={
            "state": DispatchState.STARTING,
            "start_claim": _claim(),
        }
    )
    evidence = json.dumps({"status": status}).encode() if status else None
    probe.observation = ExecutionObservation(
        ExecutionPresence.TERMINAL, _ref(HOLDER.dispatch_id), HASH, evidence
    )
    service.reconcile(NOW + SECOND)
    if status == "UNKNOWN":
        assert gate.permit is not None
        assert gate.releases == control.writes == []
    else:
        assert gate.permit is None
        proof = json.loads(gate.releases[0].proof)
        assert proof["resolution"] == "TERMINAL"
        assert proof["observation"] == ({"status": status} if status else None)
        if status:
            expected = {
                "SUCCEEDED": DispatchOutcome.SUCCEEDED,
                "FAILED": DispatchOutcome.FAILED,
                "TIMED_OUT": DispatchOutcome.FAILED,
                "ABORTED": DispatchOutcome.CANCELED,
            }
            assert control.dispatch.terminal_outcome is expected[status]
            assert control.writes[0].claim == _claim()
            assert control.writes[0].finished_at == NOW + SECOND
        else:
            assert control.writes == []


@pytest.mark.parametrize(
    "mismatch", ["missing", "identity", "claim", "token", "generation", "digest", "execution"]
)
def test_terminal_sem_vinculo_autenticado_preserva_permit_e_dispatch(native, mismatch) -> None:
    gate, probe, control, service = native
    claim = _claim()
    if mismatch in {"token", "generation"}:
        claim = claim.model_copy(
            update={"permit_token" if mismatch == "token" else "generation": 2}
        )
    control.dispatch = _dispatch().model_copy(
        update={
            "state": DispatchState.STARTING,
            "start_claim": None if mismatch == "claim" else claim,
        }
    )
    if mismatch == "missing":
        control.dispatch = None
    elif mismatch == "identity":
        control.dispatch = control.dispatch.model_copy(update={"dispatch_id": OTHER.dispatch_id})
    elif mismatch == "execution":
        gate.permit = replace(gate.permit, execution_ref=_ref(OTHER.dispatch_id))
    probe.observation = ExecutionObservation(
        ExecutionPresence.TERMINAL,
        _ref(HOLDER.dispatch_id),
        "b" * 64 if mismatch == "digest" else HASH,
    )
    original = gate.permit
    service.reconcile(NOW)
    assert gate.permit == original
    assert gate.releases == control.writes == []


@pytest.mark.parametrize("mismatch", ["missing", "identity", "claim", "token", "digest", None])
def test_execucao_viva_so_vincula_dispatch_e_permit_com_claim_autenticado(native, mismatch) -> None:
    gate, probe, control, service = native
    claim = _claim().model_copy(update={"permit_token": 2}) if mismatch == "token" else _claim()
    control.dispatch = _dispatch().model_copy(
        update={
            "state": DispatchState.STARTING,
            "start_claim": None if mismatch == "claim" else claim,
        }
    )
    if mismatch == "missing":
        control.dispatch = None
    elif mismatch == "identity":
        control.dispatch = control.dispatch.model_copy(update={"dispatch_id": OTHER.dispatch_id})
    probe.observation = ExecutionObservation(
        ExecutionPresence.RUNNING,
        _ref(HOLDER.dispatch_id),
        "b" * 64 if mismatch == "digest" else HASH,
    )
    original = gate.permit
    service.reconcile(NOW + SECOND)
    assert gate.releases == []
    if mismatch:
        assert gate.permit == original
        assert control.writes == []
    else:
        assert gate.permit.execution_ref == _ref(HOLDER.dispatch_id)
        assert control.dispatch is not None
        assert control.dispatch.state is DispatchState.STARTED
        assert control.writes[0].claim == _claim()
        assert control.writes[0].now == NOW + SECOND
        assert control.writes[0].lease_seconds == LEASE


def test_holder_vincula_request_canonico_sem_alterar_identidade() -> None:
    holder = dispatch_holder(_dispatch(), {"unit": "unit"})
    assert holder == HOLDER
    assert holder.generation == 1
    assert holder.request_sha256 is not None


def test_replay_terminal_nao_regrava_o_dispatch(native) -> None:
    gate, probe, control, service = native
    control.dispatch = _dispatch().model_copy(
        update={
            "state": DispatchState.TERMINAL,
            "start_claim": _claim(),
            "terminal_outcome": DispatchOutcome.SUCCEEDED,
        }
    )
    probe.observation = ExecutionObservation(
        ExecutionPresence.TERMINAL,
        _ref(HOLDER.dispatch_id),
        HASH,
        b'{"status":"SUCCEEDED"}',
    )
    original = control.dispatch
    service.reconcile(NOW)
    assert gate.permit is None
    assert control.dispatch == original
    assert control.writes == []


def test_execucao_iniciada_renova_permit_sem_repetir_bind_do_dispatch(native) -> None:
    gate, probe, control, service = native
    ref = _ref(HOLDER.dispatch_id)
    control.dispatch = _dispatch().model_copy(
        update={
            "state": DispatchState.STARTED,
            "start_claim": _claim(),
            "execution_ref": ref,
        }
    )
    gate.permit = replace(gate.permit, execution_ref=ref)
    probe.observation = ExecutionObservation(ExecutionPresence.RUNNING, ref, HASH)
    service.reconcile(NOW + SECOND)
    assert gate.permit.lease_until == NOW + SECOND + timedelta(seconds=LEASE)
    assert control.writes == gate.releases == []


@pytest.mark.parametrize(
    ("presence", "bound", "elapsed", "expected"),
    [
        (ExecutionPresence.TERMINAL, True, SECOND, True),
        (ExecutionPresence.RUNNING, True, GRACE * 2, False),
        (ExecutionPresence.ABSENT, True, GRACE * 2, False),
        (ExecutionPresence.ABSENT, False, GRACE - SECOND, False),
        (ExecutionPresence.ABSENT, False, GRACE, True),
    ],
)
def test_prova_de_fim_preserva_execucao_vinculada_e_carencia(presence, bound, elapsed, expected):
    from cnes_domain.orchestration.dispatch_admission import _proves_end

    ref = _ref(HOLDER.dispatch_id)
    permit = _MemoryGate().hold(HOLDER, execution_ref=ref if bound else None)
    observation = ExecutionObservation(
        presence, None if presence is ExecutionPresence.ABSENT else ref
    )
    assert _proves_end(observation, permit, NOW + elapsed) is expected
