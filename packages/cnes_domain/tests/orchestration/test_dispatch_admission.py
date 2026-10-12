"""Admissão do dispatch no gate do ambiente: acquire, reconciliação por probe e liquidação."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest

from cnes_domain.control_plane.entities import RunDispatch, RunUnit
from cnes_domain.control_plane.enums import DispatchState
from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    AcquisitionStatus,
    BindUnitPermit,
    ClosePromotionFence,
    EnvironmentGateState,
    PermitAcquisition,
    PermitHolder,
    PromotionFence,
    ReanchorUnitPermit,
    ReleaseUnitPermit,
    RenewUnitPermit,
    ReopenPromotionFence,
    UnitPermit,
    bind_permit,
    close_fence,
    decide_acquisition,
    issue_permit,
    open_fence,
    reanchor_permit,
    release_permit,
    renew_permit,
    reopen_fence,
)
from cnes_domain.orchestration.dispatch_admission import (
    ABSENCE_PROOF_GRACE_SECONDS,
    EnvironmentAdmission,
    UnitPermitLost,
    dispatch_holder,
    settle_run_dispatches,
)
from cnes_domain.ports.environment_gate import (
    DeferralReason,
    DispatchAdmissionPort,
    DispatchDeferred,
    ExecutionObservation,
    ExecutionPresence,
    UnitAdmission,
)

if TYPE_CHECKING:
    from collections.abc import Callable

NOW = datetime(2026, 10, 11, 12, tzinfo=UTC)
GRACE = timedelta(seconds=ABSENCE_PROOF_GRACE_SECONDS)
SECOND = timedelta(seconds=1)
ENV = "prod"
LEASE = 3600
HOLDER = PermitHolder("354130", "run-01", "0123456789abcdef")
OTHER = PermitHolder("354130", "run-02", "fedcba9876543210")
RIVAL = PermitHolder("354130", "run-03", "00000000000000ff")


def _ref(dispatch_id: str) -> str:
    return f"arn:aws:states:us-east-2:000000000000:execution:cnesdata-prod:{dispatch_id}"


class _MemoryGate:
    """`EnvironmentGatePort` em memória sobre as transições puras do domínio."""

    def __init__(self) -> None:
        self.fence: PromotionFence = open_fence(ENV)
        self.permit: UnitPermit | None = None
        self.tokens = 0
        self.calls: list[str] = []
        self.on_release: Callable[[], object] = lambda: None

    def observe_environment_gate(self, environment: str) -> EnvironmentGateState:
        self.calls.append("observe")
        return EnvironmentGateState(environment, self.fence, self.permit)

    def acquire_unit_permit(self, command: AcquireUnitPermit) -> PermitAcquisition:
        self.calls.append("acquire")
        status = decide_acquisition(
            EnvironmentGateState(command.environment, self.fence, self.permit), command,
        )
        if status is AcquisitionStatus.ACQUIRED:
            self.tokens += 1
            self.permit = issue_permit(command, self.tokens)
        return PermitAcquisition(status, self.permit)

    def bind_unit_permit(self, command: BindUnitPermit) -> bool:
        self.calls.append("bind")
        return self._replace(bind_permit(self.permit, command))

    def renew_unit_permit(self, command: RenewUnitPermit) -> bool:
        self.calls.append("renew")
        return self._replace(renew_permit(self.permit, command))

    def reanchor_unit_permit(self, command: ReanchorUnitPermit) -> bool:
        self.calls.append("reanchor")
        return self._replace(reanchor_permit(self.permit, command))

    def release_unit_permit(self, command: ReleaseUnitPermit) -> bool:
        self.calls.append("release")
        if not release_permit(self.permit, command):
            return False
        self.permit = None
        self.on_release()
        return True

    def close_promotion_fence(self, command: ClosePromotionFence) -> bool:
        closed = close_fence(self.fence, command)
        self.fence = closed or self.fence
        return closed is not None

    def reopen_promotion_fence(self, command: ReopenPromotionFence) -> bool:
        reopened = reopen_fence(self.fence, command)
        self.fence = reopened or self.fence
        return reopened is not None

    def hold(
        self, holder: PermitHolder, *, at: datetime = NOW, execution_ref: str | None = None,
    ) -> UnitPermit:
        self.tokens += 1
        permit = issue_permit(AcquireUnitPermit(ENV, holder, at, LEASE), self.tokens)
        self.permit = replace(permit, execution_ref=execution_ref)
        return self.permit

    def _replace(self, permit: UnitPermit | None) -> bool:
        if permit is None:
            return False
        self.permit = permit
        return True


@dataclass
class _Probe:
    presence: dict[str, ExecutionPresence] = field(default_factory=dict[str, ExecutionPresence])
    calls: list[str] = field(default_factory=list[str])
    error: Exception | None = None

    def probe(self, dispatch_id: str) -> ExecutionObservation:
        self.calls.append(dispatch_id)
        if self.error is not None:
            raise self.error
        presence = self.presence.get(dispatch_id, ExecutionPresence.ABSENT)
        execution_ref = None if presence is ExecutionPresence.ABSENT else _ref(dispatch_id)
        return ExecutionObservation(presence, execution_ref)


@dataclass
class _SettleRecorder:
    settled: list[tuple[PermitHolder, datetime]] = field(
        default_factory=list[tuple[PermitHolder, datetime]],
    )

    def admit(self, holder: PermitHolder, now: datetime) -> UnitAdmission | DispatchDeferred:
        raise AssertionError(holder)

    def confirm(self, admission: UnitAdmission, execution_ref: str, now: datetime) -> None:
        raise AssertionError(admission)

    def settle(self, holder: PermitHolder, now: datetime) -> None:
        self.settled.append((holder, now))

    def reconcile(self, now: datetime) -> None:
        raise AssertionError(now)


@pytest.fixture
def gate() -> _MemoryGate:
    return _MemoryGate()


@pytest.fixture
def probe() -> _Probe:
    return _Probe()


def _service(gate: _MemoryGate, probe: _Probe) -> EnvironmentAdmission:
    return EnvironmentAdmission(gate, probe, ENV, LEASE)


def _current(gate: _MemoryGate) -> UnitPermit:
    assert gate.permit is not None
    return gate.permit


def _closed_fence() -> PromotionFence:
    return cast("PromotionFence", close_fence(open_fence(ENV), ClosePromotionFence(ENV, "r1", NOW)))


def test_admite_o_dispatch_com_o_permit_livre(gate: _MemoryGate, probe: _Probe) -> None:
    admission = _service(gate, probe).admit(HOLDER, NOW)

    assert admission == UnitAdmission(ENV, 1)
    assert gate.permit == issue_permit(AcquireUnitPermit(ENV, HOLDER, NOW, LEASE), 1)
    assert gate.calls == ["acquire"]
    assert probe.calls == []


def test_reentrada_do_mesmo_holder_segue_sem_probe(gate: _MemoryGate, probe: _Probe) -> None:
    gate.hold(HOLDER)

    admission = _service(gate, probe).admit(HOLDER, NOW + GRACE * 2)

    assert admission == UnitAdmission(ENV, 1)
    assert gate.calls == ["acquire", "reanchor"]
    assert probe.calls == []


@pytest.mark.parametrize("orphan", [None, OTHER], ids=["sem-permit", "com-permit-orfao"])
def test_fence_fechado_adia_sem_probe(
    gate: _MemoryGate, probe: _Probe, orphan: PermitHolder | None,
) -> None:
    gate.fence = _closed_fence()
    if orphan is not None:
        gate.hold(orphan)

    admission = _service(gate, probe).admit(HOLDER, NOW + GRACE)

    assert admission == DispatchDeferred(DeferralReason.FENCE_CLOSED)
    assert gate.calls == ["acquire"]
    assert probe.calls == []


def test_permit_de_execucao_terminal_e_liberado_e_readquirido(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    gate.hold(OTHER, execution_ref=_ref(OTHER.dispatch_id))
    probe.presence[OTHER.dispatch_id] = ExecutionPresence.TERMINAL

    admission = _service(gate, probe).admit(HOLDER, NOW + SECOND)

    assert admission == UnitAdmission(ENV, 2)
    assert _current(gate).holder == HOLDER
    assert gate.calls == ["acquire", "release", "acquire"]
    assert probe.calls == [OTHER.dispatch_id]


@pytest.mark.parametrize("elapsed", [GRACE, GRACE + timedelta(hours=1)], ids=["limite", "depois"])
def test_ausencia_provada_apos_a_carencia_libera_o_permit(
    gate: _MemoryGate, probe: _Probe, elapsed: timedelta,
) -> None:
    gate.hold(OTHER)

    admission = _service(gate, probe).admit(HOLDER, NOW + elapsed)

    assert admission == UnitAdmission(ENV, 2)
    assert _current(gate).holder == HOLDER
    assert gate.calls == ["acquire", "release", "acquire"]


def test_ausencia_dentro_da_carencia_nao_libera_o_permit(gate: _MemoryGate, probe: _Probe) -> None:
    permit = gate.hold(OTHER)

    admission = _service(gate, probe).admit(HOLDER, NOW + GRACE - SECOND)

    assert admission == DispatchDeferred(DeferralReason.PERMIT_HELD)
    assert gate.permit == permit
    assert gate.calls == ["acquire"]
    assert probe.calls == [OTHER.dispatch_id]


def test_execucao_viva_sem_bind_e_vinculada_ao_permit(gate: _MemoryGate, probe: _Probe) -> None:
    permit = gate.hold(OTHER)
    probe.presence[OTHER.dispatch_id] = ExecutionPresence.RUNNING

    admission = _service(gate, probe).admit(HOLDER, NOW + GRACE * 4)

    assert admission == DispatchDeferred(DeferralReason.PERMIT_HELD)
    assert gate.permit == replace(permit, execution_ref=_ref(OTHER.dispatch_id))
    assert gate.calls == ["acquire", "bind"]


def test_execucao_viva_com_bind_renova_o_lease(gate: _MemoryGate, probe: _Probe) -> None:
    permit = gate.hold(OTHER, execution_ref=_ref(OTHER.dispatch_id))
    probe.presence[OTHER.dispatch_id] = ExecutionPresence.RUNNING
    later = NOW + GRACE * 4

    admission = _service(gate, probe).admit(HOLDER, later)

    assert admission == DispatchDeferred(DeferralReason.PERMIT_HELD)
    assert gate.permit == replace(permit, lease_until=later + timedelta(seconds=LEASE))
    assert gate.calls == ["acquire", "renew"]


def test_erro_do_probe_propaga_e_nada_e_liberado(gate: _MemoryGate, probe: _Probe) -> None:
    permit = gate.hold(OTHER)
    probe.error = TimeoutError("probe=timeout")

    with pytest.raises(TimeoutError, match="probe=timeout"):
        _service(gate, probe).admit(HOLDER, NOW + GRACE)

    assert gate.permit == permit
    assert gate.calls == ["acquire"]


def test_rival_que_vence_apos_a_liberacao_adia_sem_novo_probe(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    gate.hold(OTHER)
    gate.on_release = lambda: gate.hold(RIVAL, at=NOW + GRACE)

    admission = _service(gate, probe).admit(HOLDER, NOW + GRACE)

    assert admission == DispatchDeferred(DeferralReason.PERMIT_HELD)
    assert _current(gate).holder == RIVAL
    assert gate.calls == ["acquire", "release", "acquire"]
    assert probe.calls == [OTHER.dispatch_id]


def test_confirm_vincula_a_execucao_e_renova_o_lease(gate: _MemoryGate, probe: _Probe) -> None:
    service = _service(gate, probe)
    admission = cast("UnitAdmission", service.admit(HOLDER, NOW))
    started = NOW + timedelta(seconds=5)

    service.confirm(admission, _ref(HOLDER.dispatch_id), started)

    permit = _current(gate)
    assert permit.execution_ref == _ref(HOLDER.dispatch_id)
    assert permit.lease_until == started + timedelta(seconds=LEASE)
    assert gate.calls == ["acquire", "bind", "renew"]


def _released(gate: _MemoryGate) -> None:
    gate.permit = None


def _taken_by_rival(gate: _MemoryGate) -> None:
    gate.hold(RIVAL)


def _bound_elsewhere(gate: _MemoryGate) -> None:
    gate.permit = replace(_current(gate), execution_ref=_ref(RIVAL.dispatch_id))


@pytest.mark.parametrize("lose", [_released, _taken_by_rival, _bound_elsewhere])
def test_confirm_sem_o_permit_falha_fechado(
    gate: _MemoryGate, probe: _Probe, lose: Callable[[_MemoryGate], None],
) -> None:
    service = _service(gate, probe)
    admission = cast("UnitAdmission", service.admit(HOLDER, NOW))
    lose(gate)

    with pytest.raises(UnitPermitLost, match="reason=unit_permit_lost"):
        service.confirm(admission, _ref(HOLDER.dispatch_id), NOW)


def test_confirm_com_renovacao_recusada_falha_fechado(gate: _MemoryGate, probe: _Probe) -> None:
    service = _service(gate, probe)
    admission = cast("UnitAdmission", service.admit(HOLDER, NOW))
    cast("Any", gate).renew_unit_permit = lambda command: False

    with pytest.raises(UnitPermitLost, match="reason=unit_permit_lost"):
        service.confirm(admission, _ref(HOLDER.dispatch_id), NOW)


@pytest.mark.parametrize("admission", [UnitAdmission(None, None), UnitAdmission("staging", 1)])
def test_confirm_rejeita_admissao_de_outro_ambiente(
    gate: _MemoryGate, probe: _Probe, admission: UnitAdmission,
) -> None:
    with pytest.raises(ValueError, match="reason=environment_mismatch"):
        _service(gate, probe).confirm(admission, _ref(HOLDER.dispatch_id), NOW)

    assert gate.calls == []


@pytest.mark.parametrize("owner", [None, OTHER], ids=["permit-livre", "outro-holder"])
def test_settle_sem_o_permit_do_holder_nao_chama_o_probe(
    gate: _MemoryGate, probe: _Probe, owner: PermitHolder | None,
) -> None:
    if owner is not None:
        gate.hold(owner)

    _service(gate, probe).settle(HOLDER, NOW + GRACE)

    assert gate.calls == ["observe"]
    assert probe.calls == []


@pytest.mark.parametrize(
    ("presence", "elapsed"),
    [(ExecutionPresence.TERMINAL, SECOND), (ExecutionPresence.ABSENT, GRACE)],
    ids=["terminal", "ausencia-apos-carencia"],
)
def test_settle_libera_com_prova_do_probe(
    gate: _MemoryGate, probe: _Probe, presence: ExecutionPresence, elapsed: timedelta,
) -> None:
    gate.hold(HOLDER)
    probe.presence[HOLDER.dispatch_id] = presence

    _service(gate, probe).settle(HOLDER, NOW + elapsed)

    assert gate.permit is None
    assert gate.calls == ["observe", "release"]
    assert probe.calls == [HOLDER.dispatch_id]


@pytest.mark.parametrize(
    ("presence", "elapsed", "execution_ref"),
    [
        (ExecutionPresence.RUNNING, GRACE * 4, _ref(HOLDER.dispatch_id)),
        (ExecutionPresence.ABSENT, GRACE - SECOND, None),
    ],
    ids=["execucao-viva", "ausencia-recente"],
)
def test_settle_sem_prova_mantem_o_permit(
    gate: _MemoryGate, probe: _Probe, presence: ExecutionPresence, elapsed: timedelta,
    execution_ref: str | None,
) -> None:
    permit = gate.hold(HOLDER)
    probe.presence[HOLDER.dispatch_id] = presence

    _service(gate, probe).settle(HOLDER, NOW + elapsed)

    assert gate.permit == replace(permit, execution_ref=execution_ref)
    assert "release" not in gate.calls


def test_settle_propaga_erro_do_probe_sem_liberar(gate: _MemoryGate, probe: _Probe) -> None:
    permit = gate.hold(HOLDER)
    probe.error = TimeoutError("probe=timeout")

    with pytest.raises(TimeoutError, match="probe=timeout"):
        _service(gate, probe).settle(HOLDER, NOW + GRACE)

    assert gate.permit == permit
    assert gate.calls == ["observe"]


@pytest.mark.parametrize(
    ("environment", "lease_seconds", "message"),
    [
        ("Prod", LEASE, "reason=invalid_environment"),
        (ENV, 0, "reason=out_of_range field=lease_seconds"),
        (ENV, True, "reason=out_of_range field=lease_seconds"),
    ],
)
def test_construtor_valida_ambiente_e_lease(
    gate: _MemoryGate, probe: _Probe, environment: str, lease_seconds: int, message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        EnvironmentAdmission(gate, probe, environment, lease_seconds)


def test_admissao_do_ambiente_implementa_a_porta(gate: _MemoryGate, probe: _Probe) -> None:
    assert isinstance(_service(gate, probe), DispatchAdmissionPort)
    assert ABSENCE_PROOF_GRACE_SECONDS == 900


def test_holder_do_dispatch_e_a_identidade_da_execucao() -> None:
    dispatch = RunDispatch(
        tenant_id=HOLDER.tenant_id, run_id=HOLDER.run_id, wave_id="1111111111111111",
        dispatch_id=HOLDER.dispatch_id, generation=1, unit_ids=("unit-a",),
        state=DispatchState.RESERVED, lease_until=NOW,
    )

    assert dispatch_holder(dispatch) == HOLDER


def _unit(dispatch_id: str | None) -> RunUnit:
    return RunUnit.model_construct(
        tenant_id=HOLDER.tenant_id, run_id=HOLDER.run_id, dispatch_id=dispatch_id,
    )


def test_liquida_uma_vez_cada_dispatch_registrado_nas_unidades() -> None:
    recorder = _SettleRecorder()
    units = (_unit(OTHER.dispatch_id), _unit(None), _unit(RIVAL.dispatch_id),
             _unit(OTHER.dispatch_id))

    settle_run_dispatches(recorder, units, NOW)

    holders = [replace(HOLDER, dispatch_id=dispatch_id)
               for dispatch_id in (RIVAL.dispatch_id, OTHER.dispatch_id)]
    assert recorder.settled == [(holder, NOW) for holder in holders]


def test_unidades_sem_dispatch_nao_liquidam_nada() -> None:
    recorder = _SettleRecorder()

    settle_run_dispatches(recorder, (_unit(None),), NOW)

    assert recorder.settled == []
