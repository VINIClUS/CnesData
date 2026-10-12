"""Reancoragem da carência na reentrada e reconciliação periódica do semáforo de unidade."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest

from cnes_domain.control_plane.environment_gate import (
    BindUnitPermit,
    ClosePromotionFence,
    PermitHolder,
)
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
    _bound_elsewhere,
    _current,
    _MemoryGate,
    _Probe,
    _ref,
    _released,
    _service,
    _taken_by_rival,
    gate,
    probe,
)

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["gate", "probe"]
LATE = NOW + timedelta(minutes=30)
MINUTE = timedelta(minutes=1)
PERMIT_HELD = DispatchDeferred(DeferralReason.PERMIT_HELD)
FENCE_CLOSED = DispatchDeferred(DeferralReason.FENCE_CLOSED)
_LOGGER = "cnes_domain.orchestration.dispatch_admission"


def _nothing() -> None:
    return None


def _close_fence(gate: _MemoryGate, at: timedelta = timedelta(0)) -> None:
    assert gate.close_promotion_fence(ClosePromotionFence(ENV, "release-9", NOW + at))


@dataclass(frozen=True)
class _Common:
    """Gate antes do admit de HOLDER num caminho comum: tudo menos a reentrada sem bind."""

    owner: PermitHolder | None
    bound: bool = False
    presence: ExecutionPresence = ExecutionPresence.ABSENT
    fenced: bool = False


def _arrange(gate: _MemoryGate, probe: _Probe, common: _Common) -> None:
    if common.fenced:
        _close_fence(gate)
    if common.owner is not None:
        execution_ref = _ref(common.owner.dispatch_id) if common.bound else None
        gate.hold(common.owner, at=LATE, execution_ref=execution_ref)
        probe.presence[common.owner.dispatch_id] = common.presence


@dataclass
class _ProbeDuring(_Probe):
    """Probe que executa uma ação concorrente antes de observar a execução."""

    during: Callable[[], object] = _nothing

    def probe(self, dispatch_id: str) -> ExecutionObservation:
        during, self.during = self.during, _nothing
        during()
        return super().probe(dispatch_id)


def test_reentrada_tardia_sem_bind_reancora_a_carencia(gate: _MemoryGate, probe: _Probe) -> None:
    gate.hold(HOLDER, at=NOW)
    service = _service(gate, probe)

    assert service.admit(HOLDER, LATE) == UnitAdmission(ENV, 1)
    service.settle(HOLDER, LATE)

    assert _current(gate).acquired_at == LATE
    assert gate.calls == ["acquire", "observe", "reanchor", "observe"]
    assert probe.calls == [HOLDER.dispatch_id]
    assert service.admit(OTHER, LATE + GRACE - SECOND) == PERMIT_HELD
    assert service.admit(OTHER, LATE + GRACE) == UnitAdmission(ENV, 2)


def test_reentrada_sem_bind_com_fence_fechado_e_execucao_ausente_adia_sem_reancorar(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    gate.hold(HOLDER, at=NOW)
    _close_fence(gate, at=5 * MINUTE)
    service = _service(gate, probe)

    assert service.admit(HOLDER, NOW + 6 * MINUTE) == FENCE_CLOSED

    assert gate.calls == ["acquire", "observe"]
    assert probe.calls == [HOLDER.dispatch_id]
    assert _current(gate).acquired_at == NOW
    service.reconcile(NOW + GRACE)
    assert gate.permit is None


@pytest.mark.parametrize("presence", [ExecutionPresence.RUNNING, ExecutionPresence.TERMINAL])
def test_reentrada_sem_bind_com_fence_fechado_e_execucao_existente_reancora_e_segue(
    gate: _MemoryGate, probe: _Probe, presence: ExecutionPresence,
) -> None:
    gate.hold(HOLDER, at=NOW)
    _close_fence(gate)
    probe.presence[HOLDER.dispatch_id] = presence

    assert _service(gate, probe).admit(HOLDER, LATE) == UnitAdmission(ENV, 1)

    assert gate.calls == ["acquire", "observe", "reanchor"]
    assert probe.calls == [HOLDER.dispatch_id]
    assert _current(gate).acquired_at == LATE


def test_erro_do_probe_na_reentrada_com_fence_fechado_propaga_sem_reancorar(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    permit = gate.hold(HOLDER, at=NOW)
    _close_fence(gate)
    probe.error = TimeoutError("probe=throttled")

    with pytest.raises(TimeoutError, match="probe=throttled"):
        _service(gate, probe).admit(HOLDER, LATE)

    assert gate.permit == permit
    assert gate.calls == ["acquire", "observe"]


@pytest.mark.parametrize(
    ("common", "expected", "calls", "probes"),
    [
        pytest.param(_Common(None), UnitAdmission(ENV, 1), ["acquire"], [], id="acquired"),
        pytest.param(_Common(OTHER), PERMIT_HELD, ["acquire"], [OTHER], id="held"),
        pytest.param(
            _Common(OTHER, bound=True, presence=ExecutionPresence.TERMINAL),
            UnitAdmission(ENV, 2), ["acquire", "release", "acquire"], [OTHER], id="held-terminal",
        ),
        pytest.param(
            _Common(OTHER, bound=True, presence=ExecutionPresence.RUNNING),
            PERMIT_HELD, ["acquire", "renew"], [OTHER], id="held-vivo",
        ),
        pytest.param(_Common(None, fenced=True), FENCE_CLOSED, ["acquire"], [], id="fence"),
        pytest.param(_Common(OTHER, fenced=True), FENCE_CLOSED, ["acquire"], [], id="fence-held"),
        pytest.param(
            _Common(HOLDER, bound=True, fenced=True), UnitAdmission(ENV, 1), ["acquire"], [],
            id="reentrada-com-bind",
        ),
    ],
)
def test_caminho_comum_do_admit_nao_ganha_leitura_nem_probe(
    gate: _MemoryGate, probe: _Probe, common: _Common, expected: object, calls: list[str],
    probes: list[PermitHolder],
) -> None:
    _arrange(gate, probe, common)

    assert _service(gate, probe).admit(HOLDER, LATE) == expected

    assert gate.calls == calls
    assert probe.calls == [holder.dispatch_id for holder in probes]


def test_reentrada_com_bind_segue_sem_reancorar(gate: _MemoryGate, probe: _Probe) -> None:
    permit = gate.hold(HOLDER, execution_ref=_ref(HOLDER.dispatch_id))

    admission = _service(gate, probe).admit(HOLDER, LATE)

    assert admission == UnitAdmission(ENV, 1)
    assert gate.permit == permit
    assert gate.calls == ["acquire"]


@pytest.mark.parametrize("lose", [_released, _taken_by_rival, _bound_elsewhere])
def test_reancoragem_recusada_adia_a_reentrada(
    gate: _MemoryGate, probe: _Probe, lose: Callable[[_MemoryGate], None],
) -> None:
    gate.hold(HOLDER, at=NOW)
    reanchor = gate.reanchor_unit_permit

    def _lost_before_reanchor(command: Any) -> bool:
        lose(gate)
        return reanchor(command)

    cast("Any", gate).reanchor_unit_permit = _lost_before_reanchor

    assert _service(gate, probe).admit(HOLDER, LATE) == PERMIT_HELD
    assert probe.calls == []


def test_permit_vinculado_nao_e_liberado_por_ausencia(gate: _MemoryGate, probe: _Probe) -> None:
    permit = gate.hold(OTHER, execution_ref=_ref(OTHER.dispatch_id))
    service = _service(gate, probe)
    later = NOW + GRACE * 4

    assert service.admit(HOLDER, later) == PERMIT_HELD
    service.settle(OTHER, later)
    service.reconcile(later)

    assert gate.permit == permit
    assert "release" not in gate.calls
    assert probe.calls == [OTHER.dispatch_id] * 3


@pytest.mark.parametrize(
    ("execution_ref", "alerts"),
    [(_ref(OTHER.dispatch_id), 3), (None, 0)],
    ids=["bind", "sem-bind"],
)
def test_cada_observacao_de_permit_vinculado_sem_execucao_gera_alerta(
    gate: _MemoryGate, probe: _Probe, caplog: pytest.LogCaptureFixture,
    execution_ref: str | None, alerts: int,
) -> None:
    permit = gate.hold(OTHER, at=LATE, execution_ref=execution_ref)
    service = _service(gate, probe)
    caplog.set_level(logging.WARNING, logger=_LOGGER)

    assert service.admit(HOLDER, LATE) == PERMIT_HELD
    service.settle(OTHER, LATE)
    service.reconcile(LATE)

    alert = (
        f"unit_permit_bound_absent environment={ENV} dispatch_id={OTHER.dispatch_id} "
        f"token={permit.token}"
    )
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert [record.getMessage() for record in warnings] == [alert] * alerts
    assert gate.permit == permit


def test_rival_nao_libera_o_permit_reancorado_durante_o_probe(gate: _MemoryGate) -> None:
    gate.hold(HOLDER, at=NOW)
    holder = _service(gate, _Probe())
    reentries: list[UnitAdmission | DispatchDeferred] = []
    reentry = _ProbeDuring(during=lambda: reentries.append(holder.admit(HOLDER, LATE)))
    rival = _service(gate, reentry)

    assert rival.admit(OTHER, LATE) == PERMIT_HELD

    assert reentries == [UnitAdmission(ENV, 1)]
    assert (_current(gate).holder, _current(gate).acquired_at) == (HOLDER, LATE)
    assert gate.calls == ["acquire", "acquire", "observe", "reanchor", "release", "acquire"]


def test_rival_nao_libera_o_permit_vinculado_durante_o_probe(gate: _MemoryGate) -> None:
    gate.hold(HOLDER, at=NOW)
    bind = BindUnitPermit(ENV, 1, _ref(HOLDER.dispatch_id), NOW + GRACE)
    rival = _service(gate, _ProbeDuring(during=lambda: gate.bind_unit_permit(bind)))

    assert rival.admit(OTHER, NOW + GRACE) == PERMIT_HELD

    assert (_current(gate).holder, _current(gate).execution_ref) == (HOLDER, bind.execution_ref)


def test_reconcile_sem_permit_so_observa(gate: _MemoryGate, probe: _Probe) -> None:
    _service(gate, probe).reconcile(NOW)

    assert gate.calls == ["observe"]
    assert probe.calls == []


@pytest.mark.parametrize("execution_ref", [None, _ref(OTHER.dispatch_id)], ids=["sem-bind", "bind"])
def test_reconcile_libera_execucao_terminal_mesmo_com_fence_fechado(
    gate: _MemoryGate, probe: _Probe, execution_ref: str | None,
) -> None:
    assert gate.close_promotion_fence(ClosePromotionFence(ENV, "release-1", NOW))
    gate.hold(OTHER, execution_ref=execution_ref)
    probe.presence[OTHER.dispatch_id] = ExecutionPresence.TERMINAL

    _service(gate, probe).reconcile(NOW + SECOND)

    assert gate.permit is None
    assert gate.calls == ["observe", "release"]
    assert probe.calls == [OTHER.dispatch_id]


@pytest.mark.parametrize(("elapsed", "released"), [(GRACE - SECOND, False), (GRACE, True)])
def test_reconcile_so_prova_ausencia_apos_a_carencia_desde_o_anchor(
    gate: _MemoryGate, probe: _Probe, elapsed: timedelta, released: bool,
) -> None:
    gate.hold(OTHER, at=LATE)

    _service(gate, probe).reconcile(LATE + elapsed)

    assert (gate.permit is None) is released
    assert ("release" in gate.calls) is released


def test_reconcile_vincula_execucao_viva_sem_bind(gate: _MemoryGate, probe: _Probe) -> None:
    permit = gate.hold(OTHER)
    probe.presence[OTHER.dispatch_id] = ExecutionPresence.RUNNING

    _service(gate, probe).reconcile(LATE)

    assert gate.permit == replace(permit, execution_ref=_ref(OTHER.dispatch_id))
    assert gate.calls == ["observe", "bind"]


def test_reconcile_renova_execucao_viva_com_bind(gate: _MemoryGate, probe: _Probe) -> None:
    permit = gate.hold(OTHER, execution_ref=_ref(OTHER.dispatch_id))
    probe.presence[OTHER.dispatch_id] = ExecutionPresence.RUNNING

    _service(gate, probe).reconcile(LATE)

    assert gate.permit == replace(permit, lease_until=LATE + timedelta(seconds=LEASE))
    assert gate.calls == ["observe", "renew"]


def test_reconcile_propaga_erro_do_probe_sem_liberar(gate: _MemoryGate, probe: _Probe) -> None:
    permit = gate.hold(OTHER)
    probe.error = TimeoutError("probe=throttled")

    with pytest.raises(TimeoutError, match="probe=throttled"):
        _service(gate, probe).reconcile(NOW + GRACE * 4)

    assert gate.permit == permit
    assert gate.calls == ["observe"]
