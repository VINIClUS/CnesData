"""Quota mensal de tentativas na admissão: consumo só com o permit, liberação e adiamento."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import pytest

from cnes_domain.control_plane.environment_gate import ReanchorUnitPermit
from cnes_domain.control_plane.execution_quota import execution_period
from cnes_domain.ports.environment_gate import (
    DeferralReason,
    DispatchDeferred,
    ExecutionPresence,
    UnitAdmission,
)
from packages.cnes_domain.tests.orchestration.test_dispatch_admission import (
    ENV,
    GRACE,
    HOLDER,
    NOW,
    OTHER,
    SECOND,
    _closed_fence,
    _current,
    _MemoryGate,
    _Probe,
    _ref,
    _service,
    gate,
    probe,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from cnes_domain.control_plane.execution_quota import AttemptOutcome, ConsumeExecutionAttempt

__all__ = ["gate", "probe"]
PERIOD = execution_period(NOW)
EXHAUSTED = DispatchDeferred(DeferralReason.MONTHLY_ATTEMPTS_EXHAUSTED)
_LOGGER = "cnes_domain.orchestration.dispatch_admission"


def _fill(gate: _MemoryGate, count: int = 200) -> set[str]:
    counted = gate.counted.setdefault(PERIOD, set())
    counted.update(f"{index:016x}" for index in range(count))
    return counted


def _fence_closed(gate: _MemoryGate, probe: _Probe) -> None:
    gate.fence = _closed_fence()


def _held_by_live_rival(gate: _MemoryGate, probe: _Probe) -> None:
    gate.hold(OTHER, execution_ref=_ref(OTHER.dispatch_id))
    probe.presence[OTHER.dispatch_id] = ExecutionPresence.RUNNING


def _absent_reentry_behind_fence(gate: _MemoryGate, probe: _Probe) -> None:
    gate.hold(HOLDER)
    gate.fence = _closed_fence()


def test_quota_esgotada_apos_acquire_libera_o_permit_e_adia(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    counted = set(_fill(gate))

    assert _service(gate, probe).admit(HOLDER, NOW) == EXHAUSTED

    assert gate.permit is None
    assert gate.calls == ["acquire", "consume", "release"]
    assert gate.counted == {PERIOD: counted}
    assert probe.calls == []


@pytest.mark.parametrize(
    ("execution_ref", "calls"),
    [
        (None, ["acquire", "observe", "reanchor", "consume"]),
        (_ref(HOLDER.dispatch_id), ["acquire", "consume"]),
    ],
    ids=["sem-bind", "com-bind"],
)
def test_quota_esgotada_na_reentrada_adia_sem_liberar(
    gate: _MemoryGate, probe: _Probe, execution_ref: str | None, calls: list[str],
) -> None:
    gate.hold(HOLDER, execution_ref=execution_ref)
    _fill(gate)

    assert _service(gate, probe).admit(HOLDER, NOW + SECOND) == EXHAUSTED

    assert _current(gate).holder == HOLDER
    assert gate.calls == calls


def test_replay_ja_contado_e_admitido_mesmo_com_a_quota_no_limite(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    gate.hold(HOLDER)
    _fill(gate, 199).add(HOLDER.dispatch_id)

    assert _service(gate, probe).admit(HOLDER, NOW + SECOND) == UnitAdmission(ENV, 1)

    assert len(gate.counted[PERIOD]) == 200
    assert gate.calls == ["acquire", "observe", "reanchor", "consume"]


@pytest.mark.parametrize(
    ("arrange", "reason"),
    [
        (_fence_closed, DeferralReason.FENCE_CLOSED),
        (_held_by_live_rival, DeferralReason.PERMIT_HELD),
        (_absent_reentry_behind_fence, DeferralReason.FENCE_CLOSED),
    ],
    ids=["fence-fechado", "permit-ocupado", "reentrada-ausente-com-fence-fechado"],
)
def test_adiamento_do_gate_nao_gasta_tentativa(
    gate: _MemoryGate, probe: _Probe, arrange: Callable[[_MemoryGate, _Probe], None],
    reason: DeferralReason,
) -> None:
    arrange(gate, probe)

    assert _service(gate, probe).admit(HOLDER, NOW + GRACE) == DispatchDeferred(reason)

    assert "consume" not in gate.calls
    assert gate.counted == {}


def test_falha_de_start_conta_como_tentativa(gate: _MemoryGate, probe: _Probe) -> None:
    service = _service(gate, probe)
    assert service.admit(HOLDER, NOW) == UnitAdmission(ENV, 1)

    # StartExecution raised: the caller only settles the permit, nothing refunds the attempt.
    service.settle(HOLDER, NOW)
    service.reconcile(NOW + GRACE)

    assert gate.permit is None
    assert gate.counted == {PERIOD: {HOLDER.dispatch_id}}
    assert service.admit(OTHER, NOW + GRACE) == UnitAdmission(ENV, 2)
    assert gate.counted == {PERIOD: {HOLDER.dispatch_id, OTHER.dispatch_id}}


def test_reentrada_apos_a_virada_do_mes_conta_no_mes_novo(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    # Conservative: the monthly set starts empty, so the re-entry spends a November attempt.
    october = datetime(2026, 10, 31, 23, 59, 59, tzinfo=UTC)
    november = datetime(2026, 11, 1, tzinfo=UTC)
    service = _service(gate, probe)

    assert service.admit(HOLDER, october) == UnitAdmission(ENV, 1)
    assert service.admit(HOLDER, november) == UnitAdmission(ENV, 1)

    assert gate.counted == {"2026-10": {HOLDER.dispatch_id}, "2026-11": {HOLDER.dispatch_id}}


def test_erro_da_quota_propaga_sem_liberar_o_permit(gate: _MemoryGate, probe: _Probe) -> None:
    def _throttled(command: ConsumeExecutionAttempt) -> AttemptOutcome:
        raise TimeoutError("quota=throttled")

    cast("Any", gate).consume_execution_attempt = _throttled

    with pytest.raises(TimeoutError, match="quota=throttled"):
        _service(gate, probe).admit(HOLDER, NOW)

    assert _current(gate).holder == HOLDER
    assert gate.calls == ["acquire"]


def test_quota_esgotada_nao_libera_o_permit_reancorado_por_reentrada_concorrente(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    _fill(gate)
    consume = gate.consume_execution_attempt

    def _reentry_during_consume(command: ConsumeExecutionAttempt) -> AttemptOutcome:
        assert gate.reanchor_unit_permit(ReanchorUnitPermit(ENV, 1, NOW + SECOND))
        return consume(command)

    cast("Any", gate).consume_execution_attempt = _reentry_during_consume

    assert _service(gate, probe).admit(HOLDER, NOW) == EXHAUSTED

    assert (_current(gate).holder, _current(gate).acquired_at) == (HOLDER, NOW + SECOND)
    assert gate.calls == ["acquire", "reanchor", "consume", "release"]


def test_cada_decisao_da_quota_gera_um_evento(
    gate: _MemoryGate, probe: _Probe, caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=_LOGGER)
    _fill(gate, 199)
    service = _service(gate, probe)

    service.admit(HOLDER, NOW)
    gate.permit = None
    service.admit(OTHER, NOW)

    messages = [record.getMessage() for record in caplog.records if record.name == _LOGGER]
    assert messages == [
        f"execution_attempt_consumed environment={ENV} dispatch_id={HOLDER.dispatch_id} "
        f"period={PERIOD} status=counted attempts=200 limit=200",
        f"execution_attempt_consumed environment={ENV} dispatch_id={OTHER.dispatch_id} "
        f"period={PERIOD} status=exhausted attempts=200 limit=200",
        f"unit_permit_released environment={ENV} dispatch_id={OTHER.dispatch_id} token=2 "
        "reason=monthly_attempts_exhausted applied=True",
        f"dispatch_deferred environment={ENV} tenant_id={OTHER.tenant_id} run_id={OTHER.run_id} "
        f"dispatch_id={OTHER.dispatch_id} reason=monthly_attempts_exhausted",
    ]
