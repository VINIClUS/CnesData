"""Admissão trata o namespace sintético: normal nunca adota, canary só com binding válido."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pytest

from cnes_domain.control_plane.environment_gate import PermitHolder
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.orchestration.mode_canary import ModeCanaryGrant, SyntheticContextRequired
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
    LEASE,
    NOW,
    _closed_fence,
    _MemoryGate,
    _Probe,
    _ref,
)

SYNTHETIC = PermitHolder(
    "prod-canary-0123456789abcdef0123456789abcdef", "unit-aaaaaaaaaaaaaaaa", "0123456789abcdef"
)


@dataclass
class _Provider:
    grants: dict[PermitHolder, ModeCanaryGrant]
    calls: list[PermitHolder] = field(default_factory=list[PermitHolder])

    def binding_for(self, holder: PermitHolder, now: datetime) -> ModeCanaryGrant:
        del now
        self.calls.append(holder)
        if holder not in self.grants:
            raise SyntheticContextRequired("reason=synthetic_context_required field=tenant_id")
        return self.grants[holder]


@pytest.fixture
def gate() -> _MemoryGate:
    return _MemoryGate()


@pytest.fixture
def probe() -> _Probe:
    return _Probe()


def _service(gate: _MemoryGate, probe: _Probe, provider: _Provider | None) -> EnvironmentAdmission:
    return EnvironmentAdmission(gate, probe, ENV, LEASE, mode_canary=provider)


def test_mesma_flag_sem_registro_nao_admite(gate: _MemoryGate, probe: _Probe) -> None:
    for provider in (None, _Provider({})):
        with pytest.raises(SyntheticContextRequired):
            _service(gate, probe, provider).admit(SYNTHETIC, NOW)

    assert gate.calls == []
    assert probe.calls == []


def test_fence_closed_admite_so_holder_assinado(gate: _MemoryGate, probe: _Probe) -> None:
    gate.fence = _closed_fence()
    provider = _Provider({SYNTHETIC: ModeCanaryGrant("r1")})

    admission = _service(gate, probe, provider).admit(SYNTHETIC, NOW)

    assert admission == UnitAdmission(ENV, 1, None, "r1")
    other = _service(gate, probe, _Provider({SYNTHETIC: ModeCanaryGrant("r2")}))
    gate.permit = None
    assert other.admit(SYNTHETIC, NOW) == DispatchDeferred(DeferralReason.FENCE_CLOSED)


def test_reentrada_sem_execucao_exige_binding_ainda_valido(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    gate.fence = _closed_fence()
    provider = _Provider({SYNTHETIC: ModeCanaryGrant("r1")})
    service = _service(gate, probe, provider)
    assert service.admit(SYNTHETIC, NOW) == UnitAdmission(ENV, 1, None, "r1")
    calls = list(gate.calls)

    provider.grants.clear()
    with pytest.raises(SyntheticContextRequired):
        service.admit(SYNTHETIC, NOW + GRACE * 2)

    assert gate.calls == calls
    assert probe.calls == []


def test_normal_admission_nao_reconcilia_permit_sintetico(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    held = gate.hold(SYNTHETIC, execution_ref=_ref(SYNTHETIC.dispatch_id))
    probe.presence[SYNTHETIC.dispatch_id] = ExecutionPresence.TERMINAL

    for provider in (None, _Provider({})):
        admission = _service(gate, probe, provider).admit(HOLDER, NOW + timedelta(seconds=1))
        assert admission == DispatchDeferred(DeferralReason.PERMIT_HELD)

    assert gate.permit == held
    assert "release" not in gate.calls
    assert probe.calls == []


def test_reconcile_e_settle_normais_nao_liberam_permit_sintetico(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    held = gate.hold(SYNTHETIC, execution_ref=_ref(SYNTHETIC.dispatch_id))
    probe.presence[SYNTHETIC.dispatch_id] = ExecutionPresence.TERMINAL
    service = _service(gate, probe, None)

    service.reconcile(NOW)
    service.settle(SYNTHETIC, NOW)

    assert gate.permit == held
    assert "release" not in gate.calls


def test_contexto_autorizado_liquida_permit_sintetico_terminal(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    gate.hold(SYNTHETIC, execution_ref=_ref(SYNTHETIC.dispatch_id))
    probe.presence[SYNTHETIC.dispatch_id] = ExecutionPresence.TERMINAL
    provider = _Provider({SYNTHETIC: ModeCanaryGrant(None)})

    _service(gate, probe, provider).settle(SYNTHETIC, NOW)

    assert gate.permit is None
    assert gate.calls.count("release") == 1


def test_contexto_canary_reconcilia_permit_de_holder_normal_sem_consultar_binding(
    gate: _MemoryGate, probe: _Probe,
) -> None:
    gate.hold(HOLDER, execution_ref=_ref(HOLDER.dispatch_id))
    probe.presence[HOLDER.dispatch_id] = ExecutionPresence.TERMINAL
    provider = _Provider({})

    _service(gate, probe, provider).reconcile(NOW)

    assert gate.permit is None
    assert provider.calls == []


def test_admissao_normal_sem_provider_segue_inalterada(gate: _MemoryGate, probe: _Probe) -> None:
    assert _service(gate, probe, None).admit(HOLDER, NOW) == UnitAdmission(ENV, 1)
    assert gate.calls == ["acquire", "consume"]
