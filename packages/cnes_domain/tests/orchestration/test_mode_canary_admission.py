"""Namespace reservado, binding imutável e admissão do holder sintético."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta

import pytest

from cnes_domain.control_plane.environment_gate import PermitHolder
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.orchestration.mode_canary import (
    ModeCanaryBinding,
    ModeCanaryGrant,
    SyntheticContextRequired,
    is_canonical_mode_canary_tenant,
    is_mode_canary_namespace,
    require_context,
    require_normal_tenant,
)
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

CANARY_ID = "0123456789abcdef0123456789abcdef"
TENANT = f"prod-canary-{CANARY_ID}"
OWNER = "a" * 64
RECORD = "b" * 64


def _binding(**overrides: object) -> ModeCanaryBinding:
    fields: dict[str, object] = {
        "tenant_id": TENANT,
        "run_ids": ("unit-aaaaaaaaaaaaaaaa",),
        "owner_sha256": OWNER,
        "record_sha256": RECORD,
        "phase": "unit",
        "context_kind": "promotion",
    }
    fields.update(overrides)
    return ModeCanaryBinding(**fields)  # type: ignore[arg-type]


def test_case_ou_sufixo_invalido_reservado_nao_vira_tenant_normal() -> None:
    for tenant in (TENANT, TENANT.upper(), "Prod-Canary-xyz", "prod-canary-", "PROD-CANARY-1"):
        assert is_mode_canary_namespace(tenant)


def test_tenant_normal_nao_pertence_ao_namespace_reservado() -> None:
    for tenant in ("354130", "prod-canar", "canary-prod-1", "tenant-prod-canary-1", ""):
        assert not is_mode_canary_namespace(tenant)


def test_somente_forma_canonica_e_elegivel_ao_contexto() -> None:
    assert is_canonical_mode_canary_tenant(TENANT)
    for tenant in (
        TENANT.upper(), f"{TENANT}\n", f"prod-canary-{CANARY_ID[:-1]}", f"{TENANT}0", "354130",
    ):
        assert not is_canonical_mode_canary_tenant(tenant)


def test_synthetic_context_required_e_value_error() -> None:
    assert issubclass(SyntheticContextRequired, ValueError)


def test_binding_valido_autoriza_apenas_tenant_e_runs_declarados() -> None:
    binding = _binding(run_ids=("unit-aaaaaaaaaaaaaaaa", "unit-bbbbbbbbbbbbbbbb"))
    assert binding.permits(TENANT, "unit-aaaaaaaaaaaaaaaa")
    assert binding.permits(TENANT, "unit-bbbbbbbbbbbbbbbb")
    assert not binding.permits(TENANT, "unit-cccccccccccccccc")
    assert not binding.permits("prod-canary-" + "f" * 32, "unit-aaaaaaaaaaaaaaaa")


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tenant_id", "354130"),
        ("tenant_id", TENANT.upper()),
        ("run_ids", ()),
        ("run_ids", ("a", "b", "c")),
        ("run_ids", ("a", "a")),
        ("run_ids", ("",)),
        ("run_ids", ["unit-aaaaaaaaaaaaaaaa"]),
        ("owner_sha256", "A" * 64),
        ("owner_sha256", "a" * 63),
        ("record_sha256", "z" * 64),
        ("phase", "other"),
        ("context_kind", "other"),
    ],
)
def test_binding_rejeita_campo_invalido(field: str, value: object) -> None:
    with pytest.raises(ValueError, match=f"field={field}"):
        _binding(**{field: value})


@pytest.mark.parametrize("phase", ["unit", "recovery", "audit", "readiness"])
def test_binding_aceita_fases_fechadas(phase: str) -> None:
    assert _binding(phase=phase).phase == phase


def test_binding_aceita_contexto_readiness() -> None:
    assert _binding(phase="readiness", context_kind="readiness").context_kind == "readiness"


def test_require_normal_tenant_recusa_somente_o_namespace_reservado() -> None:
    require_normal_tenant("354130")
    with pytest.raises(SyntheticContextRequired, match="field=tenant_id"):
        require_normal_tenant(TENANT.upper())


def test_require_context_exige_binding_que_permita_o_run_sintetico() -> None:
    binding = _binding()
    require_context(None, "354130", "run-a")
    require_context(binding, "354130", "run-a")
    require_context(binding, TENANT, "unit-aaaaaaaaaaaaaaaa")
    for installed, run_id in ((None, "unit-aaaaaaaaaaaaaaaa"), (binding, "unit-x")):
        with pytest.raises(SyntheticContextRequired, match="field=tenant_id"):
            require_context(installed, TENANT, run_id)


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
