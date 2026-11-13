"""Namespace reservado e binding imutável do canary de modo."""
from __future__ import annotations

import pytest

from cnes_domain.orchestration.mode_canary import (
    ModeCanaryBinding,
    SyntheticContextRequired,
    is_canonical_mode_canary_tenant,
    is_mode_canary_namespace,
    require_context,
    require_normal_tenant,
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
