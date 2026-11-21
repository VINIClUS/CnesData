"""Tabela fechada do adapter ABI2 do produto contra as literais do keeper do infra-ansible."""

from __future__ import annotations

from typing import Any

import pytest

from scripts.promotion import promote_api
from scripts.promotion.mode_canary_contract import SESSION_OPERATIONS

# infra-ansible f6cab21e tools/cnesdata_delivery/lifecycle_contract.py ADAPTER_INPUTS.
INFRA_ADAPTER_INPUTS = {
    "inventory": "candidate dashboard-artifact archive static-baseline",
    "close-drain": "activation expirations",
    "closed-ready": "activation close-drain phase-b membership",
    "publish-static": "candidate dashboard-artifact static-plan inventory host-receipt",
    "rollback-api-static": (
        "static-plan inventory publication host-recovery-receipt recovery-decision"
    ),
    "reopen": "activation routing canaries membership",
    "terminal-readback": "routing membership host-receipt static-receipt decision",
    "retire-static": "static-plan inventory publication routing",
}
# infra-ansible f6cab21e lifecycle_contract.py ADAPTER_STAGES.
INFRA_ADAPTER_STAGES: dict[str, set[str]] = {
    "inventory": {"application-prepare"}, "close-drain": {"application-prepare"},
    "closed-ready": {"api"}, "publish-static": {"static"}, "rollback-api-static": {"recovery"},
    "reopen": {"final", "recovery"},
    "terminal-readback": {"application-prepare", "final", "recovery"}, "retire-static": set(),
}
# infra-ansible f6cab21e lifecycle_contract.py ADAPTER_OBSERVATIONS.
INFRA_ADAPTER_OBSERVATIONS = {
    "inventory": "inventory", "close-drain": "close-drain", "closed-ready": "closed-ready",
    "publish-static": "publication", "rollback-api-static": "rollback", "reopen": "routing",
    "terminal-readback": "terminal", "retire-static": "retirement",
}
# infra-ansible f6cab21e application_gate.py ROUTING_FIELDS, TERMINAL_READBACK_FIELDS,
# CLOSED_READY_FIELDS e MEMBER_IDENTITY (MEMBER_IDENTITY_TYPES).
INFRA_ROUTING_FIELDS = "version kind fence routes_match"
INFRA_TERMINAL_READBACK_FIELDS = (
    "version kind members quiescent state_clean pending unknown observed_at"
)
INFRA_CLOSED_READY_FIELDS = (
    "version kind common close_drain_sha256 phase_b_sha256 fence permit members proofs "
    "observed_at exclusion stage decision"
)
INFRA_MEMBER_IDENTITY = (
    "dispatch_id request_sha256 generation initial_state execution_ref resolution"
)
# infra-ansible f6cab21e lifecycle_contract.py MODE_SESSION_OPERATIONS:
# operation -> (code, alias, context, phase).
INFRA_MODE_SESSION_OPERATIONS = {
    "unit-seed": ("us", "mode_seed", "promotion", "unit"),
    "unit-start": ("ux", "mode_dispatch", "promotion", "unit"),
    "unit-observe": ("uo", "mode_observe", "promotion", "unit"),
    "recovery-seed": ("rs", "mode_seed", "promotion", "recovery"),
    "recovery-start": ("rx", "mode_dispatch", "promotion", "recovery"),
    "recovery-observe": ("ro", "mode_observe", "promotion", "recovery"),
    "audit-start": ("ax", "mode_audit_launch", "promotion", "audit"),
    "audit-observe": ("ao", "mode_observe", "promotion", "audit"),
    "drill-seed": ("ds", "mode_seed", "readiness", "readiness"),
    "drill-unit-start": ("dx", "mode_dispatch", "readiness", "readiness"),
    "drill-recovery-start": ("dr", "mode_dispatch", "readiness", "readiness"),
    "drill-hang-first": ("h1", "mode_dispatch", "readiness", "readiness"),
    "drill-hang-second": ("h2", "mode_dispatch", "readiness", "readiness"),
    "drill-observe": ("do", "mode_observe", "readiness", "readiness"),
}
# infra-ansible f6cab21e lifecycle_contract.py STAGES (janelas após o close).
INFRA_STAGES = frozenset({"phase-b", "api", "static", "unit", "phase-c-recovery",
                          "phase-c-audit", "final"})
HANDLED = frozenset(INFRA_ADAPTER_INPUTS) - {"retire-static"}
PROMOTION_MODES = {name: row for name, row in INFRA_MODE_SESSION_OPERATIONS.items()
                   if row[2] == "promotion"}
READINESS_MODES = sorted(set(INFRA_MODE_SESSION_OPERATIONS) - set(PROMOTION_MODES))


def _fields(text: str) -> frozenset[str]:
    return frozenset(text.split())


def test_operacoes_do_produto_sao_as_do_keeper_menos_retire_static() -> None:
    assert set(promote_api.INPUTS) == set(promote_api.STAGES) == set(promote_api.OBSERVATIONS)
    assert set(promote_api.INPUTS) == HANDLED


@pytest.mark.parametrize("operation", sorted(HANDLED))
def test_insumos_estagios_e_observacao_de_cada_operacao_sao_os_do_keeper(operation: str) -> None:
    assert promote_api.INPUTS[operation] == _fields(INFRA_ADAPTER_INPUTS[operation])
    assert promote_api.STAGES[operation] == frozenset(INFRA_ADAPTER_STAGES[operation])
    assert promote_api.OBSERVATIONS[operation] == INFRA_ADAPTER_OBSERVATIONS[operation]


def test_campos_fechados_das_observacoes_sao_os_do_keeper() -> None:
    assert _fields(INFRA_ROUTING_FIELDS) == promote_api.ROUTING_FIELDS
    assert _fields(INFRA_TERMINAL_READBACK_FIELDS) == promote_api.TERMINAL_READBACK_FIELDS
    assert _fields(INFRA_CLOSED_READY_FIELDS) == promote_api.CLOSED_READY_FIELDS
    assert tuple(INFRA_MEMBER_IDENTITY.split()) == promote_api.MEMBER_IDENTITY


def test_operacoes_de_modo_sao_as_oito_de_promotion_do_vetor_canonico() -> None:
    assert set(promote_api.MODE_OPERATIONS) == set(PROMOTION_MODES)
    assert len(promote_api.MODE_OPERATIONS) == 8
    for name, (phase, stage) in promote_api.MODE_OPERATIONS.items():
        code, alias, context, infra_phase = PROMOTION_MODES[name]
        assert (phase, context) == (infra_phase, "promotion")
        assert SESSION_OPERATIONS[name] == (code, alias, phase, context)
        assert stage in INFRA_STAGES
    stages = dict(promote_api.MODE_OPERATIONS.values())
    assert stages == {"unit": "unit", "recovery": "phase-c-recovery", "audit": "phase-c-audit"}


def test_observacao_de_modo_tem_campos_fechados_com_aceite_e_compromissos() -> None:
    expected = {"version", "kind", "accepted", "checks", "commitments"}
    assert set(promote_api.MODE_OBSERVATION_FIELDS) == expected


NO_CONTEXT: Any = None


def _request(operation: str) -> dict[str, Any]:
    return {"abi_version": 2, "operation": operation, "inputs": {}}


@pytest.mark.parametrize("operation", ["retire-static", *READINESS_MODES])
def test_retire_static_e_modos_de_readiness_seguem_recusados(operation: str) -> None:
    with pytest.raises(ValueError, match="operation=unsupported"):
        promote_api.execute_shared_operation(operation, _request(operation), NO_CONTEXT)


@pytest.mark.parametrize("operation", sorted(name for name in PROMOTION_MODES
                                             if not name.endswith("-observe")))
def test_efeito_de_modo_e_recusado_sem_ancora_do_keeper(operation: str) -> None:
    with pytest.raises(ValueError, match=f"operation=mode_effect_unanchored name={operation}"):
        promote_api.execute_shared_operation(operation, _request(operation), NO_CONTEXT)
