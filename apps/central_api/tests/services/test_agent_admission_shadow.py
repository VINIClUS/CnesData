"""Admissão de agente em shadow: o observador vê o agente novo sem mudar a decisão real."""

from dataclasses import replace
from typing import Any, cast

import pytest

from apps.central_api.tests.services.test_agent_admission import (
    NOW,
    Gate,
    Rig,
    identity,
    make_agent,
)
from central_api.services.agent_admission import AgentAdmission
from cnes_domain.billing.errors import BillingDependencyError, EntitlementDenied
from cnes_domain.billing.models import EntitlementAction
from cnes_domain.billing.shadow import (
    ShadowEntitlementObserver,
    ShadowObservation,
    ShadowObserverDependencies,
)
from cnes_domain.profiles import BillingMode

TENANT = "354130"


class SpyObserver:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls
        self.observations: list[ShadowObservation] = []

    def observe(self, observation: ShadowObservation) -> None:
        self.calls.append("observe")
        self.observations.append(observation)


def _admission(rig: Rig, observer: Any, mode: BillingMode = BillingMode.DISABLED):
    gates = cast("Any", rig.admission(mode))._gates
    return AgentAdmission(rig.registry, replace(gates, observer=observer))


def test_shadow_observa_agente_novo_antes_de_persistir() -> None:
    rig = Rig()
    observer = SpyObserver(rig.calls)

    admitted = _admission(rig, observer).admit(identity(), NOW)

    assert admitted.agent_id == "agent-1"
    assert rig.calls == ["get_agent", "gate", "observe", "register"]
    assert observer.observations == [ShadowObservation(EntitlementAction.REGISTER_AGENT, TENANT)]


def test_shadow_nao_observa_agente_existente() -> None:
    rig = Rig()
    rig.registry.agents["agent-1"] = make_agent()
    observer = SpyObserver(rig.calls)

    _admission(rig, observer).admit(identity(), NOW)

    assert observer.observations == []


def test_enforce_nao_chama_observador() -> None:
    rig = Rig()
    observer = SpyObserver(rig.calls)

    _admission(rig, observer, BillingMode.STRIPE).admit(identity(), NOW)

    assert "observe" not in rig.calls
    assert "consume" in rig.calls


def test_gate_real_negado_nao_chama_observador() -> None:
    rig = Rig()
    rig.gate = Gate(rig.calls, EntitlementDenied("reason=x"))
    observer = SpyObserver(rig.calls)

    with pytest.raises(EntitlementDenied):
        _admission(rig, observer).admit(identity(), NOW)

    assert observer.observations == []


class _BrokenCatalog:
    def get_tenant_account(self, tenant_id: str, consistency: Any) -> None:
        raise BillingDependencyError("dynamodb_unavailable")

    def get_tenant_link(self, *args: Any) -> None:
        raise AssertionError(args)


class _NoAudit:
    def append(self, event: Any) -> None:
        raise AssertionError(event)


def test_falha_do_observador_real_nao_muda_a_admissao() -> None:
    rig = Rig()
    audit = _NoAudit()
    observer = ShadowEntitlementObserver(ShadowObserverDependencies(
        cast("Any", _BrokenCatalog()), cast("Any", None), cast("Any", None), audit, lambda: NOW,
    ))

    admitted = _admission(rig, observer).admit(identity(), NOW)

    assert admitted.agent_id == "agent-1"
    assert rig.registry.agents["agent-1"] is admitted
