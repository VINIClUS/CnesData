"""Testes da aplicação das fases de revogação após perda de acesso."""

from dataclasses import replace

import pytest

from cnes_domain.billing.models import BillingAuditEvent, EntitlementSnapshot, SubscriptionStatus
from cnes_domain.billing.revocation import (
    RevocationPhase,
    RevocationProgress,
    RevocationResult,
)
from packages.cnes_domain.tests.billing.revocation_fakes import (
    ACCOUNT,
    NOW,
    TENANT,
    Harness,
    _snapshot,
    _two_runs,
)

ACTOR = "stripe-reconciler"


def _lost(version: int = 3) -> EntitlementSnapshot:
    return _snapshot(subscription_status=SubscriptionStatus.CANCELED, entitlement_version=version)


def _harness() -> Harness:
    harness = _two_runs()
    harness.projection.snapshot = _lost()
    return harness


def _canceled_events(harness: Harness, run_id: str) -> list[BillingAuditEvent]:
    return [
        e for e in harness.audit.events
        if e.event_type == "run.canceled" and e.aggregate_id == run_id
    ]


def test_perda_de_acesso_fenceia_e_cancela_sem_reescrever_snapshot() -> None:
    harness = _harness()
    result = harness.service.enforce_access_loss(_lost(), ACTOR)
    assert harness.calls[:4] == [
        "run_01_fence_incremented",
        "run_02_fence_incremented",
        "executor_run_01_cancel",
        "executor_run_02_cancel",
    ]
    assert harness.projection.writes == []
    assert harness.projection.snapshot.subscription_status is SubscriptionStatus.CANCELED
    assert harness.projection.consistencies == []
    assert result == RevocationResult(3, ("run_01", "run_02"), ())


def test_perda_de_acesso_retoma_progresso_da_mesma_versao() -> None:
    harness = _harness()
    for run_id in ("run_01", "run_02"):
        state = harness.store.states[run_id]
        harness.store.states[run_id] = replace(state, cancel_requested=True)
    harness.store.progress = RevocationProgress(
        ACCOUNT, 3, RevocationPhase.CANCELING, None, NOW
    )
    result = harness.service.enforce_access_loss(_lost(), ACTOR)
    assert harness.store.fence_requests == 0
    assert len(harness.executor.requests) == 2
    assert result == RevocationResult(3, (), ())


def test_perda_de_acesso_completa_e_idempotente() -> None:
    harness = _harness()
    harness.service.enforce_access_loss(_lost(), ACTOR)
    fences = harness.store.fence_requests
    cancels = len(harness.executor.requests)
    audits = len(harness.audit.events)
    second = harness.service.enforce_access_loss(_lost(), ACTOR)
    assert harness.store.fence_requests == fences
    assert len(harness.executor.requests) == cancels
    assert len(harness.audit.events) == audits
    assert second == RevocationResult(3, (), ())


def test_nova_versao_nao_reaudita_run_ja_cancelado() -> None:
    harness = _harness()
    harness.service.enforce_access_loss(_lost(3), ACTOR)
    harness.service.enforce_access_loss(_lost(4), ACTOR)
    expected = f"run.canceled:{ACCOUNT}:{TENANT}:run_01"
    events = _canceled_events(harness, "run_01")
    assert events
    assert {e.event_id for e in events} == {expected}
    assert harness.store.progress.entitlement_version == 4


def test_perda_de_acesso_audita_cancelamento_com_ator_informado() -> None:
    harness = _harness()
    harness.service.enforce_access_loss(_lost(), ACTOR)
    canceled = [e for e in harness.audit.events if e.event_type == "run.canceled"]
    assert len(canceled) == 2
    assert {e.actor_id for e in canceled} == {ACTOR}


@pytest.mark.parametrize("actor", ["", "   "])
def test_perda_de_acesso_rejeita_ator_vazio(actor: str) -> None:
    harness = _harness()
    with pytest.raises(ValueError):
        harness.service.enforce_access_loss(_lost(), actor)
    assert harness.store.fence_requests == 0
