"""Testes da aplicação das fases de revogação após perda de acesso."""

from dataclasses import replace

import pytest

from cnes_domain.billing.models import (
    BillingAuditEvent,
    EntitlementSnapshot,
    ReadConsistency,
    SubscriptionStatus,
)
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
    assert set(harness.projection.consistencies) == {ReadConsistency.STRONG}
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
    harness.projection.snapshot = _lost(4)
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


def test_perda_de_acesso_superada_nao_fenceia_e_conclui() -> None:
    harness = _harness()
    harness.projection.snapshot = replace(_lost(4), subscription_status=SubscriptionStatus.ACTIVE)
    result = harness.service.enforce_access_loss(_lost(3), ACTOR)
    assert result == RevocationResult(3, (), ())
    assert harness.store.fence_requests == 0
    assert harness.executor.requests == []
    assert harness.store.progress.phase is RevocationPhase.COMPLETE


def test_perda_de_acesso_superada_no_meio_do_fencing_liquida_runs_ja_fenceadas() -> None:
    harness = _two_runs(page=1)
    snapshots = [_lost(3), _lost(4)]
    harness.projection.get_snapshot = lambda *_: snapshots.pop(0) if len(snapshots) > 1 else (
        snapshots[0]
    )
    result = harness.service.enforce_access_loss(_lost(3), ACTOR)
    assert result.fenced_run_ids == ("run_01",)
    assert harness.store.fence_requests == 1
    assert [r.run_id for r in harness.executor.requests] == ["run_01"]
    assert [e.aggregate_id for e in _canceled_events(harness, "run_01")] == ["run_01"]
    assert harness.store.progress.phase is RevocationPhase.COMPLETE


def test_perda_de_acesso_sem_snapshot_e_superada() -> None:
    harness = _harness()
    harness.projection.snapshot = None
    harness.service.enforce_access_loss(_lost(3), ACTOR)
    assert harness.store.fence_requests == 0


def test_liquida_progresso_pendente_sem_fencear_novas_runs() -> None:
    harness = _two_runs()
    state = harness.store.states["run_01"]
    harness.store.states["run_01"] = replace(state, cancel_requested=True)
    harness.store.progress = RevocationProgress(ACCOUNT, 3, RevocationPhase.FENCING, None, NOW)
    result = harness.service.settle_pending(ACCOUNT, ACTOR)
    assert result == RevocationResult(3, (), ())
    assert harness.store.fence_requests == 0
    assert [r.run_id for r in harness.executor.requests] == ["run_01"]
    assert harness.store.progress.phase is RevocationPhase.COMPLETE


@pytest.mark.parametrize("phase", [None, RevocationPhase.COMPLETE])
def test_sem_progresso_pendente_nao_liquida(phase: RevocationPhase | None) -> None:
    harness = _harness()
    if phase is not None:
        harness.store.progress = RevocationProgress(ACCOUNT, 3, phase, None, NOW)
    assert harness.service.settle_pending(ACCOUNT, ACTOR) is None
    assert harness.executor.requests == []


def test_liquidacao_retoma_progresso_em_cancelamento() -> None:
    harness = _two_runs()
    harness.store.progress = RevocationProgress(ACCOUNT, 3, RevocationPhase.CANCELING, None, NOW)
    result = harness.service.settle_pending(ACCOUNT, ACTOR)
    assert result == RevocationResult(3, (), ())
    assert harness.store.progress.phase is RevocationPhase.COMPLETE


def test_liquidacao_rejeita_ator_vazio() -> None:
    with pytest.raises(ValueError):
        _harness().service.settle_pending(ACCOUNT, "")
