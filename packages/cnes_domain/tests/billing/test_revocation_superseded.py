"""Testes da regra de supersessão do progresso de revogação."""

from dataclasses import replace

import pytest

from cnes_domain.billing.models import EntitlementSnapshot, SubscriptionStatus
from cnes_domain.billing.revocation import RevocationPhase, RevocationProgress
from packages.cnes_domain.tests.billing.revocation_fakes import (
    ACCOUNT,
    NOW,
    Harness,
    _snapshot,
    _two_runs,
    present,
)

ACTOR = "admin-1"


def _with_status(version: int, status: SubscriptionStatus) -> EntitlementSnapshot:
    return _snapshot(subscription_status=status, entitlement_version=version)


def _interrupted(newer: EntitlementSnapshot | None) -> Harness:
    harness = _two_runs()
    harness.projection.snapshot = newer
    state = harness.store.states["run_01"]
    harness.store.states["run_01"] = replace(state, cancel_requested=True)
    harness.store.progress = RevocationProgress(ACCOUNT, 4, RevocationPhase.FENCING, None, NOW)
    return harness


def _assert_all_fenced(harness: Harness) -> None:
    assert all(state.cancel_requested for state in harness.store.states.values())
    assert present(harness.store.progress).phase is RevocationPhase.COMPLETE


def test_revogacao_administrativa_continua_fenceando_apos_bump_de_versao() -> None:
    harness = _interrupted(_with_status(5, SubscriptionStatus.ADMIN_REVOKED))
    result = present(harness.service.resume_pending(ACCOUNT, ACTOR))
    assert result.fenced_run_ids == ("run_02",)
    assert result.entitlement_version == 4
    _assert_all_fenced(harness)


def test_versao_nova_negada_nao_plena_continua_fenceando_sob_progresso_armazenado() -> None:
    harness = _interrupted(_with_status(5, SubscriptionStatus.CANCELED))
    result = present(harness.service.resume_pending(ACCOUNT, ACTOR))
    assert result.fenced_run_ids == ("run_02",)
    _assert_all_fenced(harness)


def test_versao_nova_plena_pula_o_fencing() -> None:
    harness = _interrupted(_with_status(5, SubscriptionStatus.ACTIVE))
    result = present(harness.service.resume_pending(ACCOUNT, ACTOR))
    assert result.fenced_run_ids == ()
    assert not harness.store.states["run_02"].cancel_requested
    assert present(harness.store.progress).phase is RevocationPhase.COMPLETE


def test_snapshot_ausente_pula_o_fencing() -> None:
    harness = _interrupted(None)
    result = present(harness.service.resume_pending(ACCOUNT, ACTOR))
    assert result.fenced_run_ids == ()
    assert harness.store.fence_requests == 0


@pytest.mark.parametrize("status", [SubscriptionStatus.ADMIN_REVOKED, SubscriptionStatus.ACTIVE])
def test_mesma_versao_continua_fenceando_independente_do_status(status: SubscriptionStatus) -> None:
    harness = _interrupted(_with_status(4, status))
    result = present(harness.service.resume_pending(ACCOUNT, ACTOR))
    assert result.fenced_run_ids == ("run_02",)
