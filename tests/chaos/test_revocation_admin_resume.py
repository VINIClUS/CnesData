"""Retomada da revogação administrativa interrompida respeita versões negadas e restauradas."""

import pytest

pytest.importorskip("moto")

from typing import Any

from cnes_domain.billing.inbox import ReconciliationRequest
from cnes_domain.billing.models import ReservationStatus, SubscriptionStatus
from cnes_domain.billing.revocation import RevocationPhase
from cnes_domain.control_plane.enums import RunState, RunUnitState
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT
from packages.cnes_infra.tests.billing.revocation_support import (
    RevEnv,
    open_env,
    stored_reservation,
    stored_run,
)
from packages.cnes_infra.tests.billing.test_dynamodb_revocation_service import (
    companion,
    snapshot_of,
    units_of,
)
from tests.integration.billing._worker_stack import (
    Interrupted,
    MetricSpy,
    build_sweep,
    interrupt_admin_revocation,
    rewrite_snapshot,
)

pytestmark = [pytest.mark.chaos]

PAGE = ReconciliationRequest(limit=10, cursor=None)


def sweep_once(env: RevEnv, interrupted: Interrupted) -> Any:
    sweep = build_sweep(env, interrupted.catalog, interrupted.service, MetricSpy())
    return sweep.run(PAGE)


def assert_fully_canceled(env: RevEnv, run_id: str) -> None:
    assert companion(env, run_id).cancel_requested
    assert companion(env, run_id).fencing_token == 1
    assert stored_run(env, run_id).state is RunState.CANCELED
    assert {unit.state for unit in units_of(env, run_id).values()} == {RunUnitState.CANCELED}
    assert stored_reservation(env, run_id).status is ReservationStatus.RELEASED


def test_revogacao_administrativa_interrompida_e_bump_de_versao_fenceia_todas_as_runs() -> None:
    with open_env() as env:
        interrupted = interrupt_admin_revocation(env)
        stale = env.store.get_revocation_progress(ACCOUNT)
        assert stale is not None
        assert stale.phase is RevocationPhase.FENCING
        assert len(interrupted.pending) == 2
        bumped = rewrite_snapshot(env)
        assert bumped.subscription_status is SubscriptionStatus.ADMIN_REVOKED
        assert bumped.entitlement_version == stale.entitlement_version + 1

        result = sweep_once(env, interrupted)

        assert (result.examined, result.resumed, result.fenced) == (1, 1, 2)
        for run_id in interrupted.run_ids:
            assert_fully_canceled(env, run_id)
        for run_id in interrupted.pending:
            assert stored_run(env, run_id).state is RunState.CANCELED
        progress = env.store.get_revocation_progress(ACCOUNT)
        assert progress is not None
        assert progress.phase is RevocationPhase.COMPLETE
        assert progress.entitlement_version == stale.entitlement_version
        assert snapshot_of(env).entitlement_version == bumped.entitlement_version
        refs = {run_id for run_id, _ in interrupted.executor.refs()}
        assert refs == set(interrupted.run_ids)


def test_retomada_com_acesso_restaurado_nao_fenceia_runs_novas() -> None:
    with open_env() as env:
        interrupted = interrupt_admin_revocation(env)
        restored = rewrite_snapshot(env, subscription_status=SubscriptionStatus.ACTIVE)
        assert restored.subscription_status is SubscriptionStatus.ACTIVE

        result = sweep_once(env, interrupted)

        assert (result.examined, result.resumed, result.fenced) == (1, 1, 0)
        assert_fully_canceled(env, interrupted.fenced)
        for run_id in interrupted.pending:
            assert not companion(env, run_id).cancel_requested
            assert companion(env, run_id).fencing_token == 0
            assert stored_run(env, run_id).state is not RunState.CANCELED
            assert stored_reservation(env, run_id).status is ReservationStatus.RESERVED
        progress = env.store.get_revocation_progress(ACCOUNT)
        assert progress is not None
        assert progress.phase is RevocationPhase.COMPLETE
        assert {run_id for run_id, _ in interrupted.executor.refs()} == {interrupted.fenced}
