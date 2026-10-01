"""Integração Task 17C: jobs do worker de billing sobre adapters DynamoDB reais em moto."""

import pytest

pytest.importorskip("moto")

from collections.abc import Callable, Iterator
from typing import Any

import boto3
from moto import mock_aws

from billing_worker.worker import BillingWorker, WorkerJobs
from cnes_domain.billing.inbox import InboxProcessingState, RecoveryRequest
from cnes_domain.billing.models import ReservationStatus, SubscriptionStatus
from cnes_domain.billing.revocation import (
    ImmediateRevocationCommand,
    RevocationPhase,
)
from cnes_domain.control_plane.enums import RunState
from packages.cnes_infra.tests.billing.billing_factories import NOW
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT
from packages.cnes_infra.tests.billing.revocation_support import (
    RUN_ID,
    RevEnv,
    create_run,
    open_env,
    put_run_state,
    stored_reservation,
    stored_run,
    usage_counters,
)
from packages.cnes_infra.tests.billing.test_dynamodb_revocation_service import (
    RecordingExecutor,
    build_service,
    snapshot_of,
)
from tests.integration.billing._billing_stack import (
    BillingStack,
    create_billing_table,
    install_fake_stripe,
)
from tests.integration.billing._worker_stack import (
    PAST_EXPIRY,
    MetricSpy,
    agent_count,
    build_sweep,
    interrupt_admin_revocation,
    recovery_request,
    rewrite_snapshot,
    seed_agent_capacity,
    stored_capacity,
)

REVOKE = ImmediateRevocationCommand(ACCOUNT, "admin-1", "fraud_confirmed", NOW)


@pytest.fixture
def env() -> Iterator[RevEnv]:
    with open_env() as opened:
        yield opened


def revoke_by_admin(env: RevEnv, service: Any) -> Any:
    return service.revoke(REVOKE)


def revoke_by_access_loss(env: RevEnv, service: Any) -> Any:
    return service.enforce_access_loss(snapshot_of(env), "system:stripe_webhook")


def renew_until_twice(env: RevEnv) -> None:
    for _ in range(2):
        env.clock.advance(PAST_EXPIRY)
        result = env.quota.reconcile_expired_reservations(recovery_request(env))
        assert (result.examined, result.released) == (1, 0)
        assert stored_reservation(env).status is ReservationStatus.RESERVED
        assert usage_counters(env)["consumed_runs"] == 1


@pytest.mark.parametrize(
    ("deny", "denied_status"),
    [
        pytest.param(revoke_by_access_loss, SubscriptionStatus.CANCELED, id="assinatura_cancelada"),
        pytest.param(revoke_by_admin, SubscriptionStatus.ADMIN_REVOKED, id="revogacao_admin"),
    ],
)
def test_publicacao_negada_repetidamente_fica_reservada_ate_o_revoke_pending(
    env: RevEnv, deny: Callable[[RevEnv, Any], Any], denied_status: SubscriptionStatus
) -> None:
    create_run(env)
    put_run_state(env, RunState.PUBLISHING)
    rewrite_snapshot(env, subscription_status=SubscriptionStatus.CANCELED)
    service = build_service(env, RecordingExecutor())
    renew_until_twice(env)

    result = deny(env, service)

    assert snapshot_of(env).subscription_status is denied_status
    assert result.failed_run_ids == (RUN_ID,)
    assert result.fenced_run_ids == ()
    assert stored_run(env).state is RunState.FAILED
    assert stored_reservation(env).status is ReservationStatus.RELEASED
    assert usage_counters(env)["consumed_runs"] == 1
    env.clock.advance(PAST_EXPIRY)
    after = env.quota.reconcile_expired_reservations(recovery_request(env))
    assert after.released == 0
    assert stored_reservation(env).status is ReservationStatus.RELEASED
    assert usage_counters(env)["consumed_runs"] == 1


def test_reserva_de_capacidade_de_agente_orfa_e_liberada_e_a_com_posse_e_consumida(
    env: RevEnv,
) -> None:
    reservations = seed_agent_capacity(env, owned=("agent-owned",), orphans=("agent-orphan",))
    env.clock.advance(PAST_EXPIRY)

    result = env.quota.reconcile_expired_reservations(recovery_request(env))

    assert (result.examined, result.released) == (2, 1)
    assert stored_capacity(env, reservations["agent-orphan"]).status is ReservationStatus.RELEASED
    assert stored_capacity(env, reservations["agent-owned"]).status is ReservationStatus.CONSUMED
    assert agent_count(env) == 1


@pytest.fixture
def stack(monkeypatch: pytest.MonkeyPatch) -> Iterator[BillingStack]:
    install_fake_stripe(monkeypatch)
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_billing_table(client, "billing-moto")
        yield BillingStack(client, "billing-moto")


def test_evento_stripe_repetido_nao_incrementa_a_versao(stack: BillingStack) -> None:
    stack.post_webhook("evt_01")
    stack.drain()
    first = stack.snapshot()

    stack.post_webhook("evt_02")
    second_drain = stack.drain()

    assert first.entitlement_version == 1
    assert stack.snapshot() == first
    assert second_drain.reprocessed == 1
    for event_id in ("evt_01", "evt_02"):
        assert stack.inbox_state(event_id) is InboxProcessingState.PROCESSED
    assert stack.stripe.state_calls == 2


class UnusedRecovery:
    def __init__(self) -> None:
        self.calls = 0

    def drain_inbox(self, limit: int) -> Any:
        self.calls += 1

    def run(self, request: RecoveryRequest) -> Any:
        self.calls += 1


class UnusedReconciler:
    def __init__(self) -> None:
        self.calls = 0

    def run(self, request: Any) -> Any:
        self.calls += 1


def build_worker(env: RevEnv, sweep: Any, metrics: MetricSpy) -> tuple[BillingWorker, Any]:
    recovery = UnusedRecovery()
    jobs = WorkerJobs(
        recovery=recovery,
        request=RecoveryRequest(72, 100),
        reconciler=UnusedReconciler(),
        revocations=sweep,
        reservations=env.quota,
        metrics=metrics,
        clock=env.clock.now,
    )
    return BillingWorker(jobs), recovery


def test_worker_revoke_pending_e_release_expired_com_componentes_reais(env: RevEnv) -> None:
    interrupted = interrupt_admin_revocation(env, run_count=2)
    metrics = MetricSpy()
    sweep = build_sweep(env, interrupted.catalog, interrupted.service, metrics)
    worker, recovery = build_worker(env, sweep, metrics)
    reservations = seed_agent_capacity(env, owned=(), orphans=("agent-orphan",))

    revoked = worker.run_revoke_pending(10)

    assert (revoked.examined, revoked.resumed, revoked.fenced) == (1, 1, 1)
    assert env.store.get_revocation_progress(ACCOUNT).phase is RevocationPhase.COMPLETE
    for run_id in interrupted.run_ids:
        assert stored_run(env, run_id).state is RunState.CANCELED
    assert metrics.named("QuotaReservationsExpired") == []
    env.clock.advance(PAST_EXPIRY)

    released = worker.run_release_expired(10)

    assert released.released == 1
    assert stored_capacity(env, reservations["agent-orphan"]).status is ReservationStatus.RELEASED
    [metric] = metrics.named("QuotaReservationsExpired")
    assert metric.value == 1
    assert recovery.calls == 0
