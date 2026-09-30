"""Corridas e reordenação de eventos Stripe no StripeEventProjector."""

import threading
from datetime import timedelta
from typing import Any

import boto3
import pytest
from hypothesis import given
from hypothesis import strategies as st
from moto import mock_aws

from cnes_domain.billing.inbox import InboxProcessingState
from cnes_domain.billing.models import SubscriptionStatus
from packages.cnes_infra.tests.billing.billing_factories import create_table
from packages.cnes_infra.tests.billing.test_projector import (
    PRICE_V2,
    ProjectorEnv,
    make_state,
    projector_env,
)

pytestmark = pytest.mark.race


class _AtomicClient:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._lock = threading.Lock()

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._inner, name)
        if not callable(target):
            return target

        def locked(*args: Any, **kwargs: Any) -> Any:
            with self._lock:
                return target(*args, **kwargs)

        return locked


class _BlockingFirstAttempt:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.ready = threading.Event()
        self.release = threading.Event()

    def get_snapshot(self, account_id: str, consistency: Any) -> Any:
        return self._inner.get_snapshot(account_id, consistency)

    def commit_claimed_snapshot(self, claim: Any, command: Any) -> bool:
        if claim.attempt == 1:
            self.ready.set()
            assert self.release.wait(timeout=10)
        return self._inner.commit_claimed_snapshot(claim, command)


def _atomic_env() -> ProjectorEnv:
    client = _AtomicClient(boto3.client("dynamodb", region_name="us-east-1"))
    create_table(client)
    return ProjectorEnv(client)


def test_retry_concorrente_aplica_evento_uma_vez(executor):
    with mock_aws():
        env = _atomic_env()
        env.accept("evt_01")
        projector = env.projector()
        barrier = threading.Barrier(2)

        def run() -> Any:
            barrier.wait()
            return projector.process("evt_01")

        futures = [executor.submit(run) for _ in range(2)]
        results = [future.result() for future in futures]
        state = env.inbox_state()
        snapshot = env.snapshot()
    assert [result.applied for result in results].count(True) == 1
    assert state is InboxProcessingState.PROCESSED
    assert snapshot.entitlement_version == 1


def test_claim_antigo_nao_regrede_snapshot_apos_reclaim(executor):
    with projector_env() as env:
        stale = make_state(subscription_status=SubscriptionStatus.PAST_DUE)
        current = make_state(stripe_price_id=PRICE_V2)
        env.stripe.get_current_state.side_effect = [stale, current]
        projection = _BlockingFirstAttempt(env.projection)
        projector = env.projector(projection=projection)
        env.accept("evt_01")
        old = executor.submit(projector.process, "evt_01")
        assert projection.ready.wait(timeout=10)
        env.clock.advance(timedelta(seconds=301))
        new_result = projector.process("evt_01")
        projection.release.set()
        old_result = old.result(timeout=10)
        snapshot = env.snapshot()
        audits = env.all_audit_payloads()
    assert new_result.applied is True
    assert old_result.applied is False
    assert snapshot.subscription_status is SubscriptionStatus.ACTIVE
    assert snapshot.plan_version_id == "plan_v2"
    assert snapshot.entitlement_version == 1
    statuses = {row["payload"]["attributes"]["subscription_status"] for row in audits}
    assert len(audits) == 2
    assert statuses == {"active"}
    assert {row["payload"]["attributes"]["entitlement_version"] for row in audits} == {1}


@given(data=st.data())
def test_ordem_arbitraria_e_duplicatas_convergem_ao_estado_atual(data):
    count = data.draw(st.integers(min_value=1, max_value=4))
    ids = [f"evt_{index:02d}" for index in range(count)]
    accepts = data.draw(st.permutations(ids))
    duplicates = data.draw(st.lists(st.sampled_from(ids), max_size=3))
    extra_calls = data.draw(st.lists(st.sampled_from(ids), max_size=4))
    calls = data.draw(st.permutations(ids + extra_calls))
    with projector_env() as env:
        env.stripe.get_current_state.return_value = make_state(stripe_price_id=PRICE_V2)
        for event_id in [*accepts, *duplicates]:
            env.accept(event_id)
        for event_id in calls:
            env.projector().process(event_id)
        for event_id in duplicates:
            env.accept(event_id)
        states = {event_id: env.inbox_state(event_id) for event_id in ids}
        snapshot = env.snapshot()
    assert set(states.values()) == {InboxProcessingState.PROCESSED}
    assert snapshot.subscription_status is SubscriptionStatus.ACTIVE
    assert snapshot.plan_version_id == "plan_v2"
    assert snapshot.entitlement_version == count
