"""Corridas concorrentes do CAS de projeção de entitlement."""

import threading
from concurrent.futures import Future
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.billing.commands import SnapshotWrite
from cnes_domain.billing.errors import StaleInboxClaim
from cnes_domain.billing.inbox import InboxClaim
from cnes_domain.billing.models import ReadConsistency
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.billing.keys import stripe_event_key
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    make_snapshot,
    make_write,
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


def _setup() -> tuple[Any, DynamoEntitlementProjection]:
    client = _AtomicClient(boto3.client("dynamodb", region_name="us-east-1"))
    create_table(client)
    return client, DynamoEntitlementProjection(client, TABLE_NAME, lambda: NOW)


def _race(executor: Any, calls: list[Any]) -> list[Future]:
    barrier = threading.Barrier(len(calls))

    def run(call: Any) -> Any:
        barrier.wait()
        return call()

    return [executor.submit(run, call) for call in calls]


@pytest.mark.parametrize("contenders", [2, 8])
def test_exatamente_um_cas_vence_por_versao_esperada(executor, contenders):
    with mock_aws():
        _, projection = _setup()
        projection.compare_and_set_snapshot(make_write(0))
        writes = [
            SnapshotWrite(1, make_snapshot(version=2, source_event_id=f"evt_race_{index}"), ())
            for index in range(contenders)
        ]
        futures = _race(
            executor, [lambda w=w: projection.compare_and_set_snapshot(w) for w in writes]
        )
        results = [future.result() for future in futures]
        stored = projection.get_snapshot("ba_01", ReadConsistency.STRONG)
    assert results.count(True) == 1
    assert results.count(False) == contenders - 1
    winner = writes[results.index(True)].snapshot
    assert stored is not None
    assert stored.entitlement_version == 2
    assert stored.source_event_id == winner.source_event_id


def _claimed_write() -> SnapshotWrite:
    return SnapshotWrite(0, make_snapshot(version=1, source_event_id="evt_01"), ())

def test_somente_o_claim_da_tentativa_atual_vence_a_corrida(executor):
    with mock_aws():
        client, projection = _setup()
        client.put_item(
            TableName=TABLE_NAME,
            Item={
                **item_key(*stripe_event_key("evt_01")),
                "entity": {"S": "STRIPEEVENTINBOX"},
                "state": {"S": "processing"},
                "attempt": {"N": "2"},
            },
        )
        claims = [
            InboxClaim("evt_01", "customer.subscription.updated", "cus_01", None, attempt, True)
            for attempt in (1, 2)
        ]
        futures = _race(
            executor,
            [lambda c=c: projection.commit_claimed_snapshot(c, _claimed_write()) for c in claims],
        )
        with pytest.raises(StaleInboxClaim):
            futures[0].result()
        assert futures[1].result() is True
