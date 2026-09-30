"""Corridas concorrentes da reserva de checkout pendente por conta."""

import threading
from concurrent.futures import Future
from datetime import timedelta
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.billing.commands import PendingCheckout, ReservePendingCheckoutCommand
from cnes_domain.billing.errors import PermanentBillingError
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME, create_table

pytestmark = pytest.mark.race

KEY_A = "a" * 64
KEY_B = "b" * 64
ITERATIONS = 20


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


def _setup() -> DynamoBillingCatalog:
    client = _AtomicClient(boto3.client("dynamodb", region_name="us-east-1"))
    create_table(client)
    return DynamoBillingCatalog(client, TABLE_NAME, lambda: NOW)


def _race(executor: Any, calls: list[Any]) -> list[Future]:
    barrier = threading.Barrier(len(calls))

    def run(call: Any) -> Any:
        barrier.wait()
        return call()

    return [executor.submit(run, call) for call in calls]


def _command(account: str, key: str) -> ReservePendingCheckoutCommand:
    return ReservePendingCheckoutCommand(account, key, NOW + timedelta(minutes=30))


def _outcome(future: Future) -> PendingCheckout | PermanentBillingError:
    try:
        return future.result()
    except PermanentBillingError as error:
        return error


def test_somente_uma_chave_diferente_reserva_o_checkout_da_conta(executor):
    with mock_aws():
        catalog = _setup()
        for index in range(ITERATIONS):
            account = f"ba_race_{index}"
            futures = _race(
                executor,
                [
                    lambda a=account, k=key: catalog.reserve_pending_checkout(_command(a, k))
                    for key in (KEY_A, KEY_B)
                ],
            )
            outcomes = [_outcome(future) for future in futures]
            winners = [item for item in outcomes if isinstance(item, PendingCheckout)]
            losers = [item for item in outcomes if isinstance(item, PermanentBillingError)]
            assert len(winners) == 1
            assert [error.code for error in losers] == ["checkout_in_progress"]


def test_reservas_concorrentes_com_a_mesma_chave_sao_idempotentes(executor):
    with mock_aws():
        catalog = _setup()
        for index in range(ITERATIONS):
            account = f"ba_same_{index}"
            futures = _race(
                executor,
                [
                    lambda a=account: catalog.reserve_pending_checkout(_command(a, KEY_A))
                    for _ in range(2)
                ],
            )
            results = [future.result() for future in futures]
            assert {result.request_key for result in results} == {KEY_A}
            assert len({result.reserved_at for result in results}) == 1
