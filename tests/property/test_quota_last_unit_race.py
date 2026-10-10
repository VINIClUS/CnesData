"""Corridas concorrentes pela última unidade de quota e budget."""

import json
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.billing.errors import QuotaExceeded, RetryableBillingError
from cnes_domain.billing.models import (
    AnalyticsAuthorization,
    BillingEnforcementMode,
    CapacityKind,
    CapacityReservation,
    RunAuthorization,
)
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from cnes_infra.billing.keys import pending_capacity_key
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    make_create_command,
    put_tenant,
)
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    TENANT,
    make_analytics_command,
    make_capacity_command,
    make_quota_snapshot,
    make_reserve_command,
    seed_capacity,
    seed_snapshot,
    table_items,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

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


@dataclass(frozen=True, slots=True)
class _Env:
    client: Any
    repo: DynamoQuotaReservations


@contextmanager
def _env(snapshot: Any) -> Iterator[_Env]:
    with mock_aws():
        client = _AtomicClient(boto3.client("dynamodb", region_name="us-east-1"))
        create_table(client)
        seed_snapshot(client, snapshot)
        seed_capacity(client)
        yield _Env(client, DynamoQuotaReservations(client, TABLE_NAME, MutableClock(NOW).now))


def _race(executor: Any, calls: list[Callable[[], Any]]) -> list[Future]:
    barrier = threading.Barrier(len(calls))

    def run(call: Callable[[], Any]) -> Any:
        barrier.wait()
        try:
            return call()
        except QuotaExceeded as error:
            return error

    return [executor.submit(run, call) for call in calls]


def _outcomes(executor: Any, calls: list[Callable[[], Any]]) -> list[Any]:
    return [future.result() for future in _race(executor, calls)]


def _split(outcomes: list[Any], success: type) -> tuple[list[Any], list[QuotaExceeded]]:
    winners = [item for item in outcomes if isinstance(item, success)]
    losers = [item for item in outcomes if isinstance(item, QuotaExceeded)]
    assert len(winners) + len(losers) == len(outcomes)
    return winners, losers


def _by_entity(client: Any, entity: str) -> list[dict[str, Any]]:
    return [item for item in table_items(client) if item["entity"]["S"] == entity]


def _counter(client: Any, sk: str, attribute: str) -> int:
    items = [item for item in table_items(client) if item["sk"]["S"] == sk]
    assert len(items) == 1
    return int(items[0][attribute]["N"])


def _events(client: Any, event_type: str) -> list[dict[str, Any]]:
    payloads = [json.loads(item["payload"]["S"]) for item in _by_entity(client, "OUTBOXEVENT")]
    return [event for event in payloads if event["event_type"] == event_type]


@pytest.mark.parametrize("contenders", [2, 8])
def test_duas_reservas_disputam_ultima_unidade(executor, contenders):
    with _env(make_quota_snapshot(max_runs_per_period=1)) as env:
        commands = [
            make_reserve_command(
                make_quota_snapshot(max_runs_per_period=1),
                run_id=f"run-{index}",
                idempotency_key=f"req-{index}",
            )
            for index in range(contenders)
        ]
        outcomes = _outcomes(
            executor, [lambda c=c: env.repo.reserve_and_create_run(c) for c in commands]
        )
        winners, losers = _split(outcomes, RunAuthorization)
        consumed = _counter(env.client, "USAGE", "consumed_runs")
        runs = _by_entity(env.client, "RUN")
        reservations = _by_entity(env.client, "QUOTARESERVATION")
        reserved = _events(env.client, "quota.reserved")
    assert len(winners) == 1
    assert len(losers) == contenders - 1
    assert all("max_runs_per_period_exceeded" in str(loser) for loser in losers)
    assert consumed == 1
    assert len(runs) == 1
    assert len(reservations) == 1
    assert len(reserved) == 1


def _capacity_race(executor: Any, kind: CapacityKind, contenders: int) -> tuple[list, Any]:
    with _env(make_quota_snapshot()) as env:
        commands = [
            make_capacity_command(
                kind,
                limit=1,
                resource_id=f"resource-{index}",
                idempotency_key=f"cap-{index}",
            )
            for index in range(contenders)
        ]
        outcomes = _outcomes(
            executor, [lambda c=c: env.repo.reserve_capacity(c) for c in commands]
        )
        counter = "agent_count" if kind is CapacityKind.AGENT else "tenant_count"
        return outcomes, _counter(env.client, "CAPACITY", counter)


@pytest.mark.parametrize("contenders", [2, 8])
def test_dois_agents_disputam_ultima_vaga(executor, contenders):
    outcomes, agent_count = _capacity_race(executor, CapacityKind.AGENT, contenders)
    winners, losers = _split(outcomes, CapacityReservation)
    assert len(winners) == 1
    assert len(losers) == contenders - 1
    assert all("max_agents_exceeded" in str(loser) for loser in losers)
    assert agent_count == 1


@pytest.mark.parametrize("contenders", [2, 8])
def test_dois_tenants_disputam_ultima_vaga(executor, contenders):
    outcomes, tenant_count = _capacity_race(executor, CapacityKind.TENANT, contenders)
    winners, losers = _split(outcomes, CapacityReservation)
    assert len(winners) == 1
    assert len(losers) == contenders - 1
    assert all("max_tenants_exceeded" in str(loser) for loser in losers)
    assert tenant_count == 1


@pytest.mark.parametrize("contenders", [2, 8])
def test_duas_consultas_disputam_ultimos_bytes(executor, contenders):
    snapshot = make_quota_snapshot(athena_scan_budget_bytes=1_500)
    with _env(snapshot) as env:
        commands = [
            make_analytics_command(
                snapshot,
                query_id=f"query-{index}",
                idempotency_key=f"aq-{index}",
                estimated_scan_bytes=1_000,
            )
            for index in range(contenders)
        ]
        outcomes = _outcomes(
            executor, [lambda c=c: env.repo.reserve_analytics(c) for c in commands]
        )
        winners, losers = _split(outcomes, AnalyticsAuthorization)
        committed = _counter(env.client, "USAGE", "analytics_committed_scan_bytes")
    assert len(winners) == 1
    assert len(losers) == contenders - 1
    assert all("athena_scan_budget_exceeded" in str(loser) for loser in losers)
    assert committed == 1_000


@pytest.mark.parametrize("contenders", [2, 8])
def test_mesma_chave_concorrente_reserva_uma_vez(executor, contenders):
    snapshot = make_quota_snapshot(max_runs_per_period=1)
    with _env(snapshot) as env:
        command = make_reserve_command(snapshot)
        outcomes = _outcomes(
            executor, [lambda: env.repo.reserve_and_create_run(command)] * contenders
        )
        consumed = _counter(env.client, "USAGE", "consumed_runs")
        reservations = _by_entity(env.client, "QUOTARESERVATION")
    assert all(isinstance(outcome, RunAuthorization) for outcome in outcomes)
    assert all(outcome == outcomes[0] for outcome in outcomes)
    assert consumed == 1
    assert len(reservations) == 1
    assert outcomes[0].billing_account_id == ACCOUNT


_SHADOW = BillingSettings(BillingMode.STRIPE, BillingEnforcementMode.SHADOW, 0)


def _shadow_plane(env: _Env) -> DynamoDBControlPlane:
    return DynamoDBControlPlane(env.client, TABLE_NAME, MutableClock(NOW).now, _SHADOW)


@contextmanager
def _account_env(linked: bool) -> Iterator[_Env]:
    with mock_aws():
        client = _AtomicClient(boto3.client("dynamodb", region_name="us-east-1"))
        create_table(client)
        seed_snapshot(client, make_quota_snapshot())
        put_tenant(client, TENANT)
        env = _Env(client, DynamoQuotaReservations(client, TABLE_NAME, MutableClock(NOW).now))
        if linked:
            _create_account(env)
        yield env


def _create_account(env: _Env) -> Any:
    catalog = DynamoBillingCatalog(env.client, TABLE_NAME, MutableClock(NOW).now)
    for _ in range(20):
        try:
            return catalog.create_account(make_create_command(ACCOUNT, TENANT))
        except RetryableBillingError:
            continue
    raise AssertionError("create_account_never_won")


@pytest.mark.parametrize("contenders", [2, 4])
def test_shadow_e_enforce_somam_no_mesmo_contador(executor, contenders):
    with _account_env(linked=True) as env:
        plane = _shadow_plane(env)
        shadow = [
            lambda i=i: plane.register_edge_agent(TENANT, f"shadow-{i}", "a" * 64, NOW)
            for i in range(contenders)
        ]
        enforce = [
            lambda i=i: env.repo.reserve_capacity(make_capacity_command(
                limit=contenders + 1, resource_id=f"enf-{i}", idempotency_key=f"cap-{i}",
            ))
            for i in range(contenders)
        ]
        outcomes = _outcomes(executor, [*shadow, *enforce])
        reserved = [item for item in outcomes if isinstance(item, CapacityReservation)]
        agent_count = _counter(env.client, "CAPACITY", "agent_count")
    assert agent_count == contenders + len(reserved)
    assert 1 <= len(reserved) <= contenders + 1


@pytest.mark.parametrize("contenders", [2, 8])
def test_mesmo_agente_novo_registrado_em_paralelo_conta_uma_vez(executor, contenders):
    with _account_env(linked=True) as env:
        plane = _shadow_plane(env)
        calls = [lambda: plane.register_edge_agent(TENANT, "agent-1", "a" * 64, NOW)]
        outcomes = _outcomes(executor, calls * contenders)
        agent_count = _counter(env.client, "CAPACITY", "agent_count")
    assert {outcome.agent_id for outcome in outcomes} == {"agent-1"}
    assert agent_count == 1


@pytest.mark.parametrize("contenders", [2, 7])
def test_criacao_da_conta_e_agentes_sem_conta_nao_perdem_contagem(executor, contenders):
    with _account_env(linked=False) as env:
        plane = _shadow_plane(env)
        agents = [
            lambda i=i: plane.register_edge_agent(TENANT, f"agent-{i}", "a" * 64, NOW)
            for i in range(contenders)
        ]
        _outcomes(executor, [lambda: _create_account(env), *agents])
        agent_count = _counter(env.client, "CAPACITY", "agent_count")
        tenant_count = _counter(env.client, "CAPACITY", "tenant_count")
        pending = [item for item in table_items(env.client)
                   if (item["pk"]["S"], item["sk"]["S"]) == pending_capacity_key(TENANT)]
    assert agent_count == contenders
    assert tenant_count == 1
    assert pending == []
