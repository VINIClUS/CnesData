"""Customer Stripe orfao por queda no anexo converge para um so Customer, inclusive apos 24 h."""

import pytest

pytest.importorskip("moto")

import logging
import re
import threading
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import SimpleNamespace
from typing import Any

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import Response
from moto import mock_aws

from central_api.auth.aws_oidc import AuthorizedTenant
from central_api.routes.billing import (
    get_billing_catalog,
    get_billing_clock,
    get_billing_mode,
    get_billing_principal,
    get_membership_authorizer,
    get_stripe_gateway,
    router,
)
from cnes_domain.billing.models import BillingAccount
from cnes_domain.profiles import BillingMode
from cnes_infra.auth.oidc import OidcPrincipal
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.stripe_gateway import StripeGateway
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    put_tenant,
)
from packages.cnes_infra.tests.billing.stripe_fakes import make_config
from packages.cnes_infra.tests.contracts.clock import MutableClock

pytestmark = [pytest.mark.chaos]

IDEMPOTENCY_WINDOW = timedelta(hours=24)
SEARCH_INDEX_LAG = timedelta(minutes=1)
TENANT = "tenant-a"
ACCOUNTS = "/api/v1/billing/accounts"
HEADERS = {"X-Tenant-Id": TENANT}
BODY = {"idempotency_key": "client-key-0123456789"}
PRINCIPAL = OidcPrincipal("https://issuer", "user-1", None, None)
_QUERY = re.compile(r"^metadata\['billing_account_id'\]:'([A-Za-z0-9_-]+)'$")
_CUSTOMER_MAP_PREFIX = "STRIPE_CUSTOMER#"


class StripeError(Exception):
    def __init__(self, http_status: int) -> None:
        super().__init__(f"http_status={http_status}")
        self.http_status = http_status


class RateLimitError(StripeError):
    pass


class APIError(StripeError):
    pass


def _replayed(outcome: SimpleNamespace | None) -> SimpleNamespace:
    if outcome is None:
        raise APIError(500)
    return outcome


class FakeStripeCustomers:
    """Customers Stripe: a chave vale 24 h, guarda até um 500, responde 409 a requisição
    concorrente com a mesma chave e a busca indexa com atraso."""

    def __init__(self, clock: MutableClock) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._keys: dict[str, tuple[datetime, SimpleNamespace | None]] = {}
        self.customers: list[SimpleNamespace] = []
        self.searches = 0
        self.search_failures = 0
        self.errors_after_create = 0
        self.errors_without_create = 0
        self.in_flight_conflicts = 0
        self.index_lag = SEARCH_INDEX_LAG

    def create(self, params: dict[str, Any], options: dict[str, Any]) -> SimpleNamespace:
        with self._lock:
            now = self._clock.now()
            cached = self._keys.get(options["idempotency_key"])
            if cached is not None and now - cached[0] < IDEMPOTENCY_WINDOW:
                return _replayed(cached[1])
            outcome = self._outcome(params["metadata"]["billing_account_id"], now)
            self._keys[options["idempotency_key"]] = (now, outcome)
            if self.in_flight_conflicts:
                self.in_flight_conflicts -= 1
                raise APIError(409)
            return _replayed(outcome)

    def search(self, params: dict[str, Any]) -> SimpleNamespace:
        account_id = _QUERY.fullmatch(params["query"]).group(1)
        indexed_until = int((self._clock.now() - self.index_lag).timestamp())
        with self._lock:
            self.searches += 1
            if self.search_failures:
                self.search_failures -= 1
                raise RateLimitError(429)
            hits = [
                customer for customer in reversed(self.customers)
                if customer.metadata.billing_account_id == account_id
                and customer.created <= indexed_until
            ]
        return SimpleNamespace(data=hits[: params["limit"]], has_more=len(hits) > params["limit"])

    def seed_orphan(self, account_id: str, created_at: datetime) -> SimpleNamespace:
        with self._lock:
            return self._append(account_id, created_at)

    def of(self, account_id: str) -> list[str]:
        return [c.id for c in self.customers if c.metadata.billing_account_id == account_id]

    def _outcome(self, account_id: str, now: datetime) -> SimpleNamespace | None:
        if self.errors_without_create:
            self.errors_without_create -= 1
            return None
        customer = self._append(account_id, now)
        if self.errors_after_create:
            self.errors_after_create -= 1
            return None
        return customer

    def _append(self, account_id: str, created_at: datetime) -> SimpleNamespace:
        customer = SimpleNamespace(
            id=f"cus_{len(self.customers) + 1:03d}",
            created=int(created_at.timestamp()),
            metadata=SimpleNamespace(billing_account_id=account_id),
        )
        self.customers.append(customer)
        return customer


def _unavailable() -> ClientError:
    error = {"Error": {"Code": "InternalServerError", "Message": "injected"}}
    return ClientError(error, "TransactWriteItems")


def _attaches_customer(actions: list[dict[str, Any]]) -> bool:
    keys = (action.get("Put", {}).get("Item", {}).get("pk", {}).get("S", "") for action in actions)
    return any(key.startswith(_CUSTOMER_MAP_PREFIX) for key in keys)


class FaultyDynamo:
    """DynamoDB serializado que derruba o anexo antes ou depois do commit."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._lock = threading.Lock()
        self.drop_before_commit = 0
        self.drop_after_commit = 0
        self.before_attach: Callable[[], None] | None = None

    def transact_write_items(self, **kwargs: Any) -> Any:
        attach = _attaches_customer(kwargs["TransactItems"])
        if attach:
            self._run_before_attach()
        with self._lock:
            if attach and self.drop_before_commit:
                self.drop_before_commit -= 1
                raise _unavailable()
            response = self._inner.transact_write_items(**kwargs)
            if attach and self.drop_after_commit:
                self.drop_after_commit -= 1
                raise _unavailable()
            return response

    def _run_before_attach(self) -> None:
        hook, self.before_attach = self.before_attach, None
        if hook is not None:
            hook()

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._inner, name)
        if not callable(target):
            return target

        def locked(*args: Any, **kwargs: Any) -> Any:
            with self._lock:
                return target(*args, **kwargs)

        return locked


class _GestorAuthorizer:
    def authorize(self, principal: OidcPrincipal, tenant_id: str) -> AuthorizedTenant:
        return AuthorizedTenant(tenant_id, principal.subject, "gestor")


@dataclass(frozen=True, slots=True)
class Env:
    dynamo: FaultyDynamo
    stripe: FakeStripeCustomers
    clock: MutableClock
    catalog: DynamoBillingCatalog
    http: TestClient


@dataclass(frozen=True, slots=True)
class Orphan:
    account_id: str
    customer_id: str


def _app(catalog: DynamoBillingCatalog, gateway: StripeGateway, clock: MutableClock) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides.update({
        get_billing_mode: lambda: BillingMode.STRIPE,
        get_billing_principal: lambda: PRINCIPAL,
        get_membership_authorizer: _GestorAuthorizer,
        get_billing_catalog: lambda: catalog,
        get_stripe_gateway: lambda: gateway,
        get_billing_clock: lambda: clock.now,
    })
    return app


@pytest.fixture
def env() -> Iterator[Env]:
    with mock_aws():
        raw = boto3.client("dynamodb", region_name="us-east-1")
        create_table(raw)
        put_tenant(raw, TENANT)
        clock = MutableClock(NOW)
        dynamo = FaultyDynamo(raw)
        catalog = DynamoBillingCatalog(dynamo, TABLE_NAME, clock.now)
        stripe = FakeStripeCustomers(clock)
        client = SimpleNamespace(v1=SimpleNamespace(customers=stripe))
        gateway = StripeGateway(client, make_config(), catalog)
        with TestClient(_app(catalog, gateway, clock)) as http:
            yield Env(dynamo, stripe, clock, catalog, http)


def _create(env: Env) -> Response:
    return env.http.post(ACCOUNTS, json=BODY, headers=HEADERS)


def _crash_before_attach(env: Env) -> Orphan:
    env.dynamo.drop_before_commit = 1
    assert _create(env).status_code == 503
    (customer,) = env.stripe.customers
    orphan = Orphan(customer.metadata.billing_account_id, customer.id)
    account = env.catalog.get_account(orphan.account_id)
    assert account is not None
    assert account.stripe_customer_id is None
    assert env.catalog.get_account_by_customer(orphan.customer_id) is None
    return orphan


def _assert_attached(env: Env, orphan: Orphan) -> None:
    assert env.stripe.of(orphan.account_id) == [orphan.customer_id]
    mapped = env.catalog.get_account_by_customer(orphan.customer_id)
    assert isinstance(mapped, BillingAccount)
    assert mapped.billing_account_id == orphan.account_id


@pytest.mark.parametrize(
    "delay",
    [
        timedelta(seconds=30),
        timedelta(minutes=5),
        timedelta(hours=23, minutes=59),
        timedelta(hours=24),
        timedelta(hours=25),
        timedelta(days=30),
    ],
    ids=["antes-da-indexacao", "busca-e-chave-validas", "fim-da-chave", "chave-expirada",
         "apos-24h", "apos-30-dias"],
)
def test_replay_apos_queda_no_anexo_converge_para_um_customer(env: Env, delay: timedelta) -> None:
    orphan = _crash_before_attach(env)
    env.clock.advance(delay)

    response = _create(env)

    assert response.status_code == 201
    assert response.json()["billing_account_id"] == orphan.account_id
    assert response.json()["stripe_customer_id"] == orphan.customer_id
    _assert_attached(env, orphan)


def test_replay_de_conta_ja_anexada_nao_chama_o_stripe(env: Env) -> None:
    orphan = _crash_before_attach(env)
    env.clock.advance(timedelta(hours=25))
    assert _create(env).status_code == 201
    searches = env.stripe.searches

    env.clock.advance(timedelta(days=2))
    response = _create(env)

    assert response.json()["stripe_customer_id"] == orphan.customer_id
    assert env.stripe.searches == searches
    _assert_attached(env, orphan)


def test_resposta_perdida_apos_commit_do_anexo_converge_sem_novo_customer(env: Env) -> None:
    env.dynamo.drop_after_commit = 1
    assert _create(env).status_code == 503
    (customer,) = env.stripe.customers
    orphan = Orphan(customer.metadata.billing_account_id, customer.id)
    searches = env.stripe.searches
    env.clock.advance(timedelta(hours=25))

    response = _create(env)

    assert response.status_code == 201
    assert response.json()["stripe_customer_id"] == orphan.customer_id
    assert env.stripe.searches == searches
    _assert_attached(env, orphan)


def test_duplicatas_legadas_convergem_para_o_customer_mais_antigo(env: Env) -> None:
    orphan = _crash_before_attach(env)
    env.clock.advance(timedelta(hours=30))
    legacy = env.stripe.seed_orphan(orphan.account_id, env.clock.now())
    env.clock.advance(timedelta(hours=1))

    first, second = _create(env), _create(env)

    assert first.json()["stripe_customer_id"] == orphan.customer_id
    assert second.json()["stripe_customer_id"] == orphan.customer_id
    assert env.stripe.of(orphan.account_id) == [orphan.customer_id, legacy.id]
    assert env.catalog.get_account_by_customer(legacy.id) is None


def test_busca_indisponivel_apos_24h_nao_cria_customer_duplicado(env: Env) -> None:
    orphan = _crash_before_attach(env)
    env.clock.advance(timedelta(hours=25))
    env.stripe.search_failures = 1

    unavailable = _create(env)

    assert unavailable.status_code == 503
    assert unavailable.json() == {"detail": "stripe_unavailable"}
    assert env.stripe.of(orphan.account_id) == [orphan.customer_id]
    assert _create(env).json()["stripe_customer_id"] == orphan.customer_id
    _assert_attached(env, orphan)


def test_replays_concorrentes_apos_24h_anexam_um_unico_customer(env: Env) -> None:
    orphan = _crash_before_attach(env)
    env.clock.advance(timedelta(hours=25))
    barrier = threading.Barrier(4)

    def replay(_: int) -> Response:
        barrier.wait()
        return _create(env)

    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(replay, range(4)))

    assert {response.status_code for response in responses} <= {201, 503}
    winners = [r.json()["stripe_customer_id"] for r in responses if r.status_code == 201]
    assert set(winners) <= {orphan.customer_id}
    assert _create(env).json()["stripe_customer_id"] == orphan.customer_id
    _assert_attached(env, orphan)


def test_erro_500_guardado_com_customer_criado_e_recuperado_pela_busca(env: Env) -> None:
    env.stripe.errors_after_create = 1
    failed = _create(env)
    assert failed.status_code == 503
    assert failed.json() == {"detail": "stripe_unavailable"}
    (customer,) = env.stripe.customers
    orphan = Orphan(customer.metadata.billing_account_id, customer.id)
    env.clock.advance(timedelta(hours=1))

    response = _create(env)

    assert response.status_code == 201
    assert response.json()["stripe_customer_id"] == orphan.customer_id
    _assert_attached(env, orphan)


def test_corrida_na_fronteira_de_24h_sem_busca_anexa_um_so_customer(env: Env, caplog) -> None:
    caplog.set_level(logging.WARNING)
    env.stripe.index_lag = timedelta(hours=30)
    orphan = _crash_before_attach(env)
    env.clock.advance(IDEMPOTENCY_WINDOW - timedelta(seconds=1))
    racers: list[Response] = []

    def replay_after_key_expiry() -> None:
        env.clock.advance(timedelta(seconds=2))
        racers.append(_create(env))

    env.dynamo.before_attach = replay_after_key_expiry
    first = _create(env)

    (second,) = racers
    winner = second.json()["stripe_customer_id"]
    assert (first.status_code, second.status_code) == (201, 201)
    assert first.json()["stripe_customer_id"] == winner
    assert env.stripe.of(orphan.account_id) == [orphan.customer_id, winner]
    assert env.catalog.get_account(orphan.account_id).stripe_customer_id == winner
    assert (
        f"billing_customer_orphaned billing_account_id={orphan.account_id} "
        f"stripe_customer_id={orphan.customer_id}"
    ) in caplog.messages


def test_chave_em_uso_por_replay_concorrente_pede_retry_e_converge(env: Env) -> None:
    env.stripe.in_flight_conflicts = 1

    conflicted = _create(env)
    response = _create(env)

    assert conflicted.status_code == 503
    assert conflicted.headers["Retry-After"] == "5"
    assert response.status_code == 201
    (customer,) = env.stripe.customers
    assert response.json()["stripe_customer_id"] == customer.id
    _assert_attached(env, Orphan(customer.metadata.billing_account_id, customer.id))


def test_erro_500_guardado_sem_customer_so_libera_apos_a_chave_expirar(env: Env) -> None:
    env.stripe.errors_without_create = 1
    assert _create(env).status_code == 503
    env.clock.advance(timedelta(hours=1))

    blocked = _create(env)
    env.clock.advance(IDEMPOTENCY_WINDOW)
    recovered = _create(env)

    assert blocked.status_code == 503
    assert recovered.status_code == 201
    (customer,) = env.stripe.customers
    assert recovered.json()["stripe_customer_id"] == customer.id
