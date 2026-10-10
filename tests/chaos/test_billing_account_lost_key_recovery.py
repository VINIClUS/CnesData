"""Chave nova recupera a conta de billing do tenant vinculado e o mesmo Customer Stripe."""

import pytest

pytest.importorskip("moto")

import logging
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import timedelta
from types import SimpleNamespace
from typing import cast

import boto3
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import Response
from moto import mock_aws

from central_api.auth.aws_oidc import AuthorizedTenant, TenantAccessDenied
from central_api.routes.billing import (
    get_billing_catalog,
    get_billing_clock,
    get_billing_mode,
    get_billing_principal,
    get_membership_authorizer,
    get_stripe_gateway,
    router,
)
from cnes_domain.profiles import BillingMode
from cnes_infra.auth.oidc import OidcPrincipal
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.stripe_gateway import StripeClientProtocol, StripeGateway
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    put_tenant,
)
from packages.cnes_infra.tests.billing.stripe_fakes import make_config
from packages.cnes_infra.tests.contracts.clock import MutableClock
from tests.chaos.test_stripe_customer_orphan_recovery import FakeStripeCustomers, FaultyDynamo

pytestmark = [pytest.mark.chaos]

TENANT = "tenant-a"
OTHER_TENANT = "tenant-b"
ACCOUNTS = "/api/v1/billing/accounts"
ORIGINAL_KEY = "client-key-0123456789"
NEW_KEY = "client-key-new-0123456789"


@dataclass
class Caller:
    subject: str = "user-1"
    memberships: dict[tuple[str, str], str] = field(default_factory=dict[tuple[str, str], str])

    def principal(self) -> OidcPrincipal:
        return OidcPrincipal("https://issuer", self.subject, None, None)

    def authorize(self, principal: OidcPrincipal, tenant_id: str) -> AuthorizedTenant:
        role = self.memberships.get((tenant_id, principal.subject))
        if role is None:
            raise TenantAccessDenied("membership_missing")
        return AuthorizedTenant(tenant_id, principal.subject, role)


@dataclass(frozen=True, slots=True)
class Env:
    dynamo: FaultyDynamo
    stripe: FakeStripeCustomers
    clock: MutableClock
    catalog: DynamoBillingCatalog
    caller: Caller
    http: TestClient


def _app(env: SimpleNamespace) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides.update({
        get_billing_mode: lambda: BillingMode.STRIPE,
        get_billing_principal: env.caller.principal,
        get_membership_authorizer: lambda: env.caller,
        get_billing_catalog: lambda: env.catalog,
        get_stripe_gateway: lambda: env.gateway,
        get_billing_clock: lambda: env.clock.now,
    })
    return app


@pytest.fixture
def env() -> Iterator[Env]:
    with mock_aws():
        raw = boto3.client("dynamodb", region_name="us-east-1")
        create_table(raw)
        put_tenant(raw, TENANT)
        put_tenant(raw, OTHER_TENANT)
        clock = MutableClock(NOW)
        dynamo = FaultyDynamo(raw)
        catalog = DynamoBillingCatalog(dynamo, TABLE_NAME, clock.now)
        stripe = FakeStripeCustomers(clock)
        client = SimpleNamespace(v1=SimpleNamespace(customers=stripe))
        gateway = StripeGateway(cast("StripeClientProtocol", client), make_config(), catalog)
        caller = Caller(memberships={(TENANT, "user-1"): "gestor"})
        parts = SimpleNamespace(caller=caller, catalog=catalog, gateway=gateway, clock=clock)
        with TestClient(_app(parts)) as http:
            yield Env(dynamo, stripe, clock, catalog, caller, http)


def _create(env: Env, key: str, tenant: str = TENANT) -> Response:
    return env.http.post(ACCOUNTS, json={"idempotency_key": key}, headers={"X-Tenant-Id": tenant})


def _crash_before_attach(env: Env) -> tuple[str, str]:
    env.dynamo.drop_before_commit = 1
    assert _create(env, ORIGINAL_KEY).status_code == 503
    (customer,) = env.stripe.customers
    account = env.catalog.get_account(customer.metadata.billing_account_id)
    assert account is not None
    assert account.stripe_customer_id is None
    return account.billing_account_id, customer.id


def _assert_single_customer(env: Env, account_id: str, customer_id: str) -> None:
    assert [c.id for c in env.stripe.customers] == [customer_id]
    account = env.catalog.get_account(account_id)
    assert account is not None
    assert account.stripe_customer_id == customer_id


@pytest.mark.parametrize(
    "delay", [timedelta(seconds=30), timedelta(hours=25)], ids=["chave-stripe-valida", "apos-24h"],
)
def test_chave_nova_apos_queda_no_anexo_recupera_conta_e_mesmo_customer(
    env: Env, delay: timedelta, caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    account_id, customer_id = _crash_before_attach(env)
    env.clock.advance(delay)

    response = _create(env, NEW_KEY)

    assert response.status_code == 201
    assert response.json()["billing_account_id"] == account_id
    assert response.json()["stripe_customer_id"] == customer_id
    assert response.json()["owner_user_id"] == "user-1"
    _assert_single_customer(env, account_id, customer_id)
    assert (
        f"billing_account_recovered billing_account_id={account_id} tenant_id={TENANT}"
        in caplog.messages
    )


def test_chave_nova_com_conta_ja_anexada_devolve_conta_sem_chamar_stripe(env: Env) -> None:
    created = _create(env, ORIGINAL_KEY).json()
    searches = env.stripe.searches

    response = _create(env, NEW_KEY)

    assert response.status_code == 201
    assert response.json() == created
    assert env.stripe.searches == searches
    _assert_single_customer(env, created["billing_account_id"], created["stripe_customer_id"])


def test_gestor_que_nao_e_dono_recupera_conta_do_tenant_vinculado(env: Env) -> None:
    account_id, customer_id = _crash_before_attach(env)
    env.caller.memberships[(TENANT, "user-2")] = "gestor"
    env.caller.subject = "user-2"

    response = _create(env, NEW_KEY)

    assert response.status_code == 201
    assert response.json()["billing_account_id"] == account_id
    assert response.json()["owner_user_id"] == "user-1"
    assert response.json()["stripe_customer_id"] == customer_id
    _assert_single_customer(env, account_id, customer_id)


def test_criacoes_concorrentes_com_chaves_distintas_convergem_para_uma_conta(
    env: Env, caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    keys = [f"client-key-race-{index:04d}-abcdef" for index in range(4)]
    barrier = threading.Barrier(len(keys))

    def create(key: str) -> Response:
        barrier.wait()
        return _create(env, key)

    with ThreadPoolExecutor(max_workers=len(keys)) as pool:
        responses = list(pool.map(create, keys))

    assert {response.status_code for response in responses} <= {201, 503}
    recovered = [m for m in caplog.messages if m.startswith("billing_account_recovered ")]
    assert len(recovered) >= len(keys) - 1
    retried = [_create(env, key).json() for key in keys]
    assert len({body["billing_account_id"] for body in retried}) == 1
    (customer,) = env.stripe.customers
    assert {body["stripe_customer_id"] for body in retried} == {customer.id}
    winners = [r.json() for r in responses if r.status_code == 201]
    assert all(body == retried[0] for body in winners)


def test_viewer_do_tenant_vinculado_nao_recupera_conta(env: Env) -> None:
    _create(env, ORIGINAL_KEY)
    env.caller.memberships[(TENANT, "user-3")] = "viewer"
    env.caller.subject = "user-3"

    response = _create(env, NEW_KEY)

    assert response.status_code == 403
    assert response.json() == {"detail": "billing_admin_required"}
    assert len(env.stripe.customers) == 1


def test_usuario_sem_membership_no_tenant_nao_recupera_conta(env: Env) -> None:
    _create(env, ORIGINAL_KEY)
    env.caller.subject = "user-4"

    response = _create(env, NEW_KEY)

    assert response.status_code == 403
    assert response.json() == {"detail": "tenant_not_allowed"}
    assert len(env.stripe.customers) == 1


def test_gestor_de_outro_tenant_cria_a_propria_conta(env: Env) -> None:
    first = _create(env, ORIGINAL_KEY).json()
    env.caller.memberships[(OTHER_TENANT, "user-2")] = "gestor"
    env.caller.subject = "user-2"

    response = _create(env, NEW_KEY, tenant=OTHER_TENANT)

    assert response.status_code == 201
    assert response.json()["billing_account_id"] != first["billing_account_id"]
    assert response.json()["owner_user_id"] == "user-2"
    assert len(env.stripe.customers) == 2
