"""Testes das rotas de billing: autorização, checkout, portal, status e modo desabilitado."""


from dataclasses import replace
from datetime import timedelta

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from central_api.auth.aws_oidc import AuthorizedTenant, TenantAccessDenied
from central_api.routes.billing import (
    BILLING_ADMIN_ROLES,
    get_billing_audit,
    get_billing_catalog,
    get_billing_clock,
    get_billing_mode,
    get_billing_principal,
    get_entitlement_projection,
    get_membership_authorizer,
    get_stripe_gateway,
    router,
)
from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingTenantConflict,
    IdempotencyConflict,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.models import (
    BillingAccountStatus,
    ReadConsistency,
    SubscriptionStatus,
)
from cnes_domain.profiles import BillingMode

from .billing_fakes import (
    CLIENT_KEY,
    HEADERS,
    MUTATIONS,
    NOW,
    PRINCIPAL,
    Env,
    checkout_body,
    make_account,
    make_link,
    make_plan,
    make_snapshot,
    portal_body,
    post,
    transfer_body,
)


@pytest.fixture
def env():
    return Env()


@pytest.fixture
def client(env):
    return TestClient(env.app())


def test_papeis_administrativos_sao_apenas_gestor():
    assert frozenset({"gestor"}) == BILLING_ADMIN_ROLES


def test_checkout_exige_billing_owner(client, env):
    response = client.post("/api/v1/billing/checkout", json=checkout_body())
    assert response.status_code == 403
    assert response.json() == {"detail": "billing_owner_required"}
    env.gateway.create_checkout.assert_not_called()


def test_admin_de_outro_tenant_nao_administra_conta(client, env):
    env.catalog.get_tenant_link.return_value = None
    response = client.post(
        "/api/v1/billing/portal", json=portal_body(), headers={"X-Tenant-Id": "tenant-b"},
    )
    assert response.status_code == 403
    env.catalog.get_tenant_link.assert_called_once_with("ba_01", "tenant-b", ReadConsistency.STRONG)
    env.gateway.create_portal.assert_not_called()


def test_link_de_conta_divergente_e_negado(client, env):
    env.catalog.get_tenant_link.return_value = make_link(account="ba_other")
    assert post(client, "portal").status_code == 403
    env.gateway.create_portal.assert_not_called()


def test_admin_do_tenant_vinculado_pode_administrar_conta(client, env):
    env.catalog.get_tenant_link.return_value = make_link("tenant-a")
    response = client.get("/api/v1/billing/status?billing_account_id=ba_01", headers=HEADERS)
    assert response.status_code == 200


def test_falha_ao_ler_link_nega_sem_chamar_stripe(client, env):
    env.catalog.get_tenant_link.side_effect = BillingDependencyError("dynamodb_unavailable")
    response = post(client, "checkout")
    assert response.status_code == 503
    assert response.json() == {"detail": "billing_dependency_unavailable"}
    env.gateway.create_checkout.assert_not_called()


def test_erro_inesperado_nao_e_mapeado_e_nao_chama_stripe(env):
    env.catalog.get_account.side_effect = RuntimeError("boom")
    client = TestClient(env.app(), raise_server_exceptions=False)
    assert post(client, "checkout").status_code == 500
    env.gateway.create_checkout.assert_not_called()


def test_dono_direto_administra_sem_ler_link(client, env):
    env.catalog.get_account.return_value = make_account(owner="user-1")
    response = client.post("/api/v1/billing/portal", json=portal_body())
    assert response.status_code == 201
    env.catalog.get_tenant_link.assert_not_called()


def test_redirect_de_sucesso_nao_altera_entitlement(client, env):
    env.catalog.get_account.return_value = make_account(owner="user-1")
    url = "/api/v1/billing/status?billing_account_id=ba_01&checkout_session_id=cs_01"
    response = client.get(url)
    assert response.json() == {"state": "pending", "billing_account_id": "ba_01"}
    env.projection.compare_and_set_snapshot.assert_not_called()
    env.projection.commit_claimed_snapshot.assert_not_called()
    env.projection.get_snapshot.return_value = make_snapshot()
    assert client.get(url).json()["state"] == "past_due"
    assert [c[0] for c in env.gateway.method_calls] == []


def test_status_com_snapshot_expoe_campos_ordenados(client, env):
    env.catalog.get_account.return_value = make_account(owner="user-1")
    env.projection.get_snapshot.return_value = make_snapshot()
    body = client.get("/api/v1/billing/status?billing_account_id=ba_01").json()
    assert body["features"] == ["a", "b"]
    assert body["entitlement_version"] == 3
    assert body["plan_version_id"] == "plan-1"
    env.projection.get_snapshot.assert_called_once_with("ba_01", ReadConsistency.STRONG)


def test_status_sem_principal_retorna_401(env):
    app = env.app()
    del app.dependency_overrides[get_billing_principal]
    response = TestClient(app).get("/api/v1/billing/status?billing_account_id=ba_01")
    assert response.status_code == 401


def test_status_conta_inexistente_retorna_404(client, env):
    env.catalog.get_account.return_value = None
    response = client.get("/api/v1/billing/status?billing_account_id=ba_01", headers=HEADERS)
    assert response.json() == {"detail": "billing_account_not_found"}


@pytest.mark.parametrize("name", ["checkout", "portal", "accounts", "transfer"])
def test_modo_desabilitado_retorna_404_sem_dependencias(name):
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_billing_mode] = lambda: BillingMode.DISABLED
    path, body = MUTATIONS[name]
    response = TestClient(app).post(path, json=body())
    assert response.status_code == 404
    assert response.json() == {"detail": "billing_disabled"}


def test_status_desabilitado_retorna_plano_local_sem_dependencias():
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_billing_mode] = lambda: BillingMode.DISABLED
    response = TestClient(app).get("/api/v1/billing/status")
    assert response.status_code == 200
    assert response.json() == {"state": "disabled", "plan_version_id": "local-unmetered-v1"}


def test_defaults_falham_fechado_com_503():
    app = FastAPI()
    app.include_router(router)
    response = TestClient(app).get("/api/v1/billing/status?billing_account_id=ba_01")
    assert response.status_code == 503
    assert response.json() == {"detail": "billing_not_configured"}


@pytest.mark.parametrize("name", ["checkout", "portal", "accounts", "transfer"])
def test_sem_principal_retorna_401(env, name):
    app = env.app()
    del app.dependency_overrides[get_billing_principal]
    assert post(TestClient(app), name).status_code == 401


@pytest.mark.parametrize("name", ["checkout", "portal", "accounts", "transfer"])
def test_tenant_sem_membership_retorna_403_sem_chamar_stripe(client, env, name):
    env.authorizer.authorize.side_effect = TenantAccessDenied("membership_not_active")
    response = post(client, name)
    assert response.status_code == 403
    assert response.json() == {"detail": "tenant_not_allowed"}
    assert env.gateway.method_calls == []


def test_cabecalho_de_tenant_em_branco_equivale_a_ausente(client, env):
    response = post(client, "checkout", headers={"X-Tenant-Id": "  "})
    assert response.json() == {"detail": "billing_owner_required"}
    env.authorizer.authorize.assert_not_called()


@pytest.mark.parametrize("name", ["checkout", "portal", "accounts"])
def test_papel_diferente_de_gestor_e_negado(client, env, name):
    env.role = "viewer"
    assert post(client, name).status_code == 403
    assert env.gateway.method_calls == []


@pytest.mark.parametrize("name", ["checkout", "portal", "accounts"])
def test_autorizado_como_outro_usuario_e_negado(client, env, name):
    env.authorizer.authorize.side_effect = None
    env.authorizer.authorize.return_value = AuthorizedTenant("tenant-a", "user-x", "gestor")
    assert post(client, name).status_code == 403
    assert env.gateway.method_calls == []


def test_checkout_grava_auditoria_sem_chave_bruta(client, env):
    response = post(client, "checkout")
    assert response.status_code == 201
    assert response.json()["session_id"] == "cs_01"
    command = env.gateway.create_checkout.call_args.args[0]
    key = command.idempotency_key
    assert key != CLIENT_KEY
    assert len(key) == 64
    int(key, 16)
    event = env.audit.append.call_args.args[0]
    assert event.event_id == "checkout:cs_01"
    assert event.event_type == "checkout.session_created"
    assert event.aggregate_id == "ba_01"
    assert event.actor_id == "user-1"
    assert event.reason_code == "checkout_requested"
    assert event.occurred_at == NOW
    assert dict(event.attributes) == {
        "plan_version_id": "plan-1",
        "stripe_checkout_session_id": "cs_01",
        "idempotency_key_sha256": key,
    }
    assert CLIENT_KEY not in repr(event)


def test_portal_usa_chave_escopada(client, env):
    assert post(client, "portal").status_code == 201
    key = env.gateway.create_portal.call_args.args[0].idempotency_key
    assert key != CLIENT_KEY
    assert len(key) == 64


@pytest.mark.parametrize(
    "case",
    [
        (RetryableBillingError("stripe_price_unmapped"), 409, "plan_price_unmapped"),
        (RetryableBillingError("stripe_unavailable"), 503, "stripe_unavailable"),
        (PermanentBillingError("stripe_rejected"), 502, "stripe_request_rejected"),
        (IdempotencyConflict("x"), 409, "idempotency_conflict"),
        (BillingTenantConflict("x"), 409, "billing_tenant_conflict"),
    ],
)
def test_erros_do_gateway_sao_mapeados(client, env, case):
    error, status, detail = case
    env.gateway.create_checkout.side_effect = error
    response = post(client, "checkout")
    assert response.status_code == status
    assert response.json() == {"detail": detail}
    env.audit.append.assert_not_called()


def test_indisponibilidade_stripe_retorna_503_com_retry_after(client, env):
    env.gateway.create_checkout.side_effect = RetryableBillingError("stripe_unavailable")
    response = post(client, "checkout")
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "5"
    env.audit.append.assert_not_called()


def test_checkout_rejeita_plano_conta_fechada_e_sem_customer(client, env):
    env.catalog.get_plan.return_value = None
    assert post(client, "checkout").json() == {"detail": "plan_not_found"}
    env.catalog.get_account.return_value = make_account(status=BillingAccountStatus.CLOSED)
    assert post(client, "checkout").json() == {"detail": "billing_account_not_active"}
    env.catalog.get_account.return_value = make_account(customer=None)
    assert post(client, "portal").json() == {"detail": "stripe_customer_missing"}
    env.catalog.get_account.return_value = None
    assert post(client, "checkout").status_code == 404
    assert env.gateway.method_calls == []


@pytest.mark.parametrize("name", ["checkout", "portal", "accounts", "transfer"])
def test_corpo_com_tenant_id_retorna_422(client, name):
    path, body = MUTATIONS[name]
    response = client.post(path, json={**body(), "tenant_id": "tenant-x"}, headers=HEADERS)
    assert response.status_code == 422


@pytest.mark.parametrize(
    "dependency",
    [
        get_billing_mode,
        get_membership_authorizer,
        get_billing_catalog,
        get_stripe_gateway,
        get_entitlement_projection,
        get_billing_audit,
    ],
)
def test_dependencias_padrao_falham_fechado(dependency):
    with pytest.raises(HTTPException) as info:
        dependency()
    assert info.value.status_code == 503
    assert info.value.detail == "billing_not_configured"


def test_relogio_padrao_entrega_instante_utc():
    assert get_billing_clock()().tzinfo is not None


def test_principal_do_middleware_e_aceito_e_ausente_retorna_401(env):
    app = env.app()
    del app.dependency_overrides[get_billing_principal]

    @app.middleware("http")
    async def inject(request, call_next):
        request.state.principal = PRINCIPAL
        return await call_next(request)

    env.catalog.get_account.return_value = make_account(owner="user-1")
    response = TestClient(app).get("/api/v1/billing/status?billing_account_id=ba_01")
    assert response.status_code == 200


@pytest.mark.parametrize(
    "status",
    [
        SubscriptionStatus.ACTIVE,
        SubscriptionStatus.TRIALING,
        SubscriptionStatus.PAST_DUE,
        SubscriptionStatus.UNPAID,
        SubscriptionStatus.PAUSED,
        SubscriptionStatus.INCOMPLETE,
        SubscriptionStatus.ADMIN_REVOKED,
    ],
)
def test_checkout_com_assinatura_vigente_retorna_409_sem_chamar_stripe(client, env, status):
    env.projection.get_snapshot.return_value = make_snapshot(status)
    response = post(client, "checkout")
    assert response.status_code == 409
    assert response.json() == {"detail": "subscription_exists"}
    env.projection.get_snapshot.assert_called_once_with("ba_01", ReadConsistency.STRONG)
    env.gateway.create_checkout.assert_not_called()


@pytest.mark.parametrize(
    "status", [SubscriptionStatus.CANCELED, SubscriptionStatus.INCOMPLETE_EXPIRED],
)
def test_checkout_apos_assinatura_encerrada_e_permitido(client, env, status):
    env.projection.get_snapshot.return_value = make_snapshot(status)
    assert post(client, "checkout").status_code == 201
    env.gateway.create_checkout.assert_called_once()


def test_checkout_rejeita_plano_ainda_nao_vigente(client, env):
    env.catalog.get_plan.return_value = replace(make_plan(), effective_from=NOW + timedelta(days=1))
    response = post(client, "checkout")
    assert response.status_code == 409
    assert response.json() == {"detail": "plan_not_effective"}
    env.gateway.create_checkout.assert_not_called()


def test_erro_retryable_fora_do_stripe_nao_aparece_como_stripe(client, env):
    env.catalog.get_account.side_effect = RetryableBillingError("dynamodb_throttled")
    response = post(client, "checkout")
    assert response.status_code == 503
    assert response.json() == {"detail": "billing_dependency_unavailable"}
    assert response.headers["Retry-After"] == "5"
    assert env.gateway.method_calls == []


def test_erro_permanente_fora_do_stripe_falha_fechado_com_500(env):
    env.catalog.get_account.side_effect = PermanentBillingError("catalog_record_corrupt")
    response = TestClient(env.app(), raise_server_exceptions=False).post(
        MUTATIONS["checkout"][0], json=checkout_body(), headers=HEADERS,
    )
    assert response.status_code == 500
    assert env.gateway.method_calls == []


def test_status_com_snapshot_inclui_conta(client, env):
    env.projection.get_snapshot.return_value = make_snapshot()
    response = client.get("/api/v1/billing/status?billing_account_id=ba_01", headers=HEADERS)
    assert response.json()["billing_account_id"] == "ba_01"


def test_transferencia_rejeita_id_de_conta_longo(client, env):
    response = client.post(
        f"/api/v1/billing/accounts/{'a' * 129}/transfer", json=transfer_body(), headers=HEADERS,
    )
    assert response.status_code == 422
    env.catalog.get_account.assert_not_called()
