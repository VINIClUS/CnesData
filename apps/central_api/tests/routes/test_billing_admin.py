"""Testes da rota administrativa de revogação imediata de billing."""

from unittest.mock import create_autospec

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from central_api.routes.billing import (
    get_billing_catalog,
    get_billing_clock,
    get_billing_mode,
    get_billing_principal,
    get_membership_authorizer,
)
from central_api.routes.billing_admin import (
    RevocationService,
    get_revocation_service,
    router,
)
from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingDisabledError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.revocation_models import (
    ImmediateRevocationCommand,
    RevocationResult,
)
from cnes_domain.profiles import BillingMode

from .billing_fakes import HEADERS, NOW, PRINCIPAL, Env, make_account, make_link

URL = "/api/v1/admin/billing/ba_01/revoke"
BODY = {"reason_code": "fraud"}


class AdminEnv(Env):
    def __init__(self):
        super().__init__()
        self.service = create_autospec(RevocationService, instance=True)
        self.service.revoke.return_value = RevocationResult(7, ("run-a", "run-b"), ("run-c",))

    def app(self, mode=BillingMode.STRIPE, configured=True):
        app = FastAPI()
        app.include_router(router)
        overrides = {
            get_billing_mode: lambda: mode,
            get_billing_principal: lambda: PRINCIPAL,
            get_membership_authorizer: lambda: self.authorizer,
            get_billing_catalog: lambda: self.catalog,
            get_billing_clock: lambda: (lambda: NOW),
        }
        if configured:
            overrides[get_revocation_service] = lambda: self.service
        app.dependency_overrides.update(overrides)
        return app


@pytest.fixture
def env():
    return AdminEnv()


@pytest.fixture
def client(env):
    return TestClient(env.app())


def revoke(client, body=None, headers=HEADERS):
    return client.post(URL, json=BODY if body is None else body, headers=headers)


def test_dono_revoga_com_reason_code(client, env):
    env.catalog.get_account.return_value = make_account(owner="user-1")
    response = revoke(client, headers={})
    assert response.status_code == 200
    assert response.json() == {
        "billing_account_id": "ba_01",
        "entitlement_version": 7,
        "fenced_run_count": 2,
        "cancel_failure_count": 1,
    }
    env.service.revoke.assert_called_once_with(
        ImmediateRevocationCommand("ba_01", "user-1", "fraud", NOW),
    )
    env.catalog.get_tenant_link.assert_not_called()


def test_gestor_de_tenant_vinculado_revoga(client, env):
    response = revoke(client)
    assert response.status_code == 200
    command = env.service.revoke.call_args.args[0]
    assert (command.billing_account_id, command.actor_id) == ("ba_01", "user-1")
    assert command.requested_at == NOW


def test_admin_de_tenant_nao_vinculado_nao_revoga(client, env):
    env.role = "gestor"
    env.catalog.get_tenant_link.return_value = None
    response = revoke(client, headers={"X-Tenant-Id": "tenant-b"})
    assert response.status_code == 403
    assert response.json() == {"detail": "billing_owner_required"}
    env.service.revoke.assert_not_called()


def test_link_divergente_nao_revoga(client, env):
    env.catalog.get_tenant_link.return_value = make_link("tenant-z")
    response = revoke(client)
    assert response.status_code == 403
    assert response.json() == {"detail": "billing_owner_required"}
    env.service.revoke.assert_not_called()


def test_role_nao_gestor_nao_revoga(client, env):
    env.role = "operador"
    response = revoke(client)
    assert response.status_code == 403
    env.service.revoke.assert_not_called()


def test_sem_tenant_nao_dono_nao_revoga(client, env):
    response = revoke(client, headers={})
    assert response.status_code == 403
    assert response.json() == {"detail": "billing_owner_required"}
    env.service.revoke.assert_not_called()


def test_erro_de_storage_no_link_falha_fechado_antes_do_servico(client, env):
    env.catalog.get_tenant_link.side_effect = BillingDependencyError("dynamodb_unavailable")
    response = revoke(client)
    assert response.status_code == 503
    env.service.revoke.assert_not_called()


def test_conta_inexistente_404_sem_servico(client, env):
    env.catalog.get_account.return_value = None
    response = revoke(client)
    assert response.status_code == 404
    assert response.json() == {"detail": "billing_account_not_found"}
    env.service.revoke.assert_not_called()


@pytest.mark.parametrize(
    ("body", "status"),
    [
        ({"reason_code": ""}, 422),
        ({"reason_code": "   "}, 422),
        ({"reason_code": "x" * 129}, 422),
        ({"reason_code": "x" * 128}, 200),
        ({"reason_code": "fraud", "extra": 1}, 422),
        ({}, 422),
    ],
)
def test_reason_code(client, env, body, status):
    response = revoke(client, body=body)
    assert response.status_code == status
    assert env.service.revoke.called is (status == 200)


def test_billing_desabilitado_404(env):
    response = revoke(TestClient(env.app(BillingMode.DISABLED)))
    assert response.status_code == 404
    assert response.json() == {"detail": "billing_disabled"}
    env.service.revoke.assert_not_called()


def test_servico_nao_configurado_503(env):
    response = revoke(TestClient(env.app(configured=False)))
    assert response.status_code == 503
    assert response.json() == {"detail": "billing_not_configured"}


def test_erro_retryable_do_servico_503(client, env):
    env.service.revoke.side_effect = RetryableBillingError("dynamodb_throttled")
    response = revoke(client)
    assert response.status_code == 503


def test_erro_permanente_do_servico_409(client, env):
    env.service.revoke.side_effect = PermanentBillingError("revocation_failed")
    response = revoke(client)
    assert response.status_code == 409
    assert response.json() == {"detail": "revocation_conflict"}


def test_billing_desabilitado_do_servico_404(client, env):
    env.service.revoke.side_effect = BillingDisabledError()
    response = revoke(client)
    assert response.status_code == 404
    assert response.json() == {"detail": "billing_disabled"}


def test_rota_nao_exige_token_admin_legado(client):
    assert "X-Admin-Token" not in revoke(client).request.headers
    assert not router.dependencies
    assert all(
        getattr(dep.call, "__name__", "") != "require_admin_token"
        for route in router.routes
        for dep in route.dependant.dependencies
    )
