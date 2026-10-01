"""Métrica de runs cancelados pela revogação administrativa."""

from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from apps.central_api.tests.routes.billing_fakes import HEADERS
from apps.central_api.tests.routes.test_billing_admin import BODY, URL, AdminEnv
from central_api.routes.billing_admin import get_revocation_service
from central_api.routes.stripe_webhook import get_billing_metrics
from cnes_domain.billing.revocation_models import RevocationResult


@pytest.fixture
def env():
    return AdminEnv()


def _client(env: AdminEnv, metrics: Mock) -> TestClient:
    app = env.app()
    app.dependency_overrides[get_billing_metrics] = lambda: metrics
    return TestClient(app)


def test_revogacao_com_runs_cancelados_emite_metrica(env) -> None:
    metrics = Mock()

    response = _client(env, metrics).post(URL, json=BODY, headers=HEADERS)

    assert response.status_code == 200
    metrics.emit.assert_called_once()
    metric = metrics.emit.call_args.args[0]
    assert metric.name == "RunsCanceledByRevocation"
    assert metric.value == 2
    assert metric.dimensions == {"Reason": "admin_revoked"}


def test_revogacao_sem_runs_cancelados_nao_emite_metrica(env) -> None:
    env.service.revoke.return_value = RevocationResult(7, (), ())
    metrics = Mock()

    _client(env, metrics).post(URL, json=BODY, headers=HEADERS)

    metrics.emit.assert_not_called()


def test_revogacao_rejeitada_nao_emite_metrica(env) -> None:
    metrics = Mock()
    app_client = _client(env, metrics)
    app_client.app.dependency_overrides.pop(get_revocation_service)

    response = app_client.post(URL, json=BODY, headers=HEADERS)

    assert response.status_code == 503
    metrics.emit.assert_not_called()
