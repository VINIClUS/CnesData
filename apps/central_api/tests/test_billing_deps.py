"""Wiring das dependências de billing da API: admissão, revogação e control plane."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from central_api import billing_deps
from central_api.routes import billing_admin, raw_jobs, tenants
from central_api.schemas.raw_api import EdgeIdentity
from central_api.services.billing_gates import ApiBillingGates, TenantAccountResolver
from cnes_domain.billing.commands import GateRequest
from cnes_domain.billing.revocation import ImmediateRevocationService
from cnes_domain.profiles import BillingMode
from cnes_infra.billing import BillingStorage
from cnes_infra.billing.metrics import CloudWatchBillingMetrics, DiscardBillingMetrics

TENANT = "354130"
AGENT = "agent-1"
FINGERPRINT = "a" * 64
NEXT_JOB = "/api/v1/edge/jobs/next"


def _gates() -> tuple[ApiBillingGates, Mock]:
    gate = Mock(name="gate")
    gates = ApiBillingGates(
        BillingMode.DISABLED, gate, Mock(name="capacity"),
        TenantAccountResolver(BillingMode.DISABLED),
    )
    return gates, gate


def _registry() -> Mock:
    registry = Mock(name="control_plane")
    registry.get_agent.return_value = None
    registry.register_edge_agent.return_value = SimpleNamespace(
        tenant_id=TENANT, agent_id=AGENT, certificate_fingerprint=FINGERPRINT,
    )
    registry.list_claimable_jobs.return_value = []
    return registry


def _edge_app(registry: Mock) -> FastAPI:
    app = FastAPI()
    app.include_router(raw_jobs.router)
    app.dependency_overrides[raw_jobs.get_edge_identity] = lambda: EdgeIdentity(
        tenant_id=TENANT, agent_id=AGENT, certificate_fingerprint=FINGERPRINT,
    )
    app.dependency_overrides[raw_jobs.get_control_plane] = lambda: registry
    return app


def test_admissao_com_gates_resolve_control_plane_pela_rota() -> None:
    gates, gate = _gates()
    app = _edge_app(_registry())
    billing_deps._install_agent_admission(app, gates)

    response = TestClient(app).get(NEXT_JOB)

    assert response.status_code != 422
    assert response.status_code == 204
    gate.authorize_register_agent.assert_called_once_with(
        GateRequest(f"local-{TENANT}", TENANT),
    )


def test_sem_gates_nao_sobrescreve_admissao() -> None:
    app = FastAPI()

    billing_deps._install_agent_admission(app, None)

    assert raw_jobs.get_agent_admission not in app.dependency_overrides


def test_install_billing_disabled_instala_admissao_com_gates_do_runtime(monkeypatch) -> None:
    monkeypatch.setenv("PROFILE", "local")
    monkeypatch.setenv("TENANT_ID", TENANT)
    monkeypatch.setenv("BILLING_MODE", "disabled")
    gates, gate = _gates()
    registry = _registry()
    app = _edge_app(registry)
    runtime = SimpleNamespace(billing_gates=gates, control_plane=registry)

    billing_deps.install_billing(app, runtime, Mock())
    response = TestClient(app).get(NEXT_JOB)

    assert raw_jobs.get_agent_admission in app.dependency_overrides
    assert response.status_code == 204
    gate.authorize_register_agent.assert_called_once()


def _admin_runtime(gates: ApiBillingGates | None) -> SimpleNamespace:
    client = Mock(name="dynamodb")
    services = SimpleNamespace(billing_storage=BillingStorage(client, "tabela"))
    return SimpleNamespace(
        services=services, billing_gates=gates, executor=Mock(name="executor"),
    )


def test_install_billing_admin_compoe_revogacao_sobre_dynamodb() -> None:
    gates, _ = _gates()
    runtime = _admin_runtime(gates)
    components = Mock(name="components")
    app = FastAPI()

    billing_deps._install_billing_admin(app, runtime, components)

    service = app.dependency_overrides[billing_admin.get_revocation_service]()
    assert isinstance(service, ImmediateRevocationService)
    assert app.dependency_overrides[tenants.get_tenant_gates]() is gates


def test_install_billing_admin_sem_gates_nao_sobrescreve_tenants() -> None:
    app = FastAPI()

    billing_deps._install_billing_admin(app, _admin_runtime(None), Mock())

    assert billing_admin.get_revocation_service in app.dependency_overrides
    assert tenants.get_tenant_gates not in app.dependency_overrides


def test_control_plane_de_billing_so_atende_rotas_de_billing() -> None:
    runtime = SimpleNamespace(control_plane=Mock(name="control_plane"))
    app = FastAPI()

    @app.get("/{path:path}")
    def _probe(
        control_plane=billing_deps.Depends(billing_deps._billing_control_plane(runtime)),
    ) -> dict[str, bool]:
        return {"ok": control_plane is runtime.control_plane}

    client = TestClient(app)
    fora = client.get("/api/v1/edge/jobs/next")
    dentro = client.get("/api/v1/billing/status")
    aninhada = client.get("/api/v1/billing/accounts/ba/tenants")

    assert (fora.status_code, fora.json()) == (503, {"detail": "control_plane_not_configured"})
    assert dentro.json() == {"ok": True}
    assert aninhada.json() == {"ok": True}


def _stripe_runtime(gates: ApiBillingGates | None, storage: BillingStorage | None):
    return SimpleNamespace(
        services=SimpleNamespace(billing_storage=storage, membership_authorizer=Mock()),
        billing_gates=gates, executor=Mock(), control_plane=Mock(),
    )


def test_install_billing_disabled_rejeita_webhook_com_404(monkeypatch) -> None:
    from central_api.routes import stripe_webhook

    monkeypatch.setenv("PROFILE", "local")
    monkeypatch.setenv("TENANT_ID", TENANT)
    monkeypatch.setenv("BILLING_MODE", "disabled")
    app = FastAPI()

    billing_deps.install_billing(app, SimpleNamespace(billing_gates=None), Mock())

    override = app.dependency_overrides[stripe_webhook.get_stripe_webhook_verifier]
    with pytest.raises(HTTPException) as error:
        override()
    assert (error.value.status_code, error.value.detail) == (404, "billing_disabled")
    assert billing_deps._utc_now().tzinfo is not None


def test_install_stripe_billing_sem_storage_falha_fechado() -> None:
    from cnes_infra.billing import BillingConfigurationError

    with pytest.raises(BillingConfigurationError):
        billing_deps._install_stripe_billing(
            FastAPI(), _stripe_runtime(None, None), Mock(), DiscardBillingMetrics(),
        )


def test_install_stripe_billing_sobrescreve_dependencias_dos_routers(monkeypatch) -> None:
    from unittest.mock import patch

    from central_api.routes import billing, stripe_webhook

    gates, _ = _gates()
    runtime = _stripe_runtime(gates, BillingStorage(Mock(), "tabela"))
    components = Mock(name="components")
    app = FastAPI()
    with (
        patch("cnes_infra.billing.build_stripe_billing", return_value=components),
        patch("cnes_infra.billing.StripeRuntimeSettings.from_mapping"),
    ):
        billing_deps._install_stripe_billing(app, runtime, Mock(), DiscardBillingMetrics())

    overrides = app.dependency_overrides
    assert overrides[billing.get_billing_catalog]() is components.catalog
    assert overrides[billing.get_membership_authorizer]() is runtime.services.membership_authorizer
    assert overrides[stripe_webhook.get_stripe_webhook_verifier]() is components.verifier
    assert overrides[billing.get_billing_clock]() is billing_deps._utc_now
    assert overrides[tenants.get_tenant_gates]() is gates
    assert billing_admin.get_revocation_service in overrides
    assert raw_jobs.get_control_plane in overrides
    assert isinstance(overrides[stripe_webhook.get_billing_metrics](), DiscardBillingMetrics)


def test_install_billing_usa_cloudwatch_quando_ambiente_configurado(monkeypatch) -> None:
    from unittest.mock import patch

    monkeypatch.setenv("PROFILE", "local")
    monkeypatch.setenv("TENANT_ID", TENANT)
    monkeypatch.setenv("BILLING_MODE", "disabled")
    monkeypatch.setenv("BILLING_METRICS_ENVIRONMENT", "prod")
    with (
        patch("cnes_infra.billing.build_secret_provider", return_value=Mock()),
        patch.object(billing_deps, "_install_stripe_billing") as install,
    ):
        billing_deps.install_billing(FastAPI(), SimpleNamespace(billing_gates=None), Mock())

    assert isinstance(install.call_args.args[3], CloudWatchBillingMetrics)


def test_install_billing_com_provider_delega_ao_stripe(monkeypatch) -> None:
    from unittest.mock import patch

    monkeypatch.setenv("PROFILE", "local")
    monkeypatch.setenv("TENANT_ID", TENANT)
    monkeypatch.setenv("BILLING_MODE", "disabled")
    runtime = SimpleNamespace(billing_gates=None)
    provider = Mock(name="provider")
    with (
        patch("cnes_infra.billing.build_secret_provider", return_value=provider),
        patch.object(billing_deps, "_install_stripe_billing") as install,
    ):
        billing_deps.install_billing(FastAPI(), runtime, Mock())

    install.assert_called_once()
    assert install.call_args.args[1:3] == (runtime, provider)
    assert isinstance(install.call_args.args[3], DiscardBillingMetrics)
