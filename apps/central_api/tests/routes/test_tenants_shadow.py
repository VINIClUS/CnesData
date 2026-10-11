"""Criação de tenant em shadow: o observador vê a conta explícita sem mudar a resposta."""

from typing import Any, cast

from fastapi.testclient import TestClient

from apps.central_api.tests.routes.billing_fakes import NOW, Env
from apps.central_api.tests.routes.test_tenants import OWNER_HEADERS, URL, TenantEnv, body
from central_api.routes import tenants
from central_api.services.billing_gates import ApiBillingGates, TenantAccountResolver
from cnes_domain.billing.errors import BillingDependencyError, EntitlementDenied
from cnes_domain.billing.models import EntitlementAction
from cnes_domain.billing.shadow import (
    ShadowEntitlementObserver,
    ShadowObservation,
    ShadowObserverDependencies,
)
from cnes_domain.profiles import BillingMode


class SpyObserver:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.observations: list[ShadowObservation] = []

    def observe(self, observation: ShadowObservation) -> None:
        self.events.append("observe")
        self.observations.append(observation)


def _client(env: TenantEnv, observer: Any) -> TestClient:
    app = Env.app(env, BillingMode.STRIPE)
    app.include_router(tenants.router)
    mode = BillingMode.DISABLED
    gates = ApiBillingGates(
        mode, cast("Any", env.gate), cast("Any", env.capacity), TenantAccountResolver(mode),
        observer=observer,
    )
    app.dependency_overrides[tenants.get_tenant_gates] = lambda: gates
    return TestClient(app)


def _create(client: TestClient):
    return client.post(URL, json=body(), headers=OWNER_HEADERS)


def test_shadow_observa_conta_explicita_entre_gate_e_reserva() -> None:
    env = TenantEnv()
    observer = SpyObserver(env.events)

    response = _create(_client(env, observer))

    assert response.status_code == 201
    assert response.json()["billing_account_id"] == "ba_01"
    assert env.events[:3] == ["gate", "observe", "reserve"]
    assert observer.observations == [
        ShadowObservation(EntitlementAction.TENANT_CREATION, "novo-tenant", "ba_01"),
    ]


def test_gate_real_negado_nao_chama_observador() -> None:
    env = TenantEnv()
    cast("Any", env.gate).error = EntitlementDenied("reason=x")
    observer = SpyObserver(env.events)

    response = _create(_client(env, observer))

    assert (response.status_code, response.json()["detail"]) == (
        403, "tenant_entitlement_denied",
    )
    assert observer.observations == []


class _BrokenProjection:
    def get_snapshot(self, billing_account_id: str, consistency: Any) -> None:
        raise BillingDependencyError("dynamodb_unavailable")


class _NoAudit:
    def append(self, event: Any) -> None:
        raise AssertionError(event)


def test_falha_do_observador_real_mantem_201() -> None:
    env = TenantEnv()
    observer = ShadowEntitlementObserver(ShadowObserverDependencies(
        cast("Any", None), cast("Any", _BrokenProjection()), cast("Any", None), _NoAudit(),
        lambda: NOW,
    ))

    response = _create(_client(env, observer))

    assert response.status_code == 201
    assert len(env.control_plane.commands) == 1
