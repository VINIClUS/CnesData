from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from apps.central_api.tests.routes.test_raw_jobs import FINGERPRINT, ControlPlane, agent
from central_api.routes.raw_jobs import (
    get_agent_admission,
    get_control_plane,
    get_edge_identity,
    router,
)
from central_api.schemas.raw_api import EdgeIdentity
from central_api.services.agent_admission import AgentAdmission
from central_api.services.billing_gates import ApiBillingGates, BillingAccountMissing
from cnes_domain.billing.errors import (
    BillingDependencyError,
    EntitlementDenied,
    IdempotencyConflict,
    PermanentBillingError,
    PublishDenied,
    QuotaExceeded,
)
from cnes_domain.control_plane.errors import Conflict, ControlPlaneErrorCode
from cnes_domain.profiles import BillingMode

if TYPE_CHECKING:
    from central_api.services.agent_admission import EdgeAgentRegistry
    from central_api.services.billing_gates import TenantAccountResolver
    from cnes_domain.billing.gate import EntitlementGate
    from cnes_domain.ports.control_plane import ControlPlanePort

if TYPE_CHECKING:
    from cnes_domain.billing.ports import QuotaReservationPort

NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)


class Resolver:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error

    def resolve(self, tenant_id: str) -> str:
        if self.error is not None:
            raise self.error
        return "acct-1"


class Gate:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    def authorize_register_agent(self, request):
        self.calls += 1
        if self.error is not None:
            raise self.error
        raise AssertionError("gate_allows_unexpected")


def client(control: ControlPlane, resolver: Resolver, gate: Gate) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    identity = EdgeIdentity(
        tenant_id="354130", agent_id="agent-1", certificate_fingerprint=FINGERPRINT,
    )
    gates = ApiBillingGates(
        BillingMode.STRIPE,
        cast("EntitlementGate", gate),
        cast("QuotaReservationPort", object()),
        cast("TenantAccountResolver", resolver),
    )
    app.dependency_overrides[get_edge_identity] = lambda: identity
    app.dependency_overrides[get_control_plane] = lambda: control
    app.dependency_overrides[get_agent_admission] = lambda: AgentAdmission(
        cast("EdgeAgentRegistry", control), gates
    )
    return TestClient(app)


@pytest.mark.parametrize(
    ("resolver_error", "gate_error", "status", "detail"),
    [
        (BillingAccountMissing(), None, 403, "billing_account_missing"),
        (None, EntitlementDenied("reason=blocked"), 403, "agent_entitlement_denied"),
        (None, QuotaExceeded("quota=agents"), 403, "agent_quota_exceeded"),
        (None, PermanentBillingError("conflict"), 409, "agent_registration_conflict"),
    ],
)
def test_agente_novo_negado_mapeia_erro_http(resolver_error, gate_error, status, detail) -> None:
    control = ControlPlane(None)

    response = client(control, Resolver(resolver_error), Gate(gate_error)).get(
        "/api/v1/edge/jobs/next",
    )

    assert response.status_code == status
    assert response.json() == {"detail": detail}
    assert "Retry-After" not in response.headers
    assert control.agent is None


def test_dependencia_de_billing_indisponivel_responde_503_com_retry_after() -> None:
    control = ControlPlane(None)

    response = client(control, Resolver(), Gate(BillingDependencyError("dynamodb_down"))).get(
        "/api/v1/edge/jobs/next",
    )

    assert response.status_code == 503
    assert response.json() == {"detail": "billing_dependency_unavailable"}
    assert response.headers["Retry-After"] == "5"


def test_agente_existente_nao_consulta_gate_pela_rota() -> None:
    control = ControlPlane(agent())
    gate = Gate(EntitlementDenied("reason=blocked"))

    response = client(control, Resolver(), gate).get("/api/v1/edge/jobs/next")

    assert response.status_code == 204
    assert gate.calls == 0


def test_dependencia_padrao_usa_control_plane_sem_gates() -> None:
    control = ControlPlane(None)

    admission = get_agent_admission(cast("ControlPlanePort", control))

    assert admission.admit(
        EdgeIdentity(tenant_id="354130", agent_id="agent-1", certificate_fingerprint=FINGERPRINT),
        NOW,
    ).agent_id == "agent-1"
    assert control.calls == ["register_agent"]


class _FailingControlPlane(ControlPlane):
    def __init__(self, error: Exception) -> None:
        super().__init__(agent())
        self.error = error

    def register_edge_agent(self, tenant_id, agent_id, fingerprint, now):
        raise self.error


def _admit_with_control_error(error: Exception):
    control = _FailingControlPlane(error)
    return client(control, Resolver(), Gate()).get("/api/v1/edge/jobs/next")


def test_contencao_no_upsert_responde_503_e_nao_agent_revoked() -> None:
    response = _admit_with_control_error(Conflict(ControlPlaneErrorCode.TRANSACTION_CONFLICT))

    assert response.status_code == 503
    assert response.json() == {"detail": "agent_registration_contended"}
    assert response.headers["Retry-After"] == "5"


def test_agente_revogado_continua_403_pelo_codigo() -> None:
    response = _admit_with_control_error(Conflict(ControlPlaneErrorCode.AGENT_REVOKED))

    assert response.status_code == 403
    assert response.json() == {"detail": "agent_revoked"}
    assert "Retry-After" not in response.headers


def test_idempotencia_conflitante_409() -> None:
    response = client(
        ControlPlane(None), Resolver(), Gate(IdempotencyConflict("key=x")),
    ).get("/api/v1/edge/jobs/next")

    assert response.status_code == 409
    assert response.json() == {"detail": "agent_registration_conflict"}


def test_erro_de_billing_generico_503() -> None:
    response = client(
        ControlPlane(None), Resolver(), Gate(PublishDenied("reason=x")),
    ).get("/api/v1/edge/jobs/next")

    assert response.status_code == 503
    assert response.json() == {"detail": "billing_dependency_unavailable"}
    assert response.headers["Retry-After"] == "5"
