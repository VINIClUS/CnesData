"""Testes da rota de criação de tenant cobrado."""

import hashlib
from dataclasses import replace
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from central_api.routes import tenants
from central_api.services.billing_gates import ApiBillingGates, TenantAccountResolver
from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingTenantConflict,
    EntitlementDenied,
    IdempotencyConflict,
    PermanentBillingError,
    QuotaExceeded,
)
from cnes_domain.billing.models import (
    AccessLevel,
    BillingAccountStatus,
    CapacityKind,
    CapacityReservation,
    EntitlementAction,
    EntitlementDecision,
    ReadConsistency,
    ReservationStatus,
)
from cnes_domain.control_plane.entities import Tenant
from cnes_domain.profiles import BillingMode

from .billing_fakes import NOW, Env, make_account, make_link

URL = "/api/v1/billing/accounts/ba_01/tenants"
KEY = "tenant-key-1"
OWNER_HEADERS: dict[str, str] = {}
MANAGER_HEADERS = {"X-Tenant-Id": "tenant-a"}


class FakeGate:
    def __init__(self, events):
        self.events = events
        self.error = None
        self.requests = []

    def authorize_tenant_creation(self, request):
        self.events.append("gate")
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return EntitlementDecision(
            EntitlementAction.TENANT_CREATION, True, AccessLevel.FULL, "ok", 3, 2,
        )


class FakeCapacity:
    def __init__(self, events):
        self.events = events
        self.error = None
        self.release_error = None
        self.forced_status = None
        self.reserved = {}
        self.commands = []
        self.releases = []

    def reserve_capacity(self, command):
        self.events.append("reserve")
        self.commands.append(command)
        if self.error is not None:
            raise self.error
        existing = self.reserved.get(command.idempotency_key)
        if existing is not None:
            return existing
        reservation = CapacityReservation(
            f"res-{len(self.reserved) + 1}", command.billing_account_id, command.resource_id,
            command.kind, self.forced_status or ReservationStatus.RESERVED, NOW,
            NOW + timedelta(minutes=5),
        )
        self.reserved[command.idempotency_key] = reservation
        return reservation

    def release_capacity(self, command):
        self.events.append("release")
        self.releases.append(command)
        if self.release_error is not None:
            raise self.release_error
        for key, item in self.reserved.items():
            if item.reservation_id == command.reservation_id:
                self.reserved[key] = replace(item, status=ReservationStatus.RELEASED)


class FakeControlPlane:
    def __init__(self, events):
        self.events = events
        self.error = None
        self.error_queue = []
        self.read_error = None
        self.tenants = {}
        self.by_key = {}
        self.commands = []

    def create_billed_tenant(self, command):
        self.events.append("create")
        self.commands.append(command)
        if self.error_queue:
            raise self.error_queue.pop(0)
        if self.error is not None:
            raise self.error
        tenant = self.by_key.setdefault(command.idempotency_key, command.tenant)
        self.tenants[tenant.tenant_id] = tenant
        return tenant

    def get_tenant(self, tenant_id):
        self.events.append("get_tenant")
        if self.read_error is not None:
            raise self.read_error
        return self.tenants.get(tenant_id)


class TenantEnv(Env):
    def __init__(self):
        super().__init__()
        self.events = []
        self.link_result = None
        self.gate = FakeGate(self.events)
        self.capacity = FakeCapacity(self.events)
        self.control_plane = FakeControlPlane(self.events)
        self.catalog.get_account.return_value = make_account(owner="user-1")
        self.catalog.get_tenant_link.side_effect = self._link

    def _link(self, account_id, tenant_id, consistency):
        self.events.append("get_tenant_link")
        return self.link_result

    def app(self, mode=BillingMode.STRIPE, with_gates=True):
        app = super().app(mode)
        app.include_router(tenants.router)
        if with_gates:
            gates = ApiBillingGates(
                mode, self.gate, self.capacity, TenantAccountResolver(mode),
            )
            app.dependency_overrides[tenants.get_tenant_gates] = lambda: gates
        return app


def body(**extra):
    return {
        "tenant_id": "novo-tenant", "municipality_name": "Presidente Epitácio",
        "idempotency_key": KEY, **extra,
    }


@pytest.fixture
def env():
    return TenantEnv()


@pytest.fixture
def client(env):
    return TestClient(env.app())


def create(client, headers=OWNER_HEADERS, **extra):
    return client.post(URL, json=body(**extra), headers=headers)


def sha(*parts):
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()


def committed_link(account="ba_01", tenant="novo-tenant"):
    return make_link(tenant, account)


def fail_creation(env, error):
    env.control_plane.error = error


def test_dono_cria_tenant_com_reserva_consumida_em_uma_transacao(client, env):
    response = create(client)
    assert response.status_code == 201
    assert response.json() == {
        "tenant_id": "novo-tenant", "municipality_name": "Presidente Epitácio",
        "created_at": "2026-09-30T12:00:00Z", "billing_account_id": "ba_01",
    }
    (command,) = env.control_plane.commands
    key = sha("tenant", "user-1", "ba_01", KEY)
    assert command.idempotency_key == key
    assert command.reservation_id == "res-1"
    assert command.tenant == Tenant(
        tenant_id="novo-tenant", municipality_name="Presidente Epitácio", created_at=NOW,
    )
    assert command.link.linked_by_user_id == "user-1"
    assert (command.link.billing_account_id, command.link.reason_code) == (
        "ba_01", "tenant_created",
    )
    assert command.link.linked_at == NOW
    assert env.events == ["gate", "reserve", "create"]
    assert env.capacity.releases == []


def test_reserva_usa_decisao_do_gate_e_hash_do_pedido(client, env):
    create(client)
    (reservation,) = env.capacity.commands
    assert env.gate.requests[0].billing_account_id == "ba_01"
    assert env.gate.requests[0].tenant_id == "novo-tenant"
    assert reservation.kind is CapacityKind.TENANT
    assert reservation.resource_id == "novo-tenant"
    assert reservation.entitlement_version == 3
    assert reservation.limit == 2
    assert reservation.idempotency_key == sha("tenant", "user-1", "ba_01", KEY)
    assert reservation.request_hash == sha("ba_01", "novo-tenant", "Presidente Epitácio")


def test_gestor_de_tenant_vinculado_cria_tenant(client, env):
    env.catalog.get_account.return_value = make_account(owner="user-9")
    env.link_result = make_link("tenant-a", "ba_01")
    response = create(client, MANAGER_HEADERS)
    assert response.status_code == 201
    env.catalog.get_tenant_link.assert_any_call("ba_01", "tenant-a", ReadConsistency.STRONG)


def test_gestor_de_tenant_nao_vinculado_e_negado_antes_do_gate(client, env):
    env.catalog.get_account.return_value = make_account(owner="user-9")
    env.link_result = None
    response = create(client, MANAGER_HEADERS)
    assert response.status_code == 403
    assert response.json() == {"detail": "billing_owner_required"}
    assert "gate" not in env.events


def test_usuario_sem_tenant_e_negado(client, env):
    env.catalog.get_account.return_value = make_account(owner="user-9")
    response = create(client)
    assert response.status_code == 403
    assert env.events == []


def test_conta_inexistente_404(client, env):
    env.catalog.get_account.return_value = None
    response = create(client)
    assert response.status_code == 404
    assert response.json() == {"detail": "billing_account_not_found"}


def test_conta_inativa_409(client, env):
    env.catalog.get_account.return_value = make_account(
        owner="user-1", status=BillingAccountStatus.CLOSED,
    )
    response = create(client)
    assert response.status_code == 409
    assert response.json() == {"detail": "billing_account_not_active"}
    assert env.events == []


def test_billing_desabilitado_responde_404(env):
    response = create(TestClient(env.app(BillingMode.DISABLED)))
    assert response.status_code == 404
    assert response.json() == {"detail": "billing_disabled"}
    assert env.events == []


@pytest.mark.parametrize("tenant_id", ["_billing", "_x"])
def test_rejeita_tenant_reservado(client, env, tenant_id):
    response = create(client, tenant_id=tenant_id)
    assert response.status_code == 422
    assert "tenant_id_reserved" in response.json()["detail"][0]["msg"]
    assert env.events == []


@pytest.mark.parametrize(
    "extra",
    [
        {"tenant_id": ""},
        {"tenant_id": "Tenant"},
        {"tenant_id": "-x"},
        {"tenant_id": "a" * 64},
        {"tenant_id": 7},
        {"municipality_name": "   "},
        {"municipality_name": "m" * 201},
        {"idempotency_key": ""},
        {"idempotency_key": "k" * 129},
        {"billing_account_id": "outra"},
    ],
)
def test_rejeita_tenant_id_invalido(client, env, extra):
    response = create(client, **extra)
    assert response.status_code == 422
    assert env.events == []


def test_aceita_tenant_id_no_limite_do_padrao(client):
    assert create(client, tenant_id="a" + "b" * 62).status_code == 201


def test_negacao_do_gate_403_sem_reserva(client, env):
    env.gate.error = EntitlementDenied("reason=snapshot_missing")
    response = create(client)
    assert response.status_code == 403
    assert response.json() == {"detail": "tenant_entitlement_denied"}
    assert env.events == ["gate"]


def test_quota_excedida_403(client, env):
    env.capacity.error = QuotaExceeded("reason=limit")
    response = create(client)
    assert response.status_code == 403
    assert response.json() == {"detail": "tenant_quota_exceeded"}
    assert env.events == ["gate", "reserve"]


def test_falha_sem_commit_libera_reserva_apos_leitura_forte(client, env):
    fail_creation(env, BillingDependencyError("dynamodb_unavailable"))
    response = create(client)
    assert response.status_code == 503
    assert env.events == [
        "gate", "reserve", "create", "create", "get_tenant", "get_tenant_link", "release",
    ]
    env.catalog.get_tenant_link.assert_called_with("ba_01", "novo-tenant", ReadConsistency.STRONG)
    (release,) = env.capacity.releases
    assert release.billing_account_id == "ba_01"
    assert release.reservation_id == "res-1"
    assert release.released_at == NOW
    assert release.reason_code == "tenant_creation_failed"


def test_falha_inesperada_libera_reserva_e_relanca(env):
    fail_creation(env, RuntimeError("boom"))
    response = create(TestClient(env.app(), raise_server_exceptions=False))
    assert response.status_code == 500
    assert len(env.capacity.releases) == 1


@pytest.mark.parametrize("link", [None, committed_link(account="ba_99")])
def test_falha_com_estado_ambiguo_nao_libera(client, env, link):
    fail_creation(env, BillingDependencyError("timeout"))
    env.control_plane.tenants["novo-tenant"] = Tenant(
        tenant_id="novo-tenant", municipality_name="Outro", created_at=NOW,
    )
    env.link_result = link
    assert create(client).status_code == 503
    assert env.capacity.releases == []


def test_falha_com_link_sem_tenant_nao_libera(client, env):
    fail_creation(env, BillingDependencyError("timeout"))
    env.link_result = committed_link()
    assert create(client).status_code == 503
    assert env.capacity.releases == []


def test_falha_na_leitura_de_prova_nao_libera(client, env):
    fail_creation(env, BillingDependencyError("timeout"))
    env.control_plane.read_error = RuntimeError("read")
    assert create(client).status_code == 503
    assert env.capacity.releases == []


def test_falha_na_leitura_do_vinculo_nao_libera(client, env):
    fail_creation(env, BillingDependencyError("timeout"))
    env.catalog.get_tenant_link.side_effect = BillingDependencyError("catalog")
    assert create(client).status_code == 503
    assert env.capacity.releases == []


def test_falha_na_liberacao_preserva_erro_original(client, env):
    fail_creation(env, IdempotencyConflict("reason=mismatch"))
    env.capacity.release_error = RuntimeError("release")
    response = create(client)
    assert response.status_code == 409
    assert response.json() == {"detail": "idempotency_conflict"}


def test_conflito_de_tenant_409(client, env):
    fail_creation(env, BillingTenantConflict("reason=exists"))
    response = create(client)
    assert response.status_code == 409
    assert response.json() == {"detail": "billing_tenant_conflict"}
    assert len(env.capacity.releases) == 1


def test_idempotencia_conflitante_409(client, env):
    fail_creation(env, IdempotencyConflict("reason=mismatch"))
    response = create(client)
    assert response.status_code == 409
    assert response.json() == {"detail": "idempotency_conflict"}


def test_erro_de_storage_503(client, env):
    fail_creation(env, BillingDependencyError("dynamodb_unavailable"))
    assert create(client).status_code == 503


@pytest.mark.parametrize(
    "code",
    [
        "capacity_reservation_invalid",
        "capacity_reservation_missing",
        "billing_account_missing",
        "billing_account_inactive",
    ],
)
def test_reserva_invalida_409(client, env, code):
    fail_creation(env, PermanentBillingError(code))
    response = create(client)
    assert response.status_code == 409
    assert response.json() == {"detail": "tenant_creation_conflict"}


def test_snapshot_alterado_na_transacao_403(client, env):
    fail_creation(env, EntitlementDenied("reason=snapshot_changed"))
    response = create(client)
    assert response.status_code == 403
    assert response.json() == {"detail": "tenant_entitlement_denied"}


def test_tenant_reservado_na_transacao_422(client, env):
    fail_creation(env, PermanentBillingError("tenant_id_reserved"))
    response = create(client)
    assert response.status_code == 422
    assert "tenant_id_reserved" in response.json()["detail"][0]["msg"]


def test_erro_permanente_desconhecido_vira_conflito(client, env):
    fail_creation(env, PermanentBillingError("outro_codigo"))
    response = create(client)
    assert response.status_code == 409
    assert response.json() == {"detail": "tenant_creation_conflict"}


def test_replay_devolve_o_mesmo_tenant(client, env):
    first = create(client)
    second = create(client)
    assert second.status_code == 201
    assert second.json() == first.json()
    assert {c.reservation_id for c in env.control_plane.commands} == {"res-1"}
    assert len(env.capacity.reserved) == 1


def test_gates_nao_configurados_503(env):
    response = create(TestClient(env.app(with_gates=False)))
    assert response.status_code == 503
    assert response.json() == {"detail": "billing_not_configured"}
