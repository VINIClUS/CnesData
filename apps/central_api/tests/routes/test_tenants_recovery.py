"""Testes da recuperação de falhas e da reserva repetida na criação de tenant."""

import pytest
from fastapi.testclient import TestClient

from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingTenantConflict,
    IdempotencyConflict,
)
from cnes_domain.billing.models import ReservationStatus
from cnes_domain.control_plane.entities import Tenant

from .billing_fakes import NOW
from .test_tenants import KEY, TenantEnv, committed_link, create, fail_creation, sha


@pytest.fixture
def env():
    return TenantEnv()


@pytest.fixture
def client(env):
    return TestClient(env.app())


def present(env, account="ba_01"):
    env.control_plane.tenants["novo-tenant"] = Tenant(
        tenant_id="novo-tenant", municipality_name="Lido", created_at=NOW,
    )
    env.link_result = committed_link(account=account)


def test_post_repetido_com_outra_chave_responde_409_e_libera_reserva(client, env):
    fail_creation(env, BillingTenantConflict("reason=exists"))
    present(env)
    response = create(client)
    assert response.status_code == 409
    assert response.json() == {"detail": "billing_tenant_conflict"}
    assert len(env.capacity.releases) == 1
    assert env.capacity.releases[0].reservation_id == "res-1"


def test_conflito_de_outra_conta_libera_reserva(client, env):
    fail_creation(env, BillingTenantConflict("reason=exists"))
    present(env, account="ba_99")
    assert create(client).status_code == 409
    assert len(env.capacity.releases) == 1


def test_reserva_replayada_consumida_nunca_e_liberada(client, env):
    env.capacity.forced_status = ReservationStatus.CONSUMED
    fail_creation(env, BillingTenantConflict("reason=exists"))
    assert create(client).status_code == 409
    assert env.capacity.releases == []


def test_falha_incerta_com_sonda_bem_sucedida_devolve_201_sem_liberar(client, env):
    env.control_plane.error_queue = [BillingDependencyError("timeout_after_commit")]
    response = create(client)
    assert response.status_code == 201
    assert response.json()["tenant_id"] == "novo-tenant"
    assert env.capacity.releases == []
    assert len(env.control_plane.commands) == 2
    assert env.control_plane.commands[0] == env.control_plane.commands[1]


def test_falha_incerta_com_sonda_deterministica_libera_e_relanca_original(client, env):
    env.control_plane.error_queue = [
        BillingDependencyError("timeout"), IdempotencyConflict("reason=mismatch"),
    ]
    response = create(client)
    assert response.status_code == 503
    assert len(env.capacity.releases) == 1
    assert "get_tenant" not in env.events


def test_falha_incerta_dupla_com_estado_presente_nao_libera_nem_devolve_201(client, env):
    fail_creation(env, BillingDependencyError("timeout"))
    present(env)
    assert create(client).status_code == 503
    assert env.capacity.releases == []


def test_retry_apos_503_com_mesma_chave_reserva_de_novo_e_cria(client, env):
    env.control_plane.error_queue = [BillingDependencyError("t")] * 2
    assert create(client).status_code == 503
    second = create(client)
    assert second.status_code == 201
    base = sha("tenant", "user-1", "ba_01", KEY)
    assert [c.idempotency_key for c in env.capacity.commands] == [base, base, f"{base}#1"]
    assert env.control_plane.commands[-1].idempotency_key == base
    assert env.control_plane.commands[-1].reservation_id == "res-2"


def test_reservas_sempre_liberadas_esgotam_com_503(client, env):
    env.capacity.forced_status = ReservationStatus.RELEASED
    response = create(client)
    assert response.status_code == 503
    assert response.json() == {"detail": "tenant_creation_retry_exhausted"}
    assert response.headers["Retry-After"] == "5"
    assert len(env.capacity.commands) == 3
    assert env.control_plane.commands == []
