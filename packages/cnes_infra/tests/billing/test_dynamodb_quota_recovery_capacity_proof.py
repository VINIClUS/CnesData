"""Testes da prova de consumo na recuperação de reservas de capacidade."""

from cnes_domain.billing.models import CapacityKind, ReservationStatus
from cnes_domain.control_plane.entities import Agent, Tenant
from cnes_domain.control_plane.enums import AgentState
from packages.cnes_infra.tests.billing.quota_support import HASH_A, NOW, TENANT, quota_env
from packages.cnes_infra.tests.billing.test_dynamodb_quota_recovery import (
    _capacity,
    _capacity_counter,
    _expire,
    _reconcile,
    _seed_capacity,
    _seed_proof,
    _stored_capacity,
)


def _agent() -> Agent:
    return Agent(
        tenant_id=TENANT,
        agent_id="agent-01",
        state=AgentState.ACTIVE,
        version="1.0",
        certificate_fingerprint=HASH_A,
        last_seen_at=None,
        created_at=NOW,
    )


def test_agente_existente_sem_marcador_da_reserva_libera() -> None:
    with quota_env() as env:
        capacity = _capacity(CapacityKind.AGENT, "agent-01")
        _seed_capacity(env, capacity)
        env.control_plane.put_agent(_agent())
        _expire(env)

        result = _reconcile(env.repo, env)

        assert result.released == 1
        assert _stored_capacity(env, capacity).status is ReservationStatus.RELEASED
        assert _capacity_counter(env, "agent_count") == 0


def test_marcador_da_reserva_consome_agente() -> None:
    with quota_env() as env:
        capacity = _capacity(CapacityKind.AGENT, "agent-01")
        _seed_capacity(env, capacity)
        _seed_proof(env, capacity)
        _expire(env)

        result = _reconcile(env.repo, env)

        assert result.released == 0
        assert _stored_capacity(env, capacity).status is ReservationStatus.CONSUMED
        assert _capacity_counter(env, "agent_count") == 1


def test_tenant_sem_link_da_conta_libera() -> None:
    with quota_env() as env:
        capacity = _capacity(CapacityKind.TENANT, TENANT)
        _seed_capacity(env, capacity)
        env.control_plane.put_tenant(
            Tenant(tenant_id=TENANT, municipality_name="Presidente Epitacio", created_at=NOW)
        )
        _expire(env)

        result = _reconcile(env.repo, env)

        assert result.released == 1
        assert _stored_capacity(env, capacity).status is ReservationStatus.RELEASED
        assert _capacity_counter(env, "tenant_count") == 0


def test_link_da_conta_consome_tenant() -> None:
    with quota_env() as env:
        capacity = _capacity(CapacityKind.TENANT, TENANT)
        _seed_capacity(env, capacity)
        _seed_proof(env, capacity)
        _expire(env)

        result = _reconcile(env.repo, env)

        assert result.released == 0
        assert _stored_capacity(env, capacity).status is ReservationStatus.CONSUMED
        assert _capacity_counter(env, "tenant_count") == 1
