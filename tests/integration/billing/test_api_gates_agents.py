"""Integração dos gates de agente Edge com control plane e capacidade reais."""

from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cnes_domain.billing.models import BillingEnforcementMode, ReservationStatus
from cnes_domain.control_plane.entities import Agent
from cnes_domain.control_plane.enums import AgentState
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from packages.cnes_infra.tests.billing.billing_factories import (
    TABLE_NAME,
    make_create_command,
    make_snapshot,
    put_tenant,
)
from packages.cnes_infra.tests.billing.quota_support import TENANT, make_limits, seed_snapshot
from tests.integration.billing._api_gates_stack import (
    FINGERPRINT,
    NEXT_JOB_URL,
    ROTATED_FINGERPRINT,
    SERVING_FEATURES,
    ApiStack,
    build_client,
    capacity_counter,
    capacity_reservations,
    default_snapshot,
    edge_headers,
    open_api_stack,
    with_capacity_hook,
)
from tests.integration.billing._enforcement_stack import DYNAMO_STRIPE, MATRIX
from tests.integration.billing._execution_stack import NOW


@pytest.fixture(params=MATRIX)
def stack(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[ApiStack]:
    with open_api_stack(request.param, tmp_path) as opened:
        yield opened


@pytest.fixture
def single_slot(tmp_path: Path) -> Iterator[ApiStack]:
    with open_api_stack(DYNAMO_STRIPE, tmp_path, default_snapshot(max_agents=1)) as opened:
        yield opened


def _next(client: TestClient, agent_id: str, fingerprint: str = FINGERPRINT) -> int:
    response = client.get(NEXT_JOB_URL, headers=edge_headers(agent_id, fingerprint))
    return response.status_code


def _denial(client: TestClient, agent_id: str, tenant: str = TENANT) -> tuple[int, str]:
    response = client.get(NEXT_JOB_URL, headers=edge_headers(agent_id, tenant=tenant))
    return response.status_code, response.json()["detail"]


UNMETERED = pytest.mark.parametrize(
    "mode", [BillingEnforcementMode.OFF, BillingEnforcementMode.SHADOW], ids=["off", "shadow"],
)


def test_agente_novo_passa_pelo_gate_e_consome_capacidade(stack: ApiStack) -> None:
    client = build_client(stack)

    assert _next(client, "agent-1") == 204

    assert stack.plane.get_agent(TENANT, "agent-1").state is AgentState.ACTIVE
    if stack.case.stripe:
        reservations = capacity_reservations(stack)
        assert [r.status for r in reservations] == [ReservationStatus.CONSUMED]
        assert reservations[0].resource_id == "agent-1"
        assert capacity_counter(stack, "agent_count") == 1
    elif stack.case.dynamo:
        assert capacity_reservations(stack) == []


def test_rotacao_de_fingerprint_e_request_comum_nao_reservam(stack: ApiStack) -> None:
    client = build_client(stack)
    assert _next(client, "agent-1") == 204
    before = capacity_reservations(stack) if stack.case.stripe else []

    assert _next(client, "agent-1") == 204
    assert _next(client, "agent-1", ROTATED_FINGERPRINT) == 204

    agent = stack.plane.get_agent(TENANT, "agent-1")
    assert agent.certificate_fingerprint == ROTATED_FINGERPRINT
    if stack.case.stripe:
        assert capacity_reservations(stack) == before
        assert capacity_counter(stack, "agent_count") == 1


def test_agente_revogado_continua_403(stack: ApiStack) -> None:
    stack.plane.put_agent(Agent(
        tenant_id=TENANT, agent_id="agent-1", state=AgentState.REVOKED, version="1.0",
        certificate_fingerprint=FINGERPRINT, last_seen_at=None, created_at=NOW,
    ))
    client = build_client(stack)

    response = client.get(NEXT_JOB_URL, headers=edge_headers("agent-1"))

    assert (response.status_code, response.json()["detail"]) == (403, "agent_revoked")
    if stack.case.stripe:
        assert capacity_reservations(stack) == []
        assert capacity_counter(stack, "agent_count") == 0


def test_disputa_pela_ultima_vaga_cria_um_unico_agente(single_slot: ApiStack) -> None:
    client = build_client(single_slot)
    outcome: dict[str, int] = {}

    def run_second_agent_first() -> None:
        outcome["second"] = _next(client, "agent-2")

    with_capacity_hook(single_slot, run_second_agent_first)

    response = client.get(NEXT_JOB_URL, headers=edge_headers("agent-1"))

    assert outcome["second"] == 204
    assert (response.status_code, response.json()["detail"]) == (403, "agent_quota_exceeded")
    assert single_slot.plane.get_agent(TENANT, "agent-1") is None
    assert single_slot.plane.get_agent(TENANT, "agent-2") is not None
    assert capacity_counter(single_slot, "agent_count") == 1
    assert [r.status for r in capacity_reservations(single_slot)] == [ReservationStatus.CONSUMED]


def test_conta_sem_link_em_stripe_nega_agente_novo(single_slot: ApiStack) -> None:
    client = build_client(single_slot)

    response = client.get(NEXT_JOB_URL, headers=edge_headers("agent-9", tenant="sem-link"))

    assert (response.status_code, response.json()["detail"]) == (403, "billing_account_missing")
    assert single_slot.plane.get_agent("sem-link", "agent-9") is None
    assert capacity_reservations(single_slot) == []


@UNMETERED
def test_agentes_admitidos_em_shadow_contam_apos_virada(
    tmp_path: Path, mode: BillingEnforcementMode,
) -> None:
    case = replace(DYNAMO_STRIPE, enforcement=mode)
    with open_api_stack(case, tmp_path, default_snapshot(max_agents=2)) as stack:
        unmetered = build_client(stack)
        assert (_next(unmetered, "a1"), _next(unmetered, "a2")) == (204, 204)

        enforced = build_client(stack, enforcement=BillingEnforcementMode.ENFORCE)

        assert _denial(enforced, "a3") == (403, "agent_quota_exceeded")
        assert _next(enforced, "a1") == 204
        assert stack.plane.get_agent(TENANT, "a3") is None
        assert capacity_counter(stack, "agent_count") == 2
        assert capacity_reservations(stack) == []


@UNMETERED
def test_agente_de_tenant_sem_conta_conta_depois_da_criacao_da_conta(
    tmp_path: Path, mode: BillingEnforcementMode,
) -> None:
    case = replace(DYNAMO_STRIPE, enforcement=mode)
    with open_api_stack(case, tmp_path) as stack:
        put_tenant(stack.client, "sem-conta")
        unmetered = build_client(stack)
        response = unmetered.get(NEXT_JOB_URL, headers=edge_headers("a1", tenant="sem-conta"))
        assert response.status_code == 204
        catalog = DynamoBillingCatalog(stack.client, TABLE_NAME, stack.clock.now)
        catalog.create_account(make_create_command("ba_02", "sem-conta", "create-02"))
        quotas = make_limits(max_agents=1)
        snapshot = make_snapshot("ba_02", quotas=quotas, features=SERVING_FEATURES)
        seed_snapshot(stack.client, snapshot)

        enforced = build_client(stack, enforcement=BillingEnforcementMode.ENFORCE)

        assert _denial(enforced, "a2", "sem-conta") == (403, "agent_quota_exceeded")
        assert capacity_counter(stack, "agent_count", "ba_02") == 1
