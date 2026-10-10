"""Integração do shadow nos gates de API: libera a requisição e audita a negação hipotética."""

from collections.abc import Iterator
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cnes_domain.billing.models import BillingEnforcementMode, SubscriptionStatus
from cnes_domain.control_plane.entities import Membership, OutboxEvent
from cnes_infra.billing.keys import tenant_account_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, TENANT
from packages.cnes_infra.tests.billing.shadow_support import (
    seed_capacity,
    shadow_attributes,
    shadow_events,
    shadow_reasons,
)
from tests.integration.billing._api_gates_stack import (
    NEXT_JOB_URL,
    SERVING_URL,
    VIEWER,
    ApiStack,
    build_client,
    create_tenant,
    default_snapshot,
    edge_headers,
    open_api_stack,
    seed_dataset,
    user_headers,
    utc_now,
)
from tests.integration.billing._enforcement_stack import DYNAMO_STRIPE
from tests.integration.billing._execution_stack import Case

DYNAMO_SHADOW = Case(
    "dynamodb-shadow", dynamo=True, stripe=True, enforcement=BillingEnforcementMode.SHADOW,
)
REVOKED = replace(default_snapshot(), subscription_status=SubscriptionStatus.ADMIN_REVOKED)


@pytest.fixture
def shadow(tmp_path: Path) -> Iterator[ApiStack]:
    with open_api_stack(DYNAMO_SHADOW, tmp_path, default_snapshot(max_agents=1)) as opened:
        yield opened


def _agent(client: TestClient, agent_id: str, tenant: str = TENANT) -> int:
    return client.get(NEXT_JOB_URL, headers=edge_headers(agent_id, tenant=tenant)).status_code


def _read_serving(stack: ApiStack) -> int:
    stack.plane.put_membership(Membership(
        tenant_id=TENANT, user_id=VIEWER, role="viewer", created_at=NOW,
    ))
    seed_dataset(stack, "cnes", utc_now())
    url = SERVING_URL.format(dataset="cnes")
    return build_client(stack).get(url, headers=user_headers(VIEWER)).status_code


def test_shadow_compoe_gate_sem_medicao(shadow: ApiStack) -> None:
    assert shadow.case.settings.enforcement_mode is BillingEnforcementMode.SHADOW
    assert shadow.gates.enforced is False


def test_agente_de_tenant_sem_link_e_admitido_e_auditado(shadow: ApiStack) -> None:
    client = build_client(shadow)

    assert _agent(client, "agent-9", tenant="sem-link") == 204

    assert shadow.plane.get_agent("sem-link", "agent-9") is not None
    [event] = shadow_events(shadow.client)
    assert event.payload["reason_code"] == "billing_account_missing"
    assert event.payload["actor_id"] == "system:shadow_observer"
    assert event.payload["attributes"] == {
        "action": "register_agent",
        "reason": "billing_account_missing",
        "tenant_id": "sem-link",
    }


def test_agentes_acima_do_limite_sao_admitidos_e_auditados(shadow: ApiStack) -> None:
    seed_capacity(shadow.client, ACCOUNT, agent_count=1, tenant_count=1)
    client = build_client(shadow)

    statuses = [_agent(client, f"agent-{index}") for index in range(3)]

    assert statuses == [204, 204, 204]
    [event] = shadow_events(shadow.client)
    assert event.payload["reason_code"] == "max_agents_exceeded"
    attributes = shadow_attributes(event)
    assert (attributes["limit"], attributes["used"]) == (1, 1)
    assert attributes["billing_account_id"] == ACCOUNT


def test_agente_sem_contador_semeado_e_auditado_como_nao_semeado(shadow: ApiStack) -> None:
    assert _agent(build_client(shadow), "agent-1") == 204

    assert shadow_reasons(shadow.client) == ["capacity_not_seeded"]


def test_tenant_acima_do_limite_e_criado_e_auditado(tmp_path: Path) -> None:
    with open_api_stack(DYNAMO_SHADOW, tmp_path, default_snapshot(max_tenants=1)) as stack:
        seed_capacity(stack.client, ACCOUNT, agent_count=0, tenant_count=1)

        response = create_tenant(build_client(stack), "novo-tenant")
        reasons = shadow_reasons(stack.client)
        tenant = stack.plane.get_tenant("novo-tenant")

    assert response.status_code == 201
    assert tenant is not None
    assert reasons == ["max_tenants_exceeded"]


def _unlink_tenant(stack: ApiStack) -> None:
    pk, sk = tenant_account_key(TENANT)
    stack.client.delete_item(TableName=TABLE_NAME, Key={"pk": {"S": pk}, "sk": {"S": sk}})


def test_canario_serving_de_tenant_sem_link_e_servido_e_auditado(tmp_path: Path) -> None:
    with open_api_stack(DYNAMO_SHADOW, tmp_path) as stack:
        _unlink_tenant(stack)

        status = _read_serving(stack)
        events = shadow_events(stack.client)

    assert status == 200
    [event] = events
    assert event.payload["reason_code"] == "billing_account_missing"
    assert shadow_attributes(event) == {
        "action": "serving_access",
        "reason": "billing_account_missing",
        "tenant_id": TENANT,
    }


def test_serving_admin_revoked_e_servido_e_auditado_uma_vez_por_hora(tmp_path: Path) -> None:
    with open_api_stack(DYNAMO_SHADOW, tmp_path, REVOKED) as stack:
        first = _read_serving(stack)
        second = _read_serving(stack)
        same_hour = shadow_events(stack.client)
        stack.clock.advance(timedelta(hours=1))
        third = _read_serving(stack)
        next_hour = shadow_events(stack.client)

    assert (first, second, third) == (200, 200, 200)
    assert [e.payload["reason_code"] for e in same_hour] == ["admin_revoked"]
    assert len(next_hour) == 2
    assert {e.event_type for e in next_hour} == {"entitlement.shadow_denied"}


def _enforce_reason_agent(tmp_path: Path) -> str:
    with open_api_stack(DYNAMO_STRIPE, tmp_path / "enforce") as stack:
        response = build_client(stack).get(
            NEXT_JOB_URL, headers=edge_headers("agent-9", tenant="sem-link"),
        )
    assert response.status_code == 403
    return str(response.json()["detail"])


def _shadow_reason_agent(tmp_path: Path) -> str:
    with open_api_stack(DYNAMO_SHADOW, tmp_path / "shadow") as stack:
        assert _agent(build_client(stack), "agent-9", tenant="sem-link") == 204
        [reason] = shadow_reasons(stack.client)
    return reason


def _enforce_reason_serving(tmp_path: Path) -> str:
    with open_api_stack(DYNAMO_STRIPE, tmp_path / "enforce", REVOKED) as stack:
        assert _read_serving(stack) == 403
        items = stack.client.scan(TableName=TABLE_NAME, ConsistentRead=True)["Items"]
        events = [
            OutboxEvent.model_validate_json(item["payload"]["S"]) for item in items
            if item.get("entity", {}).get("S") == "OUTBOXEVENT"
        ]
    [denied] = [event for event in events if event.event_type == "serving.denied"]
    return str(denied.payload["reason_code"])


def _enforce_reason_canary(tmp_path: Path) -> str:
    with open_api_stack(DYNAMO_STRIPE, tmp_path / "enforce") as stack:
        _unlink_tenant(stack)
        assert _read_serving(stack) == 403
        items = stack.client.scan(TableName=TABLE_NAME, ConsistentRead=True)["Items"]
        events = [
            OutboxEvent.model_validate_json(item["payload"]["S"]) for item in items
            if item.get("entity", {}).get("S") == "OUTBOXEVENT"
        ]
    [denied] = [event for event in events if event.event_type == "serving.denied"]
    return str(denied.payload["reason_code"])


def _shadow_reason_canary(tmp_path: Path) -> str:
    with open_api_stack(DYNAMO_SHADOW, tmp_path / "shadow") as stack:
        _unlink_tenant(stack)
        assert _read_serving(stack) == 200
        [reason] = shadow_reasons(stack.client)
    return reason


def _shadow_reason_serving(tmp_path: Path) -> str:
    with open_api_stack(DYNAMO_SHADOW, tmp_path / "shadow", REVOKED) as stack:
        assert _read_serving(stack) == 200
        [reason] = shadow_reasons(stack.client)
    return reason


@pytest.mark.parametrize(
    ("enforce", "shadow"),
    [
        (_enforce_reason_agent, _shadow_reason_agent),
        (_enforce_reason_serving, _shadow_reason_serving),
        (_enforce_reason_canary, _shadow_reason_canary),
    ],
    ids=["agente-sem-link", "serving-admin-revoked", "serving-sem-link"],
)
def test_paridade_motivo_do_enforce_igual_ao_do_shadow(tmp_path: Path, enforce, shadow) -> None:
    assert enforce(tmp_path) == shadow(tmp_path)
