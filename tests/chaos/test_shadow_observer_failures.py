"""Falhas do DynamoDB no observador de shadow nunca mudam a resposta dos gates de API."""

import pytest

pytest.importorskip("moto")

import json
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

from botocore.exceptions import ClientError

from central_api.composition import api_billing_gates
from cnes_domain.billing.models import BillingEnforcementMode, SubscriptionStatus
from cnes_domain.control_plane.entities import Membership
from cnes_infra.billing.wiring import BillingGateResources
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import TENANT
from packages.cnes_infra.tests.billing.shadow_support import shadow_events
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
from tests.integration.billing._execution_stack import Case

pytestmark = [pytest.mark.chaos]

DYNAMO_SHADOW = Case(
    "dynamodb-shadow", dynamo=True, stripe=True, enforcement=BillingEnforcementMode.SHADOW,
)
REVOKED = replace(default_snapshot(), subscription_status=SubscriptionStatus.ADMIN_REVOKED)


def _client_error(operation: str) -> ClientError:
    return ClientError({"Error": {"Code": "InternalServerError", "Message": "x"}}, operation)


class BrokenObserverClient:
    """Cliente DynamoDB exclusivo do observador, com falhas injetadas por operação."""

    def __init__(self, inner: Any, failing: frozenset[str], error: Exception | None) -> None:
        self._inner = inner
        self._failing = failing
        self._error = error

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self._inner, name)
        if name not in self._failing:
            return attribute
        error = self._error or _client_error(name)

        def fail(**request: Any) -> Any:
            raise error

        return fail


def _break_observer(stack: ApiStack, failing: frozenset[str], error: Exception | None) -> None:
    settings = replace(stack.case.settings, metrics_environment="chaos")
    client = BrokenObserverClient(stack.client, failing, error)
    resources = BillingGateResources(stack.clock.now, 4, client, TABLE_NAME)
    stack.gates = api_billing_gates(settings, resources)


@pytest.fixture
def stack(tmp_path: Path) -> Iterator[ApiStack]:
    with open_api_stack(DYNAMO_SHADOW, tmp_path, REVOKED) as opened:
        opened.plane.put_membership(Membership(
            tenant_id=TENANT, user_id=VIEWER, role="viewer", created_at=NOW,
        ))
        seed_dataset(opened, "cnes", utc_now())
        yield opened


def _statuses(stack: ApiStack) -> tuple[int, int, int]:
    client = build_client(stack)
    agent = client.get(NEXT_JOB_URL, headers=edge_headers("agent-1", tenant="sem-link"))
    serving = client.get(SERVING_URL.format(dataset="cnes"), headers=user_headers(VIEWER))
    tenant = create_tenant(client, "novo-tenant")
    return agent.status_code, serving.status_code, tenant.status_code


def _metrics(capsys: pytest.CaptureFixture[str]) -> list[str]:
    documents = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    metrics = [doc for doc in documents if doc.get("event") == "billing_metric"]
    return sorted(
        directive["Metrics"][0]["Name"]
        for doc in metrics
        for directive in doc["_aws"]["CloudWatchMetrics"]
    )


@pytest.mark.parametrize(
    "failing",
    [frozenset({"get_item"}), frozenset({"get_item", "query"})],
    ids=["get_item", "get_item-query"],
)
def test_leitura_quebrada_mantem_status_e_emite_metrica_de_falha(
    stack: ApiStack, capsys: pytest.CaptureFixture[str], failing: frozenset[str],
) -> None:
    _break_observer(stack, failing, None)

    assert _statuses(stack) == (204, 200, 201)

    assert _metrics(capsys) == ["ShadowObserverFailures"] * 3
    assert shadow_events(stack.client) == []


def test_outbox_quebrado_mantem_status_e_emite_metrica_de_outbox(
    stack: ApiStack, capsys: pytest.CaptureFixture[str],
) -> None:
    _break_observer(stack, frozenset({"transact_write_items"}), None)

    assert _statuses(stack) == (204, 200, 201)

    metrics = _metrics(capsys)
    assert metrics.count("AuditOutboxFailures") == 3
    assert "ShadowObserverFailures" not in metrics
    assert shadow_events(stack.client) == []


def test_erro_inesperado_no_outbox_mantem_status_e_emite_metrica_de_falha(
    stack: ApiStack, capsys: pytest.CaptureFixture[str],
) -> None:
    _break_observer(stack, frozenset({"transact_write_items"}), RuntimeError("boom"))

    assert _statuses(stack) == (204, 200, 201)

    assert _metrics(capsys).count("ShadowObserverFailures") == 3
    assert shadow_events(stack.client) == []
