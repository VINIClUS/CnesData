"""Integração de revogação administrativa e entitlement de serving com adapters reais."""

from collections.abc import Iterator
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cnes_domain.billing.models import SubscriptionStatus
from cnes_domain.control_plane.entities import Membership
from packages.cnes_infra.tests.billing.billing_factories import NOW
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, TENANT, seed_snapshot
from tests.integration.billing._api_gates_stack import (
    MANAGER,
    OWNER,
    REVOKE_URL,
    SERVING_URL,
    VIEWER,
    ApiStack,
    build_client,
    default_snapshot,
    open_api_stack,
    read_snapshot,
    seed_dataset,
    user_headers,
    utc_now,
)
from tests.integration.billing._enforcement_stack import DYNAMO_STRIPE

BODY = {"reason_code": "fraud"}
RETENTION_DAYS = 30


@pytest.fixture
def stack(tmp_path: Path) -> Iterator[ApiStack]:
    with open_api_stack(DYNAMO_STRIPE, tmp_path) as opened:
        opened.plane.put_membership(Membership(
            tenant_id=TENANT, user_id=VIEWER, role="viewer", created_at=NOW,
        ))
        yield opened


def _revoke(client: TestClient, headers: dict[str, str]) -> int:
    return client.post(REVOKE_URL, json=BODY, headers=headers).status_code


def _read(client: TestClient, dataset: str = "cnes"):
    return client.get(SERVING_URL.format(dataset=dataset), headers=user_headers(VIEWER))


def test_admin_de_tenant_nao_vinculado_nao_revoga(stack: ApiStack) -> None:
    stack.plane.put_membership(Membership(
        tenant_id="tenant-b", user_id=MANAGER, role="gestor", created_at=NOW,
    ))
    client = build_client(stack)

    response = client.post(REVOKE_URL, json=BODY, headers=user_headers(MANAGER, "tenant-b"))

    assert (response.status_code, response.json()["detail"]) == (403, "billing_owner_required")
    assert read_snapshot(stack).subscription_status is SubscriptionStatus.ACTIVE
    stack.executor.cancel.assert_not_called()


def test_dono_revoga_e_snapshot_fica_admin_revoked(stack: ApiStack) -> None:
    client = build_client(stack)

    response = client.post(REVOKE_URL, json=BODY, headers=user_headers(OWNER))

    assert response.status_code == 200
    assert response.json()["billing_account_id"] == ACCOUNT
    snapshot = read_snapshot(stack)
    assert snapshot.subscription_status is SubscriptionStatus.ADMIN_REVOKED
    assert response.json()["entitlement_version"] == snapshot.entitlement_version


@pytest.mark.parametrize("redirect", [False, True], ids=["stream", "redirect"])
def test_admin_revoked_nega_serving_na_hora_nos_dois_formatos(
    stack: ApiStack, redirect: bool,
) -> None:
    seed_dataset(stack, "cnes", utc_now())
    client = build_client(stack, redirect)

    before = _read(client)
    assert _revoke(client, user_headers(OWNER)) == 200
    after = _read(client)

    assert before.status_code == (307 if redirect else 200)
    if redirect:
        assert before.headers["location"].startswith("https://")
    else:
        assert before.content == b'{"documento": "overview"}'
    assert (after.status_code, after.json()["detail"]) == (403, "serving_entitlement_denied")


@pytest.mark.parametrize("redirect", [False, True], ids=["stream", "redirect"])
def test_read_only_respeita_retencao(stack: ApiStack, redirect: bool) -> None:
    canceled = replace(
        default_snapshot(retention_days=RETENTION_DAYS),
        subscription_status=SubscriptionStatus.CANCELED,
    )
    seed_snapshot(stack.client, canceled)
    seed_dataset(stack, "recente", utc_now() - timedelta(days=1))
    seed_dataset(stack, "antiga", utc_now() - timedelta(days=RETENTION_DAYS + 5))
    client = build_client(stack, redirect)

    recent = _read(client, "recente")
    old = _read(client, "antiga")

    assert recent.status_code == (307 if redirect else 200)
    assert (old.status_code, old.json()["detail"]) == (403, "serving_entitlement_denied")
