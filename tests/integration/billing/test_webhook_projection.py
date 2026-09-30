"""Integracao BIL-021: webhook assinado e a unica prova de acesso, sobre DynamoDB Local."""

from collections.abc import Iterator
from typing import Any
from uuid import uuid4

import pytest

from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.billing.inbox import InboxProcessingState
from cnes_domain.billing.models import ReadConsistency, SubscriptionStatus
from tests.integration.billing._billing_stack import (
    ACCOUNT_ID,
    PRICE_V2,
    REQUEST,
    BillingStack,
    create_billing_table,
    dynamodb_client,
    install_fake_stripe,
    make_run_request,
    make_state,
    serve_pages,
)

pytestmark = [pytest.mark.dynamodb_local]

PROCESSED = InboxProcessingState.PROCESSED


@pytest.fixture(scope="module")
def dynamodb() -> Any:
    return dynamodb_client()


@pytest.fixture
def stack(dynamodb: Any, monkeypatch: pytest.MonkeyPatch) -> Iterator[BillingStack]:
    install_fake_stripe(monkeypatch)
    table_name = f"billing-it-{uuid4().hex[:12]}"
    create_billing_table(dynamodb, table_name)
    try:
        yield BillingStack(dynamodb, table_name)
    finally:
        dynamodb.delete_table(TableName=table_name)


def test_webhook_confirmado_e_unica_prova_de_acesso(stack: BillingStack) -> None:
    stack.checkout.complete_redirect("cs_01")
    with pytest.raises(EntitlementDenied, match="reason=snapshot_missing"):
        stack.gate.authorize_create_run(make_run_request())
    response = stack.post_webhook("evt_01")
    assert response.status_code == 200
    stack.drain()
    authorization = stack.gate.authorize_create_run(make_run_request())
    assert authorization.plan_version_id == "plan_v1"
    assert stack.checkout.completed == ["cs_01"]


def test_entregas_duplicadas_geram_um_item_e_uma_versao(stack: BillingStack) -> None:
    assert stack.post_webhook("evt_01").status_code == 200
    assert stack.post_webhook("evt_01").status_code == 200
    stack.drain()
    assert stack.post_webhook("evt_01").status_code == 200
    second = stack.drain()
    assert len(stack.inbox_items()) == 1
    assert stack.snapshot().entitlement_version == 1
    assert second.scanned == 0
    assert stack.stripe.state_calls == 1


def test_entrega_em_ordem_invertida_converge_ao_estado_atual(stack: BillingStack) -> None:
    stack.stripe.state = make_state(stripe_price_id=PRICE_V2)
    stack.post_webhook("evt_newer", "customer.subscription.updated")
    stack.drain()
    stack.post_webhook("evt_older", "customer.subscription.created")
    stack.drain()
    snapshot = stack.snapshot()
    assert snapshot.plan_version_id == "plan_v2"
    assert snapshot.subscription_status is SubscriptionStatus.ACTIVE
    assert snapshot.entitlement_version == 2
    assert stack.inbox_state("evt_older") is PROCESSED


def test_assinatura_invalida_responde_400_e_nao_grava_item(stack: BillingStack) -> None:
    response = stack.post_webhook("evt_01", signing_key="whsec_other")
    assert response.status_code == 400
    assert response.json() == {"detail": "stripe_signature_invalid"}
    assert stack.inbox_items() == []
    assert stack.inbox_state("evt_01") is None


def test_price_desconhecido_fica_retryable_e_gate_continua_negando(stack: BillingStack) -> None:
    stack.stripe.state = make_state(stripe_price_id="price_unknown")
    stack.post_webhook("evt_01")
    result = stack.drain()
    item = stack.inbox_items()[0]
    assert result.failed == 1
    assert stack.inbox_state("evt_01") is InboxProcessingState.FAILED_RETRYABLE
    assert item["error_code"]["S"] == "stripe_price_unmapped"
    assert stack.snapshot() is None
    with pytest.raises(EntitlementDenied, match="reason=snapshot_missing"):
        stack.gate.authorize_create_run(make_run_request())


def test_recovery_importa_evento_nao_entregue(stack: BillingStack) -> None:
    stack.stripe.pager = serve_pages(["evt_missing"])
    result = stack.recovery.run(REQUEST)
    assert result.imported == 1
    assert result.reprocessed == 1
    assert stack.inbox_state("evt_missing") is PROCESSED
    assert stack.gate.authorize_create_run(make_run_request()).plan_version_id == "plan_v1"
    assert stack.cursor.load(ReadConsistency.STRONG) is None


def test_recovery_percorre_205_eventos_em_tres_paginas(stack: BillingStack) -> None:
    ids = [f"evt_{number:03d}" for number in range(205, 0, -1)]
    stack.stripe.pager = serve_pages(ids)
    results = [stack.recovery.run(REQUEST) for _ in range(3)]
    requested = [request.starting_after for request in stack.stripe.list_requests]
    states = {stack.inbox_state(event_id) for event_id in ids}
    assert requested == [None, "evt_106", "evt_006"]
    assert [result.next_cursor for result in results] == ["evt_106", "evt_006", None]
    assert sum(result.imported for result in results) == 205
    assert states == {PROCESSED}
    assert stack.snapshot().entitlement_version == 205
    assert stack.cursor.load(ReadConsistency.STRONG) is None
    assert stack.snapshot().billing_account_id == ACCOUNT_ID
