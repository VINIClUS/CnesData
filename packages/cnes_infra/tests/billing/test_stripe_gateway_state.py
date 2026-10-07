"""Testes do StripeGateway: estado atual da assinatura e eventos."""

import hashlib
import json
import re
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from cnes_domain.billing.commands import StripeStateRequest
from cnes_domain.billing.inbox import StripeEventListRequest
from cnes_domain.billing.models import SubscriptionStatus
from cnes_infra.billing.stripe_gateway import StripeMappingError
from packages.cnes_infra.tests.billing.stripe_fakes import (
    PERIOD_END,
    PERIOD_START,
    make_entitlements,
    make_event,
    make_gateway,
    make_invoice,
    make_item,
    make_plan,
    make_subscription,
)

BY_ID = StripeStateRequest("cus_01", "sub_01")
BY_CUSTOMER = StripeStateRequest("cus_01", None)


def _setup(subscription=None, entitlements=None):
    gateway, client, plans = make_gateway()
    client.v1.subscriptions.retrieve.return_value = subscription or make_subscription()
    client.v1.entitlements.active_entitlements.list.return_value = (
        entitlements or make_entitlements(["serving"])
    )
    return gateway, client, plans


def test_estado_por_id_le_periodo_dos_itens_e_features() -> None:
    gateway, client, _ = _setup(make_subscription(cancel_at_period_end=True))
    state = gateway.get_current_state(BY_ID)
    client.v1.subscriptions.retrieve.assert_called_once_with("sub_01")
    assert state.subscription_status is SubscriptionStatus.ACTIVE
    assert state.cancel_at_period_end is True
    assert state.stripe_price_id == "price_01"
    assert state.active_features == frozenset({"serving"})
    assert state.period_start == datetime.fromtimestamp(PERIOD_START, tz=UTC)
    assert state.period_end == datetime.fromtimestamp(PERIOD_END, tz=UTC)
    assert state.latest_invoice_id is None


def test_estado_por_cliente_lista_uma_assinatura() -> None:
    gateway, client, _ = _setup()
    client.v1.subscriptions.list.return_value = SimpleNamespace(data=[make_subscription()])
    state = gateway.get_current_state(BY_CUSTOMER)
    client.v1.subscriptions.list.assert_called_once_with(
        params={"customer": "cus_01", "limit": 2},
    )
    client.v1.subscriptions.retrieve.assert_not_called()
    assert state.stripe_subscription_id == "sub_01"


@pytest.mark.parametrize("count", [0, 2])
def test_estado_por_cliente_rejeita_assinatura_ambigua(count) -> None:
    gateway, client, _ = _setup()
    client.v1.subscriptions.list.return_value = SimpleNamespace(
        data=[make_subscription() for _ in range(count)],
    )
    with pytest.raises(StripeMappingError) as info:
        gateway.get_current_state(BY_CUSTOMER)
    assert info.value.code == "stripe_subscription_ambiguous"
    assert f"subscription_count={count}" in str(info.value)


def test_estado_rejeita_cliente_divergente() -> None:
    gateway, _, _ = _setup(make_subscription(customer="cus_other"))
    with pytest.raises(StripeMappingError) as info:
        gateway.get_current_state(BY_ID)
    assert info.value.code == "stripe_customer_mismatch"


def test_estado_aceita_cliente_expandido() -> None:
    gateway, _, _ = _setup(make_subscription(customer=SimpleNamespace(id="cus_01")))
    assert gateway.get_current_state(BY_ID).stripe_customer_id == "cus_01"


@pytest.mark.parametrize("count", [0, 2])
def test_estado_rejeita_quantidade_inesperada_de_itens(count) -> None:
    items = SimpleNamespace(data=[make_item() for _ in range(count)])
    gateway, _, _ = _setup(make_subscription(items=items))
    with pytest.raises(StripeMappingError) as info:
        gateway.get_current_state(BY_ID)
    assert info.value.code == "stripe_subscription_items_unexpected"
    assert f"item_count={count}" in str(info.value)


def test_estado_rejeita_preco_desconhecido() -> None:
    gateway, _, plans = _setup()
    plans.get_plan_by_price.return_value = None
    with pytest.raises(StripeMappingError) as info:
        gateway.get_current_state(BY_ID)
    assert info.value.code == "stripe_price_unmapped"


def test_estado_rejeita_plano_sem_o_preco_do_item() -> None:
    gateway, _, plans = _setup()
    plans.get_plan_by_price.return_value = make_plan(("price_99",))
    with pytest.raises(StripeMappingError, match="price_id=price_01"):
        gateway.get_current_state(BY_ID)


@pytest.mark.parametrize("status", ["unknown_status", "admin_revoked"])
def test_estado_rejeita_status_nao_mapeado(status) -> None:
    gateway, _, _ = _setup(make_subscription(status=status))
    with pytest.raises(StripeMappingError) as info:
        gateway.get_current_state(BY_ID)
    assert info.value.code == "stripe_status_unmapped"
    assert f"status={status}" in str(info.value)


def test_estado_valida_fatura_pela_assinatura_do_parent() -> None:
    gateway, client, _ = _setup(make_subscription(latest_invoice="in_01"))
    client.v1.invoices.retrieve.return_value = make_invoice()
    assert gateway.get_current_state(BY_ID).latest_invoice_id == "in_01"
    client.v1.invoices.retrieve.assert_called_once_with("in_01")


def test_estado_aceita_fatura_expandida_e_assinatura_expandida() -> None:
    sub = make_subscription(latest_invoice=SimpleNamespace(id="in_01"))
    gateway, client, _ = _setup(sub)
    client.v1.invoices.retrieve.return_value = make_invoice(SimpleNamespace(id="sub_01"))
    assert gateway.get_current_state(BY_ID).latest_invoice_id == "in_01"


@pytest.mark.parametrize(
    "invoice",
    [
        make_invoice("sub_other"),
        make_invoice(parent=None),
        make_invoice(parent=SimpleNamespace(subscription_details=None)),
    ],
)
def test_estado_rejeita_fatura_de_outra_assinatura(invoice) -> None:
    gateway, client, _ = _setup(make_subscription(latest_invoice="in_01"))
    client.v1.invoices.retrieve.return_value = invoice
    with pytest.raises(StripeMappingError) as info:
        gateway.get_current_state(BY_ID)
    assert info.value.code == "stripe_invoice_subscription_mismatch"


def test_estado_sem_fatura_nao_consulta_invoices() -> None:
    gateway, client, _ = _setup()
    gateway.get_current_state(BY_ID)
    client.v1.invoices.retrieve.assert_not_called()


def test_estado_pagina_entitlements_com_starting_after() -> None:
    gateway, client, _ = _setup()
    listing = client.v1.entitlements.active_entitlements.list
    listing.side_effect = [
        make_entitlements(["a", "b"], has_more=True),
        make_entitlements(["c"]),
    ]
    state = gateway.get_current_state(BY_ID)
    assert state.active_features == frozenset({"a", "b", "c"})
    first, second = listing.call_args_list
    assert first.kwargs["params"] == {"customer": "cus_01", "limit": 100}
    assert second.kwargs["params"] == {
        "customer": "cus_01", "limit": 100, "starting_after": "ent_b",
    }


def test_estado_rejeita_entitlements_sem_progresso() -> None:
    gateway, client, _ = _setup()
    client.v1.entitlements.active_entitlements.list.return_value = make_entitlements(
        [], has_more=True,
    )
    with pytest.raises(StripeMappingError) as info:
        gateway.get_current_state(BY_ID)
    assert info.value.code == "stripe_entitlements_not_progressing"


CREATED_GTE = datetime(2026, 9, 1, tzinfo=UTC)


def _events_gateway(events, has_more=False):
    gateway, client, _ = make_gateway()
    client.v1.events.list.return_value = SimpleNamespace(data=events, has_more=has_more)
    return gateway, client


def test_eventos_paginam_para_mais_antigos_com_starting_after() -> None:
    events = [
        make_event("evt_105", SimpleNamespace(id="cus_01", object="customer")),
        make_event("evt_104", SimpleNamespace(id="cus_01", object="customer")),
    ]
    gateway, client = _events_gateway(events, has_more=True)
    page = gateway.list_events(StripeEventListRequest(CREATED_GTE, "evt_106", 100))
    client.v1.events.list.assert_called_once_with(
        params={
            "created": {"gte": int(CREATED_GTE.timestamp())},
            "limit": 100,
            "starting_after": "evt_106",
        },
    )
    assert page.has_more is True
    assert [e.event_id for e in page.events] == ["evt_105", "evt_104"]


def test_eventos_omitem_cursor_nulo_e_nunca_enviam_ending_before() -> None:
    gateway, client = _events_gateway([])
    page = gateway.list_events(StripeEventListRequest(CREATED_GTE, None, 50))
    params = client.v1.events.list.call_args.kwargs["params"]
    assert "starting_after" not in params
    assert "ending_before" not in params
    assert page.has_more is False
    assert page.events == ()


def _obj(**fields: object) -> SimpleNamespace:
    return SimpleNamespace(**fields)


_INVOICE_PARENT = make_invoice().parent
_EVENT_CASES = [
    (_obj(id="cus_01", object="customer"), "cus_01", None),
    (_obj(id="sub_01", object="subscription", customer="cus_01"), "cus_01", "sub_01"),
    (_obj(id="in_01", object="invoice", customer="cus_01", parent=_INVOICE_PARENT),
     "cus_01", "sub_01"),
    (_obj(id="in_02", object="invoice", customer="cus_01", parent=None), "cus_01", None),
    (_obj(id="cs_01", object="checkout.session", customer="cus_01", subscription="sub_02"),
     "cus_01", "sub_02"),
    (_obj(id="pm_01", object="payment_method"), None, None),
    (_obj(id="sub_03", object="subscription", customer=_obj(id="cus_03")), "cus_03", "sub_03"),
]


@pytest.mark.parametrize(("obj", "customer", "subscription"), _EVENT_CASES)
def test_eventos_mapeiam_cliente_e_assinatura_por_tipo_de_objeto(
    obj, customer, subscription,
) -> None:
    gateway, _ = _events_gateway([make_event("evt_01", obj)])
    event = gateway.list_events(StripeEventListRequest(CREATED_GTE, None, 100)).events[0]
    assert event.stripe_customer_id == customer
    assert event.stripe_subscription_id == subscription
    assert event.event_type == "customer.updated"
    assert event.created_at == datetime.fromtimestamp(PERIOD_START, tz=UTC)


def test_eventos_calculam_payload_sha256_deterministico() -> None:
    event = make_event("evt_01", SimpleNamespace(id="cus_01", object="customer"))
    gateway, _ = _events_gateway([event])
    request = StripeEventListRequest(CREATED_GTE, None, 100)
    first = gateway.list_events(request).events[0].payload_sha256
    second = gateway.list_events(request).events[0].payload_sha256
    expected = hashlib.sha256(
        json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":")).encode(),
    ).hexdigest()
    assert first == second == expected
    assert re.fullmatch(r"[0-9a-f]{64}", first)
