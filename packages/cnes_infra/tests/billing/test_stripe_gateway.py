"""Testes do StripeGateway: escritas, configuracao, traducao de erros e redacao."""

import logging
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from cnes_domain.billing.commands import (
    CheckoutCommand,
    CreateStripeCustomerCommand,
    HostedSession,
    PortalCommand,
    StripeCustomer,
    StripeStateRequest,
)
from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.inbox import StripeEventListRequest
from cnes_domain.billing.ports import StripeGatewayPort
from cnes_infra.billing.stripe_gateway import (
    StripeGateway,
    StripeGatewayConfig,
    StripeMappingError,
)
from packages.cnes_infra.tests.billing.stripe_fakes import (
    CANCEL_URL,
    ORIGIN,
    PORTAL_URL,
    SUCCESS_URL,
    make_entitlements,
    make_gateway,
    make_plan,
    make_subscription,
    page,
)


def _checkout(plan=None) -> CheckoutCommand:
    return CheckoutCommand("ba_01", "cus_01", plan or make_plan(), "req_01")


def _session(url: str = "https://checkout.stripe.com/c/pay/cs_01") -> SimpleNamespace:
    return SimpleNamespace(id="cs_01", url=url)


def test_gateway_satisfaz_porta_stripe() -> None:
    gateway, _, _ = make_gateway()
    assert isinstance(gateway, StripeGatewayPort)


def test_checkout_usa_hosted_session_metadata_opaca_e_idempotencia() -> None:
    gateway, client, _ = make_gateway()
    client.v1.checkout.sessions.create.return_value = _session()
    result = gateway.create_checkout(_checkout())
    client.v1.checkout.sessions.create.assert_called_once_with(
        params={
            "mode": "subscription",
            "customer": "cus_01",
            "line_items": [{"price": "price_01", "quantity": 1}],
            "metadata": {"billing_account_id": "ba_01", "plan_version_id": "plan_v1"},
            "success_url": SUCCESS_URL,
            "cancel_url": CANCEL_URL,
            "client_reference_id": "req_01",
        },
        options={"idempotency_key": "checkout:req_01"},
    )
    assert result == HostedSession("cs_01", "https://checkout.stripe.com/c/pay/cs_01")
    assert result.url.startswith("https://checkout.stripe.com/")


def test_customer_usa_apenas_id_opaco() -> None:
    gateway, client, _ = make_gateway()
    client.v1.customers.create.return_value = SimpleNamespace(id="cus_01")
    result = gateway.create_customer(CreateStripeCustomerCommand("ba_01", "req_01"))
    client.v1.customers.create.assert_called_once_with(
        params={"metadata": {"billing_account_id": "ba_01"}},
        options={"idempotency_key": "customer:req_01"},
    )
    assert result == StripeCustomer("cus_01")


def test_portal_usa_return_url_do_servidor_e_chave_portal() -> None:
    gateway, client, _ = make_gateway()
    client.v1.billing_portal.sessions.create.return_value = SimpleNamespace(
        id="bps_01", url="https://billing.stripe.com/p/session/x",
    )
    result = gateway.create_portal(PortalCommand("ba_01", "cus_01", "req_01"))
    client.v1.billing_portal.sessions.create.assert_called_once_with(
        params={"customer": "cus_01", "return_url": PORTAL_URL},
        options={"idempotency_key": "portal:req_01"},
    )
    assert result.session_id == "bps_01"


@pytest.mark.parametrize("price_ids", [(), ("price_01", "price_02")])
def test_checkout_rejeita_plano_sem_preco_unico_sem_chamar_stripe(price_ids) -> None:
    gateway, client, _ = make_gateway()
    with pytest.raises(StripeMappingError) as info:
        gateway.create_checkout(_checkout(make_plan(price_ids)))
    assert info.value.code == "stripe_price_unmapped"
    assert isinstance(info.value, RetryableBillingError)
    client.v1.checkout.sessions.create.assert_not_called()


def test_checkout_rejeita_preco_desconhecido_no_catalogo() -> None:
    gateway, client, plans = make_gateway()
    plans.get_plan_by_price.return_value = None
    with pytest.raises(StripeMappingError, match="price_id=price_01"):
        gateway.create_checkout(_checkout())
    client.v1.checkout.sessions.create.assert_not_called()


def test_checkout_rejeita_preco_de_outro_plano() -> None:
    gateway, client, plans = make_gateway()
    other = make_plan()
    plans.get_plan_by_price.return_value = SimpleNamespace(
        plan_version_id="plan_v2", stripe_price_ids=other.stripe_price_ids,
    )
    with pytest.raises(StripeMappingError):
        gateway.create_checkout(_checkout())
    client.v1.checkout.sessions.create.assert_not_called()


def test_checkout_rejeita_plano_do_catalogo_sem_o_preco() -> None:
    gateway, client, plans = make_gateway()
    plans.get_plan_by_price.return_value = make_plan(("price_99",))
    with pytest.raises(StripeMappingError):
        gateway.create_checkout(_checkout())
    client.v1.checkout.sessions.create.assert_not_called()


class StripeError(Exception):
    def __init__(self, http_status=None) -> None:
        super().__init__("message with secret@example.com")
        self.http_status = http_status


class APIConnectionError(StripeError):
    pass


class RateLimitError(StripeError):
    pass


class APIError(StripeError):
    pass


class InvalidRequestError(StripeError):
    pass


class CardError(StripeError):
    pass


def _failing_checkout(error: Exception):
    gateway, client, _ = make_gateway()
    client.v1.checkout.sessions.create.side_effect = error
    return gateway


@pytest.mark.parametrize(
    "error",
    [
        APIConnectionError(),
        RateLimitError(429),
        APIError(500),
        InvalidRequestError(503),
        CardError(429),
    ],
)
def test_traduz_erro_transitorio_stripe_para_retryable(error) -> None:
    with pytest.raises(RetryableBillingError) as info:
        _failing_checkout(error).create_checkout(_checkout())
    assert info.value.code == "stripe_unavailable"
    assert not isinstance(info.value, StripeMappingError)


@pytest.mark.parametrize(
    ("error", "detail"),
    [(InvalidRequestError(400), "http_status=400"), (CardError(None), None)],
)
def test_traduz_erro_permanente_stripe_para_permanent(error, detail) -> None:
    with pytest.raises(PermanentBillingError) as info:
        _failing_checkout(error).create_checkout(_checkout())
    assert info.value.code == "stripe_request_rejected"
    assert (detail in str(info.value)) if detail else ("http_status" not in str(info.value))


def test_erro_traduzido_nao_vaza_causa_nem_mensagem() -> None:
    with pytest.raises(RetryableBillingError) as info:
        _failing_checkout(APIError(500)).create_checkout(_checkout())
    assert info.value.__cause__ is None
    assert info.value.__suppress_context__ is True
    assert "secret@example.com" not in str(info.value)


def test_erro_nao_stripe_propaga_sem_traducao() -> None:
    with pytest.raises(RuntimeError, match="boom"):
        _failing_checkout(RuntimeError("boom")).create_checkout(_checkout())


def test_registra_apenas_operacao_codigo_e_status_na_falha(caplog) -> None:
    caplog.set_level(logging.WARNING)
    with pytest.raises(RetryableBillingError):
        _failing_checkout(RateLimitError(429)).create_checkout(_checkout())
    assert [r.getMessage() for r in caplog.records] == [
        "stripe_call_failed operation=checkout.sessions.create code=RateLimitError"
        " http_status=429"
    ]


def _config(**overrides: object) -> StripeGatewayConfig:
    values: dict[str, object] = {
        "success_url": SUCCESS_URL,
        "cancel_url": CANCEL_URL,
        "portal_return_url": PORTAL_URL,
        "allowed_origins": frozenset({ORIGIN}),
    }
    values.update(overrides)
    return StripeGatewayConfig(**values)


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"success_url": "http://app.example.test/x"}, "return_url_not_allowed field=success_url"),
        ({"cancel_url": "https://evil.test/x"}, "return_url_not_allowed field=cancel_url"),
        (
            {"portal_return_url": "https://user:pw@app.example.test/x"},
            "return_url_not_allowed field=portal_return_url",
        ),
        ({"success_url": "https:///path"}, "return_url_not_allowed field=success_url"),
        ({"allowed_origins": frozenset({f"{ORIGIN}/path"})}, "return_origin_invalid"),
        ({"allowed_origins": frozenset({"http://app.example.test"})}, "return_origin_invalid"),
        ({"allowed_origins": frozenset({"https://u@app.example.test"})}, "return_origin_invalid"),
        ({"allowed_origins": frozenset()}, "return_origins_empty"),
    ],
)
def test_config_rejeita_urls_e_origens_invalidas(overrides, reason) -> None:
    with pytest.raises(ValueError) as info:
        _config(**overrides)
    assert str(info.value) == f"reason={reason}"
    assert "example.test/" not in str(info.value)


def test_config_aceita_host_em_maiusculas() -> None:
    config = _config(cancel_url="https://APP.Example.Test/billing/cancel")
    assert config.cancel_url.startswith("https://APP.")


def test_registra_nada_sensivel_e_nao_envia_pii_ao_cliente(caplog) -> None:
    caplog.set_level(logging.DEBUG)
    gateway, client, _ = make_gateway()
    client.v1.customers.create.return_value = SimpleNamespace(id="cus_01")
    client.v1.checkout.sessions.create.return_value = _session()
    client.v1.billing_portal.sessions.create.return_value = _session()
    client.v1.subscriptions.retrieve.return_value = make_subscription()
    client.v1.entitlements.active_entitlements.list.return_value = make_entitlements([])
    client.v1.events.list.return_value = SimpleNamespace(data=[], has_more=False)
    gateway.create_customer(CreateStripeCustomerCommand("ba_01", "req_01"))
    gateway.create_checkout(_checkout())
    gateway.create_portal(PortalCommand("ba_01", "cus_01", "req_01"))
    gateway.get_current_state(StripeStateRequest("cus_01", "sub_01"))
    gateway.list_events(
        StripeEventListRequest(datetime(2026, 9, 1, tzinfo=UTC), None, 100),
    )
    forbidden = ("@", "55.293.427/0001-17", "123.456.789-09", "4242424242424242", "Epit")
    sent = repr(client.mock_calls)
    for token in forbidden:
        assert token not in sent
        assert token not in caplog.text
    metadata = client.v1.checkout.sessions.create.call_args.kwargs["params"]["metadata"]
    assert set(metadata) <= {"billing_account_id", "plan_version_id"}
    assert isinstance(gateway, StripeGateway)


_LIVE = ["active", "trialing", "past_due", "incomplete", "unpaid", "paused"]


@pytest.mark.parametrize("status", _LIVE)
def test_checkout_bloqueia_cliente_com_assinatura_viva_no_stripe(status) -> None:
    gateway, client, _ = make_gateway()
    client.v1.subscriptions.list.return_value = page([make_subscription(status=status)])
    with pytest.raises(PermanentBillingError) as info:
        gateway.create_checkout(_checkout())
    assert info.value.code == "stripe_subscription_exists"
    client.v1.subscriptions.list.assert_called_once_with(
        params={"customer": "cus_01", "status": "all", "limit": 100},
    )
    client.v1.checkout.sessions.create.assert_not_called()


@pytest.mark.parametrize("status", ["canceled", "incomplete_expired"])
def test_checkout_permite_cliente_com_assinaturas_encerradas(status) -> None:
    gateway, client, _ = make_gateway()
    client.v1.subscriptions.list.return_value = page([make_subscription(status=status)])
    client.v1.checkout.sessions.create.return_value = _session()
    assert gateway.create_checkout(_checkout()).session_id == "cs_01"


def test_checkout_expira_sessoes_abertas_antes_de_criar_nova() -> None:
    gateway, client, _ = make_gateway()
    sessions = client.v1.checkout.sessions
    old = [SimpleNamespace(id="cs_old1"), SimpleNamespace(id="cs_old2")]
    sessions.list.return_value = page(old)
    sessions.create.return_value = _session()
    gateway.create_checkout(_checkout())
    sessions.list.assert_called_once_with(
        params={"customer": "cus_01", "status": "open", "limit": 100},
    )
    names = [call[0] for call in sessions.mock_calls if not call[0].startswith("create.")]
    assert names == ["list", "expire", "expire", "create"]
    assert [c.args for c in sessions.expire.call_args_list] == [("cs_old1",), ("cs_old2",)]


@pytest.mark.parametrize("listing", ["subscriptions", "sessions"])
def test_checkout_falha_fechado_quando_listagem_nao_cabe_em_uma_pagina(listing) -> None:
    gateway, client, _ = make_gateway()
    service = client.v1.subscriptions if listing == "subscriptions" else client.v1.checkout.sessions
    service.list.return_value = page([], has_more=True)
    with pytest.raises(StripeMappingError):
        gateway.create_checkout(_checkout())
    client.v1.checkout.sessions.create.assert_not_called()
    client.v1.checkout.sessions.expire.assert_not_called()


def test_checkout_nao_consulta_stripe_quando_preco_nao_mapeado() -> None:
    gateway, client, _ = make_gateway()
    with pytest.raises(StripeMappingError):
        gateway.create_checkout(_checkout(make_plan(())))
    assert client.v1.mock_calls == []


def test_checkout_repetido_nao_expira_a_propria_sessao_idempotente() -> None:
    gateway, client, _ = make_gateway()
    sessions = client.v1.checkout.sessions
    own = SimpleNamespace(id="cs_01", client_reference_id="req_01")
    other = SimpleNamespace(id="cs_old", client_reference_id="req_00")
    sessions.list.return_value = page([own, other])
    sessions.create.return_value = _session()
    result = gateway.create_checkout(_checkout())
    assert [c.args for c in sessions.expire.call_args_list] == [("cs_old",)]
    assert result.session_id == "cs_01"
