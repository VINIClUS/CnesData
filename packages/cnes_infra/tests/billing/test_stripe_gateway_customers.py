"""Testes da recuperacao de Customer orfao no StripeGateway (busca por metadata)."""

import logging
from types import SimpleNamespace

import pytest

from cnes_domain.billing.commands import CreateStripeCustomerCommand, StripeCustomer
from cnes_domain.billing.errors import RetryableBillingError
from cnes_infra.billing.stripe_gateway import StripeMappingError
from packages.cnes_infra.tests.billing.stripe_fakes import make_customer, make_gateway, page

QUERY = "metadata['billing_account_id']:'ba_01'"


class StripeError(Exception):
    def __init__(self, http_status: int | None = None) -> None:
        super().__init__("message")
        self.http_status = http_status


class RateLimitError(StripeError):
    pass


def _command(account_id: str = "ba_01") -> CreateStripeCustomerCommand:
    return CreateStripeCustomerCommand(account_id, account_id)


def _created(customer_id: str = "cus_new") -> SimpleNamespace:
    return SimpleNamespace(id=customer_id)


def test_busca_customer_por_metadata_exata_antes_de_criar() -> None:
    gateway, client, _ = make_gateway()
    client.v1.customers.create.return_value = _created()

    assert gateway.create_customer(_command()) == StripeCustomer("cus_new")

    client.v1.customers.search.assert_called_once_with(params={"query": QUERY, "limit": 100})
    assert [call[0] for call in client.v1.customers.mock_calls] == ["search", "create"]
    client.v1.customers.create.assert_called_once_with(
        params={"metadata": {"billing_account_id": "ba_01"}},
        options={"idempotency_key": "customer:ba_01"},
    )


def test_customer_orfao_da_conta_e_reutilizado_sem_criar_outro() -> None:
    gateway, client, _ = make_gateway()
    client.v1.customers.search.return_value = page([make_customer("cus_orphan")])

    assert gateway.create_customer(_command()) == StripeCustomer("cus_orphan")

    client.v1.customers.create.assert_not_called()


def test_customers_duplicados_reutiliza_o_mais_antigo_e_registra_contagem(caplog) -> None:
    caplog.set_level(logging.WARNING)
    gateway, client, _ = make_gateway()
    client.v1.customers.search.return_value = page([
        make_customer("cus_c", created=200),
        make_customer("cus_b", created=100),
        make_customer("cus_a", created=100),
    ])

    assert gateway.create_customer(_command()) == StripeCustomer("cus_a")

    assert caplog.messages == [
        "stripe_customer_duplicates billing_account_id=ba_01 count=3 chosen=cus_a",
    ]
    client.v1.customers.create.assert_not_called()


def test_customer_excluido_nao_e_reutilizado_nem_contado_como_duplicata(caplog) -> None:
    caplog.set_level(logging.WARNING)
    gateway, client, _ = make_gateway()
    client.v1.customers.search.return_value = page([
        make_customer("cus_live", created=200),
        make_customer("cus_deleted", created=100, deleted=True),
    ])

    assert gateway.create_customer(_command()) == StripeCustomer("cus_live")

    assert caplog.messages == []
    client.v1.customers.create.assert_not_called()


def test_apenas_customer_excluido_na_busca_cria_um_novo() -> None:
    gateway, client, _ = make_gateway()
    client.v1.customers.search.return_value = page([make_customer("cus_x", deleted=True)])
    client.v1.customers.create.return_value = _created()

    assert gateway.create_customer(_command()) == StripeCustomer("cus_new")


@pytest.mark.parametrize("account", ["ba_02", "BA_01", None])
def test_busca_ignora_customer_cuja_metadata_nao_e_exatamente_a_conta(account) -> None:
    gateway, client, _ = make_gateway()
    client.v1.customers.search.return_value = page([make_customer("cus_other", account)])
    client.v1.customers.create.return_value = _created()

    assert gateway.create_customer(_command()) == StripeCustomer("cus_new")


def test_busca_com_mais_paginas_falha_fechado_sem_criar() -> None:
    gateway, client, _ = make_gateway()
    client.v1.customers.search.return_value = page([make_customer("cus_a")], has_more=True)

    with pytest.raises(StripeMappingError) as info:
        gateway.create_customer(_command())

    assert info.value.code == "stripe_customers_unbounded"
    client.v1.customers.create.assert_not_called()


@pytest.mark.parametrize("account_id", ["ba_01' OR metadata['x']:'y", "ba 01", "ba_01\\", "bá"])
def test_rejeita_conta_insegura_para_a_query_sem_chamar_stripe(account_id) -> None:
    gateway, client, _ = make_gateway()

    with pytest.raises(ValueError, match="reason=search_value_unsafe field=billing_account_id"):
        gateway.create_customer(_command(account_id))

    assert client.v1.customers.mock_calls == []


def test_falha_transitoria_na_busca_traduz_erro_e_nao_cria_customer() -> None:
    gateway, client, _ = make_gateway()
    client.v1.customers.search.side_effect = RateLimitError(429)

    with pytest.raises(RetryableBillingError) as info:
        gateway.create_customer(_command())

    assert info.value.code == "stripe_unavailable"
    client.v1.customers.create.assert_not_called()
