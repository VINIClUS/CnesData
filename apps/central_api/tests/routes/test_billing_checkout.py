"""Testes da reserva de checkout pendente antes da chamada ao Stripe."""

from datetime import timedelta
from unittest.mock import Mock, create_autospec

import pytest
from fastapi.testclient import TestClient

from central_api.routes.billing_checkout import (
    CHECKOUT_RESERVATION_TTL,
    MAX_CHECKOUT_RESERVATION_TTL,
    get_checkout_reservation_ttl,
    pending_checkout,
    reservation_expiry,
)
from cnes_domain.billing.commands import (
    PendingCheckout,
    ReleasePendingCheckoutCommand,
    ReservePendingCheckoutCommand,
)
from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.models import BillingAccountStatus, SubscriptionStatus
from cnes_domain.billing.ports import BillingCatalogPort
from cnes_domain.profiles import BillingMode

from .billing_fakes import (
    HEADERS,
    MUTATIONS,
    NOW,
    Env,
    checkout_body,
    make_account,
    make_snapshot,
    post,
)


@pytest.fixture
def env():
    return Env()


@pytest.fixture
def client(env):
    return TestClient(env.app())


KEY = "a" * 64


def sent_key(env):
    return env.gateway.create_checkout.call_args.args[0].idempotency_key


def test_checkout_reserva_antes_de_chamar_o_gateway(client, env):
    order = Mock()
    order.attach_mock(env.catalog.reserve_pending_checkout, "reserve")
    order.attach_mock(env.gateway.create_checkout, "gateway")
    assert post(client, "checkout").status_code == 201
    assert [call[0] for call in order.mock_calls] == ["reserve", "gateway"]
    command = env.catalog.reserve_pending_checkout.call_args.args[0]
    assert command == ReservePendingCheckoutCommand(
        "ba_01", sent_key(env), NOW + timedelta(minutes=15),
    )


def test_checkout_com_reserva_de_outra_chave_retorna_409_checkout_in_progress(client, env):
    env.catalog.reserve_pending_checkout.side_effect = PermanentBillingError("checkout_in_progress")
    response = post(client, "checkout")
    assert response.status_code == 409
    assert response.json() == {"detail": "checkout_in_progress"}
    env.gateway.create_checkout.assert_not_called()
    env.catalog.release_pending_checkout.assert_not_called()
    env.audit.append.assert_not_called()


@pytest.mark.parametrize(
    ("error", "status", "detail"),
    [
        (RetryableBillingError("stripe_price_unmapped"), 409, "plan_price_unmapped"),
        (PermanentBillingError("stripe_subscription_exists"), 409, "subscription_exists"),
    ],
)
def test_checkout_libera_reserva_quando_gateway_recusa_antes_da_sessao(
    client, env, error, status, detail,
):
    env.gateway.create_checkout.side_effect = error
    response = post(client, "checkout")
    assert (response.status_code, response.json()) == (status, {"detail": detail})
    env.catalog.release_pending_checkout.assert_called_once_with(
        ReleasePendingCheckoutCommand("ba_01", sent_key(env)),
    )
    env.audit.append.assert_not_called()


@pytest.mark.parametrize(
    ("error", "status", "detail"),
    [
        (RetryableBillingError("stripe_unavailable"), 503, "stripe_unavailable"),
        (PermanentBillingError("stripe_request_rejected"), 502, "stripe_request_rejected"),
        (BillingDependencyError("dynamodb_unavailable"), 503, "billing_dependency_unavailable"),
    ],
)
def test_checkout_mantem_reserva_quando_sessao_pode_existir(client, env, error, status, detail):
    env.gateway.create_checkout.side_effect = error
    response = post(client, "checkout")
    assert (response.status_code, response.json()) == (status, {"detail": detail})
    env.catalog.release_pending_checkout.assert_not_called()


def test_checkout_mantem_erro_original_quando_liberacao_falha(client, env):
    env.gateway.create_checkout.side_effect = PermanentBillingError("stripe_subscription_exists")
    env.catalog.release_pending_checkout.side_effect = BillingDependencyError("dynamodb_down")
    response = post(client, "checkout")
    assert response.status_code == 409
    assert response.json() == {"detail": "subscription_exists"}
    env.catalog.release_pending_checkout.assert_called_once()


def test_checkout_nao_libera_reserva_apos_sessao_criada(env):
    env.audit.append.side_effect = RuntimeError("audit_down")
    response = TestClient(env.app(), raise_server_exceptions=False).post(
        MUTATIONS["checkout"][0], json=checkout_body(), headers=HEADERS,
    )
    assert response.status_code == 500
    env.gateway.create_checkout.assert_called_once()
    env.catalog.release_pending_checkout.assert_not_called()


def _account_inativa(env):
    env.catalog.get_account.return_value = make_account(
        owner="user-1", status=BillingAccountStatus.CLOSED,
    )


def _plano_ausente(env):
    env.catalog.get_plan.return_value = None


def _assinatura_vigente(env):
    env.projection.get_snapshot.return_value = make_snapshot(SubscriptionStatus.ACTIVE)


def _nao_dono(env):
    env.catalog.get_account.return_value = make_account(owner="user-9")
    env.catalog.get_tenant_link.return_value = None


SETUPS = [_account_inativa, _plano_ausente, _assinatura_vigente, _nao_dono]


@pytest.mark.parametrize("setup", SETUPS)
def test_checkout_nao_reserva_quando_validacao_falha(client, env, setup):
    setup(env)
    assert post(client, "checkout").status_code >= 400
    env.catalog.reserve_pending_checkout.assert_not_called()
    env.catalog.release_pending_checkout.assert_not_called()
    env.gateway.create_checkout.assert_not_called()


def test_checkout_desabilitado_nao_toca_reserva(env):
    response = post(TestClient(env.app(BillingMode.DISABLED)), "checkout")
    assert response.status_code == 404
    assert env.catalog.method_calls == []


def test_checkout_retry_com_mesma_chave_reusa_reserva(client, env):
    assert post(client, "checkout").status_code == 201
    assert post(client, "checkout").status_code == 201
    commands = [c.args[0] for c in env.catalog.reserve_pending_checkout.call_args_list]
    assert len(commands) == 2
    assert commands[0].request_key == commands[1].request_key
    keys = [c.args[0].idempotency_key for c in env.gateway.create_checkout.call_args_list]
    assert keys == [commands[0].request_key] * 2


def test_checkout_usa_ttl_configurado(env):
    app = env.app()
    app.dependency_overrides[get_checkout_reservation_ttl] = lambda: timedelta(hours=2)
    assert post(TestClient(app), "checkout").status_code == 201
    command = env.catalog.reserve_pending_checkout.call_args.args[0]
    assert command.expires_at == NOW + timedelta(hours=2)


def test_checkout_com_ttl_invalido_falha_antes_do_gateway(env):
    app = env.app()
    app.dependency_overrides[get_checkout_reservation_ttl] = lambda: timedelta(0)
    response = TestClient(app, raise_server_exceptions=False).post(
        MUTATIONS["checkout"][0], json=checkout_body(), headers=HEADERS,
    )
    assert response.status_code == 500
    env.catalog.reserve_pending_checkout.assert_not_called()
    env.gateway.create_checkout.assert_not_called()


def test_ttl_padrao_e_de_quinze_minutos():
    assert get_checkout_reservation_ttl() == CHECKOUT_RESERVATION_TTL == timedelta(minutes=15)


INVALID_TTLS = [timedelta(0), timedelta(seconds=-1), timedelta(hours=24, seconds=1)]


@pytest.mark.parametrize("ttl", INVALID_TTLS)
def test_reservation_expiry_rejeita_ttl_fora_do_intervalo(ttl):
    with pytest.raises(ValueError, match="invalid_checkout_reservation_ttl"):
        reservation_expiry(NOW, ttl)


def test_reservation_expiry_aceita_exatamente_24_horas():
    assert reservation_expiry(NOW, MAX_CHECKOUT_RESERVATION_TTL) == NOW + timedelta(hours=24)


@pytest.fixture
def catalog():
    port = create_autospec(BillingCatalogPort, instance=True)
    port.reserve_pending_checkout.return_value = PendingCheckout("ba_01", KEY, NOW, NOW)
    return port


def test_pending_checkout_entrega_reserva_e_nao_libera_no_sucesso(catalog):
    with pending_checkout(catalog, "ba_01", KEY, NOW) as reservation:
        assert reservation.request_key == KEY
    catalog.release_pending_checkout.assert_not_called()


def test_pending_checkout_libera_e_repropaga_recusa_anterior_a_sessao(catalog):
    refused = PermanentBillingError("stripe_subscription_exists")
    with pytest.raises(PermanentBillingError) as raised, pending_checkout(
        catalog, "ba_01", KEY, NOW,
    ):
        raise refused
    assert raised.value is refused
    catalog.release_pending_checkout.assert_called_once_with(
        ReleasePendingCheckoutCommand("ba_01", KEY),
    )


@pytest.mark.parametrize(
    "error",
    [KeyError("boom"), RetryableBillingError("stripe_unavailable"),
     PermanentBillingError("stripe_request_rejected")],
)  # fmt: skip
def test_pending_checkout_nao_libera_em_falha_ambigua(catalog, error):
    with pytest.raises(type(error)), pending_checkout(catalog, "ba_01", KEY, NOW):
        raise error
    catalog.release_pending_checkout.assert_not_called()


def test_pending_checkout_repropaga_original_quando_liberacao_falha(catalog, caplog):
    catalog.release_pending_checkout.side_effect = BillingDependencyError("dynamodb_down")
    refused = RetryableBillingError("stripe_price_unmapped")
    with pytest.raises(RetryableBillingError) as raised, pending_checkout(
        catalog, "ba_01", KEY, NOW,
    ):
        raise refused
    assert raised.value is refused
    assert "billing_checkout_release_failed code=dynamodb_down" in caplog.text


def test_pending_checkout_nao_libera_quando_reserva_falha(catalog):
    catalog.reserve_pending_checkout.side_effect = PermanentBillingError("checkout_in_progress")
    with pytest.raises(PermanentBillingError), pending_checkout(catalog, "ba_01", KEY, NOW):
        pytest.fail("corpo nao deve executar")
    catalog.release_pending_checkout.assert_not_called()
