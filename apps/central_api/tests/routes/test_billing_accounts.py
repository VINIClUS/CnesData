"""Testes da criação e transferência de contas de billing."""

import logging

import pytest
from fastapi.testclient import TestClient

from cnes_domain.billing.commands import CreateStripeCustomerCommand
from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingTenantConflict,
    IdempotencyConflict,
    PermanentBillingError,
)
from cnes_domain.billing.models import ReadConsistency

from .billing_fakes import (
    HEADERS,
    NOW,
    Env,
    make_account,
    make_link,
    make_membership,
    post,
)

ACCOUNTS = "/api/v1/billing/accounts"


@pytest.fixture
def env():
    return Env()


@pytest.fixture
def client(env):
    return TestClient(env.app())


def prepare_creation(env, existing=None, created=None):
    env.catalog.get_account.return_value = existing
    env.catalog.create_account.return_value = created or make_account(customer=None)
    env.catalog.attach_customer.return_value = make_account()


def test_criacao_de_conta_anexa_customer_idempotente(client, env):
    prepare_creation(env)
    response = post(client, "accounts")
    assert response.status_code == 201
    assert response.json()["stripe_customer_id"] == "cus_1"
    env.gateway.create_customer.assert_called_once()
    env.catalog.attach_customer.assert_called_once()
    command = env.catalog.create_account.call_args.args[0]
    assert command.initial_tenant_link.tenant_id == "tenant-a"
    assert command.account.owner_user_id == "user-1"
    assert command.account.billing_account_id.startswith("ba_")
    assert command.idempotency_key == command.account.billing_account_id
    assert command.account.created_at == NOW


def test_replay_de_conta_com_customer_nao_recria_nada(client, env):
    prepare_creation(env, existing=make_account(owner="user-1"))
    response = post(client, "accounts")
    assert response.status_code == 201
    assert response.json()["billing_account_id"] == "ba_01"
    env.catalog.create_account.assert_not_called()
    env.catalog.attach_customer.assert_not_called()
    env.gateway.create_customer.assert_not_called()


def test_replay_de_conta_sem_customer_cria_e_anexa_customer(client, env):
    prepare_creation(env, existing=make_account(owner="user-1", customer=None))
    assert post(client, "accounts").status_code == 201
    env.catalog.create_account.assert_not_called()
    env.gateway.create_customer.assert_called_once()
    attach = env.catalog.attach_customer.call_args.args[0]
    assert attach.stripe_customer_id == "cus_new"
    assert attach.expected_updated_at == NOW


def _attach_conflict(env, code, reread):
    unattached = make_account(owner="user-1", customer=None)
    prepare_creation(env, existing=unattached)
    env.catalog.get_account.side_effect = [unattached, reread]
    env.catalog.attach_customer.side_effect = PermanentBillingError(code)


def test_anexo_concorrente_de_outro_customer_retorna_conta_e_registra_orfao(client, env, caplog):
    caplog.set_level(logging.WARNING)
    _attach_conflict(env, "stripe_customer_already_attached", make_account(owner="user-1"))
    response = post(client, "accounts")
    assert response.status_code == 201
    assert response.json()["stripe_customer_id"] == "cus_1"
    assert (
        "billing_customer_orphaned billing_account_id=ba_01 stripe_customer_id=cus_new"
        in caplog.messages
    )


def test_anexo_com_conta_desatualizada_ja_anexada_ao_mesmo_customer_retorna_201(
    client, env, caplog,
):
    caplog.set_level(logging.WARNING)
    _attach_conflict(env, "billing_account_stale", make_account(owner="user-1", customer="cus_new"))
    response = post(client, "accounts")
    assert response.status_code == 201
    assert response.json()["stripe_customer_id"] == "cus_new"
    assert not any("orphaned" in message for message in caplog.messages)


def test_anexo_com_conta_desatualizada_ainda_sem_customer_pede_retry(client, env):
    unattached = make_account(owner="user-1", customer=None)
    _attach_conflict(env, "billing_account_stale", unattached)
    response = post(client, "accounts")
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "5"
    assert response.json() == {"detail": "billing_dependency_unavailable"}


def test_outro_erro_permanente_do_anexo_nao_e_tratado_como_conflito(client, env):
    prepare_creation(env, existing=make_account(owner="user-1", customer=None))
    env.catalog.attach_customer.side_effect = PermanentBillingError("stripe_customer_conflict")
    assert post(client, "accounts").status_code == 502
    assert env.catalog.get_account.call_count == 1


def test_conta_de_outro_dono_com_mesma_chave_retorna_409(client, env):
    prepare_creation(env, existing=make_account(owner="user-9"))
    response = post(client, "accounts")
    assert response.json() == {"detail": "idempotency_conflict"}
    env.gateway.create_customer.assert_not_called()


def test_conflito_de_idempotencia_do_catalogo_retorna_409(client, env):
    prepare_creation(env)
    env.catalog.create_account.side_effect = IdempotencyConflict("x")
    assert post(client, "accounts").status_code == 409
    env.gateway.create_customer.assert_not_called()


def prepare_recovery(env, recovered, link=None):
    env.catalog.get_account.side_effect = [None, recovered]
    env.catalog.create_account.side_effect = BillingTenantConflict("tenant_id=tenant-a")
    env.catalog.get_tenant_account.return_value = link or make_link("tenant-a")
    env.catalog.attach_customer.return_value = make_account(
        owner=recovered.owner_user_id, customer="cus_new",
    )


def test_chave_nova_de_tenant_vinculado_devolve_conta_existente(client, env, caplog):
    caplog.set_level(logging.INFO)
    prepare_recovery(env, make_account(owner="user-1"))
    response = post(client, "accounts")
    assert response.status_code == 201
    assert response.json()["billing_account_id"] == "ba_01"
    assert response.json()["stripe_customer_id"] == "cus_1"
    env.catalog.get_tenant_account.assert_called_once_with("tenant-a", ReadConsistency.STRONG)
    assert env.catalog.get_account.call_args.args == ("ba_01",)
    env.gateway.create_customer.assert_not_called()
    assert (
        "billing_account_recovered billing_account_id=ba_01 tenant_id=tenant-a"
        in caplog.messages
    )


def test_conta_recuperada_sem_customer_reusa_chave_da_conta_e_anexa(client, env):
    prepare_recovery(env, make_account(owner="user-1", customer=None))
    response = post(client, "accounts")
    assert response.status_code == 201
    assert response.json()["stripe_customer_id"] == "cus_new"
    env.gateway.create_customer.assert_called_once_with(
        CreateStripeCustomerCommand("ba_01", "ba_01"),
    )
    attach = env.catalog.attach_customer.call_args.args[0]
    assert (attach.billing_account_id, attach.stripe_customer_id) == ("ba_01", "cus_new")


def test_gestor_nao_dono_recupera_conta_pelo_link_forte(client, env):
    prepare_recovery(env, make_account(owner="user-9"))
    response = post(client, "accounts")
    assert response.status_code == 201
    assert response.json()["owner_user_id"] == "user-9"
    env.catalog.get_tenant_link.assert_called_once_with("ba_01", "tenant-a", ReadConsistency.STRONG)


@pytest.mark.parametrize("direct_link", [None, make_link("tenant-b")])
def test_conta_recuperada_sem_link_forte_nega_403_sem_chamar_stripe(client, env, direct_link):
    prepare_recovery(env, make_account(owner="user-9", customer=None))
    env.catalog.get_tenant_link.return_value = direct_link
    response = post(client, "accounts")
    assert response.status_code == 403
    assert response.json() == {"detail": "billing_owner_required"}
    env.gateway.create_customer.assert_not_called()
    env.catalog.attach_customer.assert_not_called()


@pytest.mark.parametrize("reverse_link", [None, make_link("tenant-a")])
def test_conflito_sem_conta_recuperavel_mantem_409(client, env, reverse_link):
    prepare_recovery(env, make_account())
    env.catalog.get_account.side_effect = [None, None]
    env.catalog.get_tenant_account.return_value = reverse_link
    response = post(client, "accounts")
    assert response.status_code == 409
    assert response.json() == {"detail": "billing_tenant_conflict"}
    env.catalog.get_tenant_account.assert_called_once_with("tenant-a", ReadConsistency.STRONG)
    env.gateway.create_customer.assert_not_called()


def test_falha_ao_ler_link_reverso_retorna_503(client, env):
    prepare_recovery(env, make_account())
    env.catalog.get_tenant_account.side_effect = BillingDependencyError("dynamodb_unavailable")
    response = post(client, "accounts")
    assert response.status_code == 503
    assert response.json() == {"detail": "billing_dependency_unavailable"}
    env.gateway.create_customer.assert_not_called()


def test_transferencia_grava_novo_dono(client, env):
    env.catalog.get_account.return_value = make_account(owner="user-1")
    env.control_plane.get_membership.return_value = make_membership()
    env.catalog.transfer_owner.return_value = make_account(owner="user-2")
    response = post(client, "transfer")
    assert response.status_code == 200
    assert response.json()["owner_user_id"] == "user-2"
    env.control_plane.get_membership.assert_called_once_with("tenant-a", "user-2")
    env.catalog.get_tenant_link.assert_called_once_with("ba_01", "tenant-a", ReadConsistency.STRONG)
    command = env.catalog.transfer_owner.call_args.args[0]
    assert command.expected_owner_user_id == "user-1"
    assert command.actor_id == "user-1"
    assert command.reason_code == "handover"
    assert command.transferred_at == NOW


def test_transferencia_por_gestor_vinculado_le_link_uma_vez(client, env):
    env.control_plane.get_membership.return_value = make_membership()
    env.catalog.transfer_owner.return_value = make_account(owner="user-2")
    assert post(client, "transfer").status_code == 200
    env.catalog.get_tenant_link.assert_called_once()


@pytest.mark.parametrize(
    "membership",
    [
        None,
        make_membership(role="viewer"),
        make_membership(tenant="tenant-z"),
        make_membership(user="user-z"),
    ],
)
def test_transferencia_rejeita_alvo_invalido(client, env, membership):
    env.control_plane.get_membership.return_value = membership
    response = post(client, "transfer")
    assert response.status_code == 422
    assert response.json() == {"detail": "transfer_target_invalid"}
    env.catalog.transfer_owner.assert_not_called()


def test_transferencia_para_o_mesmo_dono_retorna_409(client, env):
    env.control_plane.get_membership.return_value = make_membership()
    env.catalog.get_account.return_value = make_account(owner="user-2")
    env.catalog.get_tenant_link.return_value = make_link("tenant-a")
    response = post(client, "transfer")
    assert response.status_code == 409
    env.catalog.transfer_owner.assert_not_called()


def test_transferencia_de_tenant_nao_vinculado_retorna_403(client, env):
    env.catalog.get_tenant_link.return_value = None
    assert post(client, "transfer").status_code == 403
    env.catalog.transfer_owner.assert_not_called()


def test_transferencia_pelo_dono_em_tenant_nao_vinculado_retorna_403(client, env):
    env.catalog.get_account.return_value = make_account(owner="user-1")
    env.catalog.get_tenant_link.return_value = None
    assert post(client, "transfer").status_code == 403
    env.catalog.transfer_owner.assert_not_called()


def test_transferencia_sem_tenant_ou_conta_inexistente_e_negada(client, env):
    assert post(client, "transfer", headers={}).status_code == 403
    env.catalog.get_account.return_value = None
    assert post(client, "transfer", headers=HEADERS).status_code == 404
