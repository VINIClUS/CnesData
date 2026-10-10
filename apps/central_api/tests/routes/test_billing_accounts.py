"""Testes da criação e transferência de contas de billing."""

import pytest
from fastapi.testclient import TestClient

from cnes_domain.billing.errors import IdempotencyConflict
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


def test_usuario_sem_tenant_cria_conta_propria_sem_link_inicial(client, env):
    prepare_creation(env)
    response = post(client, "accounts", headers={})
    assert response.status_code == 201
    env.authorizer.authorize.assert_not_called()
    command = env.catalog.create_account.call_args.args[0]
    assert command.initial_tenant_link is None
    assert command.account.owner_user_id == "user-1"
    assert command.account.billing_account_id.startswith("ba_")
    assert command.idempotency_key == command.account.billing_account_id
    env.catalog.attach_customer.assert_called_once()


def test_conta_sem_tenant_nao_colide_com_conta_de_tenant(client, env):
    prepare_creation(env)
    post(client, "accounts", headers={})
    post(client, "accounts")
    calls = env.catalog.create_account.call_args_list
    first, second = (call.args[0].account.billing_account_id for call in calls)
    assert first != second


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
