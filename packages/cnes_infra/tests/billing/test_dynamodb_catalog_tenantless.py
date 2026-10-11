"""Testes da criação de conta de billing sem tenant inicial (onboarding)."""

from dataclasses import replace
from typing import Any

import pytest

from cnes_domain.billing.commands import CreateBillingAccountCommand
from cnes_domain.billing.errors import IdempotencyConflict, PermanentBillingError
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_items import decode_idempotency_record, encode_account
from cnes_infra.billing.keys import BILLING_AUDIT_TENANT_ID, capacity_usage_key
from cnes_infra.control_plane.dynamodb_keys import idempotency_key
from packages.cnes_infra.tests.billing.billing_factories import (
    TABLE_NAME,
    make_account,
    make_create_command,
    table_items,
)
from packages.cnes_infra.tests.billing.dynamodb_catalog_support import (
    CREATE_SCOPE,
    catalog_env,
    get_stored,
    put,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import ClientSpy


@pytest.fixture
def env() -> Any:
    with catalog_env() as value:
        yield value


def tenantless(account_id: str = "ba_01", key: str = "create-01") -> CreateBillingAccountCommand:
    return replace(make_create_command(account_id, key=key), initial_tenant_link=None)


def test_cria_conta_sem_tenant_grava_conta_lista_idempotencia_e_outbox(env: Any) -> None:
    client, _, catalog = env
    before = table_items(client)
    command = tenantless()

    account = catalog.create_account(command)

    written = [item for item in table_items(client) if item not in before]
    assert account == command.account
    assert sorted(item["entity"]["S"] for item in written) == [
        "BILLINGACCOUNT",
        "BILLINGACCOUNTLIST",
        "BILLINGUSAGE",
        "IDEMPOTENCYRECORD",
        "OUTBOXEVENT",
    ]
    usage = get_stored(client, capacity_usage_key("ba_01"))
    assert usage is not None
    assert (usage["tenant_count"]["N"], usage["agent_count"]["N"]) == ("0", "0")
    identity = (BILLING_AUDIT_TENANT_ID, CREATE_SCOPE, "create-01")
    stored = get_stored(client, idempotency_key(*identity))
    assert stored is not None
    record = decode_idempotency_record(stored, identity)
    assert record.resource_id == "ba_01"
    assert catalog.get_account("ba_01") == command.account


def test_replay_de_conta_sem_tenant_nao_escreve_de_novo(env: Any) -> None:
    client, clock, _ = env
    spy = ClientSpy(client)
    catalog = DynamoBillingCatalog(spy, TABLE_NAME, clock.now)
    command = tenantless()

    first = catalog.create_account(command)
    after_first = table_items(client)
    second = catalog.create_account(command)

    assert first == second
    assert len(spy.transactions) == 1
    assert table_items(client) == after_first


def test_mesma_chave_sem_tenant_com_conta_diferente_conflita(env: Any) -> None:
    _, _, catalog = env
    catalog.create_account(tenantless("ba_01"))

    with pytest.raises(IdempotencyConflict, match="key=create-01"):
        catalog.create_account(tenantless("ba_02"))


def test_conta_sem_tenant_ja_gravada_com_mesma_identidade_e_replay_tardio(env: Any) -> None:
    client, _, catalog = env
    command = tenantless()
    put(client, encode_account(command.account))

    assert catalog.create_account(command) == command.account


def test_conta_sem_tenant_ja_gravada_por_outro_dono_gera_erro_permanente(env: Any) -> None:
    client, _, catalog = env
    put(client, encode_account(make_account(owner_user_id="user-other")))
    before = table_items(client)

    with pytest.raises(PermanentBillingError, match="billing_account_exists"):
        catalog.create_account(tenantless())

    assert table_items(client) == before
