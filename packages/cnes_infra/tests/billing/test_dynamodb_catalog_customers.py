"""Testes do catálogo DynamoDB de customers, listagem e owner."""

from datetime import timedelta
from typing import Any
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_items import (
    encode_account,
    encode_customer_map,
)
from cnes_infra.billing.keys import (
    billing_account_key,
    billing_account_list_key,
    stripe_customer_key,
)
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    make_account,
    make_create_command,
    put_tenant,
    table_items,
)
from packages.cnes_infra.tests.billing.dynamodb_catalog_support import (
    attach,
    catalog_env,
    failing,
    get_stored,
    put,
    transfer,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import ClientSpy


@pytest.fixture
def env() -> Any:
    with catalog_env() as value:
        yield value


def test_attach_customer_atualiza_conta_lista_mapa_e_outbox(env: Any) -> None:
    client, clock, catalog = env
    catalog.create_account(make_create_command())
    clock.advance(timedelta(minutes=5))

    account = catalog.attach_customer(attach("ba_01", "cus_01"))

    assert account.stripe_customer_id == "cus_01"
    assert account.updated_at == clock.now()
    assert catalog.get_account("ba_01") == account
    row = get_stored(client, billing_account_list_key("ba_01"))
    assert row is not None
    assert row["stripe_customer_id"] == {"S": "cus_01"}
    assert get_stored(client, stripe_customer_key("cus_01")) == encode_customer_map(
        "ba_01", "cus_01"
    )
    events = [i for i in table_items(client) if i["entity"]["S"] == "OUTBOXEVENT"]
    assert len(events) == 2


def test_attach_customer_repetido_e_idempotente_sem_escrita(env: Any) -> None:
    client, clock, _ = env
    spy = ClientSpy(client)
    catalog = DynamoBillingCatalog(spy, TABLE_NAME, clock.now)
    catalog.create_account(make_create_command())
    attached = catalog.attach_customer(attach("ba_01", "cus_01"))
    transactions = len(spy.transactions)

    again = catalog.attach_customer(attach("ba_01", "cus_01", attached.updated_at))

    assert again == attached
    assert len(spy.transactions) == transactions


def test_attach_customer_em_conta_inexistente_falha(env: Any) -> None:
    _, _, catalog = env

    with pytest.raises(PermanentBillingError, match="billing_account_missing"):
        catalog.attach_customer(attach("ba_99", "cus_01"))


def test_attach_de_outro_customer_a_conta_ja_ligada_falha(env: Any) -> None:
    _, _, catalog = env
    catalog.create_account(make_create_command())
    attached = catalog.attach_customer(attach("ba_01", "cus_01"))

    with pytest.raises(PermanentBillingError, match="stripe_customer_already_attached"):
        catalog.attach_customer(attach("ba_01", "cus_02", attached.updated_at))


def test_attach_com_updated_at_desatualizado_falha(env: Any) -> None:
    _, _, catalog = env
    catalog.create_account(make_create_command())

    with pytest.raises(PermanentBillingError, match="billing_account_stale"):
        catalog.attach_customer(attach("ba_01", "cus_01", NOW - timedelta(hours=1)))


def test_customer_nao_anexa_a_duas_contas_e_nao_deixa_residuo(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command("ba_01", "tenant-a", "create-01"))
    catalog.create_account(make_create_command("ba_02", "tenant-b", "create-02"))
    catalog.attach_customer(attach("ba_01", "cus_01"))
    before = table_items(client)

    with pytest.raises(PermanentBillingError, match="stripe_customer_conflict"):
        catalog.attach_customer(attach("ba_02", "cus_01"))

    assert table_items(client) == before


def test_attach_com_conta_alterada_durante_a_transacao_e_stale(env: Any) -> None:
    client, clock, _ = env
    creator = DynamoBillingCatalog(client, TABLE_NAME, clock.now)
    creator.create_account(make_create_command())

    def mutate(_: list[dict[str, Any]]) -> None:
        put(client, encode_account(make_account(owner_user_id="user-other")))

    spy = ClientSpy(client, before_transaction=mutate)
    catalog = DynamoBillingCatalog(spy, TABLE_NAME, clock.now)

    with pytest.raises(PermanentBillingError, match="billing_account_stale"):
        catalog.attach_customer(attach("ba_01", "cus_01"))


def test_cancelamento_inesperado_no_attach_e_retryable(env: Any) -> None:
    client, clock, catalog = env
    catalog.create_account(make_create_command())

    with pytest.raises(RetryableBillingError, match="billing_transaction_conflict"):
        failing(client, clock).attach_customer(attach("ba_01", "cus_01"))


def test_conta_por_customer_inexistente_retorna_none(env: Any) -> None:
    _, _, catalog = env

    assert catalog.get_account_by_customer("cus_01") is None


def test_conta_por_customer_retorna_conta_ligada(env: Any) -> None:
    _, _, catalog = env
    catalog.create_account(make_create_command())
    attached = catalog.attach_customer(attach("ba_01", "cus_01"))

    assert catalog.get_account_by_customer("cus_01") == attached


def test_conta_por_customer_rejeita_mapa_sem_conta(env: Any) -> None:
    client, _, catalog = env
    put(client, encode_customer_map("ba_ghost", "cus_01"))

    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        catalog.get_account_by_customer("cus_01")


def test_conta_por_customer_rejeita_conta_com_outro_customer(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command())
    put(client, encode_customer_map("ba_01", "cus_01"))

    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        catalog.get_account_by_customer("cus_01")


def _seed_accounts(catalog: DynamoBillingCatalog, client: Any) -> list[str]:
    for index, tenant in enumerate(("tenant-a", "tenant-b", "tenant-c"), start=1):
        put_tenant(client, f"t{index}")
        catalog.create_account(make_create_command(f"ba_0{index}", tenant, f"create-0{index}"))
        catalog.attach_customer(attach(f"ba_0{index}", f"cus_0{index}"))
    put_tenant(client, "tenant-d")
    catalog.create_account(make_create_command("ba_04", "tenant-d", "create-04"))
    return ["ba_01", "ba_02", "ba_03"]


def test_lista_pagina_todas_as_contas_ligadas_em_ordem(env: Any) -> None:
    client, _, catalog = env
    expected = _seed_accounts(catalog, client)

    seen: list[str] = []
    cursor: str | None = None
    for _ in range(10):
        page = catalog.list_stripe_accounts(1, cursor)
        seen.extend(account.billing_account_id for account in page.accounts)
        cursor = page.next_cursor
        if cursor is None:
            break

    assert seen == expected
    assert cursor is None


def test_lista_rejeita_limite_invalido(env: Any) -> None:
    _, _, catalog = env

    for limit in (0, 101, True, "1"):
        with pytest.raises(ValueError, match="limit=invalid"):
            catalog.list_stripe_accounts(limit, None)


def test_lista_reconfirma_conta_base_e_ignora_linha_orfa(env: Any) -> None:
    client, _, catalog = env
    _seed_accounts(catalog, client)
    client.delete_item(TableName=TABLE_NAME, Key=item_key(*billing_account_key("ba_02")))

    page = catalog.list_stripe_accounts(100, None)

    assert [a.billing_account_id for a in page.accounts] == ["ba_01", "ba_03"]
    assert page.next_cursor is None


def test_lista_usa_query_consistente_com_filtro_e_cursor(env: Any) -> None:
    client, clock, _ = env
    spy = ClientSpy(client)
    catalog = DynamoBillingCatalog(spy, TABLE_NAME, clock.now)

    catalog.list_stripe_accounts(5, "ba_01")

    request = spy.query_requests[0]
    assert request["ConsistentRead"] is True
    assert request["Limit"] == 5
    assert request["FilterExpression"] == "attribute_exists(stripe_customer_id)"
    assert request["ExclusiveStartKey"] == item_key(*billing_account_list_key("ba_01"))


def test_erro_de_storage_na_lista_vira_dependencia() -> None:
    client = Mock()
    client.query.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "boom"}}, "Query"
    )
    catalog = DynamoBillingCatalog(client, TABLE_NAME, MutableClock(NOW).now)

    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        catalog.list_stripe_accounts(10, None)


def test_transfere_owner_atualiza_conta_e_emite_evento(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command())

    account = catalog.transfer_owner(transfer())

    assert account.owner_user_id == "user-new"
    assert account.updated_at == NOW + timedelta(hours=1)
    assert catalog.get_account("ba_01") == account
    events = [i for i in table_items(client) if i["entity"]["S"] == "OUTBOXEVENT"]
    assert len(events) == 2


def test_transfere_owner_repetido_e_idempotente(env: Any) -> None:
    client, clock, _ = env
    spy = ClientSpy(client)
    catalog = DynamoBillingCatalog(spy, TABLE_NAME, clock.now)
    catalog.create_account(make_create_command())
    first = catalog.transfer_owner(transfer())
    transactions = len(spy.transactions)

    assert catalog.transfer_owner(transfer()) == first
    assert len(spy.transactions) == transactions


def test_transfere_owner_de_conta_inexistente_falha(env: Any) -> None:
    _, _, catalog = env

    with pytest.raises(PermanentBillingError, match="billing_account_missing"):
        catalog.transfer_owner(transfer())


def test_transfere_owner_com_owner_esperado_divergente_falha(env: Any) -> None:
    _, _, catalog = env
    catalog.create_account(make_create_command())

    with pytest.raises(PermanentBillingError, match="billing_account_owner_mismatch"):
        catalog.transfer_owner(transfer(expected="user-other"))


def test_transfere_owner_com_troca_concorrente_falha_por_owner(env: Any) -> None:
    client, clock, catalog = env
    catalog.create_account(make_create_command())

    def mutate(_: list[dict[str, Any]]) -> None:
        put(client, encode_account(make_account(owner_user_id="user-race")))

    spy = ClientSpy(client, before_transaction=mutate)
    racing = DynamoBillingCatalog(spy, TABLE_NAME, clock.now)

    with pytest.raises(PermanentBillingError, match="billing_account_owner_mismatch"):
        racing.transfer_owner(transfer())


def test_cancelamento_inesperado_na_transferencia_e_retryable(env: Any) -> None:
    client, clock, catalog = env
    catalog.create_account(make_create_command())

    with pytest.raises(RetryableBillingError, match="billing_transaction_conflict"):
        failing(client, clock).transfer_owner(transfer())
