"""Testes de retries concorrentes e replays tardios do catálogo DynamoDB."""

from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest

from cnes_domain.billing.errors import BillingTenantConflict, PermanentBillingError
from cnes_domain.billing.models import BillingAccountStatus, ReadConsistency
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_items import encode_account, encode_customer_map
from cnes_infra.billing.keys import account_tenant_key
from cnes_infra.control_plane.dynamodb_keys import idempotency_key, item_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    make_account,
    make_create_command,
    make_link_command,
    table_items,
)
from packages.cnes_infra.tests.billing.dynamodb_catalog_support import (
    CREATE_SCOPE,
    attach,
    catalog_env,
    get_stored,
    put,
    transfer,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import ClientSpy


@pytest.fixture
def env() -> Any:
    with catalog_env() as value:
        yield value


def _racing(env: Any, winner: Any) -> DynamoBillingCatalog:
    client, clock, _ = env
    fired: list[bool] = []

    def once(_: list[dict[str, Any]]) -> None:
        if not fired:
            fired.append(True)
            winner()

    return DynamoBillingCatalog(ClientSpy(client, before_transaction=once), TABLE_NAME, clock.now)


def test_attach_concorrente_identico_retorna_conta_anexada(env: Any) -> None:
    _, _, catalog = env
    catalog.create_account(make_create_command())
    racing = _racing(env, lambda: catalog.attach_customer(attach("ba_01", "cus_01")))

    result = racing.attach_customer(attach("ba_01", "cus_01"))

    assert result.stripe_customer_id == "cus_01"
    assert result == catalog.get_account("ba_01")


def test_transferencia_concorrente_identica_retorna_conta_transferida(env: Any) -> None:
    _, _, catalog = env
    catalog.create_account(make_create_command())
    racing = _racing(env, lambda: catalog.transfer_owner(transfer()))

    result = racing.transfer_owner(transfer())

    assert result.owner_user_id == "user-new"
    assert result == catalog.get_account("ba_01")


def test_replay_de_criacao_apos_expirar_idempotencia_retorna_conta(env: Any) -> None:
    client, clock, catalog = env
    command = make_create_command()
    catalog.create_account(command)
    clock.advance(timedelta(days=2))
    before = table_items(client)

    assert catalog.create_account(command) == command.account
    assert table_items(client) == before


def test_retry_com_timestamps_novos_do_servidor_e_replay(env: Any) -> None:
    _, _, catalog = env
    command = make_create_command()
    catalog.create_account(command)
    later = NOW + timedelta(seconds=5)
    retried = replace(
        command,
        account=replace(command.account, created_at=later, updated_at=later),
        initial_tenant_link=replace(command.initial_tenant_link, linked_at=later),
    )

    assert catalog.create_account(retried) == command.account


def test_retry_de_link_com_linked_at_novo_e_replay(env: Any) -> None:
    _, _, catalog = env
    catalog.create_account(make_create_command())
    command = make_link_command()
    catalog.link_tenant(command)
    retried = replace(command, link=replace(command.link, linked_at=NOW + timedelta(seconds=5)))

    result = catalog.link_tenant(retried)

    assert result == catalog.get_tenant_link("ba_01", "tenant-b", ReadConsistency.STRONG)


def test_registro_de_idempotencia_corrompido_e_erro_estavel(env: Any) -> None:
    client, _, catalog = env
    command = make_create_command()
    catalog.create_account(command)
    item = get_stored(client, idempotency_key("tenant-a", CREATE_SCOPE, command.idempotency_key))
    assert item is not None
    put(client, item | {"payload": {"S": "{}"}})

    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        catalog.create_account(command)


def test_registro_de_idempotencia_de_outro_escopo_e_erro_estavel(env: Any) -> None:
    client, _, catalog = env
    command = make_create_command()
    catalog.create_account(command)
    key = idempotency_key("tenant-a", CREATE_SCOPE, command.idempotency_key)
    item = get_stored(client, key)
    assert item is not None
    record = item["payload"]["S"].replace(CREATE_SCOPE, "billing_account.other")
    put(client, item | {"payload": {"S": record}})

    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        catalog.create_account(command)


def test_attach_concorrente_com_mapa_proprio_e_conta_divergente_e_stale(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command())

    def winner() -> None:
        put(client, encode_customer_map("ba_01", "cus_01"))
        put(client, encode_account(make_account(updated_at=NOW + timedelta(hours=1))))

    racing = _racing(env, winner)

    with pytest.raises(PermanentBillingError, match="billing_account_stale"):
        racing.attach_customer(attach("ba_01", "cus_01"))


@pytest.mark.parametrize(
    "change",
    [{"owner_user_id": "user-other"}, {"status": BillingAccountStatus.CLOSED}],
)
def test_replay_tardio_com_conta_alterada_gera_conflito(env: Any, change: Any) -> None:
    client, clock, catalog = env
    command = make_create_command()
    catalog.create_account(command)
    clock.advance(timedelta(days=2))
    changed = replace(command, account=replace(command.account, **change))
    before = table_items(client)

    with pytest.raises(BillingTenantConflict, match="tenant_id=tenant-a"):
        catalog.create_account(changed)
    assert table_items(client) == before


def test_replay_tardio_com_motivo_do_link_alterado_gera_conflito(env: Any) -> None:
    _, clock, catalog = env
    command = make_create_command()
    catalog.create_account(command)
    clock.advance(timedelta(days=2))
    link = replace(command.initial_tenant_link, reason_code="other_reason")

    with pytest.raises(BillingTenantConflict, match="tenant_id=tenant-a"):
        catalog.create_account(replace(command, initial_tenant_link=link))


def test_replay_tardio_sem_link_direto_nao_e_tratado_como_replay(env: Any) -> None:
    client, clock, catalog = env
    command = make_create_command()
    catalog.create_account(command)
    client.delete_item(TableName=TABLE_NAME, Key=item_key(*account_tenant_key("ba_01", "tenant-a")))
    clock.advance(timedelta(days=2))

    with pytest.raises(BillingTenantConflict, match="tenant_id=tenant-a"):
        catalog.create_account(command)


def test_replay_tardio_identico_de_link_retorna_link_existente(env: Any) -> None:
    client, clock, catalog = env
    catalog.create_account(make_create_command())
    command = make_link_command()
    linked = catalog.link_tenant(command)
    clock.advance(timedelta(days=2))
    before = table_items(client)

    assert catalog.link_tenant(command) == linked
    assert table_items(client) == before


def test_replay_tardio_de_link_com_motivo_alterado_gera_conflito(env: Any) -> None:
    _, clock, catalog = env
    catalog.create_account(make_create_command())
    command = make_link_command()
    catalog.link_tenant(command)
    clock.advance(timedelta(days=2))
    changed = replace(command, link=replace(command.link, reason_code="other_reason"))

    with pytest.raises(BillingTenantConflict, match="tenant_id=tenant-b"):
        catalog.link_tenant(changed)


def test_replay_tardio_de_link_com_conta_alterada_gera_conflito(env: Any) -> None:
    client, clock, catalog = env
    catalog.create_account(make_create_command())
    command = make_link_command()
    catalog.link_tenant(command)
    put(client, encode_account(make_account(updated_at=NOW + timedelta(hours=1))))
    clock.advance(timedelta(days=2))

    with pytest.raises(BillingTenantConflict, match="tenant_id=tenant-b"):
        catalog.link_tenant(command)


def test_replay_tardio_de_link_sem_link_direto_gera_conflito(env: Any) -> None:
    client, clock, catalog = env
    catalog.create_account(make_create_command())
    command = make_link_command()
    catalog.link_tenant(command)
    client.delete_item(TableName=TABLE_NAME, Key=item_key(*account_tenant_key("ba_01", "tenant-b")))
    clock.advance(timedelta(days=2))

    with pytest.raises(BillingTenantConflict, match="tenant_id=tenant-b"):
        catalog.link_tenant(command)


def test_transferencia_com_instante_anterior_a_ultima_atualizacao_e_rejeitada(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command())
    later = NOW + timedelta(hours=2)
    put(client, encode_account(make_account(updated_at=later)))
    before = table_items(client)

    with pytest.raises(PermanentBillingError, match="billing_account_stale"):
        catalog.transfer_owner(transfer())
    assert table_items(client) == before
