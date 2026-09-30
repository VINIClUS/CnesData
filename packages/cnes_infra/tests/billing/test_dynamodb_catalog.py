"""Testes do catálogo DynamoDB de contas e links de tenant."""

from dataclasses import replace
from datetime import timedelta
from typing import Any
from unittest.mock import Mock

import pytest
from botocore.exceptions import ClientError

from cnes_domain.billing.errors import (
    BillingDependencyError,
    BillingTenantConflict,
    IdempotencyConflict,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.models import BillingAccountStatus, ReadConsistency
from cnes_domain.billing.ports import BillingCatalogPort
from cnes_domain.outbox_dispatcher import dispatch_once
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_items import (
    encode_account,
    encode_link,
)
from cnes_infra.billing.keys import (
    account_tenant_key,
    tenant_account_key,
)
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    make_account,
    make_create_command,
    make_link,
    make_link_command,
    table_items,
)
from packages.cnes_infra.tests.billing.dynamodb_catalog_support import (
    CREATE_SCOPE,
    LINK_SCOPE,
    ListSink,
    catalog_env,
    failing,
    get_stored,
    idem,
    put,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import ClientSpy


@pytest.fixture
def env() -> Any:
    with catalog_env() as value:
        yield value


def test_catalogo_satisfaz_a_porta_de_catalogo(env: Any) -> None:
    _, _, catalog = env

    assert isinstance(catalog, BillingCatalogPort)


def test_link_critico_usa_chave_base_e_leitura_forte() -> None:
    client = Mock()
    client.get_item.return_value = {}
    catalog = DynamoBillingCatalog(client, TABLE_NAME, MutableClock(NOW).now)

    link = catalog.get_tenant_link("ba_01", "tenant-a", ReadConsistency.STRONG)

    client.get_item.assert_called_once_with(
        TableName=TABLE_NAME,
        Key=item_key(*account_tenant_key("ba_01", "tenant-a")),
        ConsistentRead=True,
    )
    assert link is None


def test_link_eventual_usa_leitura_nao_consistente() -> None:
    client = Mock()
    client.get_item.return_value = {}
    catalog = DynamoBillingCatalog(client, TABLE_NAME, MutableClock(NOW).now)

    catalog.get_tenant_link("ba_01", "tenant-a", ReadConsistency.EVENTUAL)

    assert client.get_item.call_args.kwargs["ConsistentRead"] is False


def test_erro_de_storage_no_link_vira_dependencia() -> None:
    client = Mock()
    client.get_item.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "boom"}}, "GetItem"
    )
    catalog = DynamoBillingCatalog(client, TABLE_NAME, MutableClock(NOW).now)

    with pytest.raises(BillingDependencyError):
        catalog.get_tenant_link("ba_01", "tenant-a", ReadConsistency.STRONG)


def test_link_rejeita_ids_divergentes_do_item_lido(env: Any) -> None:
    client, _, catalog = env
    item = encode_link(make_link("ba_01", "tenant-x"))
    item["pk"], item["sk"] = (
        {"S": account_tenant_key("ba_01", "tenant-a")[0]},
        {"S": account_tenant_key("ba_01", "tenant-a")[1]},
    )
    put(client, item)

    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        catalog.get_tenant_link("ba_01", "tenant-a", ReadConsistency.STRONG)


def test_cria_conta_grava_todos_os_itens_atomicamente(env: Any) -> None:
    client, _, catalog = env
    tenants = table_items(client)
    command = make_create_command()

    account = catalog.create_account(command)

    written = [item for item in table_items(client) if item not in tenants]
    assert account == command.account
    assert sorted(item["entity"]["S"] for item in written) == [
        "BILLINGACCOUNT",
        "BILLINGACCOUNTLIST",
        "BILLINGACCOUNTTENANTLINK",
        "IDEMPOTENCYRECORD",
        "OUTBOXEVENT",
        "TENANTBILLINGACCOUNT",
    ]
    assert catalog.get_account("ba_01") == command.account
    link = catalog.get_tenant_link("ba_01", "tenant-a", ReadConsistency.STRONG)
    assert link == command.initial_tenant_link


def test_cria_conta_rejeita_customer_semattach(env: Any) -> None:
    client, _, catalog = env
    before = table_items(client)
    command = replace(make_create_command(), account=make_account(stripe_customer_id="cus_01"))

    with pytest.raises(PermanentBillingError, match="stripe_customer_requires_attach"):
        catalog.create_account(command)

    assert table_items(client) == before


def test_replay_identico_retorna_mesma_conta_sem_novas_escritas(env: Any) -> None:
    client, clock, _ = env
    spy = ClientSpy(client)
    catalog = DynamoBillingCatalog(spy, TABLE_NAME, clock.now)
    command = make_create_command()

    first = catalog.create_account(command)
    after_first = table_items(client)
    second = catalog.create_account(command)

    assert first == second
    assert len(spy.transactions) == 1
    assert table_items(client) == after_first


def test_mesma_chave_com_comando_diferente_gera_conflito_de_idempotencia(env: Any) -> None:
    _, _, catalog = env
    catalog.create_account(make_create_command("ba_01", "tenant-a", "create-01"))

    with pytest.raises(IdempotencyConflict, match="key=create-01"):
        catalog.create_account(make_create_command("ba_02", "tenant-a", "create-01"))


def test_tenant_ja_associado_gera_conflito_sem_residuo(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command("ba_01", "tenant-a", "create-01"))
    before = table_items(client)

    with pytest.raises(BillingTenantConflict, match="tenant_id=tenant-a"):
        catalog.create_account(make_create_command("ba_02", "tenant-a", "create-02"))

    assert table_items(client) == before


def test_tenant_inexistente_falha_sem_residuo(env: Any) -> None:
    client, _, catalog = env
    before = table_items(client)

    with pytest.raises(PermanentBillingError, match="tenant_missing"):
        catalog.create_account(make_create_command("ba_01", "tenant-zzz"))

    assert table_items(client) == before


def test_registro_de_idempotencia_expirado_e_substituido(env: Any) -> None:
    client, _, catalog = env
    command = make_create_command()
    expired = idem(
        "tenant-a",
        CREATE_SCOPE,
        command,
        "ba_old",
        request_hash="c" * 64,
        created_at=NOW - timedelta(days=3),
        expires_at=NOW - timedelta(days=2),
    )
    put(client, expired)

    account = catalog.create_account(command)

    stored = get_stored(client, (expired["pk"]["S"], expired["sk"]["S"]))
    assert account == command.account
    assert stored is not None
    assert stored["payload"] != expired["payload"]


def test_conta_ja_existente_gera_erro_permanente(env: Any) -> None:
    client, _, catalog = env
    put(client, encode_account(make_account()))

    with pytest.raises(PermanentBillingError, match="billing_account_exists"):
        catalog.create_account(make_create_command())


def test_cancelamento_inesperado_na_criacao_e_retryable(env: Any) -> None:
    client, clock, _ = env

    with pytest.raises(RetryableBillingError, match="billing_transaction_conflict"):
        failing(client, clock).create_account(make_create_command())


def test_replay_com_conta_ausente_e_incompleto(env: Any) -> None:
    client, _, catalog = env
    command = make_create_command()
    put(client, idem("tenant-a", CREATE_SCOPE, command, "ba_01"))

    with pytest.raises(RetryableBillingError, match="billing_idempotency_incomplete"):
        catalog.create_account(command)


def test_corrida_de_idempotencia_na_criacao_retorna_replay(env: Any) -> None:
    client, clock, _ = env
    command = make_create_command()

    def winner(_: list[dict[str, Any]]) -> None:
        put(client, idem("tenant-a", CREATE_SCOPE, command, "ba_01"))
        put(client, encode_account(command.account))

    spy = ClientSpy(client, before_transaction=winner)
    catalog = DynamoBillingCatalog(spy, TABLE_NAME, clock.now)

    assert catalog.create_account(command) == command.account


def test_corrida_de_idempotencia_com_hash_diferente_gera_conflito(env: Any) -> None:
    client, clock, _ = env
    command = make_create_command()

    def winner(_: list[dict[str, Any]]) -> None:
        put(client, idem("tenant-a", CREATE_SCOPE, command, "ba_01", request_hash="d" * 64))

    spy = ClientSpy(client, before_transaction=winner)
    catalog = DynamoBillingCatalog(spy, TABLE_NAME, clock.now)

    with pytest.raises(IdempotencyConflict, match="key=create-01"):
        catalog.create_account(command)


def test_conta_multi_tenant_aceita_links_distintos(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command("ba_01", "tenant-a"))

    link = catalog.link_tenant(make_link_command("ba_01", "tenant-b"))

    assert link == make_link("ba_01", "tenant-b")
    for tenant in ("tenant-a", "tenant-b"):
        found = catalog.get_tenant_link("ba_01", tenant, ReadConsistency.STRONG)
        assert found is not None
        assert found.tenant_id == tenant
    assert get_stored(client, tenant_account_key("tenant-b")) is not None


def test_link_nao_pode_reassociar_tenant_a_outra_conta(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command("ba_01", "tenant-a", "create-01"))
    catalog.create_account(make_create_command("ba_02", "tenant-c", "create-02"))
    before = table_items(client)

    with pytest.raises(BillingTenantConflict, match="tenant_id=tenant-a"):
        catalog.link_tenant(make_link_command("ba_02", "tenant-a"))

    assert table_items(client) == before


def test_link_de_conta_inexistente_falha_sem_residuo(env: Any) -> None:
    client, _, catalog = env
    before = table_items(client)

    with pytest.raises(PermanentBillingError, match="billing_account_missing"):
        catalog.link_tenant(make_link_command("ba_99", "tenant-b"))

    assert table_items(client) == before


def test_link_de_conta_inativa_falha_sem_residuo(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command())
    closed = make_account(status=BillingAccountStatus.CLOSED)
    put(client, encode_account(closed))
    before = table_items(client)

    with pytest.raises(PermanentBillingError, match="billing_account_inactive"):
        catalog.link_tenant(make_link_command("ba_01", "tenant-b"))

    assert table_items(client) == before


def test_link_com_conta_desatualizada_falha_sem_residuo(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command())
    before = table_items(client)
    stale = replace(make_link_command(), expected_account_updated_at=NOW - timedelta(hours=1))

    with pytest.raises(PermanentBillingError, match="billing_account_stale"):
        catalog.link_tenant(stale)

    assert table_items(client) == before


def test_link_de_tenant_inexistente_falha_sem_residuo(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command())
    before = table_items(client)

    with pytest.raises(PermanentBillingError, match="tenant_missing"):
        catalog.link_tenant(make_link_command("ba_01", "tenant-zzz"))

    assert table_items(client) == before


def test_replay_de_link_nao_escreve_de_novo(env: Any) -> None:
    client, clock, _ = env
    spy = ClientSpy(client)
    catalog = DynamoBillingCatalog(spy, TABLE_NAME, clock.now)
    catalog.create_account(make_create_command())
    command = make_link_command()

    first = catalog.link_tenant(command)
    second = catalog.link_tenant(command)

    assert first == second
    assert len(spy.transactions) == 2


def test_link_com_mesma_chave_e_comando_diferente_gera_conflito(env: Any) -> None:
    _, _, catalog = env
    catalog.create_account(make_create_command())
    catalog.link_tenant(make_link_command())
    other = replace(make_link_command(), expected_account_updated_at=NOW + timedelta(hours=1))

    with pytest.raises(IdempotencyConflict, match="key=link-01"):
        catalog.link_tenant(other)


def test_replay_de_link_sem_item_e_incompleto(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command())
    command = make_link_command()
    put(client, idem("tenant-b", LINK_SCOPE, command, "ba_01"))

    with pytest.raises(RetryableBillingError, match="billing_idempotency_incomplete"):
        catalog.link_tenant(command)


def test_cancelamento_inesperado_no_link_e_retryable(env: Any) -> None:
    client, clock, catalog = env
    catalog.create_account(make_create_command())

    with pytest.raises(RetryableBillingError, match="billing_transaction_conflict"):
        failing(client, clock).link_tenant(make_link_command())


def test_corrida_de_idempotencia_no_link_retorna_replay(env: Any) -> None:
    client, clock, catalog = env
    catalog.create_account(make_create_command())
    command = make_link_command()

    def winner(_: list[dict[str, Any]]) -> None:
        put(client, idem("tenant-b", LINK_SCOPE, command, "ba_01"))
        put(client, encode_link(command.link))

    spy = ClientSpy(client, before_transaction=winner)
    racing = DynamoBillingCatalog(spy, TABLE_NAME, clock.now)

    assert racing.link_tenant(command) == command.link


def test_outbox_de_escopo_de_conta_e_entregue_por_dispatch_once(env: Any) -> None:
    client, clock, catalog = env
    catalog.create_account(make_create_command())
    catalog.link_tenant(make_link_command())
    sink = ListSink()
    plane = DynamoDBControlPlane(client, TABLE_NAME, clock.now)

    first = dispatch_once(plane, sink, NOW)
    second = dispatch_once(plane, sink, NOW)

    delivered = {event.event_type: event for event in sink.events}
    assert (first.delivered, second.delivered) == (2, 0)
    assert set(delivered) == {"billing_account.created", "billing_account.tenant_linked"}
    assert {event.tenant_id for event in sink.events} == {"_billing"}
    assert delivered["billing_account.created"].payload["attributes"] == {"tenant_id": "tenant-a"}
    assert delivered["billing_account.tenant_linked"].payload["attributes"] == {
        "tenant_id": "tenant-b"
    }


def test_get_account_inexistente_retorna_none(env: Any) -> None:
    _, _, catalog = env

    assert catalog.get_account("ba_99") is None
