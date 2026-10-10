"""Testes da semente de capacidade gravada na criação da conta de billing."""

from collections.abc import Callable
from typing import Any

import pytest

from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_infra.billing.capacity_counters import PENDING_CAPACITY_ENTITY
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.keys import (
    billing_account_key,
    capacity_usage_key,
    pending_capacity_key,
    tenant_account_key,
)
from packages.cnes_infra.tests.billing.billing_factories import (
    TABLE_NAME,
    make_create_command,
)
from packages.cnes_infra.tests.billing.dynamodb_catalog_support import (
    catalog_env,
    get_stored,
    put,
)


@pytest.fixture
def env() -> Any:
    with catalog_env() as value:
        yield value


class _HookedClient:
    def __init__(self, inner: Any, hook: Callable[[], None]) -> None:
        self._inner = inner
        self._hook: Callable[[], None] | None = hook

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def transact_write_items(self, **request: Any) -> Any:
        hook, self._hook = self._hook, None
        if hook is not None:
            hook()
        return self._inner.transact_write_items(**request)


def _pending(client: Any, tenant_id: str, agents: int) -> None:
    pk, sk = pending_capacity_key(tenant_id)
    put(client, {
        "pk": {"S": pk}, "sk": {"S": sk}, "entity": {"S": PENDING_CAPACITY_ENTITY},
        "agent_count": {"N": str(agents)},
    })


def _capacity(client: Any) -> dict[str, Any]:
    item = get_stored(client, capacity_usage_key("ba_01"))
    assert item is not None
    return item


def _counters(client: Any) -> tuple[int, int]:
    item = _capacity(client)
    return int(item["tenant_count"]["N"]), int(item["agent_count"]["N"])


def test_cria_conta_semeia_capacidade_contando_o_tenant_inicial(env: Any) -> None:
    client, _, catalog = env

    catalog.create_account(make_create_command())

    assert _capacity(client)["entity"]["S"] == "BILLINGUSAGE"
    assert _counters(client) == (1, 0)


def test_cria_conta_transfere_contador_pendente_do_tenant(env: Any) -> None:
    client, _, catalog = env
    _pending(client, "tenant-a", 2)

    catalog.create_account(make_create_command())

    assert _counters(client) == (1, 2)
    assert get_stored(client, pending_capacity_key("tenant-a")) is None


def test_pendente_de_outro_tenant_nao_e_transferido(env: Any) -> None:
    client, _, catalog = env
    _pending(client, "tenant-b", 4)

    catalog.create_account(make_create_command())

    assert _counters(client) == (1, 0)
    assert get_stored(client, pending_capacity_key("tenant-b")) is not None


def test_pendente_alterado_por_admissao_concorrente_e_retentavel(env: Any) -> None:
    client, clock, _ = env
    _pending(client, "tenant-a", 1)
    hooked = _HookedClient(client, lambda: _pending(client, "tenant-a", 2))
    catalog = DynamoBillingCatalog(hooked, TABLE_NAME, clock.now)

    with pytest.raises(RetryableBillingError, match="billing_transaction_conflict"):
        catalog.create_account(make_create_command())

    assert get_stored(client, billing_account_key("ba_01")) is None
    assert get_stored(client, capacity_usage_key("ba_01")) is None
    assert catalog.create_account(make_create_command()) is not None
    assert _counters(client) == (1, 2)


def test_pendente_criado_por_admissao_concorrente_e_retentavel(env: Any) -> None:
    client, clock, _ = env
    hooked = _HookedClient(client, lambda: _pending(client, "tenant-a", 1))
    catalog = DynamoBillingCatalog(hooked, TABLE_NAME, clock.now)

    with pytest.raises(RetryableBillingError, match="billing_transaction_conflict"):
        catalog.create_account(make_create_command())

    assert get_stored(client, tenant_account_key("tenant-a")) is None
    catalog.create_account(make_create_command())
    assert _counters(client) == (1, 1)
    assert get_stored(client, pending_capacity_key("tenant-a")) is None


def test_replay_da_criacao_nao_ressemeia_capacidade(env: Any) -> None:
    client, _, catalog = env
    catalog.create_account(make_create_command())
    _pending(client, "tenant-a", 3)

    catalog.create_account(make_create_command())

    assert _counters(client) == (1, 0)


def test_capacidade_orfa_sem_conta_falha_permanente(env: Any) -> None:
    client, _, catalog = env
    pk, sk = capacity_usage_key("ba_01")
    put(client, {"pk": {"S": pk}, "sk": {"S": sk}, "entity": {"S": "BILLINGUSAGE"}})

    with pytest.raises(PermanentBillingError, match="capacity_exists"):
        catalog.create_account(make_create_command())

    assert get_stored(client, billing_account_key("ba_01")) is None
