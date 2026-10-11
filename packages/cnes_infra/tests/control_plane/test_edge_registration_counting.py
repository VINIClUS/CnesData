"""Contagem de agentes Edge novos em off/shadow no contador de capacidade da conta."""

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.billing.models import BillingEnforcementMode
from cnes_domain.control_plane.entities import Agent
from cnes_domain.control_plane.enums import AgentState
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.keys import capacity_usage_key, pending_capacity_key
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    make_create_command,
    put_tenant,
)
from packages.cnes_infra.tests.billing.dynamodb_catalog_support import get_stored
from packages.cnes_infra.tests.contracts.clock import MutableClock

LINKED = "tenant-a"
UNLINKED = "tenant-b"
FINGERPRINT = "a" * 64
OFF = BillingSettings(BillingMode.STRIPE, BillingEnforcementMode.OFF, 60)
SHADOW = BillingSettings(BillingMode.STRIPE, BillingEnforcementMode.SHADOW, 60)
ENFORCE = BillingSettings(BillingMode.STRIPE, BillingEnforcementMode.ENFORCE, 60)
DISABLED = BillingSettings(BillingMode.DISABLED, BillingEnforcementMode.OFF, 60)
COUNTING = pytest.mark.parametrize("settings", [OFF, SHADOW], ids=["off", "shadow"])


def _cancelled(code: str) -> ClientError:
    response: Any = {
        "Error": {"Code": "TransactionCanceledException", "Message": "x"},
        "CancellationReasons": [{"Code": "None"}, {"Code": code}],
    }
    return ClientError(response, "TransactWriteItems")


class _HookedClient:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.hook: Callable[[], None] | None = None
        self.transactions = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def transact_write_items(self, **request: Any) -> Any:
        self.transactions += 1
        hook, self.hook = self.hook, None
        if hook is not None:
            hook()
        return self._inner.transact_write_items(**request)


@dataclass(frozen=True, slots=True)
class _Env:
    client: Any
    hooked: _HookedClient
    catalog: DynamoBillingCatalog

    def plane(self, settings: BillingSettings) -> DynamoDBControlPlane:
        return DynamoDBControlPlane(self.hooked, TABLE_NAME, MutableClock(NOW).now, settings)

    def create_account(self, tenant_id: str = LINKED, account: str = "ba_01") -> None:
        self.catalog.create_account(make_create_command(account, tenant_id))

    def agents(self, account: str = "ba_01") -> int | None:
        item = get_stored(self.client, capacity_usage_key(account))
        return None if item is None else int(item["agent_count"]["N"])

    def pending(self, tenant_id: str = UNLINKED) -> int | None:
        item = get_stored(self.client, pending_capacity_key(tenant_id))
        return None if item is None else int(item["agent_count"]["N"])


@pytest.fixture
def env() -> Iterator[_Env]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        for tenant in (LINKED, UNLINKED):
            put_tenant(client, tenant)
        catalog = DynamoBillingCatalog(client, TABLE_NAME, MutableClock(NOW).now)
        opened = _Env(client, _HookedClient(client), catalog)
        opened.create_account()
        yield opened


def _register(plane: DynamoDBControlPlane, tenant_id: str, agent_id: str) -> Agent:
    return plane.register_edge_agent(tenant_id, agent_id, FINGERPRINT, NOW)


@COUNTING
def test_agente_novo_de_tenant_ligado_conta_na_conta(env: _Env, settings: BillingSettings) -> None:
    plane = env.plane(settings)

    _register(plane, LINKED, "agent-1")
    _register(plane, LINKED, "agent-2")

    assert env.agents() == 2
    assert plane.get_agent(LINKED, "agent-1") is not None


@COUNTING
def test_agente_existente_nao_conta_de_novo(env: _Env, settings: BillingSettings) -> None:
    plane = env.plane(settings)
    _register(plane, LINKED, "agent-1")

    plane.register_edge_agent(LINKED, "agent-1", "b" * 64, NOW)

    assert env.agents() == 1


@COUNTING
def test_agente_de_tenant_sem_conta_conta_no_pendente(env: _Env, settings: BillingSettings) -> None:
    plane = env.plane(settings)

    _register(plane, UNLINKED, "agent-1")
    _register(plane, UNLINKED, "agent-2")

    assert env.pending() == 2
    assert env.agents() == 0


@COUNTING
def test_pendente_e_transferido_quando_a_conta_e_criada(
    env: _Env, settings: BillingSettings
) -> None:
    plane = env.plane(settings)
    _register(plane, UNLINKED, "agent-1")

    env.create_account(UNLINKED, "ba_02")
    _register(plane, UNLINKED, "agent-2")

    assert env.agents("ba_02") == 2
    assert env.pending() is None


def test_conta_criada_no_meio_do_registro_faz_o_agente_contar_na_conta(env: _Env) -> None:
    plane = env.plane(SHADOW)
    env.hooked.hook = lambda: env.create_account(UNLINKED, "ba_02")

    _register(plane, UNLINKED, "agent-1")

    assert env.hooked.transactions == 2
    assert env.agents("ba_02") == 1
    assert env.pending() is None


@pytest.mark.parametrize("settings", [ENFORCE, DISABLED], ids=["enforce", "disabled"])
def test_enforce_e_disabled_nao_contam_no_registro(env: _Env, settings: BillingSettings) -> None:
    plane = env.plane(settings)

    _register(plane, LINKED, "agent-1")
    _register(plane, UNLINKED, "agent-2")

    assert env.agents() == 0
    assert env.pending() is None


def test_agente_sintetico_via_put_agent_nao_conta(env: _Env) -> None:
    plane = env.plane(SHADOW)

    plane.put_agent(Agent(
        tenant_id=LINKED, agent_id="system-datasus", state=AgentState.ACTIVE, version="1",
        certificate_fingerprint=FINGERPRINT, last_seen_at=None, created_at=NOW,
    ))

    assert env.agents() == 0


def test_conta_sem_capacidade_semeada_admite_sem_contar(
    env: _Env, caplog: pytest.LogCaptureFixture
) -> None:
    pk, sk = capacity_usage_key("ba_01")
    env.client.delete_item(TableName=TABLE_NAME, Key={"pk": {"S": pk}, "sk": {"S": sk}})
    plane = env.plane(SHADOW)

    with caplog.at_level(logging.WARNING):
        agent = _register(plane, LINKED, "agent-1")

    assert agent.state is AgentState.ACTIVE
    assert env.agents() is None
    assert "capacity_not_seeded" in caplog.text


def test_conflito_transacional_no_contador_e_repetido_e_conta_uma_vez(env: _Env) -> None:
    plane = env.plane(SHADOW)

    def conflict() -> None:
        raise _cancelled("TransactionConflict")

    env.hooked.hook = conflict

    _register(plane, LINKED, "agent-1")

    assert env.hooked.transactions == 2
    assert env.agents() == 1


def test_cancelamento_nao_transacional_propaga(env: _Env) -> None:
    plane = env.plane(SHADOW)

    def throttled() -> None:
        raise _cancelled("ThrottlingError")

    env.hooked.hook = throttled

    with pytest.raises(ClientError):
        _register(plane, LINKED, "agent-1")

    assert env.agents() == 0


def test_capacidade_parcial_sem_agent_count_nao_e_tratada_como_semeada(
    env: _Env, caplog: pytest.LogCaptureFixture
) -> None:
    pk, sk = capacity_usage_key("ba_01")
    env.client.put_item(TableName=TABLE_NAME, Item={
        "pk": {"S": pk}, "sk": {"S": sk}, "entity": {"S": "BILLINGUSAGE"},
        "tenant_count": {"N": "1"},
    })
    plane = env.plane(SHADOW)

    with caplog.at_level(logging.WARNING):
        _register(plane, LINKED, "agent-1")

    item = get_stored(env.client, capacity_usage_key("ba_01"))
    assert item is not None
    assert "agent_count" not in item
    assert "capacity_not_seeded" in caplog.text
