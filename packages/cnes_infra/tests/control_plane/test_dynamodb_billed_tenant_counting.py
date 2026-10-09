"""Contagem de tenants faturados criados em off/shadow no contador da conta."""

import logging
from collections.abc import Iterator

import pytest

from cnes_domain.billing.models import BillingEnforcementMode
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.keys import capacity_usage_key
from cnes_infra.billing.settings import BillingSettings
from packages.cnes_infra.tests.billing.billing_factories import TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT
from packages.cnes_infra.tests.control_plane.billed_tenant_support import (
    DISABLED,
    NEW,
    STRIPE_OFF,
    Env,
    open_env,
)

SHADOW = BillingSettings(BillingMode.STRIPE, BillingEnforcementMode.SHADOW, 60)


@pytest.fixture(params=[STRIPE_OFF, SHADOW], ids=["stripe_off", "shadow"])
def env(request: pytest.FixtureRequest) -> Iterator[Env]:
    with open_env(request.param) as opened:
        yield opened


def test_tenant_criado_em_off_ou_shadow_soma_no_contador(env: Env) -> None:
    before = env.counter()

    env.plane.create_billed_tenant(env.command("unmetered"))

    assert before == 1
    assert env.counter() == 2


def test_replay_idempotente_nao_conta_duas_vezes(env: Env) -> None:
    command = env.command("unmetered")
    env.plane.create_billed_tenant(command)

    assert env.plane.create_billed_tenant(command) == command.tenant

    assert env.counter() == 2


def test_conta_sem_capacidade_semeada_cria_sem_contar(
    env: Env, caplog: pytest.LogCaptureFixture
) -> None:
    pk, sk = capacity_usage_key(ACCOUNT)
    env.client.delete_item(TableName=TABLE_NAME, Key={"pk": {"S": pk}, "sk": {"S": sk}})

    with caplog.at_level(logging.WARNING):
        created = env.plane.create_billed_tenant(env.command("unmetered"))

    assert env.plane.get_tenant(NEW) == created
    assert env.stored(capacity_usage_key(ACCOUNT)) is None
    assert "capacity_not_seeded" in caplog.text


def test_disabled_nao_toca_o_contador() -> None:
    with open_env(DISABLED) as env:
        env.plane.create_billed_tenant(env.command("unmetered"))

        assert env.counter() == 1
