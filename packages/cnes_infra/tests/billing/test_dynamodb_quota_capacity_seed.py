"""Testes da guarda estrita de capacidade: contador não semeado falha fechado."""

import logging

import pytest

from cnes_domain.billing.errors import EntitlementDenied, QuotaExceeded
from cnes_domain.billing.models import CapacityKind, RunAuthorization
from cnes_infra.billing.keys import capacity_usage_key, usage_key
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    make_capacity_command,
    make_quota_snapshot,
    make_reserve_command,
    quota_env,
    seed_capacity,
    table_items,
)


@pytest.mark.parametrize("kind", [CapacityKind.AGENT, CapacityKind.TENANT])
@pytest.mark.parametrize("limit", [5, None])
def test_reserva_sem_capacidade_semeada_falha_fechado(
    kind: CapacityKind, limit: int | None, caplog: pytest.LogCaptureFixture
) -> None:
    with quota_env(seeded=False) as env:
        before = table_items(env.client)

        with caplog.at_level(logging.WARNING), pytest.raises(
            EntitlementDenied, match="reason=capacity_not_seeded"
        ):
            env.repo.reserve_capacity(make_capacity_command(kind, limit))

        assert table_items(env.client) == before
    assert "capacity_not_seeded" in caplog.text


def test_reserva_com_contador_do_tipo_ausente_falha_fechado() -> None:
    with quota_env(seeded=False) as env:
        pk, sk = capacity_usage_key(ACCOUNT)
        env.client.put_item(TableName=TABLE_NAME, Item={
            "pk": {"S": pk}, "sk": {"S": sk}, "entity": {"S": "BILLINGUSAGE"},
            "tenant_count": {"N": "1"},
        })

        with pytest.raises(EntitlementDenied, match="reason=capacity_not_seeded"):
            env.repo.reserve_capacity(make_capacity_command(CapacityKind.AGENT))


def test_reserva_com_capacidade_semeada_respeita_o_valor_semeado() -> None:
    with quota_env(seeded=False) as env:
        seed_capacity(env.client, tenants=1, agents=2)

        with pytest.raises(QuotaExceeded, match="max_agents_exceeded limit=2"):
            env.repo.reserve_capacity(make_capacity_command(limit=2))

        assert env.repo.reserve_capacity(make_capacity_command(limit=3)) is not None


def test_primeiro_run_do_periodo_sem_usage_ainda_reserva() -> None:
    snapshot = make_quota_snapshot()
    with quota_env(snapshot, seeded=False) as env:
        key = usage_key(ACCOUNT, snapshot.period_start)
        assert "Item" not in env.client.get_item(TableName=TABLE_NAME, Key=item_key(*key))

        authorization = env.repo.reserve_and_create_run(make_reserve_command(snapshot))

        assert isinstance(authorization, RunAuthorization)
