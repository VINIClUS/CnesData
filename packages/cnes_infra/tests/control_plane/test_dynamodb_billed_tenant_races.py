"""Testes de corrida e classificação de falha da criação de tenant faturado."""

from collections.abc import Iterator

import pytest

from cnes_domain.billing.commands import ConsumeCapacityCommand
from cnes_domain.billing.errors import (
    BillingTenantConflict,
    EntitlementDenied,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.models import ReservationStatus
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.control_plane.billed_tenant import TENANT_SCOPE
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import idempotency_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME, make_snapshot
from packages.cnes_infra.tests.billing.dynamodb_catalog_support import raise_conflict
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, seed_snapshot
from packages.cnes_infra.tests.control_plane.billed_tenant_support import (
    ALL_MODES,
    ENFORCE,
    NEW,
    Env,
    assert_nothing_written,
    before_transaction,
    open_env,
)


@pytest.fixture
def enforce_env() -> Iterator[Env]:
    with open_env(ENFORCE) as opened:
        yield opened


def test_snapshot_alterado_nega(enforce_env: Env) -> None:
    reservation_id = enforce_env.reserve()
    before_transaction(
        enforce_env,
        lambda: seed_snapshot(enforce_env.client, make_snapshot(ACCOUNT, version=2)),
    )

    with pytest.raises(EntitlementDenied, match="reason=snapshot_changed"):
        enforce_env.plane.create_billed_tenant(enforce_env.command(reservation_id))

    assert_nothing_written(enforce_env)
    assert enforce_env.reservation(reservation_id).status is ReservationStatus.RESERVED


def test_reserva_consumida_durante_a_transacao_nega(enforce_env: Env) -> None:
    reservation_id = enforce_env.reserve()
    consume = ConsumeCapacityCommand(ACCOUNT, reservation_id, NOW)
    before_transaction(enforce_env, lambda: enforce_env.repo.consume_capacity(consume))

    with pytest.raises(PermanentBillingError, match="capacity_reservation_invalid"):
        enforce_env.plane.create_billed_tenant(enforce_env.command(reservation_id))

    assert_nothing_written(enforce_env)


@ALL_MODES
def test_conflito_sem_causa_identificavel_pede_nova_tentativa(settings: BillingSettings) -> None:
    with open_env(settings) as env:
        reservation_id = env.reserve()
        env.spy.before_transaction = raise_conflict

        with pytest.raises(RetryableBillingError, match="billing_transaction_conflict"):
            env.plane.create_billed_tenant(env.command(reservation_id))

        assert_nothing_written(env)


@ALL_MODES
def test_retry_concorrente_do_mesmo_pedido_devolve_o_tenant_vencedor(
    settings: BillingSettings,
) -> None:
    with open_env(settings) as env:
        command = env.command(env.reserve())
        rival = DynamoDBControlPlane(env.client, TABLE_NAME, env.clock.now, settings)
        before_transaction(env, lambda: rival.create_billed_tenant(command))

        replayed = env.plane.create_billed_tenant(command)

        assert replayed == command.tenant
        assert env.plane.get_tenant(NEW) == command.tenant


@ALL_MODES
def test_disputa_pelo_mesmo_tenant_tem_exatamente_um_vencedor(settings: BillingSettings) -> None:
    with open_env(settings) as env:
        winner_reservation = env.reserve(key="win")
        loser_reservation = env.reserve(key="lose")
        rival = DynamoDBControlPlane(env.client, TABLE_NAME, env.clock.now, settings)
        winner = env.command(winner_reservation, key="bt-win")
        loser = env.command(loser_reservation, key="bt-lose", municipality_name="Rival")
        before_transaction(env, lambda: rival.create_billed_tenant(winner))

        with pytest.raises(BillingTenantConflict, match=f"tenant_id={NEW}"):
            env.plane.create_billed_tenant(loser)

        assert env.plane.get_tenant(NEW) == winner.tenant
        assert env.stored(idempotency_key(NEW, TENANT_SCOPE, "bt-lose")) is None
        consumed = ReservationStatus.CONSUMED if settings.enforced else ReservationStatus.RESERVED
        assert env.reservation(winner_reservation).status is consumed
        assert env.reservation(loser_reservation).status is ReservationStatus.RESERVED
