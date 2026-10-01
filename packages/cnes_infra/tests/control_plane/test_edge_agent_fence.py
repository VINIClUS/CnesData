"""Testes do fence de entitlement na criação de agente Edge novo."""

from collections.abc import Iterator
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest

from cnes_domain.billing.commands import ReleaseCapacityCommand
from cnes_domain.billing.errors import EntitlementDenied, RetryableBillingError
from cnes_domain.billing.models import CapacityKind, ReservationStatus, SubscriptionStatus
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.dynamodb_keys import entity_key, idempotency_key, item_key
from cnes_infra.control_plane.edge_registration import (
    EDGE_AGENT_SCOPE,
    EntitlementFence,
    NewEdgeAgent,
)
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    QuotaEnv,
    make_capacity_command,
    make_quota_snapshot,
    quota_env,
    seed_snapshot,
)

TENANT = "354130"
AGENT = "agent-1"
VERSION = make_quota_snapshot().entitlement_version


_RESERVED: dict[str, str] = {}


def _command(version: int = VERSION, reservation: str | None = None) -> NewEdgeAgent:
    return NewEdgeAgent(
        tenant_id=TENANT, agent_id=AGENT, fingerprint="a" * 64, now=NOW,
        reservation_id=reservation or _RESERVED.get("id", "res-1"),
        fence=EntitlementFence(ACCOUNT, version),
    )


def _reserve(env: QuotaEnv) -> str:
    command = make_capacity_command(
        CapacityKind.AGENT, 5, tenant_id=TENANT, resource_id=AGENT,
        entitlement_version=VERSION,
    )
    return env.repo.reserve_capacity(command).reservation_id


def _reservation_status(env: QuotaEnv) -> ReservationStatus:
    from cnes_infra.billing.dynamodb_quota_items import decode_capacity_reservation
    from cnes_infra.billing.keys import capacity_reservation_key

    item = _stored(env, capacity_reservation_key(ACCOUNT, _RESERVED["id"]))
    return decode_capacity_reservation(item)[0].status


def _stored(env: QuotaEnv, key: tuple[str, str]) -> Any:
    response = env.client.get_item(TableName=TABLE_NAME, Key=item_key(*key))
    return response.get("Item")


def _nothing_written(env: QuotaEnv) -> bool:
    agent = _stored(env, entity_key(TENANT, "AGENT", AGENT))
    marker = _stored(env, idempotency_key(TENANT, EDGE_AGENT_SCOPE, _RESERVED["id"]))
    return agent is None and marker is None


@pytest.fixture
def env() -> Iterator[QuotaEnv]:
    with quota_env() as opened:
        _RESERVED["id"] = _reserve(opened)
        yield opened
    _RESERVED.clear()


def test_fence_com_snapshot_atual_cria_agente(env: QuotaEnv) -> None:
    creation = env.control_plane.create_edge_agent(_command())

    assert creation.created is True
    assert _stored(env, entity_key(TENANT, "AGENT", AGENT)) is not None
    assert _reservation_status(env) is ReservationStatus.CONSUMED


def test_reserva_liberada_antes_do_commit_nega_e_nao_grava(env: QuotaEnv) -> None:
    release = ReleaseCapacityCommand(ACCOUNT, _RESERVED["id"], NOW, "expired")
    env.repo.release_capacity(release)

    with pytest.raises(RetryableBillingError, match="capacity_reservation_expired"):
        env.control_plane.create_edge_agent(_command())

    assert _nothing_written(env)


def test_reserva_vencida_nega_e_nao_grava(env: QuotaEnv) -> None:
    env.clock.advance(timedelta(minutes=16))
    seed_snapshot(env.client, replace(
        make_quota_snapshot(), valid_until=NOW + timedelta(days=30),
    ))

    with pytest.raises(RetryableBillingError, match="capacity_reservation_expired"):
        env.control_plane.create_edge_agent(_command())

    assert _nothing_written(env)


def test_reserva_ausente_nega(env: QuotaEnv) -> None:
    with pytest.raises(RetryableBillingError, match="capacity_reservation_expired"):
        env.control_plane.create_edge_agent(_command(reservation="res-ausente"))


def test_reserva_liberada_na_corrida_com_o_commit_nega(env: QuotaEnv) -> None:
    from cnes_infra.billing.keys import capacity_reservation_key

    plane = env.control_plane
    original = plane._fence_actions

    def release_after_read(command: NewEdgeAgent) -> Any:
        actions = original(command)
        env.repo.release_capacity(ReleaseCapacityCommand(ACCOUNT, _RESERVED["id"], NOW, "x"))
        return actions

    plane._fence_actions = release_after_read
    with pytest.raises(RetryableBillingError, match="capacity_reservation_expired"):
        plane.create_edge_agent(_command())

    assert _nothing_written(env)
    assert _stored(env, capacity_reservation_key(ACCOUNT, _RESERVED["id"])) is not None


@pytest.mark.parametrize("changes", [
    {"subscription_status": SubscriptionStatus.ADMIN_REVOKED, "entitlement_version": 9},
    {"entitlement_version": 9},
])
def test_snapshot_alterado_apos_o_gate_nega_e_nao_grava(
    env: QuotaEnv, changes: dict[str, Any],
) -> None:
    seed_snapshot(env.client, replace(make_quota_snapshot(), **changes))

    with pytest.raises(EntitlementDenied, match="reason=snapshot_changed"):
        env.control_plane.create_edge_agent(_command())

    assert _nothing_written(env)


def test_snapshot_expirado_nega(env: QuotaEnv) -> None:
    past = NOW - timedelta(seconds=1)
    expired = replace(make_quota_snapshot(), valid_until=past, updated_at=past - timedelta(days=1))
    seed_snapshot(env.client, expired)

    with pytest.raises(EntitlementDenied, match="reason=snapshot_changed"):
        env.control_plane.create_edge_agent(_command())

    assert _nothing_written(env)


def test_snapshot_ausente_nega(env: QuotaEnv) -> None:
    from cnes_infra.billing.keys import entitlement_snapshot_key

    env.client.delete_item(
        TableName=TABLE_NAME, Key=item_key(*entitlement_snapshot_key(ACCOUNT)),
    )

    with pytest.raises(EntitlementDenied, match="reason=snapshot_changed"):
        env.control_plane.create_edge_agent(_command())


def test_agente_existente_prevalece_sobre_o_fence(env: QuotaEnv) -> None:
    env.control_plane.register_edge_agent(TENANT, AGENT, "b" * 64, NOW)
    seed_snapshot(env.client, replace(make_quota_snapshot(), entitlement_version=9))

    creation = env.control_plane.create_edge_agent(_command())

    assert creation.created is False


@pytest.mark.parametrize(("fenced", "version"), [(False, 9), (True, VERSION)])
def test_conflito_sem_agente_com_snapshot_intacto_continua_conflict(
    env: QuotaEnv, fenced: bool, version: int,
) -> None:
    command = _command() if fenced else replace(_command(), fence=None)
    seed_snapshot(env.client, replace(make_quota_snapshot(), entitlement_version=version))

    with pytest.raises(Conflict):
        env.control_plane._existing_creation(command, entity_key(TENANT, "AGENT", AGENT))


def test_sqlite_ignora_fence_em_modo_disabled(tmp_path) -> None:
    plane = SQLiteControlPlane(tmp_path / "cp.db", lambda: NOW)
    plane.initialize()

    assert plane.create_edge_agent(_command(version=99)).created is True


def test_expiracao_e_checada_no_relogio_do_adapter_no_commit(env: QuotaEnv) -> None:
    current = make_quota_snapshot()
    expiry = NOW + timedelta(seconds=5)
    seed_snapshot(env.client, replace(current, valid_until=expiry))
    env.clock.advance(timedelta(seconds=10))

    with pytest.raises(EntitlementDenied, match="reason=snapshot_changed"):
        env.control_plane.create_edge_agent(_command())

    assert _nothing_written(env)
