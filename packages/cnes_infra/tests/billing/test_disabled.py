"""Testes do modo local de billing desativado."""

import inspect
from datetime import UTC, datetime, timedelta

import pytest

from cnes_domain.billing.commands import (
    AnalyticsRequest,
    CapacityReservationCommand,
    ConsumeCapacityCommand,
    ConsumeReservationCommand,
    CreateRunRequest,
    ReleaseCapacityCommand,
    ReleaseReservationCommand,
    ReserveAnalyticsCommand,
    ReserveRunCommand,
    SnapshotWrite,
)
from cnes_domain.billing.errors import BillingDisabledError
from cnes_domain.billing.inbox import InboxClaim
from cnes_domain.billing.models import (
    CapacityKind,
    QuotaLimits,
    ReadConsistency,
    ReservationKind,
    ReservationStatus,
    SubscriptionStatus,
)
from cnes_domain.billing.ports import EntitlementProjectionPort, QuotaReservationPort
from cnes_domain.control_plane.entities import RunDependency
from cnes_infra.billing import disabled
from cnes_infra.billing.disabled import (
    DisabledEntitlementProjection,
    DisabledQuotaReservations,
    disabled_snapshot,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

_NOW = datetime(2026, 9, 1, 12, tzinfo=UTC)
_MAX = datetime.max.replace(tzinfo=UTC)
_HASH = "a" * 64


def _run_request() -> CreateRunRequest:
    return CreateRunRequest(
        billing_account_id="local",
        tenant_id="354130",
        run_id="run-1",
        competencia="2026-08",
        dataset_name="cnes",
        dependencies=(RunDependency(source_type="CNES", file_subtype="LOCAL", required=True),),
        idempotency_key="req-1",
        request_hash=_HASH,
        requested_concurrency=8,
        estimated_scan_bytes=10,
    )


def _capacity_command(kind: CapacityKind = CapacityKind.AGENT) -> CapacityReservationCommand:
    return CapacityReservationCommand(
        billing_account_id="local",
        tenant_id="354130",
        resource_id="agent#1",
        kind=kind,
        idempotency_key="cap-1",
        request_hash=_HASH,
        entitlement_version=1,
        limit=None,
    )


def _claim() -> InboxClaim:
    return InboxClaim("evt_1", "invoice.paid", "cus_1", None, 1, acquired=True)


def test_disabled_module_nao_importa_sdk_remoto() -> None:
    source = inspect.getsource(disabled)
    assert "import stripe" not in source
    assert "import boto3" not in source
    assert "botocore" not in source
    assert "secretsmanager" not in source


def test_snapshot_disabled_e_local_sem_medicao() -> None:
    snapshot = disabled_snapshot("local", _NOW)
    assert snapshot.plan_version_id == "local-unmetered-v1"
    assert snapshot.subscription_status is SubscriptionStatus.ACTIVE
    assert snapshot.features == frozenset({"*"})
    assert snapshot.quotas == QuotaLimits(None, None, None, None, None, None)
    assert (snapshot.period_start, snapshot.updated_at) == (_NOW, _NOW)
    assert (snapshot.period_end, snapshot.valid_until) == (_MAX, _MAX)
    assert snapshot.grace_until is None
    assert snapshot.stripe_subscription_id is None
    assert not snapshot.cancel_at_period_end
    assert snapshot.entitlement_version == 1
    assert snapshot.source_event_id == "local-disabled"


@pytest.mark.parametrize("consistency", list(ReadConsistency))
def test_projecao_devolve_snapshot_local_para_qualquer_conta(
    consistency: ReadConsistency,
) -> None:
    clock = MutableClock(_NOW)
    projection = DisabledEntitlementProjection(clock.now)
    clock.advance(timedelta(hours=1))
    snapshot = projection.get_snapshot("ba-qualquer", consistency)
    assert snapshot == disabled_snapshot("ba-qualquer", _NOW + timedelta(hours=1))


def test_projecao_rejeita_conta_vazia() -> None:
    projection = DisabledEntitlementProjection(MutableClock(_NOW).now)
    with pytest.raises(ValueError, match="blank_value"):
        projection.get_snapshot(" ", ReadConsistency.STRONG)


def test_escrita_de_snapshot_levanta_billing_disabled() -> None:
    projection = DisabledEntitlementProjection(MutableClock(_NOW).now)
    write = SnapshotWrite(0, disabled_snapshot("local", _NOW), ())
    message = "billing_mode=disabled operation=write_snapshot"
    with pytest.raises(BillingDisabledError, match=message):
        projection.compare_and_set_snapshot(write)
    with pytest.raises(BillingDisabledError, match=message):
        projection.commit_claimed_snapshot(_claim(), write)


def test_reserva_de_run_usa_concorrencia_do_deployment_sem_reserva() -> None:
    quotas = DisabledQuotaReservations(MutableClock(_NOW).now)
    command = ReserveRunCommand(_run_request(), disabled_snapshot("local", _NOW), 4, "r-1", _MAX)
    authorization = quotas.reserve_and_create_run(command)
    assert authorization.plan_version_id == "local-unmetered-v1"
    assert authorization.billing_account_id == "local"
    assert authorization.entitlement_version == 1
    assert authorization.max_concurrency == 4
    assert authorization.budget_reservation_id is None
    assert authorization.authorized_at == _NOW
    assert quotas.reserve_and_create_run(command) == authorization


def test_reserva_analytics_nao_cria_reserva() -> None:
    request = AnalyticsRequest("local", "354130", "q-1", "req-1", _HASH, 2048)
    command = ReserveAnalyticsCommand(request, disabled_snapshot("local", _NOW), "r-1", _MAX)
    authorization = DisabledQuotaReservations(MutableClock(_NOW).now).reserve_analytics(command)
    assert authorization.budget_reservation_id is None
    assert authorization.max_scan_bytes == 2048
    assert authorization.entitlement_version == 1


@pytest.mark.parametrize("kind", list(CapacityKind))
def test_capacity_reserva_consumo_e_liberacao_sao_idempotentes(kind: CapacityKind) -> None:
    quotas = DisabledQuotaReservations(MutableClock(_NOW).now)
    reserved = quotas.reserve_capacity(_capacity_command(kind))
    assert quotas.reserve_capacity(_capacity_command(kind)) == reserved
    assert (reserved.kind, reserved.resource_id) == (kind, "agent#1")
    assert reserved.status is ReservationStatus.RESERVED
    consume = ConsumeCapacityCommand("local", reserved.reservation_id, _NOW)
    consumed = quotas.consume_capacity(consume)
    assert quotas.consume_capacity(consume) == consumed
    assert consumed.status is ReservationStatus.CONSUMED
    assert (consumed.kind, consumed.resource_id) == (kind, "agent#1")
    release = ReleaseCapacityCommand("local", reserved.reservation_id, _NOW, "agent_removed")
    released = quotas.release_capacity(release)
    assert quotas.release_capacity(release) == released
    assert released.status is ReservationStatus.RELEASED


@pytest.mark.parametrize("reservation_id", ["cap-externa", "local-capacity#other#x"])
def test_capacity_rejeita_reserva_nao_local(reservation_id: str) -> None:
    quotas = DisabledQuotaReservations(MutableClock(_NOW).now)
    with pytest.raises(ValueError, match="reason=unknown_local_reservation"):
        quotas.consume_capacity(ConsumeCapacityCommand("local", reservation_id, _NOW))


def test_consumo_e_liberacao_de_run_sao_noops_idempotentes() -> None:
    quotas = DisabledQuotaReservations(MutableClock(_NOW).now)
    consume = ConsumeReservationCommand("local", "r-1", 99, _NOW)
    consumed = quotas.consume(consume)
    assert quotas.consume(consume) == consumed
    assert consumed.status is ReservationStatus.CONSUMED
    assert consumed.kind is ReservationKind.RUN
    counters = (
        consumed.reserved_runs,
        consumed.reserved_scan_bytes,
        consumed.consumed_runs,
        consumed.consumed_scan_bytes,
    )
    assert counters == (0, 0, 0, 0)
    release = ReleaseReservationCommand("local", "r-1", _NOW, "run_canceled")
    released = quotas.release(release)
    assert quotas.release(release) == released
    assert released.status is ReservationStatus.RELEASED


def test_adapters_disabled_satisfazem_ports() -> None:
    clock = MutableClock(_NOW).now
    assert isinstance(DisabledEntitlementProjection(clock), EntitlementProjectionPort)
    assert isinstance(DisabledQuotaReservations(clock), QuotaReservationPort)
