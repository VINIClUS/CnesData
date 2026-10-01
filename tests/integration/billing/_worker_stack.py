"""Montagem moto da varredura de revogação, reservas de capacidade e queda no fencing."""

from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any

import pytest

from cnes_domain.billing.commands import SnapshotWrite
from cnes_domain.billing.inbox import ReservationRecoveryRequest
from cnes_domain.billing.models import (
    CapacityKind,
    CapacityReservation,
    EntitlementSnapshot,
    ReservationStatus,
)
from cnes_domain.billing.revocation import (
    ImmediateRevocationCommand,
    RevocationPhase,
    RevocationSettings,
)
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.billing.dynamodb_quota_items import (
    decode_capacity_reservation,
    encode_capacity_reservation,
)
from cnes_infra.billing.keys import (
    capacity_reservation_key,
    capacity_usage_key,
    revocation_sweep_cursor_key,
)
from cnes_infra.billing.reconciliation_cursor import DynamoReconciliationCursor
from cnes_infra.billing.revocation_sweep import RevocationSweep, RevocationSweepDependencies
from cnes_infra.control_plane.dynamodb_codec import item_key
from cnes_infra.control_plane.dynamodb_keys import idempotency_key
from cnes_infra.control_plane.edge_registration import EDGE_AGENT_SCOPE
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    make_create_command,
    make_plan,
    put_tenant,
)
from packages.cnes_infra.tests.billing.dynamodb_catalog_support import attach
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    RESERVATION_TTL,
    TENANT,
)
from packages.cnes_infra.tests.billing.revocation_support import RevEnv
from packages.cnes_infra.tests.billing.test_dynamodb_revocation_service import (
    RecordingExecutor,
    ServiceOptions,
    build_service,
    seed_simple_run,
    snapshot_of,
)

PAST_EXPIRY = RESERVATION_TTL + timedelta(minutes=1)
PAGE_OF_ONE = RevocationSettings(run_page_size=1)


class MetricSpy:
    def __init__(self) -> None:
        self.emitted: list[Any] = []

    def emit(self, metric: Any) -> None:
        self.emitted.append(metric)

    def named(self, name: str) -> list[Any]:
        return [metric for metric in self.emitted if metric.name == name]


class CrashOnSecondPage:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.crashes = 1

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def save_revocation_progress(self, expected: Any, replacement: Any) -> bool:
        paging = replacement.phase is RevocationPhase.FENCING and replacement.run_cursor
        if paging and self.crashes > 0:
            self.crashes -= 1
            raise RuntimeError("process_crash")
        return self._inner.save_revocation_progress(expected, replacement)


@dataclass(frozen=True, slots=True)
class Interrupted:
    service: Any
    executor: RecordingExecutor
    catalog: DynamoBillingCatalog
    run_ids: tuple[str, ...]
    fenced: str
    pending: tuple[str, ...]


def seed_catalog(env: RevEnv) -> DynamoBillingCatalog:
    catalog = DynamoBillingCatalog(env.client, env.table, env.clock.now)
    catalog.publish_plan(make_plan())
    put_tenant(env.client, "tenant-0")
    catalog.create_account(make_create_command(ACCOUNT, "tenant-0", "create-0"))
    catalog.attach_customer(attach(ACCOUNT, "cus_1"))
    return catalog


def projection_of(env: RevEnv) -> DynamoEntitlementProjection:
    return DynamoEntitlementProjection(env.client, env.table, env.clock.now)


def rewrite_snapshot(env: RevEnv, **changes: Any) -> EntitlementSnapshot:
    current = snapshot_of(env)
    version = current.entitlement_version + 1
    updated = replace(
        current, entitlement_version=version, source_event_id=f"evt_{version:03d}", **changes
    )
    assert projection_of(env).compare_and_set_snapshot(
        SnapshotWrite(current.entitlement_version, updated, ())
    )
    return updated


def interrupt_admin_revocation(env: RevEnv, run_count: int = 3) -> Interrupted:
    catalog = seed_catalog(env)
    run_ids = tuple(f"run-{number:02d}" for number in range(1, run_count + 1))
    for run_id in run_ids:
        seed_simple_run(env, run_id)
    executor = RecordingExecutor()
    options = ServiceOptions(settings=PAGE_OF_ONE, store=CrashOnSecondPage(env.store))
    service = build_service(env, executor, options)
    with pytest.raises(RuntimeError, match="process_crash"):
        service.revoke(ImmediateRevocationCommand(ACCOUNT, "admin-1", "fraud_confirmed", NOW))
    fenced = fenced_runs(env, run_ids)
    assert len(fenced) == 1
    pending = tuple(run_id for run_id in run_ids if run_id not in fenced)
    return Interrupted(service, executor, catalog, run_ids, fenced[0], pending)


def fenced_runs(env: RevEnv, run_ids: tuple[str, ...]) -> tuple[str, ...]:
    states = {run_id: env.store.get_run_billing_state(TENANT, run_id) for run_id in run_ids}
    return tuple(run_id for run_id, state in states.items() if state.cancel_requested)


def build_sweep(
    env: RevEnv, catalog: DynamoBillingCatalog, service: Any, metrics: Any
) -> RevocationSweep:
    cursor = DynamoReconciliationCursor(
        env.client, env.table, env.clock.now, revocation_sweep_cursor_key()
    )
    return RevocationSweep(
        RevocationSweepDependencies(catalog, cursor, service, metrics, env.clock.now)
    )


def recovery_request(env: RevEnv, limit: int = 50) -> ReservationRecoveryRequest:
    return ReservationRecoveryRequest(now=env.clock.now(), limit=limit, cursor=None)


def capacity_reservation(resource_id: str) -> CapacityReservation:
    return CapacityReservation(
        reservation_id=f"cap-{resource_id}",
        billing_account_id=ACCOUNT,
        resource_id=resource_id,
        kind=CapacityKind.AGENT,
        status=ReservationStatus.RESERVED,
        created_at=NOW,
        expires_at=NOW + RESERVATION_TTL,
    )


def seed_agent_capacity(
    env: RevEnv, owned: tuple[str, ...], orphans: tuple[str, ...]
) -> dict[str, CapacityReservation]:
    usage_pk, usage_sk = capacity_usage_key(ACCOUNT)
    usage = {
        "pk": {"S": usage_pk},
        "sk": {"S": usage_sk},
        "agent_count": {"N": str(len(owned) + len(orphans))},
    }
    env.client.put_item(TableName=env.table, Item=usage)
    reservations = {name: capacity_reservation(name) for name in (*owned, *orphans)}
    for reservation in reservations.values():
        item = encode_capacity_reservation(reservation, TENANT)
        env.client.put_item(TableName=env.table, Item=item)
    for name in owned:
        pk, sk = idempotency_key(TENANT, EDGE_AGENT_SCOPE, reservations[name].reservation_id)
        marker = {"pk": {"S": pk}, "sk": {"S": sk}, "payload": {"S": "{}"}}
        env.client.put_item(TableName=env.table, Item=marker)
    return reservations


def stored_capacity(env: RevEnv, reservation: CapacityReservation) -> CapacityReservation:
    key = capacity_reservation_key(ACCOUNT, reservation.reservation_id)
    item = env.client.get_item(TableName=env.table, Key=item_key(*key), ConsistentRead=True)
    return decode_capacity_reservation(item["Item"])[0]


def agent_count(env: RevEnv) -> int:
    key = capacity_usage_key(ACCOUNT)
    item = env.client.get_item(TableName=env.table, Key=item_key(*key), ConsistentRead=True)
    return int(item["Item"]["agent_count"]["N"])
