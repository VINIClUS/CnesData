"""Factories e ambiente compartilhados pelos testes de reservas de quota."""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Any

import boto3
from moto import mock_aws

from cnes_domain.billing.commands import (
    AnalyticsRequest,
    CapacityReservationCommand,
    CreateRunRequest,
    ReserveAnalyticsCommand,
    ReserveRunCommand,
)
from cnes_domain.billing.models import CapacityKind, EntitlementSnapshot, QuotaLimits
from cnes_domain.control_plane.entities import RunDependency
from cnes_infra.billing.dynamodb_items import encode_snapshot
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    make_snapshot,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

ACCOUNT = "ba_01"
TENANT = "354130"
HASH_A = "a" * 64
HASH_B = "b" * 64
RESERVATION_TTL = timedelta(minutes=15)
DEPENDENCIES = (
    RunDependency(source_type="CNES", file_subtype="LFCES", required=True),
    RunDependency(source_type="SIHD", file_subtype="AIH", required=False),
)


@dataclass(frozen=True, slots=True)
class QuotaEnv:
    client: Any
    repo: DynamoQuotaReservations
    control_plane: DynamoDBControlPlane
    clock: MutableClock


def make_limits(**changes: int | None) -> QuotaLimits:
    limits = QuotaLimits(
        max_tenants=3,
        max_agents=5,
        max_runs_per_period=100,
        max_concurrency=8,
        retention_days=365,
        athena_scan_budget_bytes=10**9,
    )
    return replace(limits, **changes)


def make_quota_snapshot(**limits: int | None) -> EntitlementSnapshot:
    return make_snapshot(ACCOUNT, quotas=make_limits(**limits))


def seed_snapshot(client: Any, snapshot: EntitlementSnapshot) -> None:
    client.put_item(TableName=TABLE_NAME, Item=encode_snapshot(snapshot))


@contextmanager
def quota_env(snapshot: EntitlementSnapshot | None = None) -> Iterator[QuotaEnv]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        seed_snapshot(client, snapshot or make_quota_snapshot())
        clock = MutableClock(NOW)
        repo = DynamoQuotaReservations(client, TABLE_NAME, clock.now)
        control_plane = DynamoDBControlPlane(client, TABLE_NAME, clock.now)
        yield QuotaEnv(client, repo, control_plane, clock)


def make_run_request(**changes: Any) -> CreateRunRequest:
    request = CreateRunRequest(
        billing_account_id=ACCOUNT,
        tenant_id=TENANT,
        run_id="run-01",
        competencia="2026-08",
        dataset_name="cnes_vinculos",
        dependencies=DEPENDENCIES,
        idempotency_key="req-01",
        request_hash=HASH_A,
        requested_concurrency=4,
        estimated_scan_bytes=1_000,
    )
    return replace(request, **changes)


def make_reserve_command(
    snapshot: EntitlementSnapshot | None = None,
    deployment_max_concurrency: int = 4,
    **request_changes: Any,
) -> ReserveRunCommand:
    request = make_run_request(**request_changes)
    return ReserveRunCommand(
        request=request,
        snapshot=snapshot or make_quota_snapshot(),
        deployment_max_concurrency=deployment_max_concurrency,
        reservation_id=f"res-{request.run_id}",
        expires_at=NOW + RESERVATION_TTL,
    )


def make_analytics_command(
    snapshot: EntitlementSnapshot | None = None, **request_changes: Any
) -> ReserveAnalyticsCommand:
    request = AnalyticsRequest(
        billing_account_id=ACCOUNT,
        tenant_id=TENANT,
        query_id="query-01",
        idempotency_key="aq-01",
        request_hash=HASH_A,
        estimated_scan_bytes=1_000,
    )
    request = replace(request, **request_changes)
    return ReserveAnalyticsCommand(
        request=request,
        snapshot=snapshot or make_quota_snapshot(),
        reservation_id=f"res-{request.query_id}",
        expires_at=NOW + RESERVATION_TTL,
    )


def make_capacity_command(
    kind: CapacityKind = CapacityKind.AGENT, limit: int | None = 5, **changes: Any
) -> CapacityReservationCommand:
    command = CapacityReservationCommand(
        billing_account_id=ACCOUNT,
        tenant_id=TENANT,
        resource_id="agent-01",
        kind=kind,
        idempotency_key="cap-01",
        request_hash=HASH_A,
        entitlement_version=1,
        limit=limit,
    )
    return replace(command, **changes)


def table_items(client: Any) -> list[dict[str, Any]]:
    return client.scan(TableName=TABLE_NAME, ConsistentRead=True)["Items"]
