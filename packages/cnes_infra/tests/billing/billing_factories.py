"""Factories compartilhadas pelos testes DynamoDB de billing."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any

from cnes_domain.billing.commands import (
    CreateBillingAccountCommand,
    SnapshotWrite,
)
from cnes_domain.billing.models import (
    BillingAccount,
    BillingAccountStatus,
    BillingAccountTenantLink,
    BillingAuditEvent,
    EntitlementSnapshot,
    PlanVersion,
    QuotaLimits,
    SubscriptionStatus,
)
from cnes_domain.control_plane.entities import Tenant
from cnes_infra.billing.keys import tenant_entity_key
from cnes_infra.control_plane.dynamodb_codec import encode_model
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import (
    _TABLE_NAME,
    _create_table,
)

TABLE_NAME = _TABLE_NAME
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
create_table = _create_table


def make_quotas(max_agents: int = 5) -> QuotaLimits:
    return QuotaLimits(
        max_tenants=3,
        max_agents=max_agents,
        max_runs_per_period=100,
        max_concurrency=2,
        retention_days=365,
        athena_scan_budget_bytes=10**9,
    )


def make_snapshot(
    account_id: str = "ba_01", version: int = 1, **changes: Any
) -> EntitlementSnapshot:
    snapshot = EntitlementSnapshot(
        billing_account_id=account_id,
        stripe_subscription_id="sub_01",
        subscription_status=SubscriptionStatus.ACTIVE,
        cancel_at_period_end=False,
        plan_version_id="plan_v1",
        features=frozenset({"create_run", "serving_access"}),
        quotas=make_quotas(),
        period_start=NOW,
        period_end=NOW + timedelta(days=30),
        grace_until=None,
        valid_until=NOW + timedelta(days=30),
        entitlement_version=version,
        updated_at=NOW,
        source_event_id=f"evt_{version:03d}",
    )
    return replace(snapshot, **changes)


def make_audit(event_id: str = "audit-01", account_id: str = "ba_01") -> BillingAuditEvent:
    return BillingAuditEvent(
        event_id=event_id,
        event_type="entitlement.changed",
        aggregate_id=account_id,
        actor_id="stripe",
        reason_code="webhook_projection",
        occurred_at=NOW,
        attributes={"entitlement_version": 1},
    )


def make_write(
    expected_version: int = 0, account_id: str = "ba_01", audits: tuple[str, ...] = ()
) -> SnapshotWrite:
    return SnapshotWrite(
        expected_version=expected_version,
        snapshot=make_snapshot(account_id, expected_version + 1),
        audit_events=tuple(make_audit(event_id, account_id) for event_id in audits),
    )


def make_account(account_id: str = "ba_01", **changes: Any) -> BillingAccount:
    account = BillingAccount(
        billing_account_id=account_id,
        stripe_customer_id=None,
        owner_user_id="user-owner",
        status=BillingAccountStatus.ACTIVE,
        created_at=NOW,
        updated_at=NOW,
    )
    return replace(account, **changes)


def make_link(account_id: str = "ba_01", tenant_id: str = "tenant-a") -> BillingAccountTenantLink:
    return BillingAccountTenantLink(
        billing_account_id=account_id,
        tenant_id=tenant_id,
        linked_by_user_id="user-owner",
        reason_code="owner_request",
        linked_at=NOW,
    )


def make_create_command(
    account_id: str = "ba_01", tenant_id: str = "tenant-a", key: str = "create-01"
) -> CreateBillingAccountCommand:
    return CreateBillingAccountCommand(
        account=make_account(account_id),
        initial_tenant_link=make_link(account_id, tenant_id),
        idempotency_key=key,
    )


def make_plan(plan_version_id: str = "plan_v1", max_agents: int = 5, **changes: Any) -> PlanVersion:
    plan = PlanVersion(
        plan_version_id=plan_version_id,
        plan_key="basico",
        stripe_product_id="prod_01",
        stripe_price_ids=("price_monthly", "price_yearly"),
        features=frozenset({"create_run"}),
        quotas=make_quotas(max_agents),
        grace_period_days=7,
        effective_from=NOW,
    )
    return replace(plan, **changes)


def put_tenant(client: Any, tenant_id: str) -> None:
    tenant = Tenant(tenant_id=tenant_id, municipality_name="Presidente Epitacio", created_at=NOW)
    item = encode_model(tenant, "TENANT", tenant_entity_key(tenant_id))
    client.put_item(TableName=TABLE_NAME, Item=item)


def table_items(client: Any) -> list[dict[str, Any]]:
    items = client.scan(TableName=TABLE_NAME, ConsistentRead=True)["Items"]
    return sorted(items, key=lambda item: (item["pk"]["S"], item["sk"]["S"]))
