"""Ambiente moto compartilhado pelos testes de criação de tenant faturado."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import boto3
import pytest
from moto import mock_aws

from cnes_domain.billing.commands import CreateBilledTenantCommand
from cnes_domain.billing.models import (
    CapacityKind,
)
from cnes_domain.control_plane.entities import Tenant
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_quota import DynamoQuotaReservations
from cnes_infra.billing.dynamodb_quota_items import (
    decode_capacity_reservation,
    usage_counter,
)
from cnes_infra.billing.keys import (
    account_tenant_key,
    capacity_reservation_key,
    capacity_usage_key,
    tenant_account_key,
    tenant_entity_key,
)
from cnes_infra.billing.settings import BillingEnforcementMode, BillingSettings
from cnes_infra.control_plane.billed_tenant import (
    TENANT_SCOPE,
)
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import idempotency_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    make_create_command,
    make_link,
    put_tenant,
)
from packages.cnes_infra.tests.billing.dynamodb_catalog_support import (
    get_stored,
)
from packages.cnes_infra.tests.billing.quota_support import (
    ACCOUNT,
    make_capacity_command,
    make_quota_snapshot,
    seed_snapshot,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import ClientSpy

NEW = "tenant-new"
OTHER = "tenant-other"
ENFORCE = BillingSettings(BillingMode.STRIPE, BillingEnforcementMode.ENFORCE, 60)
DISABLED = BillingSettings(BillingMode.DISABLED, BillingEnforcementMode.OFF, 60)
STRIPE_OFF = BillingSettings(BillingMode.STRIPE, BillingEnforcementMode.OFF, 60)
ALL_MODES = pytest.mark.parametrize(
    "settings", [ENFORCE, DISABLED, STRIPE_OFF], ids=["enforce", "disabled", "stripe_off"]
)


@dataclass(slots=True)
class Env:
    client: Any
    clock: MutableClock
    spy: ClientSpy
    plane: DynamoDBControlPlane
    repo: DynamoQuotaReservations

    def reserve(
        self, resource_id: str = NEW, kind: CapacityKind = CapacityKind.TENANT, key: str = "cap"
    ) -> str:
        command = make_capacity_command(
            kind,
            3,
            tenant_id=resource_id,
            resource_id=resource_id,
            idempotency_key=f"{key}-{resource_id}-{kind.value}",
        )
        return self.repo.reserve_capacity(command).reservation_id

    def command(
        self, reservation_id: str, tenant_id: str = NEW, key: str = "bt-01", **changes: Any
    ) -> CreateBilledTenantCommand:
        tenant = Tenant(tenant_id=tenant_id, municipality_name="Epitacio", created_at=NOW)
        command = CreateBilledTenantCommand(
            tenant=tenant.model_copy(update=changes),
            link=make_link(ACCOUNT, tenant_id),
            reservation_id=reservation_id,
            idempotency_key=key,
        )
        return command

    def stored(self, key: tuple[str, str]) -> Any:
        return get_stored(self.client, key)

    def reservation(self, reservation_id: str) -> Any:
        item = self.stored(capacity_reservation_key(ACCOUNT, reservation_id))
        return decode_capacity_reservation(item)[0]

    def counter(self) -> int:
        return usage_counter(self.stored(capacity_usage_key(ACCOUNT)), "tenant_count")

    def outbox_types(self) -> set[str]:
        return {event.event_type for event in self.plane.pending_outbox(100)}


@contextmanager
def open_env(settings: BillingSettings) -> Iterator[Env]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        put_tenant(client, "tenant-a")
        clock = MutableClock(NOW)
        DynamoBillingCatalog(client, TABLE_NAME, clock.now).create_account(
            make_create_command(ACCOUNT, "tenant-a")
        )
        seed_snapshot(client, make_quota_snapshot())
        spy = ClientSpy(client)
        plane = DynamoDBControlPlane(spy, TABLE_NAME, clock.now, settings)
        yield Env(client, clock, spy, plane, DynamoQuotaReservations(client, TABLE_NAME, clock.now))


def before_transaction(env: Env, action: Callable[[], None]) -> None:
    env.spy.before_transaction = lambda _: action()


def assert_nothing_written(env: Env, tenant_id: str = NEW) -> None:
    assert env.stored(tenant_entity_key(tenant_id)) is None
    assert env.stored(account_tenant_key(ACCOUNT, tenant_id)) is None
    assert env.stored(tenant_account_key(tenant_id)) is None
    assert env.stored(idempotency_key(tenant_id, TENANT_SCOPE, "bt-01")) is None
    assert "tenant.created" not in env.outbox_types()
    assert env.plane.get_membership(tenant_id, "user-owner") is None
