"""Helpers compartilhados pelos testes do catálogo DynamoDB de billing."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from typing import Any

import boto3
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.billing.commands import AttachStripeCustomerCommand, TransferOwnerCommand
from cnes_domain.control_plane.entities import IdempotencyRecord
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog, idempotency_digest
from cnes_infra.billing.dynamodb_items import idempotency_item
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    put_tenant,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import ClientSpy

CREATE_SCOPE = "billing_account.create"
LINK_SCOPE = "billing_account.link_tenant"


def raise_conflict(_: list[dict[str, Any]]) -> None:
    raise ClientError(
        {
            "Error": {"Code": "TransactionCanceledException", "Message": "cancelled"},
            "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
        },
        "TransactWriteItems",
    )


class ListSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    def append(self, event: Any) -> None:
        self.events.append(event)


@contextmanager
def catalog_env() -> Iterator[Any]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        for tenant in ("tenant-a", "tenant-b", "tenant-c"):
            put_tenant(client, tenant)
        clock = MutableClock(NOW)
        yield client, clock, DynamoBillingCatalog(client, TABLE_NAME, clock.now)


def put(client: Any, item: dict[str, Any]) -> None:
    client.put_item(TableName=TABLE_NAME, Item=item)


def get_stored(client: Any, key: tuple[str, str]) -> dict[str, Any] | None:
    response = client.get_item(TableName=TABLE_NAME, Key=item_key(*key), ConsistentRead=True)
    return response.get("Item")


def idem(tenant: str, scope: str, command: Any, resource: str, **changes: Any) -> dict[str, Any]:
    record = IdempotencyRecord(
        tenant_id=tenant,
        scope=scope,
        key=command.idempotency_key,
        request_hash=idempotency_digest(command),
        status="COMPLETED",
        resource_id=resource,
        created_at=NOW,
        expires_at=NOW + timedelta(days=1),
    )
    return idempotency_item(record.model_copy(update=changes))


def failing(client: Any, clock: MutableClock) -> DynamoBillingCatalog:
    spy = ClientSpy(client, before_transaction=raise_conflict)
    return DynamoBillingCatalog(spy, TABLE_NAME, clock.now)


def attach(account_id: str, customer: str, expected: Any = NOW) -> AttachStripeCustomerCommand:
    return AttachStripeCustomerCommand(account_id, customer, expected)


def transfer(new_owner: str = "user-new", expected: str = "user-owner") -> TransferOwnerCommand:
    return TransferOwnerCommand(
        billing_account_id="ba_01",
        expected_owner_user_id=expected,
        new_owner_user_id=new_owner,
        actor_id="admin-01",
        reason_code="owner_change",
        transferred_at=NOW + timedelta(hours=1),
    )
