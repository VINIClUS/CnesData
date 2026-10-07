"""Testes do DynamoBillingAudit sobre o outbox canônico (moto)."""

import logging
from collections.abc import Iterator
from typing import Any
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.billing.errors import BillingDependencyError
from cnes_domain.billing.ports import BillingAuditPort
from cnes_domain.control_plane.entities import OutboxEvent
from cnes_infra.billing.audit_outbox import BestEffortBillingAudit, DynamoBillingAudit
from cnes_infra.billing.keys import BILLING_AUDIT_TENANT_ID
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_codec import decode_model
from cnes_infra.control_plane.dynamodb_keys import item_key, outbox_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    make_audit,
)


@pytest.fixture
def client() -> Iterator[Any]:
    with mock_aws():
        dynamo = boto3.client("dynamodb", region_name="us-east-1")
        create_table(dynamo)
        yield dynamo


def _stored(client: Any, event_id: str) -> dict[str, Any] | None:
    key = item_key(*outbox_key(event_id))
    return client.get_item(TableName=TABLE_NAME, Key=key, ConsistentRead=True).get("Item")


def _count_outbox(client: Any) -> int:
    scan = client.scan(TableName=TABLE_NAME)["Items"]
    return sum(1 for item in scan if item.get("entity", {}).get("S") == "OUTBOXEVENT")


def test_append_grava_evento_de_outbox_pendente_no_tenant_billing(client: Any) -> None:
    audit = make_audit("checkout:cs_1")

    DynamoBillingAudit(client, TABLE_NAME).append(audit)

    item = _stored(client, "checkout:cs_1")
    assert item is not None
    assert item["gsi6pk"]["S"] == "OUTBOX#PENDING"
    event = decode_model(item, OutboxEvent)
    assert event.tenant_id == BILLING_AUDIT_TENANT_ID
    assert event.event_type == audit.event_type
    assert event.aggregate_id == audit.aggregate_id
    assert event.created_at == NOW
    assert event.delivered_at is None
    assert event.payload == {
        "actor_id": "stripe",
        "reason_code": "webhook_projection",
        "attributes": {"entitlement_version": 1},
    }


def test_append_aparece_na_listagem_de_outbox_pendente(client: Any) -> None:
    DynamoBillingAudit(client, TABLE_NAME).append(make_audit("checkout:cs_2"))

    control_plane = DynamoDBControlPlane(client, TABLE_NAME, lambda: NOW)

    pending = control_plane.pending_outbox(10)

    assert [event.event_id for event in pending] == ["checkout:cs_2"]


def test_append_repetido_e_idempotente(client: Any) -> None:
    adapter = DynamoBillingAudit(client, TABLE_NAME)
    adapter.append(make_audit("checkout:cs_3"))
    original = _stored(client, "checkout:cs_3")
    changed = make_audit("checkout:cs_3", account_id="ba_other")

    adapter.append(changed)

    assert _stored(client, "checkout:cs_3") == original
    assert _count_outbox(client) == 1


def test_append_repetido_registra_duplicata_sem_atributos(
    client: Any, caplog: pytest.LogCaptureFixture
) -> None:
    adapter = DynamoBillingAudit(client, TABLE_NAME)
    adapter.append(make_audit("checkout:cs_4"))

    with caplog.at_level(logging.INFO, logger="cnes_infra.billing.audit_outbox"):
        adapter.append(make_audit("checkout:cs_4"))

    assert "billing_audit_duplicate event_id=checkout:cs_4" in caplog.text
    assert "entitlement_version" not in caplog.text
    assert "webhook_projection" not in caplog.text


def test_append_propaga_falha_de_storage_como_dependencia_indisponivel() -> None:
    failing = Mock()
    failing.transact_write_items.side_effect = ClientError(
        {"Error": {"Code": "InternalServerError", "Message": "boom"}}, "TransactWriteItems"
    )

    with pytest.raises(BillingDependencyError) as raised:
        DynamoBillingAudit(failing, TABLE_NAME).append(make_audit())

    assert "dynamodb_unavailable" in str(raised.value)


def test_adapter_satisfaz_o_port_de_auditoria() -> None:
    assert isinstance(DynamoBillingAudit(Mock(), "t"), BillingAuditPort)


class _FailingAudit:
    def append(self, event: Any) -> None:
        raise BillingDependencyError("dynamodb_unavailable")


class _SpyAudit:
    def __init__(self) -> None:
        self.events: list[Any] = []

    def append(self, event: Any) -> None:
        self.events.append(event)


class _SpyMetrics:
    def __init__(self) -> None:
        self.emitted: list[Any] = []

    def emit(self, metric: Any) -> None:
        self.emitted.append(metric)


def test_best_effort_delega_ao_audit_interno() -> None:
    inner, metrics = _SpyAudit(), _SpyMetrics()
    audit = make_audit("best-effort-1")

    BestEffortBillingAudit(inner, metrics, lambda: NOW).append(audit)

    assert inner.events == [audit]
    assert metrics.emitted == []


def test_best_effort_engole_billing_error_registra_e_emite_metrica(
    caplog: pytest.LogCaptureFixture,
) -> None:
    metrics = _SpyMetrics()
    best_effort = BestEffortBillingAudit(_FailingAudit(), metrics, lambda: NOW)

    with caplog.at_level(logging.WARNING):
        best_effort.append(make_audit("best-effort-2"))

    [metric] = metrics.emitted
    assert metric.name == "AuditOutboxFailures"
    assert metric.value == 1
    assert dict(metric.dimensions) == {"EventType": "entitlement.changed"}
    assert metric.occurred_at == NOW
    assert [r.getMessage() for r in caplog.records] == [
        "billing_audit_append_failed event_type=entitlement.changed code=dynamodb_unavailable"
    ]
