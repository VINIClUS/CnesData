"""Testes do DynamoEntitlementProjection (BIL-012) sobre moto e cliente low-level."""

from typing import Any
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    RetryableBillingError,
    StaleInboxClaim,
)
from cnes_domain.billing.inbox import InboxClaim
from cnes_domain.billing.models import BillingAuditEvent, ReadConsistency
from cnes_domain.billing.ports import EntitlementProjectionPort
from cnes_domain.outbox_dispatcher import dispatch_once
from cnes_infra.billing.dynamodb_items import utc_attribute
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.billing.keys import (
    BILLING_AUDIT_TENANT_ID,
    entitlement_snapshot_key,
    stripe_event_key,
)
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import item_key, outbox_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    make_snapshot,
    make_write,
    table_items,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

SNAPSHOT_KEY = item_key(*entitlement_snapshot_key("ba_01"))
INBOX_KEY = item_key(*stripe_event_key("evt_01"))
OUTBOX_KEY = item_key(*outbox_key("audit-01"))


class _CollectingSink:
    def __init__(self) -> None:
        self.events: list[Any] = []

    def append(self, event: Any) -> None:
        self.events.append(event)


class _ThrottlingClient:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def transact_write_items(self, **_: Any) -> None:
        raise ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "x"}},
            "TransactWriteItems",
        )


@pytest.fixture
def context():
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        clock = MutableClock(NOW)
        yield client, clock, DynamoEntitlementProjection(client, TABLE_NAME, clock.now)


def _seed_inbox(client: Any, state: str = "processing", attempt: int = 2) -> None:
    client.put_item(
        TableName=TABLE_NAME,
        Item={
            **INBOX_KEY,
            "entity": {"S": "STRIPEEVENTINBOX"},
            "state": {"S": state},
            "attempt": {"N": str(attempt)},
            "lease_until": {"S": "2026-09-30T12:05:00Z"},
            "gsi1pk": {"S": "STRIPE_RECOVERY#DUE"},
            "gsi1sk": {"S": "2026-09-30T12:05:00Z#evt_01"},
        },
    )


def _claim(attempt: int = 2) -> InboxClaim:
    return InboxClaim("evt_01", "customer.subscription.updated", "cus_01", "sub_01", attempt, True)


def _stored(client: Any, key: dict[str, Any]) -> dict[str, Any]:
    return client.get_item(TableName=TABLE_NAME, Key=key, ConsistentRead=True)["Item"]


def test_critical_read_usa_chave_base_e_consistent_read():
    client = Mock()
    client.get_item.return_value = {}
    projection = DynamoEntitlementProjection(client, TABLE_NAME, lambda: NOW)
    assert projection.get_snapshot("ba_01", ReadConsistency.STRONG) is None
    client.get_item.assert_called_once_with(
        TableName=TABLE_NAME, Key=item_key(*entitlement_snapshot_key("ba_01")), ConsistentRead=True
    )


def test_leitura_eventual_usa_consistent_read_falso():
    client = Mock()
    client.get_item.return_value = {}
    projection = DynamoEntitlementProjection(client, TABLE_NAME, lambda: NOW)
    projection.get_snapshot("ba_01", ReadConsistency.EVENTUAL)
    assert client.get_item.call_args.kwargs["ConsistentRead"] is False


def test_satisfaz_a_porta_de_projecao(context):
    assert isinstance(context[2], EntitlementProjectionPort)


def test_snapshot_ausente_retorna_none(context):
    assert context[2].get_snapshot("ba_01", ReadConsistency.STRONG) is None


def test_snapshot_gravado_e_lido_de_volta(context):
    projection = context[2]
    assert projection.compare_and_set_snapshot(make_write(0)) is True
    assert projection.get_snapshot("ba_01", ReadConsistency.STRONG) == make_snapshot()


def test_snapshot_cas_rejeita_versao_concorrente(context):
    client, _, projection = context
    assert projection.compare_and_set_snapshot(make_write(0)) is True
    assert projection.compare_and_set_snapshot(make_write(1)) is True
    before = table_items(client)
    assert projection.compare_and_set_snapshot(make_write(1)) is False
    assert table_items(client) == before


def test_snapshot_cas_rejeita_versao_esperada_sobre_item_ausente(context):
    client, _, projection = context
    assert projection.compare_and_set_snapshot(make_write(4)) is False
    assert table_items(client) == []


def test_cas_grava_exatamente_esperada_mais_um_sem_campos_de_cartao(context):
    client, _, projection = context
    projection.compare_and_set_snapshot(make_write(0))
    item = _stored(client, SNAPSHOT_KEY)
    assert set(item) == {
        "pk",
        "sk",
        "entity",
        "payload",
        "entitlement_version",
        "subscription_status",
        "valid_until",
    }
    assert item["entitlement_version"] == {"N": "1"}


def test_cas_com_auditoria_grava_outbox_pendente_atomicamente(context):
    client, _, projection = context
    assert projection.compare_and_set_snapshot(make_write(0, audits=("audit-01",))) is True
    assert _stored(client, OUTBOX_KEY)["gsi6pk"] == {"S": "OUTBOX#PENDING"}


def test_cas_ambiguo_com_outbox_existente_levanta_retryable_sem_efeitos(context):
    client, _, projection = context
    projection.compare_and_set_snapshot(make_write(0, audits=("audit-01",)))
    before = table_items(client)
    with pytest.raises(RetryableBillingError, match="billing_commit_ambiguous"):
        projection.compare_and_set_snapshot(make_write(1, audits=("audit-01",)))
    assert table_items(client) == before


def test_cas_com_versao_corrompida_levanta_item_corrompido(context):
    client, _, projection = context
    projection.compare_and_set_snapshot(make_write(0))
    item = _stored(client, SNAPSHOT_KEY)
    del item["entitlement_version"]
    client.put_item(TableName=TABLE_NAME, Item=item)
    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        projection.compare_and_set_snapshot(make_write(1))


def test_cas_com_erro_nao_condicional_levanta_dependencia(context):
    client, clock, _ = context
    projection = DynamoEntitlementProjection(_ThrottlingClient(client), TABLE_NAME, clock.now)
    with pytest.raises(BillingDependencyError):
        projection.compare_and_set_snapshot(make_write(0))


def test_commit_claimed_snapshot_rejeita_fence_sem_efeitos(context):
    client, _, projection = context
    _seed_inbox(client, attempt=2)
    before = table_items(client)
    with pytest.raises(StaleInboxClaim, match="inbox_claim_stale"):
        projection.commit_claimed_snapshot(_claim(1), make_write(0, audits=("audit-01",)))
    assert table_items(client) == before


def test_commit_claimed_snapshot_grava_snapshot_inbox_e_auditoria(context):
    client, _, projection = context
    _seed_inbox(client)
    assert projection.commit_claimed_snapshot(_claim(), make_write(0, audits=("audit-01",)))
    inbox = _stored(client, INBOX_KEY)
    assert inbox["state"] == {"S": "processed"}
    assert inbox["attempt"] == {"N": "2"}
    assert inbox["entitlement_version"] == {"N": "1"}
    assert inbox["processed_at"] == {"S": utc_attribute(NOW)}
    for name in ("lease_until", "gsi1pk", "gsi1sk"):
        assert name not in inbox
    assert _stored(client, SNAPSHOT_KEY)["entitlement_version"] == {"N": "1"}
    assert _stored(client, OUTBOX_KEY)["gsi6pk"] == {"S": "OUTBOX#PENDING"}


def test_commit_claimed_snapshot_perde_versao_sem_efeitos(context):
    client, _, projection = context
    _seed_inbox(client)
    projection.compare_and_set_snapshot(make_write(0))
    before = table_items(client)
    assert projection.commit_claimed_snapshot(_claim(), make_write(0)) is False
    assert table_items(client) == before


def test_commit_claimed_snapshot_sem_inbox_levanta_stale(context):
    with pytest.raises(StaleInboxClaim):
        context[2].commit_claimed_snapshot(_claim(), make_write(0))


def test_commit_claimed_snapshot_inbox_fora_de_processing_levanta_stale(context):
    client, _, projection = context
    _seed_inbox(client, state="processed")
    with pytest.raises(StaleInboxClaim):
        projection.commit_claimed_snapshot(_claim(), make_write(0))


def test_commit_claimed_snapshot_inbox_malformado_conta_como_fence_perdido(context):
    client, _, projection = context
    client.put_item(TableName=TABLE_NAME, Item={**INBOX_KEY, "entity": {"S": "STRIPEEVENTINBOX"}})
    with pytest.raises(StaleInboxClaim):
        projection.commit_claimed_snapshot(_claim(), make_write(0))


def test_commit_claimed_snapshot_sem_claim_adquirido_nao_faz_io():
    client = Mock()
    projection = DynamoEntitlementProjection(client, TABLE_NAME, lambda: NOW)
    claim = InboxClaim("evt_01", "customer.subscription.updated", "cus_01", None, None, False)
    with pytest.raises(StaleInboxClaim):
        projection.commit_claimed_snapshot(claim, make_write(0))
    assert client.mock_calls == []


def test_commit_claimed_snapshot_ambiguo_levanta_retryable_sem_efeitos(context):
    client, _, projection = context
    projection.compare_and_set_snapshot(make_write(0, audits=("audit-01",)))
    _seed_inbox(client)
    before = table_items(client)
    with pytest.raises(RetryableBillingError, match="billing_commit_ambiguous"):
        projection.commit_claimed_snapshot(_claim(), make_write(1, audits=("audit-01",)))
    assert table_items(client) == before


def test_commit_claimed_snapshot_com_erro_nao_condicional_levanta_dependencia(context):
    client, clock, _ = context
    _seed_inbox(client)
    projection = DynamoEntitlementProjection(_ThrottlingClient(client), TABLE_NAME, clock.now)
    with pytest.raises(BillingDependencyError):
        projection.commit_claimed_snapshot(_claim(), make_write(0))


def test_auditoria_de_conta_e_entregue_pelo_dispatcher_uma_vez(context):
    client, clock, projection = context
    _seed_inbox(client)
    projection.commit_claimed_snapshot(_claim(), make_write(0, audits=("audit-01",)))
    sink = _CollectingSink()
    plane = DynamoDBControlPlane(client, TABLE_NAME, clock.now)
    assert dispatch_once(plane, sink, NOW).delivered == 1
    event: BillingAuditEvent = sink.events[0]
    assert event.tenant_id == BILLING_AUDIT_TENANT_ID
    assert event.event_type == "entitlement.changed"
    assert event.aggregate_id == "ba_01"
    assert event.payload["reason_code"] == "webhook_projection"
    assert dispatch_once(plane, sink, NOW).delivered == 0
