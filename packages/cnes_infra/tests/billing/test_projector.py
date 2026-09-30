"""Testes do StripeEventProjector (BIL-021): falhas, fence, CAS e recuperação."""

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from typing import Any
from unittest.mock import Mock

import boto3
import pytest
from moto import mock_aws

from cnes_domain.billing.commands import StripeBillingState, StripeStateRequest
from cnes_domain.billing.errors import (
    PermanentBillingError,
    RetryableBillingError,
    StaleInboxClaim,
)
from cnes_domain.billing.inbox import InboxProcessingState, StripeEvent
from cnes_domain.billing.models import ReadConsistency, SubscriptionStatus
from cnes_infra.billing.dynamodb_catalog import DynamoBillingCatalog
from cnes_infra.billing.dynamodb_items import (
    deterministic_id,
    encode_account,
    encode_customer_map,
    encode_plan,
    encode_price_map,
    encode_snapshot,
)
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.billing.projector import (
    STRIPE_PROJECTION_CAS_RETRIES,
    ProjectorDependencies,
    StripeEventProjector,
)
from cnes_infra.billing.webhook_inbox import WebhookInbox
from cnes_infra.control_plane.dynamodb_keys import outbox_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    make_account,
    make_plan,
    make_snapshot,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

STRONG = ReadConsistency.STRONG
ACCOUNT_ID = "ba_01"
CUSTOMER = "cus_01"
SUBSCRIPTION = "sub_01"
PRICE_V1 = "price_monthly"
PRICE_V2 = "price_v2"
SHA = "a" * 64
UPDATED = "customer.subscription.updated"


def make_state(**changes: Any) -> StripeBillingState:
    state = StripeBillingState(
        stripe_customer_id=CUSTOMER,
        stripe_subscription_id=SUBSCRIPTION,
        subscription_status=SubscriptionStatus.ACTIVE,
        cancel_at_period_end=False,
        stripe_price_id=PRICE_V1,
        active_features=frozenset({"create_run"}),
        period_start=NOW,
        period_end=NOW + timedelta(days=30),
        latest_invoice_id=None,
    )
    return replace(state, **changes)


class ProjectorEnv:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.clock = MutableClock(NOW)
        self.stripe = Mock()
        self.stripe.get_current_state.return_value = make_state()
        self.inbox = WebhookInbox(client, TABLE_NAME, self.clock.now)
        self.catalog = DynamoBillingCatalog(client, TABLE_NAME, self.clock.now)
        self.projection = DynamoEntitlementProjection(client, TABLE_NAME, self.clock.now)
        self._seed()

    def _seed(self) -> None:
        account = make_account(ACCOUNT_ID, stripe_customer_id=CUSTOMER)
        plans = (make_plan("plan_v1"), make_plan("plan_v2", 9, stripe_price_ids=(PRICE_V2,)))
        items = [encode_account(account), encode_customer_map(ACCOUNT_ID, CUSTOMER)]
        for plan in plans:
            items.append(encode_plan(plan))
            prices = plan.stripe_price_ids
            items.extend(encode_price_map(price, plan.plan_version_id) for price in prices)
        for item in items:
            self.client.put_item(TableName=TABLE_NAME, Item=item)

    def projector(self, inbox: Any = None, projection: Any = None) -> StripeEventProjector:
        return StripeEventProjector(
            ProjectorDependencies(
                inbox=inbox or self.inbox,
                catalog=self.catalog,
                stripe=self.stripe,
                projection=projection or self.projection,
                clock=self.clock.now,
            )
        )

    def accept(
        self,
        event_id: str = "evt_01",
        event_type: str = UPDATED,
        subscription: str | None = SUBSCRIPTION,
        customer: str = CUSTOMER,
    ) -> None:
        event = StripeEvent(event_id, event_type, NOW, customer, subscription, SHA)
        self.inbox.accept(event)

    def inbox_state(self, event_id: str = "evt_01") -> InboxProcessingState | None:
        return self.inbox.get_state(event_id, STRONG)

    def snapshot(self) -> Any:
        return self.projection.get_snapshot(ACCOUNT_ID, STRONG)

    def audit_rows(self, event_type: str, event_id: str, version: int) -> list[dict[str, Any]]:
        audit_id = deterministic_id(event_type, event_id, str(version))
        return self._rows(outbox_key(audit_id))

    def failed_final_rows(self, event_id: str, attempt: int) -> list[dict[str, Any]]:
        audit_id = deterministic_id("billing.webhook_failed_final", event_id, str(attempt))
        return self._rows(outbox_key(audit_id))

    def all_audit_payloads(self) -> list[dict[str, Any]]:
        items = self.client.scan(TableName=TABLE_NAME, ConsistentRead=True)["Items"]
        rows = [i for i in items if i["entity"]["S"] == "OUTBOXEVENT"]
        return [json.loads(row["payload"]["S"]) for row in rows]

    def _rows(self, key: tuple[str, str]) -> list[dict[str, Any]]:
        response = self.client.get_item(
            TableName=TABLE_NAME,
            Key={"pk": {"S": key[0]}, "sk": {"S": key[1]}},
            ConsistentRead=True,
        )
        item = response.get("Item")
        return [] if item is None else [json.loads(item["payload"]["S"])]


@contextmanager
def projector_env() -> Iterator[ProjectorEnv]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        yield ProjectorEnv(client)


class ScriptedProjection:
    def __init__(self, inner: Any, outcomes: list[Any]) -> None:
        self._inner = inner
        self._outcomes = outcomes
        self.commits = 0

    def get_snapshot(self, account_id: str, consistency: ReadConsistency) -> Any:
        return self._inner.get_snapshot(account_id, consistency)

    def commit_claimed_snapshot(self, claim: Any, command: Any) -> bool:
        self.commits += 1
        if self._outcomes:
            outcome = self._outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise outcome
            if outcome is False:
                return False
        return self._inner.commit_claimed_snapshot(claim, command)


class StaleFailInbox:
    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def mark_failed(self, claim: Any, error_code: str, retryable: bool) -> None:
        raise StaleInboxClaim(claim.event_id)


def test_falha_retryable_persiste_estado_recuperavel():
    with projector_env() as env:
        env.stripe.get_current_state.side_effect = RetryableBillingError("stripe_unavailable")
        env.accept()
        with pytest.raises(RetryableBillingError) as raised:
            env.projector().process("evt_01")
        record = env.inbox.get_recovery_record("evt_01", STRONG)
    assert raised.value.code == "stripe_unavailable"
    assert record.state is InboxProcessingState.FAILED_RETRYABLE
    assert record.due_at > NOW


def test_falha_permanente_e_fenced_e_auditada():
    with projector_env() as env:
        error = PermanentBillingError("stripe_event_schema_invalid")
        env.stripe.get_current_state.side_effect = error
        env.accept()
        result = env.projector().process("evt_01")
        state = env.inbox_state()
        rows = env.failed_final_rows("evt_01", 1)
    assert result.applied is False
    assert state is InboxProcessingState.FAILED_FINAL
    assert len(rows) == 1
    assert rows[0]["event_type"] == "billing.webhook_failed_final"
    assert rows[0]["payload"]["reason_code"] == "stripe_event_schema_invalid"


def test_eventos_fora_de_ordem_convergem_ao_estado_atual():
    with projector_env() as env:
        env.stripe.get_current_state.return_value = make_state(stripe_price_id=PRICE_V2)
        env.accept("evt_older")
        env.accept("evt_newer")
        env.projector().process("evt_newer")
        env.projector().process("evt_older")
        snapshot = env.snapshot()
    assert snapshot.subscription_status is SubscriptionStatus.ACTIVE
    assert snapshot.plan_version_id == "plan_v2"
    assert snapshot.entitlement_version == 2


def test_claim_nao_adquirido_nao_chama_stripe():
    with projector_env() as env:
        env.accept()
        first = env.projector().process("evt_01")
        second = env.projector().process("evt_01")
    assert first.applied is True
    assert second.applied is False
    assert second.entitlement_version is None
    assert env.stripe.get_current_state.call_count == 1


def test_mapeamento_de_customer_ausente_e_retryable():
    with projector_env() as env:
        env.accept(customer="cus_unknown")
        with pytest.raises(RetryableBillingError) as raised:
            env.projector().process("evt_01")
        state = env.inbox_state()
    assert raised.value.code == "stripe_customer_mapping_missing"
    assert state is InboxProcessingState.FAILED_RETRYABLE
    env.stripe.get_current_state.assert_not_called()


def test_price_sem_mapeamento_e_retryable():
    with projector_env() as env:
        env.stripe.get_current_state.return_value = make_state(stripe_price_id="price_unknown")
        env.accept()
        with pytest.raises(RetryableBillingError) as raised:
            env.projector().process("evt_01")
        state = env.inbox_state()
        snapshot = env.snapshot()
    assert raised.value.code == "stripe_price_unmapped"
    assert state is InboxProcessingState.FAILED_RETRYABLE
    assert snapshot is None


def test_perda_de_cas_rebusca_estado_e_grava_o_segundo():
    with projector_env() as env:
        first = make_state(stripe_price_id=PRICE_V1)
        second = make_state(stripe_price_id=PRICE_V2)
        env.stripe.get_current_state.side_effect = [first, second]
        projection = ScriptedProjection(env.projection, [False])
        env.accept()
        result = env.projector(projection=projection).process("evt_01")
        snapshot = env.snapshot()
    assert result.applied is True
    assert env.stripe.get_current_state.call_count == 2
    assert snapshot.plan_version_id == "plan_v2"
    assert projection.commits == 2


def test_cas_esgotado_marca_falha_retryable():
    with projector_env() as env:
        losses = [False] * STRIPE_PROJECTION_CAS_RETRIES
        projection = ScriptedProjection(env.projection, losses)
        env.accept()
        with pytest.raises(RetryableBillingError) as raised:
            env.projector(projection=projection).process("evt_01")
        state = env.inbox_state()
    assert raised.value.code == "snapshot_cas_exhausted"
    assert state is InboxProcessingState.FAILED_RETRYABLE
    assert env.stripe.get_current_state.call_count == STRIPE_PROJECTION_CAS_RETRIES


def test_claim_obsoleto_no_commit_nao_aplica_nem_marca_falha():
    with projector_env() as env:
        projection = ScriptedProjection(env.projection, [StaleInboxClaim("evt_01")])
        env.accept()
        result = env.projector(projection=projection).process("evt_01")
        state = env.inbox_state()
        snapshot = env.snapshot()
    assert result.applied is False
    assert state is InboxProcessingState.PROCESSING
    assert snapshot is None


def test_claim_obsoleto_ao_marcar_falha_retryable_nao_relanca():
    with projector_env() as env:
        env.stripe.get_current_state.side_effect = RetryableBillingError("stripe_unavailable")
        env.accept()
        inbox = StaleFailInbox(env.inbox)
        result = env.projector(inbox=inbox).process("evt_01")
        state = env.inbox_state()
    assert result.applied is False
    assert state is InboxProcessingState.PROCESSING


def test_claim_obsoleto_ao_marcar_falha_permanente_nao_aplica():
    with projector_env() as env:
        env.stripe.get_current_state.side_effect = PermanentBillingError("stripe_gone")
        env.accept()
        inbox = StaleFailInbox(env.inbox)
        result = env.projector(inbox=inbox).process("evt_01")
        state = env.inbox_state()
    assert result.applied is False
    assert state is InboxProcessingState.PROCESSING


def test_excecao_desconhecida_propaga_e_mantem_processing():
    with projector_env() as env:
        env.stripe.get_current_state.side_effect = RuntimeError("boom")
        env.accept()
        with pytest.raises(RuntimeError):
            env.projector().process("evt_01")
        state = env.inbox_state()
    assert state is InboxProcessingState.PROCESSING


def test_admin_revoked_nao_e_levantado_por_webhook():
    with projector_env() as env:
        revoked = make_snapshot(ACCOUNT_ID, 1, subscription_status=SubscriptionStatus.ADMIN_REVOKED)
        env.client.put_item(TableName=TABLE_NAME, Item=encode_snapshot(revoked))
        env.accept()
        result = env.projector().process("evt_01")
        snapshot = env.snapshot()
    assert result.entitlement_version == 2
    assert snapshot.subscription_status is SubscriptionStatus.ADMIN_REVOKED
    assert snapshot.plan_version_id == "plan_v1"


def test_requisicao_ao_stripe_usa_customer_e_assinatura_do_claim():
    with projector_env() as env:
        env.accept()
        env.projector().process("evt_01")
    env.stripe.get_current_state.assert_called_once_with(StripeStateRequest(CUSTOMER, SUBSCRIPTION))
