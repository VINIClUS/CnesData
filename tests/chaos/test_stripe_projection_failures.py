"""Matriz de falhas BIL-021: Stripe, DynamoDB, leases, cursor e expiracao de snapshot."""

import threading
import time
from collections.abc import Iterator
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from botocore.exceptions import EndpointConnectionError

from cnes_domain.billing.commands import SnapshotWrite
from cnes_domain.billing.errors import (
    BillingDependencyError,
    EntitlementDenied,
    RetryableBillingError,
    StaleInboxClaim,
)
from cnes_domain.billing.inbox import (
    InboxProcessingState,
    RecoveryRequest,
    StripeRecoveryCursor,
)
from cnes_domain.billing.models import EntitlementSnapshot, SubscriptionStatus
from cnes_infra.billing.webhook_inbox_items import retry_delay_seconds
from tests.integration.billing._billing_stack import (
    ACCOUNT_ID,
    NOW,
    REQUEST,
    STRONG,
    BillingStack,
    FaultyClient,
    create_billing_table,
    dynamodb_client,
    install_fake_stripe,
    make_quotas,
    make_run_request,
    serve_pages,
    unavailable_error,
)

pytestmark = [pytest.mark.chaos, pytest.mark.chaos_infra, pytest.mark.dynamodb_local]

PROCESSED = InboxProcessingState.PROCESSED
FAILED_RETRYABLE = InboxProcessingState.FAILED_RETRYABLE
STRIPE_DOWN = "stripe_unavailable"
ENDPOINT_DOWN = EndpointConnectionError(endpoint_url="http://127.0.0.1:1")


@pytest.fixture(scope="module")
def dynamodb() -> Any:
    return dynamodb_client()


@pytest.fixture
def table_name(dynamodb: Any) -> Iterator[str]:
    name = f"billing-chaos-{uuid4().hex[:12]}"
    create_billing_table(dynamodb, name)
    try:
        yield name
    finally:
        dynamodb.delete_table(TableName=name)


@pytest.fixture
def faulty(dynamodb: Any) -> FaultyClient:
    return FaultyClient(dynamodb)


@pytest.fixture
def stack(
    faulty: FaultyClient, table_name: str, monkeypatch: pytest.MonkeyPatch,
) -> BillingStack:
    install_fake_stripe(monkeypatch)
    return BillingStack(faulty, table_name)


def project_first_event(stack: BillingStack) -> None:
    stack.post_webhook("evt_01")
    stack.drain()
    assert stack.snapshot().entitlement_version == 1


def test_stripe_503_agenda_retry_com_backoff_e_gate_inalterado(stack: BillingStack) -> None:
    project_first_event(stack)
    stack.post_webhook("evt_02")
    stack.stripe.failures = [RetryableBillingError(STRIPE_DOWN)]
    first = stack.drain()
    record = stack.inbox.get_recovery_record("evt_02", STRONG)
    assert first.failed == 1
    assert record.state is FAILED_RETRYABLE
    assert record.due_at == NOW + timedelta(seconds=retry_delay_seconds(1))
    assert stack.snapshot().entitlement_version == 1
    assert stack.gate.authorize_create_run(make_run_request()).entitlement_version == 1
    stack.clock.advance(timedelta(seconds=30))
    stack.stripe.failures = [RetryableBillingError(STRIPE_DOWN)]
    stack.drain()
    second = stack.inbox.get_recovery_record("evt_02", STRONG)
    assert second.due_at == stack.clock.now() + timedelta(seconds=retry_delay_seconds(2))
    assert stack.snapshot().entitlement_version == 1


def test_dynamodb_indisponivel_no_webhook_responde_503(stack: BillingStack, faulty: Any) -> None:
    faulty.fail("put_item", unavailable_error("PutItem"))
    response = stack.post_webhook("evt_01")
    faulty.heal()
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "5"
    assert response.json() == {"detail": "billing_dependency_unavailable"}
    assert stack.inbox_items() == []


def test_dynamodb_inalcancavel_no_webhook_nao_confirma_entrega(
    stack: BillingStack, faulty: Any,
) -> None:
    faulty.fail("put_item", ENDPOINT_DOWN)
    response = stack.post_webhook("evt_01")
    faulty.heal()
    assert 500 <= response.status_code < 600
    assert stack.inbox_items() == []


@pytest.mark.parametrize(
    "error", [unavailable_error("GetItem"), ENDPOINT_DOWN], ids=["client_error", "endpoint"],
)
def test_dynamodb_indisponivel_faz_gate_critico_falhar_fechado(
    stack: BillingStack, faulty: Any, error: Exception,
) -> None:
    project_first_event(stack)
    reservations_before = len(stack.quotas.commands)
    faulty.fail("get_item", error)
    with pytest.raises((BillingDependencyError, EndpointConnectionError)):
        stack.gate.authorize_create_run(make_run_request())
    faulty.heal()
    assert len(stack.quotas.commands) == reservations_before


def test_dynamodb_indisponivel_no_commit_deixa_estado_recuperavel(
    stack: BillingStack, faulty: Any,
) -> None:
    stack.post_webhook("evt_01")
    faulty.fail("transact_write_items", unavailable_error("TransactWriteItems"))
    result = stack.drain()
    faulty.heal()
    assert result.failed == 1
    assert stack.inbox_state("evt_01") is FAILED_RETRYABLE
    assert stack.snapshot() is None
    stack.clock.advance(timedelta(seconds=30))
    assert stack.drain().reprocessed == 1
    assert stack.inbox_state("evt_01") is PROCESSED


def test_queda_total_do_dynamodb_apos_claim_recupera_por_lease(
    stack: BillingStack, faulty: Any,
) -> None:
    stack.post_webhook("evt_01")

    def outage() -> None:
        for operation in ("get_item", "update_item", "transact_write_items", "query"):
            faulty.fail(operation, unavailable_error(operation))

    stack.stripe.on_state = outage
    with pytest.raises(RetryableBillingError):
        stack.projector.process("evt_01")
    faulty.heal()
    stack.stripe.on_state = None
    assert stack.inbox_state("evt_01") is InboxProcessingState.PROCESSING
    assert stack.snapshot() is None
    stack.clock.advance(timedelta(seconds=301))
    assert stack.drain().reprocessed == 1
    assert stack.inbox_state("evt_01") is PROCESSED


def test_retries_simultaneos_aplicam_a_projecao_uma_unica_vez(stack: BillingStack) -> None:
    stack.post_webhook("evt_01")
    stack.stripe.on_state = lambda: time.sleep(0.3)
    barrier = threading.Barrier(2)
    applied: list[bool] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            barrier.wait()
            applied.append(stack.projector.process("evt_01").applied)
        except BaseException as error:
            errors.append(error)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []
    assert sorted(applied) == [False, True]
    assert stack.snapshot().entitlement_version == 1
    assert stack.stripe.state_calls == 1
    assert stack.outbox_count() == 2


def test_lease_processing_so_e_reclamada_apos_300_segundos(stack: BillingStack) -> None:
    stack.post_webhook("evt_01")
    stack.stripe.failures = [RuntimeError("worker_crash")]
    with pytest.raises(RuntimeError, match="worker_crash"):
        stack.projector.process("evt_01")
    stack.clock.advance(timedelta(seconds=299))
    early = stack.drain()
    blocked = stack.projector.process("evt_01")
    assert early.scanned == 0
    assert blocked.applied is False
    assert stack.inbox_state("evt_01") is InboxProcessingState.PROCESSING
    stack.clock.advance(timedelta(seconds=2))
    late = stack.drain()
    assert late.reprocessed == 1
    assert stack.inbox_state("evt_01") is PROCESSED
    assert stack.inbox_items()[0]["attempt"]["N"] == "2"


def test_failed_retryable_vencido_e_reprocessado(stack: BillingStack) -> None:
    stack.post_webhook("evt_01")
    stack.stripe.failures = [RetryableBillingError(STRIPE_DOWN)]
    stack.drain()
    early = stack.drain()
    stack.clock.advance(timedelta(seconds=retry_delay_seconds(1)))
    due = stack.drain()
    assert early.scanned == 0
    assert (due.scanned, due.reprocessed) == (1, 1)
    assert stack.inbox_state("evt_01") is PROCESSED


def test_handoff_duravel_e_recuperado_fora_do_lookback_de_72h(stack: BillingStack) -> None:
    stack.stripe.pager = serve_pages(["evt_failed"])
    stack.stripe.failures = [RetryableBillingError(STRIPE_DOWN)]
    first = stack.recovery.run(REQUEST)
    assert first.failed == 1
    assert stack.inbox_state("evt_failed") is FAILED_RETRYABLE
    assert stack.cursor.load(STRONG) is None
    stack.stripe.pager = serve_pages([])
    stack.clock.advance(timedelta(hours=73))
    second = stack.recovery.run(REQUEST)
    assert second.reprocessed == 1
    assert stack.inbox_state("evt_failed") is PROCESSED
    assert stack.stripe.list_requests[-1].created_gte > NOW


def test_commit_de_claim_antigo_apos_reclaim_nao_muta_snapshot_nem_auditoria(
    stack: BillingStack,
) -> None:
    stack.post_webhook("evt_01")

    def reclaim_by_other_worker() -> None:
        stack.stripe.on_state = None
        stack.clock.advance(timedelta(seconds=301))
        assert stack.inbox.claim("evt_01", stack.clock.now()).attempt == 2

    stack.stripe.on_state = reclaim_by_other_worker
    result = stack.projector.process("evt_01")
    assert result.applied is False
    assert stack.snapshot() is None
    assert stack.outbox_count() == 0
    assert stack.inbox_state("evt_01") is InboxProcessingState.PROCESSING
    assert stack.inbox_items()[0]["attempt"]["N"] == "2"
    stack.clock.advance(timedelta(seconds=301))
    assert stack.drain().reprocessed == 1
    assert stack.snapshot().entitlement_version == 1
    assert stack.outbox_count() == 2


def test_commit_direto_com_claim_antigo_e_rejeitado(stack: BillingStack) -> None:
    stack.post_webhook("evt_01")
    old_claim = stack.inbox.claim("evt_01", stack.clock.now())
    stack.clock.advance(timedelta(seconds=301))
    stack.inbox.claim("evt_01", stack.clock.now())
    stack.stripe.on_state = None
    write = SnapshotWrite(0, _snapshot_from("evt_01"), ())
    with pytest.raises(StaleInboxClaim):
        stack.projection.commit_claimed_snapshot(old_claim, write)
    assert stack.snapshot() is None
    assert stack.outbox_count() == 0


def _snapshot_from(event_id: str) -> Any:
    return EntitlementSnapshot(
        billing_account_id=ACCOUNT_ID,
        stripe_subscription_id="sub_01",
        subscription_status=SubscriptionStatus.ACTIVE,
        cancel_at_period_end=False,
        plan_version_id="plan_v1",
        features=frozenset({"create_run"}),
        quotas=make_quotas(),
        period_start=NOW,
        period_end=NOW + timedelta(days=30),
        grace_until=None,
        valid_until=NOW + timedelta(days=33),
        entitlement_version=1,
        updated_at=NOW,
        source_event_id=event_id,
    )


def test_ciclo_antigo_nao_avanca_nem_completa_apos_novo_ciclo(stack: BillingStack) -> None:
    old = StripeRecoveryCursor("cycle-old", NOW - timedelta(hours=72), None, 1)
    assert stack.cursor.start(old) is True
    assert stack.cursor.complete(old, stack.clock.now()) is True
    new = StripeRecoveryCursor("cycle-new", NOW - timedelta(hours=72), None, 1)
    assert stack.cursor.start(new) is True
    assert stack.cursor.advance(old, old.advance("evt_x")) is False
    assert stack.cursor.complete(old, stack.clock.now()) is False
    assert stack.cursor.load(STRONG) == new
    assert stack.cursor.advance(new, new.advance("evt_y")) is True
    assert stack.cursor.load(STRONG).starting_after == "evt_y"


def test_ultimo_snapshot_valido_governa_somente_ate_valid_until(stack: BillingStack) -> None:
    project_first_event(stack)
    stack.post_webhook("evt_02")
    stack.stripe.failures = [RetryableBillingError(STRIPE_DOWN)] * 10
    stack.drain()
    valid_until = stack.snapshot().valid_until
    stack.clock.instant = valid_until
    assert stack.gate.authorize_create_run(make_run_request()).entitlement_version == 1
    stack.clock.instant = valid_until + timedelta(seconds=1)
    with pytest.raises(EntitlementDenied, match="reason=snapshot_expired"):
        stack.gate.authorize_create_run(make_run_request())


def test_nenhum_evento_retryable_fica_preso_atras_do_cursor_movido(stack: BillingStack) -> None:
    stack.stripe.pager = serve_pages(["evt_a", "evt_b"])
    stack.stripe.failures = [RetryableBillingError(STRIPE_DOWN)]
    request = RecoveryRequest(72, 1)
    first = stack.recovery.run(request)
    assert first.next_cursor == "evt_a"
    assert stack.inbox_state("evt_a") is FAILED_RETRYABLE
    assert stack.cursor.load(STRONG).starting_after == "evt_a"
    stack.clock.advance(timedelta(seconds=retry_delay_seconds(1)))
    second = stack.recovery.run(request)
    assert second.reprocessed == 1
    assert stack.inbox_state("evt_a") is PROCESSED
    assert stack.inbox_state("evt_b") is None
    third = stack.recovery.run(request)
    assert third.imported == 1
    assert stack.inbox_state("evt_b") is PROCESSED
    assert [r.starting_after for r in stack.stripe.list_requests] == [None, "evt_a"]
    assert stack.cursor.load(STRONG) is None
