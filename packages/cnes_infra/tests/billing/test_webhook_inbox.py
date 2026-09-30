"""Testes do WebhookInbox (BIL-021) sobre moto: accept, claim e transições."""

import json
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    StaleInboxClaim,
)
from cnes_domain.billing.inbox import (
    InboxAcceptResult,
    InboxClaim,
    InboxDisposition,
    StripeEvent,
)
from cnes_domain.billing.models import ReadConsistency
from cnes_domain.billing.ports import WebhookInboxPort
from cnes_infra.billing.keys import STRIPE_RECOVERY_DUE_PARTITION, stripe_event_key
from cnes_infra.billing.webhook_inbox import WebhookInbox
from cnes_infra.billing.webhook_inbox_items import (
    STRIPE_INBOX_RETRY_MAX_SECONDS,
    STRIPE_PROCESSING_LEASE_SECONDS,
    STRIPE_WEBHOOK_EVENT_TYPES,
    retry_delay_seconds,
)
from cnes_infra.control_plane.dynamodb_keys import item_key, outbox_key
from packages.cnes_infra.tests.billing.billing_factories import (
    NOW,
    TABLE_NAME,
    create_table,
    table_items,
)
from packages.cnes_infra.tests.contracts.clock import MutableClock

STRONG = ReadConsistency.STRONG
EVENTUAL = ReadConsistency.EVENTUAL
SHA = "a" * 64
TRANSIENT = ("lease_until", "next_attempt_at", "due_at", "gsi1pk", "gsi1sk")


class Context:
    def __init__(self, client: Any, clock: MutableClock, inbox: WebhookInbox) -> None:
        self.client = client
        self.clock = clock
        self.inbox = inbox

    def raw(self, event_id: str = "evt_01") -> dict[str, Any] | None:
        key = item_key(*stripe_event_key(event_id))
        response = self.client.get_item(TableName=TABLE_NAME, Key=key, ConsistentRead=True)
        return response.get("Item")

    def put_raw(self, item: dict[str, Any]) -> None:
        self.client.put_item(TableName=TABLE_NAME, Item=item)

    def acquire(self, event_id: str = "evt_01") -> InboxClaim:
        claim = self.inbox.claim(event_id, self.clock.now())
        assert claim.acquired
        return claim

    def advance(self, seconds: float) -> None:
        self.clock.advance(timedelta(seconds=seconds))

    def with_client(self, client: Any) -> WebhookInbox:
        return WebhookInbox(client, TABLE_NAME, self.clock.now)


class FailingClient:
    def __init__(self, inner: Any, operation: str) -> None:
        self._inner = inner
        self._operation = operation

    def __getattr__(self, name: str) -> Any:
        if name == self._operation:
            return self._raise
        return getattr(self._inner, name)

    def _raise(self, **_: Any) -> None:
        error = {"Error": {"Code": "InternalServerError", "Message": "x"}}
        raise ClientError(error, self._operation)


class RecordingClient:
    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __getattr__(self, name: str) -> Any:
        target = getattr(self._inner, name)

        def call(**kwargs: Any) -> Any:
            self.calls.append((name, kwargs))
            return target(**kwargs)

        return call

    def names(self) -> list[str]:
        return [name for name, _ in self.calls]


def make_event(event_id: str = "evt_01", **changes: Any) -> StripeEvent:
    values: dict[str, Any] = {
        "event_id": event_id,
        "event_type": "invoice.paid",
        "created_at": NOW,
        "stripe_customer_id": "cus_01",
        "stripe_subscription_id": "sub_01",
        "payload_sha256": SHA,
    }
    values.update(changes)
    return StripeEvent(**values)


@pytest.fixture
def context() -> Iterator[Context]:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        create_table(client)
        clock = MutableClock(NOW)
        yield Context(client, clock, WebhookInbox(client, TABLE_NAME, clock.now))


def _attr(item: dict[str, Any] | None, name: str) -> str:
    assert item is not None
    return next(iter(item[name].values()))


def _state(ctx: Context, event_id: str = "evt_01") -> str:
    return _attr(ctx.raw(event_id), "state")


def _failed_retryable(ctx: Context) -> InboxClaim:
    ctx.inbox.accept(make_event())
    claim = ctx.acquire()
    ctx.inbox.mark_failed(claim, "boom", True)
    return claim


def _processed(ctx: Context) -> None:
    ctx.inbox.accept(make_event())
    ctx.inbox.mark_processed(ctx.acquire(), 2)


def _final(ctx: Context) -> None:
    ctx.inbox.accept(make_event())
    ctx.inbox.mark_failed(ctx.acquire(), "boom", False)


def test_implementa_a_porta_do_inbox(context: Context) -> None:
    assert isinstance(context.inbox, WebhookInboxPort)


def test_calcula_atraso_exponencial_com_teto() -> None:
    assert [retry_delay_seconds(n) for n in (1, 2, 3, 8, 9, 50)] == [
        30, 60, 120, 3600, 3600, STRIPE_INBOX_RETRY_MAX_SECONDS,
    ]


def test_aceita_evento_suportado_como_pending(context: Context) -> None:
    result = context.inbox.accept(make_event())
    item = context.raw()
    assert result == InboxAcceptResult("evt_01", InboxDisposition.ACCEPTED)
    assert (_attr(item, "state"), item["attempt"]["N"]) == ("pending", "0")
    due = NOW.isoformat(timespec="microseconds")
    assert _attr(item, "due_at") == due
    assert _attr(item, "gsi1pk") == STRIPE_RECOVERY_DUE_PARTITION
    assert _attr(item, "gsi1sk").startswith(f"{due}#")
    assert _attr(item, "received_at") == due
    assert _attr(item, "stripe_subscription_id") == "sub_01"
    assert "payload" not in item


def test_aceita_evento_sem_subscription_omitindo_atributo(context: Context) -> None:
    context.inbox.accept(make_event(stripe_subscription_id=None))
    assert "stripe_subscription_id" not in context.raw()


def test_todos_os_tipos_suportados_entram_como_pending(context: Context) -> None:
    for index, event_type in enumerate(sorted(STRIPE_WEBHOOK_EVENT_TYPES)):
        event = make_event(f"evt_{index}", event_type=event_type)
        assert context.inbox.accept(event).disposition is InboxDisposition.ACCEPTED


def test_duplicado_nao_altera_item_mesmo_com_payload_diferente(context: Context) -> None:
    context.inbox.accept(make_event())
    before = table_items(context.client)
    context.advance(10)
    result = context.inbox.accept(make_event(payload_sha256="b" * 64))
    assert result.disposition is InboxDisposition.DUPLICATE
    assert table_items(context.client) == before
    assert len(before) == 1


def test_ignora_tipo_nao_suportado_sem_atributos_gsi(context: Context) -> None:
    result = context.inbox.accept(make_event(event_type="charge.refunded"))
    item = context.raw()
    assert result.disposition is InboxDisposition.IGNORED
    assert (_attr(item, "state"), item["attempt"]["N"]) == ("ignored", "0")
    assert not set(TRANSIENT) & set(item)


def test_ignora_evento_suportado_sem_customer(context: Context) -> None:
    result = context.inbox.accept(make_event(stripe_customer_id=None))
    assert result.disposition is InboxDisposition.IGNORED
    assert "stripe_customer_id" not in context.raw()
    assert not set(TRANSIENT) & set(context.raw())


def test_accept_converte_erro_do_cliente_em_dependency_error(context: Context) -> None:
    inbox = context.with_client(FailingClient(context.client, "put_item"))
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        inbox.accept(make_event())


@pytest.mark.parametrize("prepare", ["accept", "retry_due", "lease_expired"])
def test_claim_adquire_estados_reivindicaveis(context: Context, prepare: str) -> None:
    context.inbox.accept(make_event())
    expected_attempt = 1
    if prepare != "accept":
        first = context.acquire()
        if prepare == "retry_due":
            context.inbox.mark_failed(first, "boom", True)
            context.advance(retry_delay_seconds(1))
        else:
            context.advance(STRIPE_PROCESSING_LEASE_SECONDS)
        expected_attempt = 2
    claim = context.inbox.claim("evt_01", context.clock.now())
    lease = (context.clock.now() + timedelta(seconds=STRIPE_PROCESSING_LEASE_SECONDS))
    item = context.raw()
    assert claim == InboxClaim(
        "evt_01", "invoice.paid", "cus_01", "sub_01", expected_attempt, True
    )
    assert _attr(item, "state") == "processing"
    assert _attr(item, "lease_until") == _attr(item, "due_at") == lease.isoformat(
        timespec="microseconds"
    )
    assert "next_attempt_at" not in item
    assert "error_code" not in item


def test_claim_sem_subscription_devolve_none(context: Context) -> None:
    context.inbox.accept(make_event(stripe_subscription_id=None))
    assert context.acquire().subscription_id is None


@pytest.mark.parametrize("state", ["retry_not_due", "live_lease", "processed", "final"])
def test_claim_nao_adquire_estados_bloqueados(context: Context, state: str) -> None:
    if state == "retry_not_due":
        _failed_retryable(context)
        context.advance(retry_delay_seconds(1) - 1)
    elif state == "live_lease":
        context.inbox.accept(make_event())
        context.acquire()
        context.advance(STRIPE_PROCESSING_LEASE_SECONDS - 1)
    elif state == "processed":
        _processed(context)
    else:
        _final(context)
    before = context.raw()
    claim = context.inbox.claim("evt_01", context.clock.now())
    assert (claim.acquired, claim.attempt) == (False, None)
    assert (claim.customer_id, claim.event_type) == ("cus_01", "invoice.paid")
    assert context.raw() == before


def test_claim_de_evento_ausente_falha_permanente(context: Context) -> None:
    with pytest.raises(PermanentBillingError, match="inbox_event_missing"):
        context.inbox.claim("evt_missing", NOW)


def test_claim_de_evento_ignorado_sem_customer_nao_e_reivindicavel(context: Context) -> None:
    context.inbox.accept(make_event(stripe_customer_id=None))
    with pytest.raises(PermanentBillingError, match="inbox_event_not_claimable"):
        context.inbox.claim("evt_01", NOW)


def test_claim_rejeita_now_nao_utc(context: Context) -> None:
    with pytest.raises(ValueError, match="datetime_not_utc"):
        context.inbox.claim("evt_01", NOW.replace(tzinfo=None))


def test_claim_converte_erro_do_update_em_dependency_error(context: Context) -> None:
    inbox = context.with_client(FailingClient(context.client, "update_item"))
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        inbox.claim("evt_01", NOW)


def test_claim_converte_erro_da_releitura_em_dependency_error(context: Context) -> None:
    _processed(context)
    inbox = context.with_client(FailingClient(context.client, "get_item"))
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        inbox.claim("evt_01", context.clock.now())


def test_mark_processed_remove_atributos_transitorios(context: Context) -> None:
    context.inbox.accept(make_event())
    context.inbox.mark_processed(context.acquire(), 7)
    item = context.raw()
    assert _attr(item, "state") == "processed"
    assert item["entitlement_version"]["N"] == "7"
    assert _attr(item, "processed_at") == NOW.isoformat(timespec="microseconds")
    assert not set(TRANSIENT) & set(item)


def test_mark_processed_de_claim_nao_adquirido_e_obsoleto(context: Context) -> None:
    _processed(context)
    claim = context.inbox.claim("evt_01", context.clock.now())
    with pytest.raises(StaleInboxClaim, match="inbox_claim_stale"):
        context.inbox.mark_processed(claim, 2)
    with pytest.raises(StaleInboxClaim, match="inbox_claim_stale"):
        context.inbox.mark_failed(claim, "boom", True)


def test_mark_processed_converte_erro_do_cliente(context: Context) -> None:
    context.inbox.accept(make_event())
    claim = context.acquire()
    inbox = context.with_client(FailingClient(context.client, "update_item"))
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        inbox.mark_processed(claim, 2)


def test_mark_failed_rejeita_error_code_nao_sanitizado(context: Context) -> None:
    context.inbox.accept(make_event())
    claim = context.acquire()
    before = context.raw()
    for code in ("Bad Code", "", "9abc", "x" * 65):
        with pytest.raises(ValueError, match="unsanitized_error_code"):
            context.inbox.mark_failed(claim, code, True)
    assert context.raw() == before


def test_mark_failed_retryable_agenda_nova_tentativa(context: Context) -> None:
    context.inbox.accept(make_event())
    claim = context.acquire()
    context.inbox.mark_failed(claim, "boom", True)
    item = context.raw()
    due = (NOW + timedelta(seconds=retry_delay_seconds(1))).isoformat(timespec="microseconds")
    assert _attr(item, "state") == "failed_retryable"
    assert _attr(item, "error_code") == "boom"
    assert _attr(item, "next_attempt_at") == _attr(item, "due_at") == due
    assert _attr(item, "gsi1sk").startswith(f"{due}#")
    assert "lease_until" not in item


def test_failed_retryable_vencido_volta_a_fila(context: Context) -> None:
    event = make_event()
    context.inbox.accept(event)
    context.inbox.mark_failed(context.acquire(), "boom", True)
    context.advance(retry_delay_seconds(1))
    assert context.inbox.list_recoverable(context.clock.now(), 100) == (event,)


def test_backoff_exponencial_usa_clock_injetado_e_teto(context: Context) -> None:
    event = make_event()
    context.inbox.accept(event)
    micro = timedelta(microseconds=1)
    for attempt in range(1, 10):
        claim = context.acquire()
        assert claim.attempt == attempt
        failed_at = context.clock.now()
        context.inbox.mark_failed(claim, "boom", True)
        delay = timedelta(seconds=min(30 * 2 ** min(attempt - 1, 7), 3600))
        assert context.inbox.list_recoverable(failed_at + delay - micro, 100) == ()
        context.clock.advance(delay)
        assert context.inbox.list_recoverable(context.clock.now(), 100) == (event,)


def test_claim_antigo_nao_conclui_apos_reclaim(context: Context) -> None:
    context.inbox.accept(make_event())
    old = context.acquire()
    context.advance(STRIPE_PROCESSING_LEASE_SECONDS + 1)
    current = context.acquire()
    assert current.attempt == old.attempt + 1
    before = context.raw()
    with pytest.raises(StaleInboxClaim, match="inbox_claim_stale"):
        context.inbox.mark_processed(old, 2)
    with pytest.raises(StaleInboxClaim, match="inbox_claim_stale"):
        context.inbox.mark_failed(old, "late_failure", True)
    assert _state(context) == "processing"
    assert context.raw() == before
    context.inbox.mark_processed(current, 2)
    assert _state(context) == "processed"


def test_mark_failed_retryable_converte_erro_do_cliente(context: Context) -> None:
    context.inbox.accept(make_event())
    claim = context.acquire()
    inbox = context.with_client(FailingClient(context.client, "update_item"))
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        inbox.mark_failed(claim, "boom", True)


def _outbox_payload(ctx: Context, audit_id: str) -> dict[str, Any]:
    key = item_key(*outbox_key(audit_id))
    item = ctx.client.get_item(TableName=TABLE_NAME, Key=key, ConsistentRead=True)["Item"]
    return json.loads(item["payload"]["S"])


def test_falha_permanente_grava_final_e_uma_auditoria_em_uma_transacao(
    context: Context,
) -> None:
    context.inbox.accept(make_event())
    claim = context.acquire()
    recorder = RecordingClient(context.client)
    context.with_client(recorder).mark_failed(claim, "bad_schema", False)
    item = context.raw()
    audit_id = _attr(item, "final_audit_id")
    assert recorder.names() == ["transact_write_items"]
    assert (_attr(item, "state"), _attr(item, "error_code")) == ("failed_final", "bad_schema")
    assert _attr(item, "failed_at") == NOW.isoformat(timespec="microseconds")
    assert not set(TRANSIENT) & set(item)
    outbox = [i for i in table_items(context.client) if i["sk"]["S"].startswith("OUTBOX#")]
    assert len(outbox) == 1
    event = _outbox_payload(context, audit_id)
    assert event["event_type"] == "billing.webhook_failed_final"
    assert event["payload"]["attributes"] == {
        "stripe_event_id": "evt_01",
        "stripe_event_type": "invoice.paid",
        "attempt": 1,
        "error_code": "bad_schema",
    }


def test_falha_permanente_obsoleta_nao_grava_auditoria(context: Context) -> None:
    context.inbox.accept(make_event())
    old = context.acquire()
    context.advance(STRIPE_PROCESSING_LEASE_SECONDS + 1)
    context.acquire()
    before = table_items(context.client)
    with pytest.raises(StaleInboxClaim, match="inbox_claim_stale"):
        context.inbox.mark_failed(old, "late_failure", False)
    assert table_items(context.client) == before


def test_falha_permanente_converte_erro_de_transacao(context: Context) -> None:
    context.inbox.accept(make_event())
    claim = context.acquire()
    inbox = context.with_client(FailingClient(context.client, "transact_write_items"))
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        inbox.mark_failed(claim, "boom", False)
