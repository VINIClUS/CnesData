"""Testes de leitura, decoder estrito e list_recoverable do WebhookInbox (BIL-021)."""

from collections.abc import Callable
from typing import Any

import pytest
from botocore.exceptions import EndpointConnectionError

from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
from cnes_domain.billing.inbox import InboxProcessingState, InboxRecoveryRecord
from cnes_infra.billing.keys import STRIPE_RECOVERY_DUE_PARTITION, stripe_event_key
from cnes_infra.billing.webhook_inbox import WebhookInbox
from cnes_infra.billing.webhook_inbox_items import (
    STRIPE_PROCESSING_LEASE_SECONDS,
    retry_delay_seconds,
)
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.test_webhook_inbox import (
    EVENTUAL,
    STRONG,
    Context,
    FailingClient,
    RecordingClient,
    _attr,
    _failed_retryable,
    _final,
    _processed,
    context,
    make_event,
)

__all__ = ["context"]

ISO = "%Y-%m-%dT%H:%M:%S.%f+00:00"


def _pending(ctx: Context) -> None:
    ctx.inbox.accept(make_event())


def _processing(ctx: Context) -> None:
    _pending(ctx)
    ctx.acquire()


def _retryable(ctx: Context) -> None:
    _failed_retryable(ctx)


PREPARE: dict[str, Callable[[Context], None]] = {
    "pending": _pending,
    "processing": _processing,
    "retryable": _retryable,
    "processed": _processed,
    "final": _final,
}


def _set(name: str, value: str, kind: str = "S") -> Callable[[dict[str, Any]], None]:
    return lambda item: item.__setitem__(name, {kind: value})


def _drop(*names: str) -> Callable[[dict[str, Any]], object]:
    return lambda item: [item.pop(name) for name in names]


def _sk_from_other_event(item: dict[str, Any]) -> None:
    item["gsi1sk"] = {"S": item["gsi1sk"]["S"].rsplit("#", 1)[0] + "#00"}


EXPECTED_STATE = {
    "pending": InboxProcessingState.PENDING,
    "processing": InboxProcessingState.PROCESSING,
    "retryable": InboxProcessingState.FAILED_RETRYABLE,
    "processed": InboxProcessingState.PROCESSED,
    "final": InboxProcessingState.FAILED_FINAL,
}
CORRUPTIONS = {
    "entidade_errada": ("pending", _set("entity", "OUTRA")),
    "event_id_divergente": ("pending", _set("event_id", "evt_outro")),
    "estado_invalido": ("pending", _set("state", "desconhecido")),
    "estado_ausente": ("pending", _drop("state")),
    "tentativa_nao_numerica": ("pending", _set("attempt", "x")),
    "tentativa_negativa": ("pending", _set("attempt", "-1", "N")),
    "pending_sem_due_at": ("pending", _drop("due_at")),
    "pending_sem_gsi1pk": ("pending", _drop("gsi1pk")),
    "pending_sem_gsi1sk": ("pending", _drop("gsi1sk")),
    "gsi1pk_divergente": ("pending", _set("gsi1pk", "OUTRA")),
    "gsi1sk_divergente": ("pending", _sk_from_other_event),
    "due_at_sem_utc": ("pending", _set("due_at", "2026-09-30T12:00:00")),
    "pending_com_lease": ("pending", _set("lease_until", "x")),
    "processing_sem_lease": ("processing", _drop("lease_until")),
    "processing_lease_diferente": ("processing", _set("lease_until", "2099-01-01")),
    "processing_tentativa_zero": ("processing", _set("attempt", "0", "N")),
    "retryable_sem_next_attempt": ("retryable", _drop("next_attempt_at")),
    "retryable_next_diferente": ("retryable", _set("next_attempt_at", "2099-01-01")),
    "retryable_tentativa_zero": ("retryable", _set("attempt", "0", "N")),
    "processed_com_due_at": ("processed", _set("due_at", "x")),
    "processed_com_gsi": ("processed", _set("gsi1pk", STRIPE_RECOVERY_DUE_PARTITION)),
    "final_com_lease": ("final", _set("lease_until", "x")),
    "final_sem_audit_id": ("final", _drop("final_audit_id")),
}


@pytest.mark.parametrize("name", sorted(CORRUPTIONS))
def test_decoder_estrito_rejeita_item_corrompido(context: Context, name: str) -> None:
    prepare, mutate = CORRUPTIONS[name]
    PREPARE[prepare](context)
    item = context.raw()
    assert item is not None
    mutate(item)
    context.put_raw(item)
    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        context.inbox.get_recovery_record("evt_01", STRONG)
    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        context.inbox.get_state("evt_01", STRONG)


def test_decoder_aceita_todos_os_estados_validos(context: Context) -> None:
    key = item_key(*stripe_event_key("evt_01"))
    for name, prepare in PREPARE.items():
        prepare(context)
        assert context.inbox.get_state("evt_01", STRONG) is EXPECTED_STATE[name]
        context.client.delete_item(TableName=TABLE_NAME, Key=key)


def test_get_recovery_record_de_pending_expoe_vencimento_e_chave_do_indice(
    context: Context,
) -> None:
    _pending(context)
    record = context.inbox.get_recovery_record("evt_01", STRONG)
    item = context.raw()
    assert record == InboxRecoveryRecord(
        InboxProcessingState.PENDING, 0, NOW, _attr(item, "gsi1sk")
    )


def test_get_recovery_record_de_estado_terminal_nao_tem_vencimento(context: Context) -> None:
    _processed(context)
    record = context.inbox.get_recovery_record("evt_01", STRONG)
    assert record == InboxRecoveryRecord(InboxProcessingState.PROCESSED, 1, None, None)
    assert context.inbox.get_state("evt_01", STRONG) is InboxProcessingState.PROCESSED


def test_get_recovery_record_de_ignorado(context: Context) -> None:
    context.inbox.accept(make_event(stripe_customer_id=None))
    record = context.inbox.get_recovery_record("evt_01", EVENTUAL)
    assert record == InboxRecoveryRecord(InboxProcessingState.IGNORED, 0, None, None)


def test_leituras_de_item_ausente_retornam_none(context: Context) -> None:
    assert context.inbox.get_state("evt_01", STRONG) is None
    assert context.inbox.get_recovery_record("evt_01", EVENTUAL) is None


@pytest.mark.parametrize(
    ("consistency", "expected"), [(STRONG, True), (EVENTUAL, False)]
)
def test_leituras_enviam_consistent_read_conforme_consistencia(
    context: Context, consistency: Any, expected: bool
) -> None:
    _pending(context)
    recorder = RecordingClient(context.client)
    inbox = context.with_client(recorder)
    inbox.get_state("evt_01", consistency)
    inbox.get_recovery_record("evt_01", consistency)
    flags = [kwargs["ConsistentRead"] for _, kwargs in recorder.calls]
    assert flags == [expected, expected]


def test_leituras_convertem_erro_do_cliente(context: Context) -> None:
    inbox = context.with_client(FailingClient(context.client, "get_item"))
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        inbox.get_state("evt_01", STRONG)
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        inbox.get_recovery_record("evt_01", STRONG)


def test_lista_eventos_em_ordem_de_vencimento_respeitando_limite(context: Context) -> None:
    events = [make_event(f"evt_{name}") for name in "abc"]
    for event in events:
        context.inbox.accept(event)
        context.advance(1)
    now = context.clock.now()
    assert context.inbox.list_recoverable(now, 2) == tuple(events[:2])
    assert context.inbox.list_recoverable(now, 100) == tuple(events)


def test_lista_reconstroi_evento_sem_subscription(context: Context) -> None:
    event = make_event(stripe_subscription_id=None)
    context.inbox.accept(event)
    assert context.inbox.list_recoverable(NOW, 10) == (event,)


def test_lista_exclui_itens_ainda_nao_vencidos(context: Context) -> None:
    _failed_retryable(context)
    context.inbox.accept(make_event("evt_02"))
    context.acquire("evt_02")
    context.advance(retry_delay_seconds(1) - 1)
    assert context.inbox.list_recoverable(context.clock.now(), 100) == ()


def test_lista_inclui_lease_expirado_e_exclui_estados_terminais(context: Context) -> None:
    event = make_event()
    context.inbox.accept(event)
    context.acquire()
    context.inbox.accept(make_event("evt_02"))
    context.inbox.mark_processed(context.acquire("evt_02"), 2)
    context.inbox.accept(make_event("evt_03", event_type="charge.refunded"))
    context.advance(STRIPE_PROCESSING_LEASE_SECONDS)
    assert context.inbox.list_recoverable(context.clock.now(), 100) == (event,)


@pytest.mark.parametrize("limit", [0, -1, True, "1"])
def test_lista_rejeita_limite_invalido(context: Context, limit: Any) -> None:
    with pytest.raises(ValueError, match="positive_value_required"):
        context.inbox.list_recoverable(NOW, limit)


def test_lista_rejeita_now_nao_utc(context: Context) -> None:
    with pytest.raises(ValueError, match="datetime_not_utc"):
        context.inbox.list_recoverable(NOW.replace(tzinfo=None), 1)


def test_lista_converte_erro_da_query(context: Context) -> None:
    inbox = context.with_client(FailingClient(context.client, "query"))
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        inbox.list_recoverable(NOW, 1)


class _StaleIndexClient:
    def __init__(self, inner: Any, sort_keys: tuple[str, ...]) -> None:
        self._inner = inner
        self._sort_keys = sort_keys

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def query(self, **_: Any) -> dict[str, Any]:
        return {"Items": [{"gsi1sk": {"S": key}} for key in self._sort_keys]}


def _stale(ctx: Context, *sort_keys: str) -> tuple[Any, ...]:
    inbox = ctx.with_client(_StaleIndexClient(ctx.client, sort_keys))
    return inbox.list_recoverable(ctx.clock.now(), 100)


def _index_key(ctx: Context, event_id: str = "evt_01") -> str:
    return _attr(ctx.raw(event_id), "gsi1sk")


def test_candidato_obsoleto_de_item_ja_processado_e_descartado(context: Context) -> None:
    _pending(context)
    stale_key = _index_key(context)
    context.inbox.mark_processed(context.acquire(), 2)
    assert _stale(context, stale_key) == ()


def test_candidato_obsoleto_com_gsi1sk_adiado_e_descartado(context: Context) -> None:
    _failed_retryable(context)
    context.advance(retry_delay_seconds(1) + 1)
    stale_key = NOW.strftime(ISO) + "#" + stale_hex()
    assert _stale(context, stale_key) == ()


def stale_hex() -> str:
    return b"evt_01".hex()


def test_candidato_de_item_ausente_e_descartado(context: Context) -> None:
    assert _stale(context, f"{NOW.strftime(ISO)}#{stale_hex()}") == ()


def test_candidato_atual_e_vencido_e_mantido(context: Context) -> None:
    event = make_event()
    context.inbox.accept(event)
    assert _stale(context, _index_key(context)) == (event,)


@pytest.mark.parametrize("sort_key", ["sem_separador", "2026#zz", "2026#ff"])
def test_candidato_com_chave_de_indice_invalida_e_corrupto(
    context: Context, sort_key: str
) -> None:
    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        _stale(context, sort_key)


def test_lista_rejeita_evento_com_created_at_corrompido(context: Context) -> None:
    _pending(context)
    item = context.raw()
    assert item is not None
    item["created_at"] = {"S": "nao-e-data"}
    context.put_raw(item)
    with pytest.raises(PermanentBillingError, match="billing_item_corrupt"):
        context.inbox.list_recoverable(NOW, 10)


class _UnreachableClient:
    def __init__(self, inner: Any, operation: str) -> None:
        self._inner = inner
        self._operation = operation

    def __getattr__(self, name: str) -> Any:
        if name == self._operation:
            return self._raise
        return getattr(self._inner, name)

    def _raise(self, **_: Any) -> None:
        raise EndpointConnectionError(endpoint_url="http://dynamodb.invalid")


@pytest.mark.parametrize(
    ("operation", "call"),
    [
        ("put_item", lambda ctx: ctx.inbox.accept(make_event())),
        ("update_item", lambda ctx: ctx.inbox.claim(make_event().event_id, NOW)),
        ("query", lambda ctx: ctx.inbox.list_recoverable(NOW, 10)),
        ("get_item", lambda ctx: ctx.inbox.get_state(make_event().event_id, STRONG)),
    ],
)
def test_falha_de_conexao_com_dynamodb_vira_dependencia_indisponivel(
    context: Context, operation: str, call: Callable[[Context], Any],
) -> None:
    _pending(context)
    unreachable = _UnreachableClient(context.client, operation)
    inbox = WebhookInbox(unreachable, TABLE_NAME, context.clock.now)
    ctx = Context(unreachable, context.clock, inbox)
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        call(ctx)


def test_falha_de_conexao_ao_concluir_claim_vira_dependencia_indisponivel(
    context: Context,
) -> None:
    _pending(context)
    claim = context.acquire()
    unreachable = _UnreachableClient(context.client, "update_item")
    inbox = WebhookInbox(unreachable, TABLE_NAME, context.clock.now)
    with pytest.raises(BillingDependencyError, match="dynamodb_unavailable"):
        inbox.mark_processed(claim, entitlement_version=1)
    assert context.inbox.get_state(claim.event_id, STRONG) is InboxProcessingState.PROCESSING
