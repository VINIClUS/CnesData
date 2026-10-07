"""Codec puro do item de inbox de webhooks Stripe no DynamoDB."""

import re
from datetime import datetime

from cnes_domain.billing.inbox import InboxProcessingState, InboxRecoveryRecord, StripeEvent
from cnes_infra.billing.dynamodb_items import corrupt_item, utc_attribute
from cnes_infra.billing.dynamodb_projection import INBOX_TRANSIENT_ATTRIBUTES
from cnes_infra.billing.keys import (
    STRIPE_RECOVERY_DUE_PARTITION,
    stripe_event_key,
    stripe_recovery_due_sort_key,
)
from cnes_infra.control_plane.dynamodb_codec import Item

STRIPE_WEBHOOK_EVENT_TYPES = frozenset({
    "checkout.session.completed",
    "customer.subscription.created",
    "customer.subscription.updated",
    "customer.subscription.deleted",
    "customer.subscription.paused",
    "customer.subscription.resumed",
    "invoice.paid",
    "invoice.payment_failed",
    "invoice.payment_action_required",
    "entitlements.active_entitlement_summary.updated",
})
STRIPE_INBOX_RETRY_BASE_SECONDS = 30
STRIPE_INBOX_RETRY_MAX_SECONDS = 3600
STRIPE_PROCESSING_LEASE_SECONDS = 300
STRIPE_INBOX_MAX_ATTEMPTS = 75
INBOX_ENTITY = "STRIPEEVENTINBOX"
WEBHOOK_FAILED_FINAL_EVENT = "billing.webhook_failed_final"
_MAX_BACKOFF_EXPONENT = 7
_ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_DECODE_ERRORS = (KeyError, TypeError, ValueError, AttributeError)
_DUE_ATTRIBUTES = frozenset({"due_at", "gsi1pk", "gsi1sk"})
_TRANSIENT_BY_STATE = {
    InboxProcessingState.PENDING: _DUE_ATTRIBUTES,
    InboxProcessingState.PROCESSING: _DUE_ATTRIBUTES | {"lease_until"},
    InboxProcessingState.FAILED_RETRYABLE: _DUE_ATTRIBUTES | {"next_attempt_at"},
}
_MIRROR_BY_STATE = {
    InboxProcessingState.PROCESSING: "lease_until",
    InboxProcessingState.FAILED_RETRYABLE: "next_attempt_at",
}


def retry_delay_seconds(attempt: int) -> int:
    """Calcula o atraso exponencial com teto para a tentativa informada.

    Args: Número da tentativa (>= 1).
    Returns: Segundos até a próxima tentativa.
    """
    exponent = min(attempt - 1, _MAX_BACKOFF_EXPONENT)
    return min(STRIPE_INBOX_RETRY_BASE_SECONDS * 2**exponent, STRIPE_INBOX_RETRY_MAX_SECONDS)


def require_error_code(error_code: str) -> None:
    """Valida o código de erro sanitizado.

    Args: Código de erro.
    Raises: ValueError se o código não for sanitizado.
    """
    if not isinstance(error_code, str) or not _ERROR_CODE.fullmatch(error_code):
        raise ValueError("reason=unsanitized_error_code")


def due_attributes(due_at: datetime, event_id: str) -> Item:
    """Monta os atributos de vencimento e do índice de recovery."""
    return {
        "due_at": {"S": utc_attribute(due_at)},
        "gsi1pk": {"S": STRIPE_RECOVERY_DUE_PARTITION},
        "gsi1sk": {"S": stripe_recovery_due_sort_key(due_at, event_id)},
    }


def due_values(due_at: datetime, event_id: str) -> Item:
    """Monta os valores de expressão para atualizar vencimento e índice."""
    attributes = due_attributes(due_at, event_id)
    return {":due": attributes["due_at"], ":p": attributes["gsi1pk"], ":sk": attributes["gsi1sk"]}


def _optional_ids(event: StripeEvent) -> Item:
    item: Item = {}
    if event.stripe_customer_id is not None:
        item["stripe_customer_id"] = {"S": event.stripe_customer_id}
    if event.stripe_subscription_id is not None:
        item["stripe_subscription_id"] = {"S": event.stripe_subscription_id}
    return item


def encode_inbox_item(event: StripeEvent, received_at: datetime) -> Item:
    """Codifica o item de inbox de um evento recém-recebido.

    Args: Evento Stripe e instante de recebimento UTC.
    Returns: Item PENDING (evento suportado com customer) ou IGNORED.
    """
    supported = event.event_type in STRIPE_WEBHOOK_EVENT_TYPES
    actionable = supported and event.stripe_customer_id is not None
    state = InboxProcessingState.PENDING if actionable else InboxProcessingState.IGNORED
    pk, sk = stripe_event_key(event.event_id)
    item: Item = {
        "pk": {"S": pk},
        "sk": {"S": sk},
        "entity": {"S": INBOX_ENTITY},
        "event_id": {"S": event.event_id},
        "event_type": {"S": event.event_type},
        "created_at": {"S": utc_attribute(event.created_at)},
        "payload_sha256": {"S": event.payload_sha256},
        "received_at": {"S": utc_attribute(received_at)},
        "state": {"S": state.value},
        "attempt": {"N": "0"},
        **_optional_ids(event),
    }
    if actionable:
        item.update(due_attributes(received_at, event.event_id))
    return item


def _require(condition: bool) -> None:
    if not condition:
        raise corrupt_item(INBOX_ENTITY)


def _identity(item: Item, event_id: str) -> tuple[InboxProcessingState, int]:
    stored = (item["entity"]["S"], item["pk"]["S"], item["sk"]["S"], item["event_id"]["S"])
    _require(stored == (INBOX_ENTITY, *stripe_event_key(event_id), event_id))
    return InboxProcessingState(item["state"]["S"]), int(item["attempt"]["N"])


def _check_transient(item: Item, state: InboxProcessingState, attempt: int, event_id: str) -> None:
    expected = _TRANSIENT_BY_STATE.get(state, frozenset())
    _require(expected == {name for name in INBOX_TRANSIENT_ATTRIBUTES if name in item})
    if not expected:
        return
    due_at = item["due_at"]["S"]
    sort_key = stripe_recovery_due_sort_key(datetime.fromisoformat(due_at), event_id)
    _require(item["gsi1pk"]["S"] == STRIPE_RECOVERY_DUE_PARTITION)
    _require(item["gsi1sk"]["S"] == sort_key)
    mirror = _MIRROR_BY_STATE.get(state)
    _require(mirror is None or (item[mirror]["S"] == due_at and attempt >= 1))


def decode_recovery_record(item: Item, event_id: str) -> InboxRecoveryRecord:
    """Decodifica o item validando identidade, estado e atributos transitórios.

    Args: Item base e identificador esperado do evento.
    Returns: Registro de recovery.
    Raises: PermanentBillingError billing_item_corrupt se o item violar o contrato.
    """
    try:
        state, attempt = _identity(item, event_id)
        _check_transient(item, state, attempt, event_id)
        _require(state is not InboxProcessingState.FAILED_FINAL or "final_audit_id" in item)
        active = state in _TRANSIENT_BY_STATE
        due_at = datetime.fromisoformat(item["due_at"]["S"]) if active else None
        index_key = item["gsi1sk"]["S"] if active else None
        return InboxRecoveryRecord(state, attempt, due_at, index_key)
    except _DECODE_ERRORS as error:
        raise corrupt_item(INBOX_ENTITY) from error


def decode_event(item: Item) -> StripeEvent:
    """Reconstrói o evento Stripe de um item de inbox já validado.

    Args: Item base.
    Returns: Evento Stripe.
    Raises: PermanentBillingError billing_item_corrupt se faltar ou violar atributo.
    """
    try:
        return StripeEvent(
            event_id=item["event_id"]["S"],
            event_type=item["event_type"]["S"],
            created_at=datetime.fromisoformat(item["created_at"]["S"]),
            stripe_customer_id=item.get("stripe_customer_id", {}).get("S"),
            stripe_subscription_id=item.get("stripe_subscription_id", {}).get("S"),
            payload_sha256=item["payload_sha256"]["S"],
        )
    except _DECODE_ERRORS as error:
        raise corrupt_item(INBOX_ENTITY) from error


def candidate_event_id(sort_key: str) -> str:
    """Extrai o event_id do sufixo hexadecimal da sort key do índice.

    Args: Sort key do candidato do índice de recovery.
    Returns: Identificador do evento.
    Raises: PermanentBillingError billing_item_corrupt se o sufixo for inválido.
    """
    try:
        return bytes.fromhex(sort_key.rsplit("#", 1)[1]).decode()
    except (IndexError, ValueError) as error:
        raise corrupt_item(INBOX_ENTITY) from error
