"""Inbox DynamoDB de webhooks Stripe com claim por lease e fila de recovery."""

from datetime import datetime, timedelta
from typing import Any, cast

from botocore.exceptions import BotoCoreError, ClientError

from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    StaleInboxClaim,
)
from cnes_domain.billing.inbox import (
    InboxAcceptResult,
    InboxClaim,
    InboxDisposition,
    InboxProcessingState,
    InboxRecoveryRecord,
    StripeEvent,
)
from cnes_domain.billing.models import BillingAuditEvent, ReadConsistency
from cnes_domain.billing.ports import ClockPort
from cnes_domain.billing.validation import require_positive, require_utc
from cnes_infra.billing.dynamodb_items import (
    UNAVAILABLE_CODE,
    audit_outbox_event,
    deterministic_id,
    get_item,
    outbox_item,
    put_new,
    transact,
    utc_attribute,
)
from cnes_infra.billing.dynamodb_projection import (
    INBOX_PROCESSED_AT_ATTRIBUTE,
    INBOX_TRANSIENT_ATTRIBUTES,
    INBOX_VERSION_ATTRIBUTE,
)
from cnes_infra.billing.keys import (
    STRIPE_RECOVERY_DUE_INDEX,
    STRIPE_RECOVERY_DUE_PARTITION,
    stripe_event_key,
)
from cnes_infra.billing.webhook_inbox_items import (
    STRIPE_PROCESSING_LEASE_SECONDS,
    WEBHOOK_FAILED_FINAL_EVENT,
    candidate_event_id,
    decode_event,
    decode_recovery_record,
    due_values,
    encode_inbox_item,
    require_error_code,
    retry_delay_seconds,
)
from cnes_infra.control_plane.dynamodb_codec import Action, Item
from cnes_infra.control_plane.dynamodb_keys import item_key

_CONDITIONAL_FAILED = "ConditionalCheckFailedException"
_CLAIMABLE = (
    "attribute_exists(pk) AND (#state = :pending OR (#state = :failed AND next_attempt_at <= :now)"
    " OR (#state = :processing AND lease_until <= :now))"
)
_CLAIM_UPDATE = (
    "SET #state = :processing, attempt = attempt + :one, lease_until = :due, due_at = :due, "
    "gsi1pk = :p, gsi1sk = :sk REMOVE next_attempt_at, error_code"
)
_OWNED = "#state = :processing AND attempt = :attempt"
_RETRY_UPDATE = (
    "SET #state = :failed, error_code = :code, next_attempt_at = :due, due_at = :due, "
    "gsi1pk = :p, gsi1sk = :sk REMOVE lease_until"
)
_FINAL_UPDATE = (
    "SET #state = :final, error_code = :code, final_audit_id = :audit, failed_at = :failed_at "
    f"REMOVE {', '.join(INBOX_TRANSIENT_ATTRIBUTES)}"
)
_PROCESSED_UPDATE = (
    f"SET #state = :processed, {INBOX_VERSION_ATTRIBUTE} = :version, "
    f"{INBOX_PROCESSED_AT_ATTRIBUTE} = :processed_at REMOVE {', '.join(INBOX_TRANSIENT_ATTRIBUTES)}"
)
_DUE_QUERY = "gsi1pk = :p AND gsi1sk <= :upper"


def _state(state: InboxProcessingState) -> dict[str, str]:
    return {"S": state.value}


def _require_acquired(claim: InboxClaim) -> None:
    if not claim.acquired:
        raise StaleInboxClaim(claim.event_id)


def _is_conditional(error: ClientError) -> bool:
    return error.response.get("Error", {}).get("Code") == _CONDITIONAL_FAILED


def _claim_from(event: StripeEvent, attempt: int | None) -> InboxClaim:
    return InboxClaim(
        event_id=event.event_id,
        event_type=event.event_type,
        customer_id=cast("str", event.stripe_customer_id),
        subscription_id=event.stripe_subscription_id,
        attempt=attempt,
        acquired=attempt is not None,
    )


class WebhookInbox:
    """Inbox de webhooks Stripe sobre DynamoDB single-table."""

    def __init__(self, client: Any, table_name: str, clock: ClockPort) -> None:
        self._client = client
        self._table_name = table_name
        self._clock = clock

    def accept(self, event: StripeEvent) -> InboxAcceptResult:
        """Grava o evento se ainda não existir; nunca altera item existente.

        Args: Evento Stripe verificado.
        Returns: ACCEPTED, IGNORED ou DUPLICATE (dedupe só por event_id).
        Raises: BillingDependencyError se o storage falhar.
        """
        item = encode_inbox_item(event, self._clock())
        try:
            self._client.put_item(
                TableName=self._table_name,
                Item=item,
                ConditionExpression="attribute_not_exists(pk)",
            )
        except ClientError as error:
            if not _is_conditional(error):
                raise BillingDependencyError(UNAVAILABLE_CODE) from error
            return InboxAcceptResult(event.event_id, InboxDisposition.DUPLICATE)
        except BotoCoreError as error:
            raise BillingDependencyError(UNAVAILABLE_CODE) from error
        pending = item["state"] == _state(InboxProcessingState.PENDING)
        disposition = InboxDisposition.ACCEPTED if pending else InboxDisposition.IGNORED
        return InboxAcceptResult(event.event_id, disposition)

    def claim(self, event_id: str, now: datetime) -> InboxClaim:
        """Adquire o lease de processamento se o evento estiver reivindicável.

        Args: Identificador do evento e instante UTC atual.
        Returns: Claim adquirido, ou não adquirido se o evento não estiver vencido.
        Raises: PermanentBillingError se ausente ou não reivindicável;
            ValueError se now não for UTC; BillingDependencyError.
        """
        require_utc(now, "now")
        lease = now + timedelta(seconds=STRIPE_PROCESSING_LEASE_SECONDS)
        values = {
            ":pending": _state(InboxProcessingState.PENDING),
            ":failed": _state(InboxProcessingState.FAILED_RETRYABLE),
            ":processing": _state(InboxProcessingState.PROCESSING),
            ":now": {"S": utc_attribute(now)},
            ":one": {"N": "1"},
            **due_values(lease, event_id),
        }
        try:
            response = self._client.update_item(
                TableName=self._table_name,
                Key=item_key(*stripe_event_key(event_id)),
                ConditionExpression=_CLAIMABLE,
                UpdateExpression=_CLAIM_UPDATE,
                ExpressionAttributeNames={"#state": "state"},
                ExpressionAttributeValues=values,
                ReturnValues="ALL_NEW",
            )
        except ClientError as error:
            if not _is_conditional(error):
                raise BillingDependencyError(UNAVAILABLE_CODE) from error
            return self._unacquired(event_id)
        except BotoCoreError as error:
            raise BillingDependencyError(UNAVAILABLE_CODE) from error
        item = response["Attributes"]
        record = decode_recovery_record(item, event_id)
        return _claim_from(decode_event(item), record.attempt)

    def mark_processed(self, claim: InboxClaim, entitlement_version: int) -> None:
        """Conclui o evento sob o fence do claim, sem transações adicionais.

        Args: Claim adquirido e versão de entitlement aplicada.
        Raises: StaleInboxClaim se o fence foi perdido; BillingDependencyError.
        """
        _require_acquired(claim)
        values = {
            ":processed": _state(InboxProcessingState.PROCESSED),
            ":version": {"N": str(entitlement_version)},
            ":processed_at": {"S": utc_attribute(self._clock())},
        }
        self._update_claimed(claim, _PROCESSED_UPDATE, values)

    def mark_failed(self, claim: InboxClaim, error_code: str, retryable: bool) -> None:
        """Registra a falha sob o fence do claim.

        Args: Claim adquirido, código sanitizado e se a falha é reprocessável.
        Raises: StaleInboxClaim; ValueError se o código não for sanitizado;
            BillingDependencyError.
        """
        _require_acquired(claim)
        require_error_code(error_code)
        if retryable:
            self._fail_retryable(claim, error_code)
        else:
            self._fail_final(claim, error_code)

    def get_state(
        self, event_id: str, consistency: ReadConsistency
    ) -> InboxProcessingState | None:
        """Lê o estado do evento pela base key.

        Args: Identificador do evento e consistência.
        Returns: Estado ou None se ausente.
        Raises: PermanentBillingError para item corrompido.
        """
        record = self.get_recovery_record(event_id, consistency)
        return None if record is None else record.state

    def get_recovery_record(
        self, event_id: str, consistency: ReadConsistency
    ) -> InboxRecoveryRecord | None:
        """Lê o registro de recovery do evento pela base key.

        Args: Identificador do evento e consistência.
        Returns: Registro ou None se ausente.
        Raises: PermanentBillingError para item corrompido.
        """
        strong = consistency is ReadConsistency.STRONG
        item = get_item(self._client, self._table_name, stripe_event_key(event_id), strong)
        return None if item is None else decode_recovery_record(item, event_id)

    def list_recoverable(self, now: datetime, limit: int) -> tuple[StripeEvent, ...]:
        """Lista eventos vencidos em ordem de vencimento, revalidando cada candidato.

        Args: Instante UTC atual e limite positivo de candidatos do índice.
        Returns: Eventos vencidos cujo item base ainda confirma o candidato.
        Raises: ValueError; PermanentBillingError; BillingDependencyError.
        """
        require_utc(now, "now")
        require_positive(limit, "limit")
        values = {
            ":p": {"S": STRIPE_RECOVERY_DUE_PARTITION},
            ":upper": {"S": f"{utc_attribute(now)}#￿"},
        }
        try:
            response = self._client.query(
                TableName=self._table_name,
                IndexName=STRIPE_RECOVERY_DUE_INDEX,
                KeyConditionExpression=_DUE_QUERY,
                ExpressionAttributeValues=values,
                Limit=limit,
                ScanIndexForward=True,
            )
        except (ClientError, BotoCoreError) as error:
            raise BillingDependencyError(UNAVAILABLE_CODE) from error
        rows = (row["gsi1sk"]["S"] for row in response["Items"])
        events = (self._confirmed_event(sort_key, now) for sort_key in rows)
        return tuple(event for event in events if event is not None)

    def _confirmed_event(self, sort_key: str, now: datetime) -> StripeEvent | None:
        event_id = candidate_event_id(sort_key)
        item = get_item(self._client, self._table_name, stripe_event_key(event_id), True)
        if item is None:
            return None
        record = decode_recovery_record(item, event_id)
        due = record.due_at is not None and record.due_at <= now
        return decode_event(item) if due and record.due_index_key == sort_key else None

    def _unacquired(self, event_id: str) -> InboxClaim:
        key = stripe_event_key(event_id)
        item = get_item(self._client, self._table_name, key, True)
        if item is None:
            raise PermanentBillingError("inbox_event_missing")
        decode_recovery_record(item, event_id)
        event = decode_event(item)
        if event.stripe_customer_id is None:
            raise PermanentBillingError("inbox_event_not_claimable")
        return _claim_from(event, None)

    def _guarded(self, claim: InboxClaim, expression: str, values: Item) -> dict[str, Any]:
        return {
            "TableName": self._table_name,
            "Key": item_key(*stripe_event_key(claim.event_id)),
            "ConditionExpression": _OWNED,
            "UpdateExpression": expression,
            "ExpressionAttributeNames": {"#state": "state"},
            "ExpressionAttributeValues": {
                ":processing": _state(InboxProcessingState.PROCESSING),
                ":attempt": {"N": str(claim.attempt)},
                **values,
            },
        }

    def _update_claimed(self, claim: InboxClaim, expression: str, values: Item) -> None:
        try:
            self._client.update_item(**self._guarded(claim, expression, values))
        except ClientError as error:
            if not _is_conditional(error):
                raise BillingDependencyError(UNAVAILABLE_CODE) from error
            raise StaleInboxClaim(claim.event_id) from error
        except BotoCoreError as error:
            raise BillingDependencyError(UNAVAILABLE_CODE) from error

    def _fail_retryable(self, claim: InboxClaim, error_code: str) -> None:
        due = self._clock() + timedelta(seconds=retry_delay_seconds(cast("int", claim.attempt)))
        values = {
            ":failed": _state(InboxProcessingState.FAILED_RETRYABLE),
            ":code": {"S": error_code},
            **due_values(due, claim.event_id),
        }
        self._update_claimed(claim, _RETRY_UPDATE, values)

    def _fail_final(self, claim: InboxClaim, error_code: str) -> None:
        now = self._clock()
        audit = BillingAuditEvent(
            event_id=deterministic_id(
                WEBHOOK_FAILED_FINAL_EVENT, claim.event_id, str(claim.attempt)
            ),
            event_type=WEBHOOK_FAILED_FINAL_EVENT,
            aggregate_id=claim.event_id,
            actor_id="stripe_webhook",
            reason_code=error_code,
            occurred_at=now,
            attributes={
                "stripe_event_id": claim.event_id,
                "stripe_event_type": claim.event_type,
                "attempt": claim.attempt,
                "error_code": error_code,
            },
        )
        values = {
            ":final": _state(InboxProcessingState.FAILED_FINAL),
            ":code": {"S": error_code},
            ":audit": {"S": audit.event_id},
            ":failed_at": {"S": utc_attribute(now)},
        }
        update: Action = {"Update": self._guarded(claim, _FINAL_UPDATE, values)}
        outbox = put_new(self._table_name, outbox_item(audit_outbox_event(audit)))
        if not transact(self._client, (update, outbox)):
            raise StaleInboxClaim(claim.event_id)
