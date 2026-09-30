"""Projeção DynamoDB do snapshot de entitlement com CAS e fence do inbox."""

from typing import Any

from cnes_domain.billing.commands import SnapshotWrite
from cnes_domain.billing.errors import RetryableBillingError, StaleInboxClaim
from cnes_domain.billing.inbox import InboxClaim, InboxProcessingState
from cnes_domain.billing.models import EntitlementSnapshot, ReadConsistency
from cnes_domain.billing.ports import ClockPort
from cnes_infra.billing.dynamodb_items import (
    SNAPSHOT_ENTITY,
    audit_outbox_event,
    corrupt_item,
    decode_snapshot,
    encode_snapshot,
    get_item,
    outbox_item,
    put_new,
    transact,
    utc_attribute,
)
from cnes_infra.billing.keys import entitlement_snapshot_key, stripe_event_key
from cnes_infra.control_plane.dynamodb_codec import Action, Item
from cnes_infra.control_plane.dynamodb_keys import item_key

INBOX_STATE_ATTRIBUTE = "state"
INBOX_ATTEMPT_ATTRIBUTE = "attempt"
INBOX_VERSION_ATTRIBUTE = "entitlement_version"
INBOX_PROCESSED_AT_ATTRIBUTE = "processed_at"
INBOX_TRANSIENT_ATTRIBUTES = ("lease_until", "next_attempt_at", "due_at", "gsi1pk", "gsi1sk")
AMBIGUOUS_COMMIT_CODE = "billing_commit_ambiguous"


def _current_version(item: Item | None) -> int:
    if item is None:
        return 0
    try:
        return int(item["entitlement_version"]["N"])
    except (KeyError, TypeError, ValueError) as error:
        raise corrupt_item(SNAPSHOT_ENTITY) from error


def _fence_lost(inbox: Item | None, attempt: int | None) -> bool:
    if inbox is None:
        return True
    state = inbox.get(INBOX_STATE_ATTRIBUTE, {}).get("S")
    stored_attempt = inbox.get(INBOX_ATTEMPT_ATTRIBUTE, {}).get("N")
    processing = state == InboxProcessingState.PROCESSING.value
    return not processing or stored_attempt != str(attempt)


class DynamoEntitlementProjection:
    """Projeção de entitlement sobre DynamoDB single-table."""

    def __init__(self, client: Any, table_name: str, clock: ClockPort) -> None:
        self._client = client
        self._table_name = table_name
        self._clock = clock

    def get_snapshot(
        self, billing_account_id: str, consistency: ReadConsistency
    ) -> EntitlementSnapshot | None:
        """Lê o snapshot pela base key.

        Args: Conta de billing e consistência de leitura.
        Returns: Snapshot ou None se ausente.
        Raises: PermanentBillingError para item corrompido.
        """
        strong = consistency is ReadConsistency.STRONG
        key = entitlement_snapshot_key(billing_account_id)
        item = get_item(self._client, self._table_name, key, strong)
        return None if item is None else decode_snapshot(item, billing_account_id)

    def compare_and_set_snapshot(self, command: SnapshotWrite) -> bool:
        """Grava snapshot e auditoria se a versão esperada for a atual.

        Args: Escrita com versão esperada, snapshot e auditoria.
        Returns: True se gravou; False se a versão mudou.
        Raises: RetryableBillingError se o resultado for ambíguo.
        """
        actions = (self._snapshot_put(command), *self._audit_puts(command))
        if transact(self._client, actions):
            return True
        if self._snapshot_version(command) != command.expected_version:
            return False
        raise RetryableBillingError(AMBIGUOUS_COMMIT_CODE)

    def commit_claimed_snapshot(self, claim: InboxClaim, command: SnapshotWrite) -> bool:
        """Grava snapshot, auditoria e conclui o inbox sob o fence do claim.

        Args: Claim adquirido e escrita do snapshot.
        Returns: True se gravou; False se a versão esperada mudou.
        Raises: StaleInboxClaim se o fence foi perdido; RetryableBillingError se ambíguo;
            ValueError se o snapshot não vier do evento reivindicado.
        """
        if not claim.acquired:
            raise StaleInboxClaim(claim.event_id)
        if command.snapshot.source_event_id != claim.event_id:
            raise ValueError("reason=snapshot_event_mismatch")
        actions = (
            self._snapshot_put(command),
            self._inbox_processed(claim, command),
            *self._audit_puts(command),
        )
        if transact(self._client, actions):
            return True
        return self._classify_claimed_failure(claim, command)

    def _snapshot_put(self, command: SnapshotWrite) -> Action:
        put: dict[str, Any] = {
            "TableName": self._table_name,
            "Item": encode_snapshot(command.snapshot),
        }
        if command.expected_version == 0:
            put["ConditionExpression"] = "attribute_not_exists(pk)"
        else:
            put["ConditionExpression"] = "entitlement_version = :expected"
            put["ExpressionAttributeValues"] = {":expected": {"N": str(command.expected_version)}}
        return {"Put": put}

    def _audit_puts(self, command: SnapshotWrite) -> tuple[Action, ...]:
        return tuple(
            put_new(self._table_name, outbox_item(audit_outbox_event(event)))
            for event in command.audit_events
        )

    def _inbox_processed(self, claim: InboxClaim, command: SnapshotWrite) -> Action:
        removed = ", ".join(INBOX_TRANSIENT_ATTRIBUTES)
        return {
            "Update": {
                "TableName": self._table_name,
                "Key": item_key(*stripe_event_key(claim.event_id)),
                "ConditionExpression": "#state = :processing AND attempt = :attempt",
                "UpdateExpression": (
                    f"SET #state = :processed, {INBOX_VERSION_ATTRIBUTE} = :version, "
                    f"{INBOX_PROCESSED_AT_ATTRIBUTE} = :processed_at REMOVE {removed}"
                ),
                "ExpressionAttributeNames": {"#state": INBOX_STATE_ATTRIBUTE},
                "ExpressionAttributeValues": {
                    ":processing": {"S": InboxProcessingState.PROCESSING.value},
                    ":processed": {"S": InboxProcessingState.PROCESSED.value},
                    ":attempt": {"N": str(claim.attempt)},
                    ":version": {"N": str(command.snapshot.entitlement_version)},
                    ":processed_at": {"S": utc_attribute(self._clock())},
                },
            }
        }

    def _snapshot_version(self, command: SnapshotWrite) -> int:
        key = entitlement_snapshot_key(command.snapshot.billing_account_id)
        return _current_version(get_item(self._client, self._table_name, key, True))

    def _classify_claimed_failure(self, claim: InboxClaim, command: SnapshotWrite) -> bool:
        inbox = get_item(self._client, self._table_name, stripe_event_key(claim.event_id), True)
        if _fence_lost(inbox, claim.attempt):
            raise StaleInboxClaim(claim.event_id)
        if self._snapshot_version(command) != command.expected_version:
            return False
        raise RetryableBillingError(AMBIGUOUS_COMMIT_CODE)
