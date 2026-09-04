"""DynamoDB idempotency, publication, and outbox transactions."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cnes_domain.control_plane.commands import (
    BeginIdempotency,
    IdempotencyOutcome,
    PublishDataset,
)
from cnes_domain.control_plane.entities import (
    DatasetPointer,
    DatasetVersion,
    IdempotencyRecord,
    OutboxEvent,
    Run,
)
from cnes_domain.control_plane.enums import RunState
from cnes_domain.control_plane.errors import Conflict, NotFound
from cnes_domain.control_plane.transitions import transition_run
from cnes_infra.control_plane.dynamodb_codec import (
    Action,
    Item,
    decode_model,
    encode_model,
    payload,
    put_action,
)
from cnes_infra.control_plane.dynamodb_keys import (
    idempotency_key,
    key_component,
    outbox_key,
    pointer_key,
    run_entity_key,
    timestamp,
    version_key,
)

if TYPE_CHECKING:
    from datetime import datetime


class DynamoDBPublication:
    """Implementa idempotência, publicação atômica e entrega de outbox."""

    def _idempotency_item(self, record: IdempotencyRecord) -> Item:
        key = idempotency_key(record.tenant_id, record.scope, record.key)
        item = encode_model(record, "IDEMPOTENCYRECORD", key)
        item["expires_at"] = {"N": str(int(record.expires_at.timestamp()))}
        return item

    def _version_item(self, version: DatasetVersion) -> Item:
        key = version_key(version.tenant_id, version.dataset_name, version.version_id)
        return encode_model(version, "DATASETVERSION", key)

    def _pointer_item(self, pointer: DatasetPointer) -> Item:
        key = pointer_key(pointer.tenant_id, pointer.dataset_name, pointer.pointer_name)
        return encode_model(pointer, "DATASETPOINTER", key)

    def _outbox_item(self, event: OutboxEvent) -> Item:
        attributes = {}
        if event.delivered_at is None:
            attributes = {
                "gsi6pk": "OUTBOX#PENDING",
                "gsi6sk": f"{timestamp(event.created_at)}#{key_component(event.event_id)}",
            }
        return encode_model(event, "OUTBOXEVENT", outbox_key(event.event_id), attributes)

    def _event_action(self, tenant_id: str, event: OutboxEvent) -> Action:
        if event.delivered_at is not None:
            raise Conflict("event_delivery_conflict")
        if event.tenant_id != tenant_id:
            raise Conflict("event_tenant_conflict")
        existing = self._get_outbox_event(event.event_id)
        if existing is not None:
            raise Conflict("event_id_conflict")
        return put_action(self._table_name, self._outbox_item(event), None)

    def begin_idempotency(self, command: BeginIdempotency) -> IdempotencyOutcome:
        """Inicia ou reproduz uma operação idempotente."""
        key = idempotency_key(command.tenant_id, command.scope, command.key)
        item = self._get_item(key)
        outcome = self._existing_idempotency(item, command)
        if outcome is not None:
            return outcome
        record = IdempotencyRecord(
            tenant_id=command.tenant_id,
            scope=command.scope,
            key=command.key,
            request_hash=command.request_hash,
            status="STARTED",
            resource_id=command.resource_id,
            created_at=command.now,
            expires_at=command.expires_at,
        )
        expected = payload(item) if item is not None else None
        try:
            self._transact(
                (put_action(self._table_name, self._idempotency_item(record), expected),)
            )
        except Conflict:
            current = self._get_item(key)
            replay = self._existing_idempotency(current, command)
            if replay is not None:
                return replay
            raise
        return IdempotencyOutcome(record=record, created=True)

    @staticmethod
    def _existing_idempotency(
        item: Item | None, command: BeginIdempotency
    ) -> IdempotencyOutcome | None:
        if item is None:
            return None
        record = decode_model(item, IdempotencyRecord)
        if record.expires_at <= command.now:
            return None
        if record.request_hash != command.request_hash:
            raise Conflict("idempotency_hash_conflict")
        return IdempotencyOutcome(record=record, created=False)

    def publish_dataset(self, command: PublishDataset) -> DatasetPointer:
        """Publica versão, ponteiro, run e evento atomicamente."""
        run_key = run_entity_key(command.version.tenant_id, command.version.run_id)
        run_item = self._get_item(run_key)
        if run_item is None:
            raise NotFound("run_missing")
        run = decode_model(run_item, Run)
        self._validate_publication_run(command, run)
        replay = self._publication_replay(command, run)
        if replay is not None:
            return replay
        if run.state is not RunState.PUBLISHING:
            raise Conflict("run_not_publishing")
        pointer_key_value = pointer_key(
            command.version.tenant_id, command.version.dataset_name, command.pointer_name
        )
        pointer_item = self._get_item(pointer_key_value)
        current = decode_model(pointer_item, DatasetPointer) if pointer_item else None
        current_version = current.version_id if current is not None else None
        if current_version != command.expected_version_id:
            raise Conflict("pointer_version_conflict")
        pointer = DatasetPointer(
            tenant_id=command.version.tenant_id,
            dataset_name=command.version.dataset_name,
            pointer_name=command.pointer_name,
            version_id=command.version.version_id,
            updated_at=self._clock(),
        )
        updated_run = transition_run(run, command.final_state).model_copy(
            update={"missing_sources": command.missing_sources}
        )
        expected_pointer = payload(pointer_item) if pointer_item is not None else None
        actions = (
            put_action(self._table_name, self._version_item(command.version), None),
            put_action(self._table_name, self._pointer_item(pointer), expected_pointer),
            put_action(self._table_name, self._run_item(updated_run), payload(run_item)),
            self._event_action(command.version.tenant_id, command.event),
        )
        try:
            self._transact(actions)
        except Conflict:
            winner_item = self._get_item(run_key)
            if winner_item is None:
                raise
            winner = self._publication_replay(command, decode_model(winner_item, Run))
            if winner is None:
                raise
            return winner
        return pointer

    @staticmethod
    def _validate_publication_run(command: PublishDataset, run: Run) -> None:
        expected_key = (
            f"reconciliation/{run.tenant_id}/{run.competencia}/{run.run_id}/run-manifest.json"
        )
        if (command.version.dataset_name, command.version.run_manifest_key) != (
            run.dataset_name,
            expected_key,
        ):
            raise Conflict("publication_run_conflict")

    def _publication_replay(self, command: PublishDataset, run: Run) -> DatasetPointer | None:
        terminal = {RunState.PUBLISHED, RunState.PUBLISHED_DEGRADED}
        if run.state not in terminal:
            return None
        version = self.get_dataset_version(
            command.version.tenant_id,
            command.version.dataset_name,
            command.version.version_id,
        )
        key = pointer_key(
            command.version.tenant_id, command.version.dataset_name, command.pointer_name
        )
        pointer = self._get_model(key, DatasetPointer)
        event = self._get_outbox_event(command.event.event_id)
        exact = (
            run.state is command.final_state
            and run.missing_sources == command.missing_sources
            and version == command.version
            and pointer is not None
            and pointer.version_id == command.version.version_id
            and self._event_replay_matches(event, command.event)
        )
        if not exact:
            raise Conflict("publication_replay_conflict")
        return pointer

    def get_dataset_pointer(self, tenant_id: str, dataset_name: str) -> DatasetPointer | None:
        """Retorna o ponteiro current do dataset."""
        key = pointer_key(tenant_id, dataset_name, "current")
        return self._get_model(key, DatasetPointer)

    def get_dataset_version(
        self, tenant_id: str, dataset_name: str, version_id: str
    ) -> DatasetVersion | None:
        """Retorna uma versão publicada do dataset."""
        key = version_key(tenant_id, dataset_name, version_id)
        return self._get_model(key, DatasetVersion)

    def _get_outbox_event(self, event_id: str) -> OutboxEvent | None:
        return self._get_model(outbox_key(event_id), OutboxEvent)

    @staticmethod
    def _event_replay_matches(current: OutboxEvent | None, event: OutboxEvent) -> bool:
        if current is None:
            return False
        return current.model_copy(update={"delivered_at": event.delivered_at}) == event

    def _require_event_replay(self, tenant_id: str, event: OutboxEvent) -> None:
        current = self._get_outbox_event(event.event_id)
        if event.tenant_id != tenant_id or not self._event_replay_matches(current, event):
            raise Conflict("event_id_conflict")

    def get_outbox_event(self, event_id: str) -> OutboxEvent | None:
        """Retorna um evento pela identidade global."""
        return self._get_outbox_event(event_id)

    def _pending_outbox_event(self, candidate: Item, key: tuple[str, str]) -> OutboxEvent | None:
        item = self._get_item(key)
        if item is None or item.get("entity", {}).get("S") != "OUTBOXEVENT":
            return None
        event = decode_model(item, OutboxEvent)
        current_index = (item.get("gsi6pk"), item.get("gsi6sk"))
        candidate_index = (candidate.get("gsi6pk"), candidate.get("gsi6sk"))
        if event.delivered_at is not None or current_index != candidate_index:
            return None
        return event

    def pending_outbox(self, limit: int) -> tuple[OutboxEvent, ...]:
        """Lista eventos pendentes globalmente."""
        if limit <= 0:
            return ()
        request = {
            "TableName": self._table_name,
            "IndexName": "gsi6",
            "KeyConditionExpression": "gsi6pk = :partition",
            "ExpressionAttributeValues": {":partition": {"S": "OUTBOX#PENDING"}},
            "ScanIndexForward": True,
        }
        events = []
        seen = set()
        while True:
            request["Limit"] = limit - len(events)
            response = self._client.query(**request)
            for candidate in response.get("Items", ()):
                key = (
                    candidate.get("base_pk", candidate["pk"])["S"],
                    candidate.get("base_sk", candidate["sk"])["S"],
                )
                if key in seen:
                    continue
                event = self._pending_outbox_event(candidate, key)
                if event is None:
                    continue
                seen.add(key)
                events.append(event)
                if len(events) == limit:
                    return tuple(events)
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                return tuple(events)
            request["ExclusiveStartKey"] = last_key

    def mark_outbox_delivered(self, event_id: str, delivered_at: datetime) -> None:
        """Marca um evento global como entregue."""
        key = outbox_key(event_id)
        item = self._get_item(key)
        if item is None:
            raise NotFound("outbox_event_missing")
        event = decode_model(item, OutboxEvent)
        if event.delivered_at is not None:
            if event.delivered_at != delivered_at:
                raise Conflict("outbox_delivery_conflict")
            return
        updated = event.model_copy(update={"delivered_at": delivered_at})
        self._transact((put_action(self._table_name, self._outbox_item(updated), payload(item)),))
