"""Registro condicional de agentes Edge nos backends raw."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from hashlib import sha256
from typing import TYPE_CHECKING, Any

from cnes_domain.control_plane.entities import Agent, IdempotencyRecord
from cnes_domain.control_plane.enums import AgentState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.control_plane.dynamodb_codec import (
    Item,
    decode_model,
    encode_model,
    payload,
    put_action,
)
from cnes_infra.control_plane.dynamodb_keys import entity_key, idempotency_key
from cnes_infra.control_plane.edge_capacity import (
    RESERVATION_EXPIRED,
    consume_reservation_actions,
    usable_agent_reservation,
)
from cnes_infra.control_plane.sqlite_schema import deserialize_model, serialize_model

if TYPE_CHECKING:
    from datetime import datetime

EDGE_AGENT_SCOPE = "edge_agent.register"
_IDEMPOTENCY_TTL = timedelta(days=1)


@dataclass(frozen=True, slots=True)
class EntitlementFence:
    billing_account_id: str
    entitlement_version: int


@dataclass(frozen=True, slots=True)
class NewEdgeAgent:
    tenant_id: str
    agent_id: str
    fingerprint: str
    now: datetime
    reservation_id: str
    fence: EntitlementFence | None = None


@dataclass(frozen=True, slots=True)
class EdgeAgentCreation:
    agent: Agent
    created: bool


def _creation_record(command: NewEdgeAgent) -> IdempotencyRecord:
    digest = sha256(f"{command.tenant_id}\x1f{command.agent_id}".encode()).hexdigest()
    return IdempotencyRecord(
        tenant_id=command.tenant_id, scope=EDGE_AGENT_SCOPE, key=command.reservation_id,
        request_hash=digest, status="COMPLETED", resource_id=command.agent_id,
        created_at=command.now, expires_at=command.now + _IDEMPOTENCY_TTL,
    )


def _new_agent(command: NewEdgeAgent) -> Agent:
    return edge_agent(
        None, command.tenant_id, command.agent_id, command.fingerprint, command.now
    )


def edge_agent(current: Agent | None, tenant_id: str, agent_id: str,
               fingerprint: str, now: datetime) -> Agent:
    if current is not None and current.state is AgentState.REVOKED:
        raise Conflict(ErrorCode.AGENT_REVOKED)
    if current is not None:
        return current.model_copy(update={
            "certificate_fingerprint": fingerprint, "last_seen_at": now,
        })
    return Agent(
        tenant_id=tenant_id, agent_id=agent_id, state=AgentState.ACTIVE,
        version="unknown", certificate_fingerprint=fingerprint,
        last_seen_at=now, created_at=now,
    )


class DynamoEdgeRegistrationMixin:
    def register_edge_agent(
        self, tenant_id: str, agent_id: str, fingerprint: str, now: datetime
    ) -> Agent:
        key = entity_key(tenant_id, "AGENT", agent_id)
        for _ in range(3):
            current_item = self._get_item(key)
            current = decode_model(current_item, Agent) if current_item else None
            agent = edge_agent(current, tenant_id, agent_id, fingerprint, now)
            try:
                self._transact((put_action(
                    self._table_name, encode_model(agent, "AGENT", key),
                    payload(current_item) if current_item else None,
                ),))
            except Conflict:
                continue
            return agent
        raise Conflict(ErrorCode.TRANSACTION_CONFLICT)

    def create_edge_agent(self, command: NewEdgeAgent) -> EdgeAgentCreation:
        """Cria o agente novo e o registro de idempotência da reserva.

        Args: command: Agente novo e reserva de capacidade que o originou.
        Returns: Agente gravado; created indica se esta reserva o criou.
        Raises: Conflict: Cancelamento condicional sem agente presente.
        """
        agent = _new_agent(command)
        key = entity_key(command.tenant_id, "AGENT", command.agent_id)
        actions = (
            put_action(self._table_name, encode_model(agent, "AGENT", key), None),
            put_action(self._table_name, self._creation_item(command), None),
        )
        try:
            self._transact(actions + self._fence_actions(command))
        except Conflict:
            return self._existing_creation(command, key)
        return EdgeAgentCreation(agent=agent, created=True)

    def _fence_actions(self, command: NewEdgeAgent) -> tuple[Item, ...]:
        from cnes_infra.billing.dynamodb_quota_items import SnapshotExpectation, snapshot_check

        fence = command.fence
        if fence is None:
            return ()
        now = self._clock()
        item = self._usable_reservation(command, now)
        expected = SnapshotExpectation(fence.billing_account_id, fence.entitlement_version, None)
        return (
            snapshot_check(self._table_name, expected, now),
            *consume_reservation_actions(self._table_name, item, now),
        )

    def _usable_reservation(self, command: NewEdgeAgent, now: datetime) -> Item:
        from cnes_domain.billing.errors import RetryableBillingError
        from cnes_infra.billing.keys import capacity_reservation_key

        fence = command.fence
        item = self._get_item(capacity_reservation_key(fence.billing_account_id,
                                                       command.reservation_id))
        if not usable_agent_reservation(item, command, now):
            raise RetryableBillingError(RESERVATION_EXPIRED)
        return item

    def _creation_item(self, command: NewEdgeAgent) -> Item:
        record = _creation_record(command)
        key = idempotency_key(record.tenant_id, record.scope, record.key)
        # No TTL attribute: capacity recovery reads this marker as durable ownership proof.
        return encode_model(record, "IDEMPOTENCYRECORD", key)

    def _existing_creation(
        self, command: NewEdgeAgent, key: tuple[str, str]
    ) -> EdgeAgentCreation:
        stored = self._get_item(key)
        if stored is None:
            self._raise_fence_failure(command)
            raise Conflict(ErrorCode.TRANSACTION_CONFLICT)
        marker = self._get_item(
            idempotency_key(command.tenant_id, EDGE_AGENT_SCOPE, command.reservation_id)
        )
        created = (
            marker is not None
            and decode_model(marker, IdempotencyRecord).resource_id == command.agent_id
        )
        return EdgeAgentCreation(agent=decode_model(stored, Agent), created=created)

    def _raise_fence_failure(self, command: NewEdgeAgent) -> None:
        from cnes_domain.billing.errors import EntitlementDenied
        from cnes_infra.billing.dynamodb_items import decode_snapshot
        from cnes_infra.billing.keys import entitlement_snapshot_key

        fence = command.fence
        if fence is None:
            return
        account = fence.billing_account_id
        item = self._get_item(entitlement_snapshot_key(account))
        snapshot = None if item is None else decode_snapshot(item, account)
        if snapshot is None or (
            snapshot.entitlement_version != fence.entitlement_version
            or snapshot.valid_until <= self._clock()
        ):
            raise EntitlementDenied("reason=snapshot_changed")
        self._usable_reservation(command, self._clock())


class SQLiteEdgeRegistrationMixin:
    def register_edge_agent(
        self, tenant_id: str, agent_id: str, fingerprint: str, now: datetime
    ) -> Agent:
        with self.write_transaction() as connection:
            current = self.get_agent_record(connection, tenant_id, agent_id)
            agent = edge_agent(current, tenant_id, agent_id, fingerprint, now)
            connection.execute(
                "INSERT INTO agents (tenant_id, agent_id, state, data) VALUES (?, ?, ?, ?) "
                "ON CONFLICT (tenant_id, agent_id) DO UPDATE SET data = excluded.data",
                (tenant_id, agent_id, agent.state.value, serialize_model(agent)),
            )
            return agent

    def create_edge_agent(self, command: NewEdgeAgent) -> EdgeAgentCreation:
        """Cria o agente novo e o registro de idempotência da reserva.

        Args: command: Agente novo e reserva de capacidade que o originou.
        Returns: Agente gravado; created indica se esta reserva o criou.
        """
        with self.write_transaction() as connection:
            stored = self.get_agent_record(connection, command.tenant_id, command.agent_id)
            if stored is not None:
                return self._existing_creation(connection, command, stored)
            agent = _new_agent(command)
            record = _creation_record(command)
            connection.execute(
                "INSERT INTO agents (tenant_id, agent_id, state, data) VALUES (?, ?, ?, ?)",
                (agent.tenant_id, agent.agent_id, agent.state.value, serialize_model(agent)),
            )
            connection.execute(
                "INSERT INTO idempotency_records (tenant_id, scope, key, data) "
                "VALUES (?, ?, ?, ?)",
                (record.tenant_id, record.scope, record.key, serialize_model(record)),
            )
            return EdgeAgentCreation(agent=agent, created=True)

    def _existing_creation(
        self, connection: Any, command: NewEdgeAgent, stored: Agent
    ) -> EdgeAgentCreation:
        row = connection.execute(
            "SELECT data FROM idempotency_records WHERE tenant_id = ? AND scope = ? AND key = ?",
            (command.tenant_id, EDGE_AGENT_SCOPE, command.reservation_id),
        ).fetchone()
        created = (
            row is not None
            and deserialize_model(row[0], IdempotencyRecord).resource_id == command.agent_id
        )
        return EdgeAgentCreation(agent=stored, created=created)
