"""Registro condicional de agentes Edge nos backends raw."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cnes_domain.control_plane.entities import Agent
from cnes_domain.control_plane.enums import AgentState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.control_plane.dynamodb_codec import decode_model, encode_model, payload, put_action
from cnes_infra.control_plane.dynamodb_keys import entity_key
from cnes_infra.control_plane.sqlite_schema import serialize_model

if TYPE_CHECKING:
    from datetime import datetime


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
