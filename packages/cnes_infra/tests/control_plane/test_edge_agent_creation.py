"""Criação idempotente de agente Edge ancorada na reserva de billing."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any, cast

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from cnes_domain.control_plane.entities import Agent
from cnes_domain.control_plane.enums import AgentState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import idempotency_key
from cnes_infra.control_plane.edge_registration import (
    EDGE_AGENT_SCOPE,
    EdgeAgentCreation,
    NewEdgeAgent,
)
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import _create_table

NOW = datetime(2026, 9, 26, tzinfo=UTC)
TENANT = "354130"
AGENT = "agent-1"
FINGERPRINT = "a" * 64
TABLE = "cnesdata-control-plane"


def _command(reservation: str = "res-1") -> NewEdgeAgent:
    return NewEdgeAgent(
        tenant_id=TENANT, agent_id=AGENT, fingerprint=FINGERPRINT,
        now=NOW, reservation_id=reservation,
    )


class _ConflictingClient:
    def __init__(self, client: Any, before: Any = None, cancel: bool = False) -> None:
        self._client = client
        self._before = before
        self._cancel = cancel

    def __getattr__(self, name: str) -> Any:
        return getattr(self._client, name)

    def transact_write_items(self, **request: Any) -> None:
        if self._before is not None:
            before, self._before = self._before, None
            before()
        if self._cancel:
            raise ClientError(
                {
                    "Error": {"Code": "TransactionCanceledException"},
                    "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
                },
                "TransactWriteItems",
            )
        self._client.transact_write_items(**request)


class _Backend:
    def __init__(self, kind: str, tmp_path: Any, raw_client: Any = None) -> None:
        self.kind = kind
        self.raw_client = raw_client
        self.tmp_path = tmp_path
        self.adapter = self.build(raw_client)

    def build(self, client: Any) -> Any:
        if self.kind == "sqlite":
            adapter = SQLiteControlPlane(self.tmp_path / "raw.db", lambda: NOW)
            adapter.initialize()
            return adapter
        return DynamoDBControlPlane(client, TABLE, lambda: NOW)

    def agents_stored(self) -> int:
        if self.kind == "sqlite":
            with self.adapter.read_connection() as connection:
                return connection.execute("SELECT COUNT(*) FROM agents").fetchone()[0]
        items = self.raw_client.scan(TableName=TABLE)["Items"]
        return sum(1 for item in items if item["entity"]["S"] == "AGENT")

    def idempotency_resource(self, reservation: str) -> str | None:
        if self.kind == "sqlite":
            with self.adapter.read_connection() as connection:
                row = connection.execute(
                    "SELECT data FROM idempotency_records "
                    "WHERE tenant_id = ? AND scope = ? AND key = ?",
                    (TENANT, EDGE_AGENT_SCOPE, reservation),
                ).fetchone()
            return None if row is None else _resource_id(row[0])
        pk, sk = idempotency_key(TENANT, EDGE_AGENT_SCOPE, reservation)
        item = self.raw_client.get_item(
            TableName=TABLE, Key={"pk": {"S": pk}, "sk": {"S": sk}}, ConsistentRead=True
        ).get("Item")
        return None if item is None else _resource_id(item["payload"]["S"])


def _resource_id(payload: str) -> str:
    from cnes_domain.control_plane.entities import IdempotencyRecord

    return IdempotencyRecord.model_validate_json(payload).resource_id


@pytest.fixture(params=["sqlite", "dynamodb"])
def backend(request, tmp_path) -> Iterator[_Backend]:
    if request.param == "sqlite":
        yield _Backend("sqlite", tmp_path)
        return
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        yield _Backend("dynamodb", tmp_path, client)


def test_cria_agente_novo_com_reserva_como_chave_de_idempotencia(backend) -> None:
    result = backend.adapter.create_edge_agent(_command())

    assert result.created is True
    assert result.agent == Agent(
        tenant_id=TENANT, agent_id=AGENT, state=AgentState.ACTIVE, version="unknown",
        certificate_fingerprint=FINGERPRINT, last_seen_at=NOW, created_at=NOW,
    )
    assert backend.adapter.get_agent(TENANT, AGENT) == result.agent
    assert backend.idempotency_resource("res-1") == AGENT


def test_registro_de_idempotencia_tem_hash_e_expiracao(backend) -> None:
    backend.adapter.create_edge_agent(_command())

    if backend.kind == "sqlite":
        with backend.adapter.read_connection() as connection:
            data = connection.execute("SELECT data FROM idempotency_records").fetchone()[0]
    else:
        pk, sk = idempotency_key(TENANT, EDGE_AGENT_SCOPE, "res-1")
        data = backend.raw_client.get_item(
            TableName=TABLE, Key={"pk": {"S": pk}, "sk": {"S": sk}}
        )["Item"]["payload"]["S"]
    from cnes_domain.control_plane.entities import IdempotencyRecord

    record = IdempotencyRecord.model_validate_json(data)
    assert record.request_hash == sha256(f"{TENANT}\x1f{AGENT}".encode()).hexdigest()
    assert record.status == "COMPLETED"
    assert record.expires_at == NOW + timedelta(days=1)


def test_replay_da_mesma_reserva_devolve_created_true(backend) -> None:
    first = backend.adapter.create_edge_agent(_command())
    replay = backend.adapter.create_edge_agent(_command())

    assert replay == EdgeAgentCreation(agent=first.agent, created=True)
    assert backend.agents_stored() == 1


def test_agente_criado_por_outra_reserva_devolve_created_false(backend) -> None:
    backend.adapter.create_edge_agent(_command("res-1"))
    result = backend.adapter.create_edge_agent(_command("res-2"))

    assert result.created is False
    assert result.agent.agent_id == AGENT
    assert backend.idempotency_resource("res-2") is None


def test_agente_preexistente_via_upsert_devolve_created_false(backend) -> None:
    backend.adapter.register_edge_agent(TENANT, AGENT, FINGERPRINT, NOW)
    result = backend.adapter.create_edge_agent(_command())

    assert result.created is False
    assert backend.idempotency_resource("res-1") is None


def test_agente_revogado_preexistente_devolve_created_false(backend) -> None:
    created = backend.adapter.register_edge_agent(TENANT, AGENT, FINGERPRINT, NOW)
    backend.adapter.put_agent(created.model_copy(update={"state": AgentState.REVOKED}))

    result = backend.adapter.create_edge_agent(_command())

    assert result.created is False
    assert result.agent.state is AgentState.REVOKED


def test_corrida_de_duas_reservas_cria_um_unico_agente() -> None:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        winner = DynamoDBControlPlane(client, TABLE, lambda: NOW)
        racing = DynamoDBControlPlane(
            _ConflictingClient(client, before=lambda: winner.create_edge_agent(_command("res-2"))),
            TABLE,
            lambda: NOW,
        )

        loser = racing.create_edge_agent(_command("res-1"))

        assert loser.created is False
        items = cast("Any", client.scan(TableName=TABLE)["Items"])
        assert sum(1 for item in items if item["entity"]["S"] == "AGENT") == 1
        assert not any(item["entity"]["S"] == "IDEMPOTENCYRECORD"
                       and item["sk"]["S"].endswith("res-1") for item in items)


def test_corrida_da_mesma_reserva_devolve_created_true() -> None:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        winner = DynamoDBControlPlane(client, TABLE, lambda: NOW)
        racing = DynamoDBControlPlane(
            _ConflictingClient(client, before=lambda: winner.create_edge_agent(_command())),
            TABLE,
            lambda: NOW,
        )

        assert racing.create_edge_agent(_command()).created is True


def test_conflito_sem_agente_levanta_conflict() -> None:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        adapter = DynamoDBControlPlane(_ConflictingClient(client, cancel=True), TABLE, lambda: NOW)

        with pytest.raises(Conflict) as raised:
            adapter.create_edge_agent(_command())

        assert raised.value.code is ErrorCode.TRANSACTION_CONFLICT
        assert client.scan(TableName=TABLE)["Items"] == []


def test_upsert_existente_continua_igual(backend) -> None:
    first = backend.adapter.register_edge_agent(TENANT, AGENT, FINGERPRINT, NOW)
    rotated = backend.adapter.register_edge_agent(TENANT, AGENT, "b" * 64, NOW)
    assert first.certificate_fingerprint == FINGERPRINT
    assert rotated.certificate_fingerprint == "b" * 64

    backend.adapter.put_agent(rotated.model_copy(update={"state": AgentState.REVOKED}))
    with pytest.raises(Conflict) as raised:
        backend.adapter.register_edge_agent(TENANT, AGENT, "c" * 64, NOW)
    assert raised.value.code is ErrorCode.AGENT_REVOKED


def test_marcador_de_posse_do_agente_nao_expira_por_ttl() -> None:
    with mock_aws():
        client = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(client)
        DynamoDBControlPlane(client, TABLE, lambda: NOW).create_edge_agent(_command())

        pk, sk = idempotency_key(TENANT, EDGE_AGENT_SCOPE, "res-1")
        item = cast("Any", client.get_item(
            TableName=TABLE, Key={"pk": {"S": pk}, "sk": {"S": sk}}, ConsistentRead=True,
        ))["Item"]

    assert "expires_at" not in item
