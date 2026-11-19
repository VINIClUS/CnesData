"""Fábricas de refresh vazias falham antes da persistência SQLite e DynamoDB."""

import json
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import boto3
import pytest
from moto import mock_aws

from cnes_domain.ports.refresh_tokens import RefreshTokenStore
from cnes_domain.refresh_tokens import (
    RefreshTokenContext,
    RefreshTokenInput,
    RefreshTokenRecord,
    RefreshTokenUnavailable,
)
from cnes_infra.auth.dynamodb_refresh_tokens import DynamoDBRefreshTokenStore
from cnes_infra.auth.sqlite_refresh_tokens import SQLiteRefreshTokenStore

if TYPE_CHECKING:
    from botocore.client import BaseClient

_TABLE = "refresh-factory-contract"
_TENANT = "11111111-1111-4111-8111-111111111111"
_AGENT = "33333333-3333-4333-8333-333333333333"
_DIGEST = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
_INPUT = RefreshTokenInput(
    _AGENT,
    _TENANT,
    "factory-contract-host",
    datetime(2026, 1, 2, 3, tzinfo=UTC),
    datetime(2026, 1, 3, 3, tzinfo=UTC),
)
_SCHEMA = """
CREATE TABLE auth_refresh_tokens (
    token_sha256 TEXT NOT NULL, agent_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
    machine_fingerprint TEXT NOT NULL, issued_at TEXT NOT NULL, expires_at TEXT NOT NULL,
    last_used_at TEXT, revoked_at TEXT, version INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, token_sha256)
)
"""
_SELECT = (
    "SELECT token_sha256, agent_id, tenant_id, machine_fingerprint, issued_at, expires_at, "
    "last_used_at, revoked_at, version FROM auth_refresh_tokens ORDER BY tenant_id, token_sha256"
)
_WRITES = {
    "put_item",
    "update_item",
    "delete_item",
    "batch_write_item",
    "transact_write_items",
    "execute_statement",
    "batch_execute_statement",
    "execute_transaction",
}


class _SQLiteBoundary:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.attempts = 0
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(_SCHEMA)
            connection.commit()

    def connect(self) -> sqlite3.Connection:
        self.attempts += 1
        return sqlite3.connect(self.path)

    def snapshot(self) -> list[tuple[object, ...]]:
        with closing(sqlite3.connect(self.path)) as connection:
            return connection.execute(_SELECT).fetchall()

    def store(self, factory: Callable[[], str]) -> RefreshTokenStore:
        return SQLiteRefreshTokenStore(self.connect, factory)


class _DynamoBoundary:
    def __init__(self, client: Any) -> None:
        self.client = client
        self.attempts = 0

    def __getattr__(self, name: str) -> Any:
        operation = getattr(self.client, name)
        if not callable(operation):
            return operation

        def invoke(**request: Any) -> Any:
            if name in _WRITES:
                self.attempts += 1
            return operation(**request)

        return invoke

    def snapshot(self) -> list[dict[str, Any]]:
        return sorted(
            self.client.scan(TableName=_TABLE, ConsistentRead=True)["Items"],
            key=lambda item: (item["pk"]["S"], item["sk"]["S"]),
        )

    def store(self, factory: Callable[[], str]) -> RefreshTokenStore:
        return DynamoDBRefreshTokenStore(cast("BaseClient", self), _TABLE, factory)


@contextmanager
def _boundary(backend: str, path: Path) -> Iterator[_SQLiteBoundary | _DynamoBoundary]:
    if backend == "sqlite":
        yield _SQLiteBoundary(path)
        return
    with mock_aws():
        client = boto3.client(
            "dynamodb",
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",  # noqa: S106
        )
        client.create_table(
            TableName=_TABLE,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        yield _DynamoBoundary(client)


def _positive_control(boundary: _SQLiteBoundary | _DynamoBoundary) -> None:
    store = boundary.store(lambda: "abc")
    assert store.create(_INPUT) == "abc"
    assert store.validate("abc", RefreshTokenContext(_TENANT, _INPUT.issued_at)) == (
        RefreshTokenRecord(
            _DIGEST,
            _AGENT,
            _TENANT,
            _INPUT.machine_fingerprint,
            _INPUT.issued_at,
            _INPUT.expires_at,
            None,
            None,
            0,
        )
    )
    assert boundary.snapshot()
    assert boundary.attempts > 0


@pytest.mark.parametrize("backend", ["sqlite", "dynamodb"])
def test_rejeita_segredo_vazio_antes_de_persistir(
    backend: str,
    tmp_path: Path,
    record_property: Callable[[str, object], None],
) -> None:
    with _boundary(backend, tmp_path / "refresh.sqlite") as boundary:
        _positive_control(boundary)
        before = boundary.snapshot()
        attempts = boundary.attempts
        error = None
        try:
            boundary.store(lambda: "").create(_INPUT)
        except RefreshTokenUnavailable as unavailable:
            error = unavailable
        after = boundary.snapshot()
        record_property("native_before", json.dumps(before, sort_keys=True))
        record_property("native_after", json.dumps(after, sort_keys=True))
        record_property("persistence_attempts", boundary.attempts - attempts)
        record_property("public_error_args", repr(error.args if error else None))
        assert after == before
        assert boundary.attempts == attempts
        assert type(error) is RefreshTokenUnavailable
        assert error.args == ("operation=create_secret",)
