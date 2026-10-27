"""Conformidade pública de refresh entre SQLite e DynamoDB."""

import json
import logging
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from cnes_domain.ports.refresh_tokens import RefreshTokenStore
from cnes_domain.refresh_tokens import (
    RefreshTokenContext,
    RefreshTokenInput,
    RefreshTokenMutation,
    RefreshTokenRecord,
)
from cnes_infra.auth.dynamodb_refresh_tokens import DynamoDBRefreshTokenStore
from cnes_infra.auth.sqlite_refresh_tokens import SQLiteRefreshTokenStore
from packages.cnes_infra.tests.auth.dynamodb_refresh_support import (
    DynamoBoundary,
    assert_log_privacy,
)

pytest_plugins = ("packages.cnes_infra.tests.auth.dynamodb_refresh_support",)
PLAINTEXT = "abc"
DIGEST = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
TENANT = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
AGENT = "33333333-3333-4333-8333-333333333333"
FINGERPRINT = "synthetic-refresh-ddb-fingerprint"
ISSUED_RAW = "2026-01-02T03:04:05.123456+00:00"
EXPIRES_RAW = "2026-01-03T03:04:05.654321+00:00"
SIBLING_EXPIRES_RAW = "2026-01-04T03:04:05.654321+00:00"
ISSUED = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC)
EXPIRES = datetime(2026, 1, 3, 3, 4, 5, 654321, tzinfo=UTC)
SIBLING_EXPIRES = datetime(2026, 1, 4, 3, 4, 5, 654321, tzinfo=UTC)
IMPORTED_USE = datetime(2026, 1, 2, 4, 5, 6, 234567, tzinfo=UTC)
USE1 = datetime(2026, 1, 2, 6, 0, 0, 100002, tzinfo=UTC)
USE2 = datetime(2026, 1, 2, 6, 0, 0, 100003, tzinfo=UTC)
REVOKE = datetime(2026, 1, 2, 6, 0, 0, 100004, tzinfo=UTC)
LAST_MICROSECOND = datetime(2026, 1, 3, 3, 4, 5, 654320, tzinfo=UTC)
AFTER_EXPIRY = datetime(2026, 1, 3, 3, 4, 5, 654322, tzinfo=UTC)
PUBLIC = {
    "created": RefreshTokenRecord(
        DIGEST, AGENT, TENANT, FINGERPRINT, ISSUED, EXPIRES, None, None, 0
    ),
    "used": RefreshTokenRecord(DIGEST, AGENT, TENANT, FINGERPRINT, ISSUED, EXPIRES, USE1, None, 1),
    "reused": RefreshTokenRecord(
        DIGEST, AGENT, TENANT, FINGERPRINT, ISSUED, EXPIRES, USE2, None, 2
    ),
    "last": RefreshTokenRecord(
        DIGEST, AGENT, TENANT, FINGERPRINT, ISSUED, EXPIRES, LAST_MICROSECOND, None, 1
    ),
    "sibling": RefreshTokenRecord(
        DIGEST, AGENT, OTHER, FINGERPRINT, ISSUED, SIBLING_EXPIRES, IMPORTED_USE, None, 7
    ),
    "inactive": None,
}
RAW_STATES = {
    "created": (None, None, 0),
    "used": ("2026-01-02T06:00:00.100002+00:00", None, 1),
    "reused": ("2026-01-02T06:00:00.100003+00:00", None, 2),
    "revoked": ("2026-01-02T06:00:00.100003+00:00", "2026-01-02T06:00:00.100004+00:00", 3),
    "last": ("2026-01-03T03:04:05.654320+00:00", None, 1),
    "sibling": ("2026-01-02T04:05:06.234567+00:00", None, 7),
}
SCHEMA = """
CREATE TABLE auth_refresh_tokens (
    token_sha256 TEXT NOT NULL, agent_id TEXT NOT NULL, tenant_id TEXT NOT NULL,
    machine_fingerprint TEXT NOT NULL, issued_at TEXT NOT NULL, expires_at TEXT NOT NULL,
    last_used_at TEXT, revoked_at TEXT, version INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, token_sha256)
)
"""
SELECT = (
    "SELECT token_sha256, agent_id, tenant_id, machine_fingerprint, issued_at, expires_at, "
    "last_used_at, revoked_at, version FROM auth_refresh_tokens ORDER BY tenant_id, token_sha256"
)
ASSET = Path(__file__).parents[1] / "fixtures/auth/refresh_dynamodb_cas_schema1.json"
PAIRS: dict[str, Any] = json.loads("".join(json.loads(ASSET.read_text(encoding="utf-8"))))
OBSERVATIONS: list[dict[str, Any]] = []


class SQLiteBoundary:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.connections: list[sqlite3.Connection] = []
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(SCHEMA)
            connection.commit()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        self.connections.append(connection)
        return connection

    def native(self) -> list[Any]:
        with closing(sqlite3.connect(self.path)) as connection:
            return connection.execute(SELECT).fetchall()

    def assert_closed(self) -> None:
        assert self.connections
        for connection in self.connections:
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                connection.execute("SELECT 1")


class CrossCase:
    def __init__(
        self,
        make: Callable[[Callable[[], str]], RefreshTokenStore],
        native: Callable[[], list[Any]],
        backend: str,
    ) -> None:
        self.make, self.native, self.backend = make, native, backend
        self.factory_calls = 0
        self.observations: list[dict[str, Any]] = []

    def create_secret(self) -> str:
        self.factory_calls += 1
        assert self.factory_calls == 1
        return PLAINTEXT

    def forbidden_secret(self) -> str:
        raise AssertionError("unexpected_token_factory")

    def store(self) -> RefreshTokenStore:
        return self.make(self.forbidden_secret)


def dynamodb_native(observer: DynamoBoundary) -> list[Any]:
    items = observer.client.scan(TableName="refresh-tokens", ConsistentRead=True)["Items"]
    return sorted(items, key=lambda item: (item["pk"]["S"], item["sk"]["S"]))


@pytest.fixture(params=["sqlite", "dynamodb"])
def cross_adapter(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> Iterator[CrossCase]:
    caplog.set_level(logging.DEBUG)
    database = None
    boundaries: list[DynamoBoundary] = []
    if request.param == "sqlite":
        database = SQLiteBoundary(tmp_path / "refresh.sqlite3")
        case = CrossCase(
            lambda secret: SQLiteRefreshTokenStore(database.connect, secret),
            database.native,
            "sqlite",
        )
    else:
        original, _ = request.getfixturevalue("dynamodb_case")
        observer = original.independent()
        boundaries.extend((original, observer))

        def make(secret: Callable[[], str]) -> RefreshTokenStore:
            boundary = original.independent()
            boundaries.append(boundary)
            return DynamoDBRefreshTokenStore(boundary.client, "refresh-tokens", secret)

        case = CrossCase(make, lambda: dynamodb_native(observer), "dynamodb")
    try:
        yield case
    finally:
        OBSERVATIONS.append({"nodeid": request.node.nodeid, "states": case.observations})
        assert case.factory_calls == 1
        assert_log_privacy(caplog.records)
        if database is not None:
            database.assert_closed()
        for boundary in boundaries:
            assert boundary.client.describe_table(TableName="refresh-tokens")["Table"]
            boundary.client.close()


def assert_public_state(case: CrossCase, tenant: str, state: str, now: datetime) -> None:
    store = case.store()
    context = RefreshTokenContext(tenant, now)
    assert store.validate(PLAINTEXT, context) == PUBLIC[state]
    assert store.has_active_for_agent(AGENT, context) is (state != "inactive")


def expected_rows(states: tuple[str, ...]) -> list[Any]:
    rows = {
        "created": (DIGEST, AGENT, TENANT, FINGERPRINT, ISSUED_RAW, EXPIRES_RAW),
        "used": (DIGEST, AGENT, TENANT, FINGERPRINT, ISSUED_RAW, EXPIRES_RAW),
        "reused": (DIGEST, AGENT, TENANT, FINGERPRINT, ISSUED_RAW, EXPIRES_RAW),
        "revoked": (DIGEST, AGENT, TENANT, FINGERPRINT, ISSUED_RAW, EXPIRES_RAW),
        "last": (DIGEST, AGENT, TENANT, FINGERPRINT, ISSUED_RAW, EXPIRES_RAW),
        "sibling": (DIGEST, AGENT, OTHER, FINGERPRINT, ISSUED_RAW, SIBLING_EXPIRES_RAW),
    }
    return [rows[state] + RAW_STATES[state] for state in states]


def expected_items(states: tuple[str, ...]) -> list[Any]:
    items = []
    for state in states:
        pair = deepcopy(PAIRS["states"]["other2" if state == "sibling" else "base7"])
        last, revoked, version = RAW_STATES[state]
        for item in pair.values():
            item["last_used_at"] = {"NULL": True} if last is None else {"S": last}
            item["revoked_at"] = {"NULL": True} if revoked is None else {"S": revoked}
            item["version"] = {"N": str(version)}
            if state == "sibling":
                item["token_expires_at"] = {"S": SIBLING_EXPIRES_RAW}
                item["expires_at"] = {"N": "1767495846"}
        items.extend(pair.values())
    return sorted(items, key=lambda item: (item["pk"]["S"], item["sk"]["S"]))


def assert_native_state(case: CrossCase, states: tuple[str, ...]) -> None:
    actual = case.native()
    case.observations.append({"states": states, "native": actual})
    expected = expected_rows(states) if case.backend == "sqlite" else expected_items(states)
    assert actual == expected


def coexist(case: CrossCase) -> tuple[RefreshTokenStore, RefreshTokenStore]:
    first = case.make(case.create_secret)
    second = case.store()
    assert_native_state(case, ())
    assert first.create(RefreshTokenInput(AGENT, TENANT, FINGERPRINT, ISSUED, EXPIRES)) == PLAINTEXT
    assert case.factory_calls == 1
    assert second.validate(PLAINTEXT, RefreshTokenContext(TENANT, USE1)) == PUBLIC["created"]
    assert second.has_active_for_agent(AGENT, RefreshTokenContext(TENANT, USE1)) is True
    assert_public_state(case, OTHER, "inactive", USE1)
    assert_native_state(case, ("created",))
    sibling = PUBLIC["sibling"]
    assert sibling is not None
    second.import_hashed(sibling)
    assert_public_state(case, TENANT, "created", USE1)
    assert_public_state(case, OTHER, "sibling", USE1)
    assert_native_state(case, ("created", "sibling"))
    return first, second


def test_reuso_e_revogacao_isolam_tenants_com_digest_compartilhado(
    cross_adapter: CrossCase,
) -> None:
    case = cross_adapter
    first, second = coexist(case)
    assert first.mark_used(PLAINTEXT, RefreshTokenMutation(TENANT, USE1, 0)) is True
    assert second.validate(PLAINTEXT, RefreshTokenContext(TENANT, USE1)) == PUBLIC["used"]
    assert second.has_active_for_agent(AGENT, RefreshTokenContext(TENANT, USE1)) is True
    assert_public_state(case, OTHER, "sibling", USE1)
    assert_native_state(case, ("used", "sibling"))
    assert second.revoke(PLAINTEXT, RefreshTokenMutation(TENANT, USE2, 0)) is False
    assert_native_state(case, ("used", "sibling"))
    assert second.mark_used(PLAINTEXT, RefreshTokenMutation(TENANT, USE2, 1)) is True
    assert_public_state(case, TENANT, "reused", USE2)
    assert_public_state(case, OTHER, "sibling", USE2)
    assert_native_state(case, ("reused", "sibling"))
    assert case.store().revoke(PLAINTEXT, RefreshTokenMutation(TENANT, REVOKE, 2)) is True
    assert_public_state(case, TENANT, "inactive", REVOKE)
    assert_public_state(case, OTHER, "sibling", REVOKE)
    assert_native_state(case, ("revoked", "sibling"))
    assert case.store().mark_used(PLAINTEXT, RefreshTokenMutation(TENANT, REVOKE, 3)) is False
    assert case.store().revoke(PLAINTEXT, RefreshTokenMutation(TENANT, REVOKE, 3)) is False
    assert_public_state(case, TENANT, "inactive", REVOKE)
    assert_public_state(case, OTHER, "sibling", REVOKE)
    assert_native_state(case, ("revoked", "sibling"))


def test_uso_reaberto_preserva_expiracao_logica_e_tenant_irmao(cross_adapter: CrossCase) -> None:
    case = cross_adapter
    coexist(case)
    assert (
        case.store().mark_used(PLAINTEXT, RefreshTokenMutation(TENANT, LAST_MICROSECOND, 0)) is True
    )
    assert_public_state(case, TENANT, "last", LAST_MICROSECOND)
    assert_public_state(case, OTHER, "sibling", LAST_MICROSECOND)
    assert_native_state(case, ("last", "sibling"))
    assert_public_state(case, TENANT, "inactive", EXPIRES)
    assert_public_state(case, OTHER, "sibling", EXPIRES)
    assert case.store().mark_used(PLAINTEXT, RefreshTokenMutation(TENANT, EXPIRES, 1)) is False
    assert case.store().revoke(PLAINTEXT, RefreshTokenMutation(TENANT, EXPIRES, 1)) is False
    assert_public_state(case, TENANT, "inactive", AFTER_EXPIRY)
    assert_public_state(case, OTHER, "sibling", AFTER_EXPIRY)
    assert_native_state(case, ("last", "sibling"))
