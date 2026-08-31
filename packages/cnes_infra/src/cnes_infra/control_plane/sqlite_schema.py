"""SQLite control-plane schema and serialization."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS memberships (
    tenant_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, user_id)
);
CREATE TABLE IF NOT EXISTS agents (
    tenant_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    state TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, agent_id)
);
CREATE TABLE IF NOT EXISTS jobs (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    file_subtype TEXT NOT NULL,
    competencia TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id)
);
CREATE INDEX IF NOT EXISTS ix_jobs_claimable
ON jobs (tenant_id, agent_id, state, created_at, job_id);
CREATE TABLE IF NOT EXISTS raw_manifests (
    tenant_id TEXT NOT NULL,
    manifest_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    file_subtype TEXT NOT NULL,
    competencia TEXT NOT NULL,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, manifest_id)
);
CREATE TABLE IF NOT EXISTS runs (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    competencia TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id)
);
CREATE TABLE IF NOT EXISTS run_dependencies (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    file_subtype TEXT NOT NULL,
    required INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, run_id, source_type, file_subtype),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS run_units (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    state TEXT NOT NULL,
    lease_until TEXT,
    dispatch_id TEXT,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, unit_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS run_dispatches (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    dispatch_id TEXT NOT NULL,
    wave_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state TEXT NOT NULL,
    lease_until TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id),
    UNIQUE (tenant_id, dispatch_id)
);
CREATE TABLE IF NOT EXISTS idempotency_records (
    tenant_id TEXT NOT NULL,
    scope TEXT NOT NULL,
    key TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, scope, key)
);
CREATE TABLE IF NOT EXISTS dataset_versions (
    tenant_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    version_id TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, dataset_name, version_id)
);
CREATE TABLE IF NOT EXISTS dataset_pointers (
    tenant_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    pointer_name TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, dataset_name, pointer_name)
);
CREATE TABLE IF NOT EXISTS access_requests (
    tenant_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, request_id)
);
CREATE TABLE IF NOT EXISTS outbox_events (
    event_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    data TEXT NOT NULL
);
"""

_NETWORK_FILESYSTEMS = {"9p", "afs", "cifs", "fuse.sshfs", "nfs", "nfs4", "smbfs"}
_NETWORK_PATH_PREFIXES = ("//", "smb:/", "nfs:/", "afp:/", "/net/", "/Network/Servers/")


class _SQLiteWALUnavailable(RuntimeError):
    pass

def serialize_model(model: BaseModel) -> str:
    return json.dumps(model.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def deserialize_model[Model: BaseModel](payload: str, model: type[Model]) -> Model:
    return model.model_validate_json(payload)


def is_network_filesystem(path: Path) -> bool:
    raw_path = str(path).replace("\\", "/")
    if raw_path.startswith(_NETWORK_PATH_PREFIXES):
        return True
    try:
        mounts = Path("/proc/self/mounts").read_text(encoding="utf-8").splitlines()
    except OSError:
        return False
    resolved = path.parent.resolve()
    matches = []
    for line in mounts:
        fields = line.split()
        if len(fields) >= 3 and resolved.is_relative_to(Path(fields[1])):
            matches.append((len(fields[1]), fields[2]))
    return bool(matches and max(matches)[1] in _NETWORK_FILESYSTEMS)


def initialize_schema(connect: Callable[[], sqlite3.Connection], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect()
    try:
        result = connection.execute("PRAGMA journal_mode=WAL").fetchone()
        if result is None or str(result[0]).lower() != "wal":
            raise _SQLiteWALUnavailable("sqlite_wal_unavailable")
        connection.executescript(_SCHEMA)
        connection.commit()
    finally:
        connection.close()
