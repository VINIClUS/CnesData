"""SQLite control-plane schema and serialization."""
from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from cnes_domain.control_plane.entities import RunDispatch
from cnes_domain.control_plane.enums import JobState
from cnes_domain.control_plane.errors import Conflict

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
    lease_until TEXT,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id)
);
CREATE INDEX IF NOT EXISTS ix_jobs_claimable_v2
ON jobs (tenant_id, agent_id, state, lease_until, created_at, job_id);
CREATE TABLE IF NOT EXISTS job_creation_writes (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    job_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, job_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS job_terminal_writes (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, job_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS job_cancellation_writes (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id),
    FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, job_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS raw_manifests (
    tenant_id TEXT NOT NULL,
    manifest_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    file_subtype TEXT NOT NULL,
    competencia TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    base_snapshot_id TEXT,
    sequence INTEGER NOT NULL,
    previous_manifest_sha256 TEXT,
    manifest_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, manifest_id)
);
CREATE INDEX IF NOT EXISTS ix_raw_manifest_heads
ON raw_manifests (
    tenant_id, source_type, file_subtype, competencia,
    created_at DESC, agent_id DESC, snapshot_id DESC, manifest_id DESC
);
CREATE INDEX IF NOT EXISTS ix_raw_manifest_ancestry
ON raw_manifests (
    tenant_id, source_type, file_subtype, competencia, agent_id,
    sequence, manifest_sha256, base_snapshot_id, snapshot_id
);
CREATE TABLE IF NOT EXISTS runs (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    competencia TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL,
    unit_registry_data TEXT,
    PRIMARY KEY (tenant_id, run_id)
);
CREATE TABLE IF NOT EXISTS run_transition_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    expected_state TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, expected_state),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
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
CREATE TABLE IF NOT EXISTS run_unit_terminal_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    unit_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, unit_id),
    FOREIGN KEY (tenant_id, run_id, unit_id)
        REFERENCES run_units (tenant_id, run_id, unit_id) ON DELETE CASCADE
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
CREATE TABLE IF NOT EXISTS run_dispatch_wave_identities (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    wave_id TEXT NOT NULL,
    unit_ids TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, wave_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS run_dispatch_bind_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    dispatch_id TEXT NOT NULL,
    command_data TEXT NOT NULL,
    response_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, dispatch_id),
    FOREIGN KEY (tenant_id, run_id)
        REFERENCES run_dispatches (tenant_id, run_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS run_dispatch_terminal_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    dispatch_id TEXT NOT NULL,
    command_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id, dispatch_id)
);
CREATE TABLE IF NOT EXISTS run_cancellation_writes (
    tenant_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    command_data TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, run_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES runs (tenant_id, run_id) ON DELETE CASCADE
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
CREATE TABLE IF NOT EXISTS dataset_publications (
    tenant_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    version_id TEXT NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, dataset_name, version_id),
    FOREIGN KEY (tenant_id, dataset_name, version_id)
        REFERENCES dataset_versions (tenant_id, dataset_name, version_id) ON DELETE CASCADE
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
    creation_request_data TEXT NOT NULL,
    creation_event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, request_id)
);
CREATE TABLE IF NOT EXISTS access_request_decision_writes (
    tenant_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    event_data TEXT NOT NULL,
    PRIMARY KEY (tenant_id, request_id),
    FOREIGN KEY (tenant_id, request_id)
        REFERENCES access_requests (tenant_id, request_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS outbox_events (
    event_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    delivered_at TEXT,
    data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_outbox_pending
ON outbox_events (created_at, event_id) WHERE delivered_at IS NULL;
"""
_NETWORK_FILESYSTEMS = {"9p", "afs", "cifs", "fuse.sshfs", "nfs", "nfs4", "smbfs"}
_NETWORK_PATH_PREFIXES = ("//", "smb:/", "nfs:/", "afp:/", "/net/", "/Network/Servers/")
class _SQLiteWALUnavailable(RuntimeError):
    pass
def serialize_model(model: BaseModel) -> str:
    return json.dumps(model.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
def deserialize_model[Model: BaseModel](payload: str, model: type[Model]) -> Model:
    return model.model_validate_json(payload)
def put_job_creation_write(connection: Any, job: Any, event: Any) -> None:
    connection.execute(
        "INSERT INTO job_creation_writes (tenant_id, job_id, job_data, event_data) "
        "VALUES (?, ?, ?, ?)",
        (job.tenant_id, job.job_id, serialize_model(job), serialize_model(event)),
    )
def validate_job_creation_replay(connection: Any, job: Any, event: Any) -> None:
    row = connection.execute(
        "SELECT job_data, event_data FROM job_creation_writes WHERE tenant_id = ? AND job_id = ?",
        (job.tenant_id, job.job_id),
    ).fetchone()
    if row is None or tuple(row) != (serialize_model(job), serialize_model(event)):
        raise Conflict("job_creation_conflict")
def put_job_cancellation(connection: Any, command: Any, event: Any) -> None:
    connection.execute(
        "INSERT INTO job_cancellation_writes "
        "(tenant_id, job_id, command_data, event_data) VALUES (?, ?, ?, ?)",
        (command.tenant_id, command.job_id, serialize_model(command), serialize_model(event)),
    )
def validate_job_cancellation(connection: Any, command: Any, event: Any) -> None:
    row = connection.execute(
        "SELECT command_data, event_data FROM job_cancellation_writes "
        "WHERE tenant_id = ? AND job_id = ?", (command.tenant_id, command.job_id),
    ).fetchone()
    if row is None or tuple(row) != (serialize_model(command), serialize_model(event)):
        raise Conflict("job_cancellation_conflict")
def put_access_request_decision(connection: Any, request: Any, event: Any) -> None:
    connection.execute(
        "INSERT INTO access_request_decision_writes "
        "(tenant_id, request_id, event_data) VALUES (?, ?, ?)",
        (request.tenant_id, request.request_id, serialize_model(event)),
    )
def validate_access_request_decision(connection: Any, request: Any, event: Any) -> None:
    row = connection.execute(
        "SELECT event_data FROM access_request_decision_writes "
        "WHERE tenant_id = ? AND request_id = ?", (request.tenant_id, request.request_id),
    ).fetchone()
    if row is None or row[0] != serialize_model(event):
        raise Conflict("access_request_decision_conflict")
def put_run_transition(connection: Any, command: Any, event: Any) -> None:
    connection.execute(
        "INSERT INTO run_transition_writes "
        "(tenant_id, run_id, expected_state, command_data, event_data) VALUES (?, ?, ?, ?, ?)",
        (command.tenant_id, command.run_id, command.expected_state.value,
         serialize_model(command), serialize_model(event)),
    )
def validate_run_transition(connection: Any, command: Any, event: Any) -> bool:
    row = connection.execute(
        "SELECT command_data, event_data FROM run_transition_writes "
        "WHERE tenant_id = ? AND run_id = ? AND expected_state = ?",
        (command.tenant_id, command.run_id, command.expected_state.value),
    ).fetchone()
    if row is None:
        return False
    if tuple(row) != (serialize_model(command), serialize_model(event)):
        raise Conflict("run_transition_conflict")
    return True
def get_job_terminal_write(
    connection: Any, tenant_id: str, job_id: str
) -> tuple[str, ...] | None:
    row = connection.execute(
        "SELECT operation, command_data, event_data FROM job_terminal_writes "
        "WHERE tenant_id = ? AND job_id = ?",
        (tenant_id, job_id),
    ).fetchone()
    return None if row is None else tuple(row)
def put_job_terminal_write(connection: Any, operation: str, command: Any, event: Any) -> None:
    connection.execute(
        "INSERT INTO job_terminal_writes "
        "(tenant_id, job_id, operation, command_data, event_data) VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT (tenant_id, job_id) DO UPDATE SET "
        "operation = excluded.operation, command_data = excluded.command_data, "
        "event_data = excluded.event_data",
        (
            command.tenant_id, command.job_id, operation,
            serialize_model(command), serialize_model(event),
        ),
    )
def validate_job_terminal_replay(connection: Any, job: Any, command: Any, event: Any) -> None:
    manifest = getattr(command, "manifest", None)
    operation = "complete" if manifest is not None else "fail"
    canonical = (operation, serialize_model(command), serialize_model(event))
    current = get_job_terminal_write(connection, command.tenant_id, command.job_id)
    if manifest is not None:
        result_matches = (
            job.state is JobState.SUCCEEDED
            and job.fencing_token == command.fencing_token
            and job.result_manifest_id == manifest.manifest_id
            and job.result_manifest_key == manifest.manifest_key
        )
    else:
        expected = JobState.FAILED_RETRYABLE if command.retryable else JobState.FAILED_FINAL
        result_matches = (
            job.state is expected
            and job.fencing_token == command.fencing_token
            and job.error_code == command.error_code
        )
    if current != canonical or not result_matches:
        raise Conflict("job_terminal_conflict")
def get_run_unit_terminal_write(connection: Any, command: Any) -> tuple[str, ...] | None:
    row = connection.execute(
        "SELECT operation, command_data, event_data FROM run_unit_terminal_writes "
        "WHERE tenant_id = ? AND run_id = ? AND unit_id = ?",
        (command.tenant_id, command.run_id, command.unit_id),
    ).fetchone()
    return None if row is None else tuple(row)
def put_run_unit_terminal_write(
    connection: Any, operation: str, command: Any, event: Any
) -> None:
    connection.execute(
        "INSERT INTO run_unit_terminal_writes "
        "(tenant_id, run_id, unit_id, operation, command_data, event_data) "
        "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (tenant_id, run_id, unit_id) DO UPDATE SET "
        "operation = excluded.operation, command_data = excluded.command_data, "
        "event_data = excluded.event_data",
        (
            command.tenant_id, command.run_id, command.unit_id, operation,
            serialize_model(command), serialize_model(event),
        ),
    )
def validate_run_unit_terminal_replay(
    connection: Any, unit: Any, command: Any, event: Any
) -> Any:
    operation = "commit" if hasattr(command, "output_manifests") else "fail"
    canonical = (operation, serialize_model(command), serialize_model(event))
    if get_run_unit_terminal_write(connection, command) != canonical:
        raise Conflict("unit_terminal_conflict")
    return unit
def put_run_dispatch_finish(connection: Any, command: Any) -> None:
    connection.execute(
        "INSERT INTO run_dispatch_terminal_writes "
        "(tenant_id, run_id, dispatch_id, command_data) VALUES (?, ?, ?, ?)",
        (command.tenant_id, command.run_id, command.dispatch_id, serialize_model(command)),
    )
def put_run_dispatch_bind(connection: Any, command: Any, dispatch: Any) -> None:
    connection.execute(
        "INSERT INTO run_dispatch_bind_writes "
        "(tenant_id, run_id, dispatch_id, command_data, response_data) VALUES (?, ?, ?, ?, ?)",
        (
            command.tenant_id,
            command.run_id,
            command.dispatch_id,
            serialize_model(command),
            serialize_model(dispatch),
        ),
    )


def validate_run_dispatch_bind(connection: Any, command: Any) -> RunDispatch | None:
    row = connection.execute(
        "SELECT command_data, response_data FROM run_dispatch_bind_writes "
        "WHERE tenant_id = ? AND run_id = ? AND dispatch_id = ?",
        (command.tenant_id, command.run_id, command.dispatch_id),
    ).fetchone()
    if row is None:
        return None
    if row[0] != serialize_model(command):
        raise Conflict("dispatch_bind_conflict")
    return None if row[1] is None else deserialize_model(row[1], RunDispatch)
def validate_run_dispatch_finish(connection: Any, command: Any) -> None:
    row = connection.execute(
        "SELECT command_data FROM run_dispatch_terminal_writes "
        "WHERE tenant_id = ? AND run_id = ? AND dispatch_id = ?",
        (command.tenant_id, command.run_id, command.dispatch_id),
    ).fetchone()
    if row is None or row[0] != serialize_model(command):
        raise Conflict("dispatch_finish_conflict")
def put_run_cancellation(connection: Any, command: Any, event: Any) -> None:
    connection.execute(
        "INSERT INTO run_cancellation_writes "
        "(tenant_id, run_id, command_data, event_data) VALUES (?, ?, ?, ?)",
        (command.tenant_id, command.run_id, serialize_model(command), serialize_model(event)),
    )
def validate_run_cancellation(connection: Any, command: Any, event: Any) -> None:
    row = connection.execute(
        "SELECT command_data, event_data FROM run_cancellation_writes "
        "WHERE tenant_id = ? AND run_id = ?",
        (command.tenant_id, command.run_id),
    ).fetchone()
    canonical = (serialize_model(command), serialize_model(event))
    if row is None or tuple(row) != canonical:
        raise Conflict("run_cancellation_conflict")
def validate_run_dispatch_wave(connection: Any, command: Any) -> None:
    unit_ids = json.dumps(command.unit_ids, separators=(",", ":"))
    row = connection.execute(
        "SELECT unit_ids FROM run_dispatch_wave_identities "
        "WHERE tenant_id = ? AND run_id = ? AND wave_id = ?",
        (command.tenant_id, command.run_id, command.wave_id),
    ).fetchone()
    if row is not None:
        if row[0] != unit_ids:
            raise Conflict("dispatch_units_conflict")
        return
    connection.execute(
        "INSERT INTO run_dispatch_wave_identities (tenant_id, run_id, wave_id, unit_ids) "
        "VALUES (?, ?, ?, ?)",
        (command.tenant_id, command.run_id, command.wave_id, unit_ids),
    )
def is_network_filesystem(path: Path) -> bool:
    raw_path = str(path).replace("\\", "/")
    if raw_path.startswith(_NETWORK_PATH_PREFIXES):
        return True
    try:
        mounts = Path("/proc/self/mounts").read_text(encoding="utf-8").splitlines()
    except OSError:
        return False
    resolved = path.resolve()
    matches = []
    for line in mounts:
        fields = line.split()
        if len(fields) >= 3 and resolved.is_relative_to(Path(fields[1])):
            matches.append((len(fields[1]), fields[2]))
    return bool(matches and max(matches)[1] in _NETWORK_FILESYSTEMS)
def initialize_schema(connect: Callable[[], sqlite3.Connection], path: Path) -> None:
    from cnes_infra.control_plane.sqlite_migration import migrate_schema

    path.parent.mkdir(parents=True, exist_ok=True)
    connection = connect()
    try:
        result = connection.execute("PRAGMA journal_mode=WAL").fetchone()
        if result is None or str(result[0]).lower() != "wal":
            raise _SQLiteWALUnavailable("sqlite_wal_unavailable")
        connection.executescript(_SCHEMA)
        migrate_schema(connection)
        connection.commit()
    finally:
        connection.close()
