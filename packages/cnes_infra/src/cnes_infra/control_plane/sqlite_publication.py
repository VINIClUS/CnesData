"""SQLite run discovery and dataset publication operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cnes_domain.control_plane.commands import PublishDataset
from cnes_domain.control_plane.entities import (
    AccessRequest,
    DatasetPointer,
    DatasetVersion,
    Job,
    ManifestRef,
    RawManifestRecord,
)
from cnes_domain.control_plane.enums import AccessRequestState, JobState, RunState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.transitions import transition_run as apply_run_transition
from cnes_infra.control_plane.sqlite_schema import deserialize_model, serialize_model

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.control_plane.commands import TransitionRun
    from cnes_domain.control_plane.entities import OutboxEvent, Run

LATEST_JOB_FIELDS = (
    "tenant_id",
    "agent_id",
    "source_type",
    "file_subtype",
    "competencia",
)
DEPENDENCY_FIELDS = ("tenant_id", "source_type", "file_subtype", "competencia", "limit")
_HEAD_SCAN_PAGES = 4


def normalize_long_call(
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    fields: tuple[str, ...],
    default_limit: int | None = None,
) -> tuple[Any, ...]:
    if len(args) > len(fields):
        raise TypeError(f"too_many_arguments={len(args)}")
    values = dict(zip(fields, args, strict=False))
    for name, value in kwargs.items():
        if name not in fields:
            raise TypeError(f"unexpected_argument={name}")
        if name in values:
            raise TypeError(f"duplicate_argument={name}")
        values[name] = value
    if default_limit is not None and "limit" not in values:
        values["limit"] = default_limit
    missing = tuple(name for name in fields if name not in values)
    if missing:
        raise TypeError(f"missing_arguments={','.join(missing)}")
    return tuple(values[name] for name in fields)


def latest_succeeded_job(store: Any, values: tuple[str, ...]) -> Job | None:
    tenant_id, agent_id, source_type, file_subtype, competencia = values
    with store.read_connection() as connection:
        row = connection.execute(
            "SELECT data FROM jobs WHERE tenant_id = ? AND agent_id = ? "
            "AND source_type = ? AND file_subtype = ? AND competencia = ? "
            "AND state = ? ORDER BY created_at DESC, job_id DESC LIMIT 1",
            (
                tenant_id,
                agent_id,
                source_type,
                file_subtype,
                competencia,
                JobState.SUCCEEDED.value,
            ),
        ).fetchone()
    return None if row is None else deserialize_model(row[0], Job)


def _predecessors(
    connection: Any, identity: tuple[str, ...], current: RawManifestRecord, limit: int
) -> tuple[RawManifestRecord, ...]:
    rows = connection.execute(
        "SELECT data FROM raw_manifests WHERE tenant_id = ? AND source_type = ? "
        "AND file_subtype = ? AND competencia = ? AND agent_id = ? AND sequence = ? "
        "AND manifest_sha256 = ? AND (snapshot_id = ? OR base_snapshot_id = ?) "
        "ORDER BY created_at DESC, snapshot_id DESC LIMIT ?",
        (*identity, current.agent_id, current.sequence - 1,
         current.previous_manifest_sha256, current.base_snapshot_id,
         current.base_snapshot_id, max(limit, 1)),
    )
    return tuple(deserialize_model(row[0], RawManifestRecord) for row in rows)


def _build_ancestry(
    connection: Any, identity: tuple[str, ...], current: RawManifestRecord, limit: int
) -> tuple[RawManifestRecord, ...] | None:
    paths = [(current, (current,))]
    while paths:
        item, ancestry = paths.pop()
        if len(ancestry) > limit:
            return None
        if item.sequence == 1:
            return tuple(reversed(ancestry))
        predecessors = _predecessors(connection, identity, item, limit)
        paths.extend((predecessor, (*ancestry, predecessor))
                     for predecessor in reversed(predecessors))
    return ()


def _select_raw_chain(
    connection: Any, identity: tuple[str, ...], limit: int,
) -> tuple[RawManifestRecord, ...]:
    cursor = (None, None, None, None)
    page_size = max(limit, 1)
    remaining = page_size * _HEAD_SCAN_PAGES
    while remaining:
        rows = connection.execute(
            "SELECT data, created_at, agent_id, snapshot_id, manifest_id FROM raw_manifests "
            "WHERE tenant_id = ? AND source_type = ? AND file_subtype = ? AND competencia = ? "
            "AND (? IS NULL OR (created_at, agent_id, snapshot_id, manifest_id) "
            "< (?, ?, ?, ?)) ORDER BY created_at DESC, agent_id DESC, snapshot_id DESC, "
            "manifest_id DESC LIMIT ?",
            (*identity, cursor[0], *cursor, min(page_size, remaining)),
        ).fetchall()
        if not rows:
            return ()
        remaining -= len(rows)
        for row in rows:
            head = deserialize_model(row[0], RawManifestRecord)
            chain = _build_ancestry(connection, identity, head, limit)
            if chain is None:
                return ()
            if len(chain) == head.sequence:
                return chain
        cursor = tuple(rows[-1][1:])
    return ()


def list_raw_manifest_chain(store: Any, values: tuple[Any, ...]) -> tuple[ManifestRef, ...]:
    tenant_id, source_type, file_subtype, competencia, limit = values
    identity = (tenant_id, source_type, file_subtype, competencia)
    with store.read_connection() as connection:
        selected = _select_raw_chain(connection, identity, limit)
    return tuple(
        ManifestRef(manifest_id=item.manifest_id, manifest_key=item.manifest_key)
        for item in selected
    )


def put_run(store: Any, run: Run) -> None:
    with store.write_transaction() as connection:
        store.put_run_record(connection, run)


def get_run(store: Any, tenant_id: str, run_id: str) -> Run | None:
    with store.read_connection() as connection:
        return store.get_run_record(connection, tenant_id, run_id)


def list_waiting_runs(store: Any, values: tuple[Any, ...]) -> tuple[Run, ...]:
    tenant_id, source_type, file_subtype, competencia, limit = values
    with store.read_connection() as connection:
        rows = connection.execute(
            "SELECT r.data FROM runs r JOIN run_dependencies d "
            "ON d.tenant_id = r.tenant_id AND d.run_id = r.run_id "
            "WHERE r.tenant_id = ? AND d.source_type = ? AND d.file_subtype = ? "
            "AND r.competencia = ? AND r.state = ? "
            "ORDER BY r.created_at, r.run_id LIMIT ?",
            (
                tenant_id,
                source_type,
                file_subtype,
                competencia,
                RunState.WAITING_INPUTS.value,
                limit,
            ),
        ).fetchall()
    return tuple(store.decode_run(row[0]) for row in rows)


def list_recoverable_runs(store: Any, now: datetime, limit: int) -> tuple[Run, ...]:
    del now
    states = (
        RunState.WAITING_INPUTS.value,
        RunState.PROCESSING.value,
        RunState.PUBLISHING.value,
        RunState.CANCEL_REQUESTED.value,
    )
    with store.read_connection() as connection:
        rows = connection.execute(
            "SELECT data FROM runs WHERE state IN (?, ?, ?, ?) "
            "ORDER BY created_at, tenant_id, run_id LIMIT ?",
            (*states, limit),
        ).fetchall()
    return tuple(store.decode_run(row[0]) for row in rows)


def transition_run(store: Any, command: TransitionRun, event: OutboxEvent) -> Run:
    with store.write_transaction() as connection:
        run = store.get_run_record(connection, command.tenant_id, command.run_id)
        if run is None or run.state is not command.expected_state:
            raise Conflict("run_state_conflict")
        updated = apply_run_transition(run, command.new_state).model_copy(
            update={"missing_sources": command.missing_sources}
        )
        store.put_run_record(connection, updated)
        store.put_outbox_event(connection, event)
        return updated


def _get_version(
    connection: Any, tenant_id: str, dataset_name: str, version_id: str
) -> DatasetVersion | None:
    row = connection.execute(
        "SELECT data FROM dataset_versions "
        "WHERE tenant_id = ? AND dataset_name = ? AND version_id = ?",
        (tenant_id, dataset_name, version_id),
    ).fetchone()
    return None if row is None else deserialize_model(row[0], DatasetVersion)


def _get_pointer(
    connection: Any, tenant_id: str, dataset_name: str, pointer_name: str
) -> DatasetPointer | None:
    row = connection.execute(
        "SELECT data FROM dataset_pointers "
        "WHERE tenant_id = ? AND dataset_name = ? AND pointer_name = ?",
        (tenant_id, dataset_name, pointer_name),
    ).fetchone()
    return None if row is None else deserialize_model(row[0], DatasetPointer)


def get_dataset_pointer(store: Any, tenant_id: str, dataset_name: str) -> DatasetPointer | None:
    with store.read_connection() as connection:
        return _get_pointer(connection, tenant_id, dataset_name, "current")


def get_dataset_version(store: Any, values: tuple[str, str, str]) -> DatasetVersion | None:
    with store.read_connection() as connection:
        return _get_version(connection, *values)


def _get_publication(connection: Any, version: DatasetVersion) -> PublishDataset | None:
    row = connection.execute(
        "SELECT data FROM dataset_publications "
        "WHERE tenant_id = ? AND dataset_name = ? AND version_id = ?",
        (version.tenant_id, version.dataset_name, version.version_id),
    ).fetchone()
    return None if row is None else deserialize_model(row[0], PublishDataset)


def _put_version(connection: Any, version: DatasetVersion) -> None:
    connection.execute(
        "INSERT INTO dataset_versions (tenant_id, dataset_name, version_id, data) "
        "VALUES (?, ?, ?, ?)",
        (
            version.tenant_id,
            version.dataset_name,
            version.version_id,
            serialize_model(version),
        ),
    )


def _put_publication(connection: Any, command: PublishDataset) -> None:
    version = command.version
    connection.execute(
        "INSERT INTO dataset_publications (tenant_id, dataset_name, version_id, data) "
        "VALUES (?, ?, ?, ?)",
        (version.tenant_id, version.dataset_name, version.version_id, serialize_model(command)),
    )


def _put_pointer(connection: Any, pointer: DatasetPointer) -> None:
    connection.execute(
        "INSERT INTO dataset_pointers (tenant_id, dataset_name, pointer_name, data) "
        "VALUES (?, ?, ?, ?) ON CONFLICT (tenant_id, dataset_name, pointer_name) "
        "DO UPDATE SET data = excluded.data",
        (
            pointer.tenant_id,
            pointer.dataset_name,
            pointer.pointer_name,
            serialize_model(pointer),
        ),
    )


def _validate_publication_replay(
    store: Any, connection: Any, command: PublishDataset, pointer: DatasetPointer
) -> DatasetPointer:
    canonical = _get_publication(connection, command.version)
    run = store.get_run_record(connection, command.version.tenant_id, command.version.run_id)
    terminal_matches = (
        run is not None
        and run.state is command.final_state
        and run.missing_sources == command.missing_sources
    )
    if canonical != command or not terminal_matches:
        raise Conflict("publication_replay_conflict")
    return pointer


def publish_dataset(store: Any, command: PublishDataset) -> DatasetPointer:
    if command.pointer_name != "current":
        raise Conflict("pointer_name_not_current")
    version = command.version
    with store.write_transaction() as connection:
        current_version = _get_version(
            connection, version.tenant_id, version.dataset_name, version.version_id
        )
        pointer = _get_pointer(
            connection, version.tenant_id, version.dataset_name, command.pointer_name
        )
        if current_version is not None:
            if current_version != version:
                raise Conflict("version_immutable")
            if pointer is not None and pointer.version_id == version.version_id:
                return _validate_publication_replay(store, connection, command, pointer)
        actual = None if pointer is None else pointer.version_id
        if actual != command.expected_version_id:
            raise Conflict("pointer_cas")
        run = store.get_run_record(connection, version.tenant_id, version.run_id)
        if run is None or run.state is not RunState.PUBLISHING:
            raise Conflict("run_not_publishing")
        if run.dataset_name != version.dataset_name:
            raise Conflict("run_dataset_mismatch")
        updated = run.model_copy(
            update={"state": command.final_state, "missing_sources": command.missing_sources}
        )
        result = DatasetPointer(
            tenant_id=version.tenant_id,
            dataset_name=version.dataset_name,
            pointer_name=command.pointer_name,
            version_id=version.version_id,
            updated_at=store.now(),
        )
        _put_version(connection, version)
        _put_publication(connection, command)
        _put_pointer(connection, result)
        store.put_run_record(connection, updated)
        store.put_outbox_event(connection, command.event)
        return result


def _get_access_request(
    connection: Any, tenant_id: str, request_id: str
) -> AccessRequest | None:
    row = connection.execute(
        "SELECT data FROM access_requests WHERE tenant_id = ? AND request_id = ?",
        (tenant_id, request_id),
    ).fetchone()
    return None if row is None else deserialize_model(row[0], AccessRequest)


def _put_access_request(connection: Any, request: AccessRequest) -> None:
    connection.execute(
        "INSERT INTO access_requests (tenant_id, request_id, data) VALUES (?, ?, ?) "
        "ON CONFLICT (tenant_id, request_id) DO UPDATE SET data = excluded.data",
        (request.tenant_id, request.request_id, serialize_model(request)),
    )


def get_access_request(store: Any, tenant_id: str, request_id: str) -> AccessRequest | None:
    with store.read_connection() as connection:
        return _get_access_request(connection, tenant_id, request_id)


def put_access_request(store: Any, request: AccessRequest, event: OutboxEvent) -> None:
    with store.write_transaction() as connection:
        current = _get_access_request(connection, request.tenant_id, request.request_id)
        if current is not None and current != request:
            raise Conflict("access_request_conflict")
        if current is None:
            _put_access_request(connection, request)
            store.put_outbox_event(connection, event)


def decide_access_request(store: Any, request: AccessRequest, event: OutboxEvent) -> AccessRequest:
    with store.write_transaction() as connection:
        current = _get_access_request(connection, request.tenant_id, request.request_id)
        if current is None:
            raise Conflict("access_request_state_conflict")
        identity = (current.tenant_id, current.request_id, current.user_id)
        if identity != (request.tenant_id, request.request_id, request.user_id):
            raise Conflict("access_request_identity_conflict")
        if request.state not in {AccessRequestState.APPROVED, AccessRequestState.REJECTED}:
            raise Conflict("access_request_decision_state")
        if current == request:
            return request
        if current.state is not AccessRequestState.PENDING:
            raise Conflict("access_request_state_conflict")
        _put_access_request(connection, request)
        store.put_outbox_event(connection, event)
        return request
