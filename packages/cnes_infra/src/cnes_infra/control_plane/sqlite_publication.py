"""SQLite run discovery and dataset publication operations."""

from __future__ import annotations

from dataclasses import dataclass
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
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_domain.control_plane.transitions import transition_run as apply_run_transition
from cnes_infra.control_plane.sqlite_schema import (
    deserialize_model,
    put_access_request_decision,
    put_run_transition,
    serialize_model,
    validate_access_request_decision,
    validate_run_transition,
)

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.control_plane.commands import TransitionRun
    from cnes_domain.control_plane.entities import OutboxEvent, Run
    from cnes_domain.control_plane.queries import (
        LatestSucceededJobQuery,
        RawManifestChainQuery,
        WaitingRunsForDependencyQuery,
    )

_HEAD_SCAN_PAGES = 4


def query_latest_succeeded_job(store: Any, query: LatestSucceededJobQuery) -> Job | None:
    identity = query.identity
    with store.read_connection() as connection:
        row = connection.execute(
            "SELECT data FROM jobs WHERE tenant_id = ? AND agent_id = ? "
            "AND source_type = ? AND file_subtype = ? AND competencia = ? "
            "AND state = ? ORDER BY created_at DESC, job_id DESC LIMIT 1",
            (
                identity.tenant_id,
                query.agent_id,
                identity.source_type,
                identity.file_subtype,
                identity.competencia,
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


@dataclass
class _AncestrySearch:
    identity: tuple[str, ...]
    remaining: int
    predecessors: dict[Any, tuple[RawManifestRecord, ...]]


def _build_ancestry(connection: Any, identity: Any, current: RawManifestRecord, limit: int):
    search = identity if isinstance(identity, _AncestrySearch) else _AncestrySearch(
        identity, max(limit, 1) * _HEAD_SCAN_PAGES, {})
    paths = [(current, (current,))]
    expanded = set()
    while paths:
        item, ancestry = paths.pop()
        if item.manifest_id in expanded:
            continue
        if search.remaining == 0:
            return None
        expanded.add(item.manifest_id)
        search.remaining -= 1
        if len(ancestry) > limit:
            return None
        if item.sequence == 1:
            return tuple(reversed(ancestry))
        key = (item.agent_id, item.sequence, item.previous_manifest_sha256, item.base_snapshot_id)
        if key not in search.predecessors:
            search.predecessors[key] = _predecessors(
                connection, search.identity, item, limit)
        predecessors = search.predecessors[key]
        paths.extend((predecessor, (*ancestry, predecessor))
                     for predecessor in reversed(predecessors))
    return ()


def _select_raw_chain(
    connection: Any, identity: tuple[str, ...], limit: int,
) -> tuple[RawManifestRecord, ...]:
    cursor = (None, None, None, None)
    page_size = max(limit, 1)
    remaining = page_size * _HEAD_SCAN_PAGES
    search = _AncestrySearch(identity, remaining, {})
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
            chain = _build_ancestry(connection, search, head, limit)
            if chain is None:
                return ()
            if len(chain) == head.sequence:
                return chain
        cursor = tuple(rows[-1][1:])
    return ()


def query_raw_manifest_chain(store: Any, query: RawManifestChainQuery) -> tuple[ManifestRef, ...]:
    if query.limit <= 0:
        return ()
    raw = query.identity
    identity = (raw.tenant_id, raw.source_type, raw.file_subtype, raw.competencia)
    with store.read_connection() as connection:
        selected = _select_raw_chain(connection, identity, query.limit)
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


def query_waiting_runs_for_dependency(
    store: Any, query: WaitingRunsForDependencyQuery
) -> tuple[Run, ...]:
    if query.limit <= 0:
        return ()
    return fetch_waiting_runs_for_dependency(store, query)


def fetch_waiting_runs_for_dependency(
    store: Any, query: WaitingRunsForDependencyQuery
) -> tuple[Run, ...]:
    # Legacy SQLite queries preserve negative LIMIT as an unbounded result.
    identity = query.identity
    with store.read_connection() as connection:
        rows = connection.execute(
            "SELECT r.data FROM runs r JOIN run_dependencies d "
            "ON d.tenant_id = r.tenant_id AND d.run_id = r.run_id "
            "WHERE r.tenant_id = ? AND d.source_type = ? AND d.file_subtype = ? "
            "AND r.competencia = ? AND r.state = ? "
            "ORDER BY r.created_at, r.run_id LIMIT ?",
            (
                identity.tenant_id,
                identity.source_type,
                identity.file_subtype,
                identity.competencia,
                RunState.WAITING_INPUTS.value,
                query.limit,
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
        if run is not None and validate_run_transition(connection, command, event):
            return run
        if run is None or run.state is not command.expected_state:
            raise Conflict(ErrorCode.RUN_STATE_CONFLICT)
        updated = apply_run_transition(run, command.new_state).model_copy(
            update={"missing_sources": command.missing_sources}
        )
        store.put_outbox_event(connection, event, command.tenant_id)
        store.put_run_record(connection, updated)
        put_run_transition(connection, command, event)
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


def _put_publication(connection: Any, command: PublishDataset, result: DatasetPointer) -> None:
    version = command.version
    permit = command.publication_permit.model_copy(update={"binding_context": None})
    canonical = command.model_copy(update={"publication_permit": permit})
    connection.execute(
        "INSERT INTO dataset_publications "
        "(tenant_id, dataset_name, version_id, data, response_data) "
        "VALUES (?, ?, ?, ?, ?)",
        (version.tenant_id, version.dataset_name, version.version_id, serialize_model(canonical),
         serialize_model(result)),
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
    store: Any, connection: Any, command: PublishDataset, pointer: DatasetPointer | None
) -> DatasetPointer | None:
    row = connection.execute(
        "SELECT data, response_data FROM dataset_publications "
        "WHERE tenant_id = ? AND dataset_name = ? AND version_id = ?",
        (command.version.tenant_id, command.version.dataset_name, command.version.version_id),
    ).fetchone()
    canonical = None if row is None else deserialize_model(row[0], PublishDataset)
    run = store.get_run_record(connection, command.version.tenant_id, command.version.run_id)
    terminal_matches = (
        run is not None
        and run.state is command.final_state
        and run.missing_sources == command.missing_sources
    )
    permit = command.publication_permit.model_copy(update={"binding_context": None})
    command = command.model_copy(update={"publication_permit": permit})
    if canonical != command or not terminal_matches:
        raise Conflict(ErrorCode.PUBLICATION_REPLAY_CONFLICT)
    if row[1] is None:
        raise Conflict(ErrorCode.PUBLICATION_REPLAY_RESPONSE_MISSING)
    return deserialize_model(row[1], DatasetPointer)


def publish_dataset(store: Any, command: PublishDataset) -> DatasetPointer:
    if command.pointer_name != "current":
        raise Conflict(ErrorCode.POINTER_NAME_NOT_CURRENT)
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
                raise Conflict(ErrorCode.VERSION_IMMUTABLE)
            return _validate_publication_replay(store, connection, command, pointer)
        actual = None if pointer is None else pointer.version_id
        if actual != command.expected_version_id:
            raise Conflict(ErrorCode.POINTER_CAS)
        run = store.get_run_record(connection, version.tenant_id, version.run_id)
        if run is None or run.state is not RunState.PUBLISHING:
            raise Conflict(ErrorCode.RUN_NOT_PUBLISHING)
        if run.dataset_name != version.dataset_name:
            raise Conflict(ErrorCode.RUN_DATASET_MISMATCH)
        if version.run_manifest_key.split("/")[2] != run.competencia:
            raise Conflict(ErrorCode.RUN_COMPETENCIA_MISMATCH)
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
        store.put_outbox_event(connection, command.event, version.tenant_id)
        _put_version(connection, version)
        _put_publication(connection, command, result)
        _put_pointer(connection, result)
        store.put_run_record(connection, updated)
        return result


def _get_access_request(
    connection: Any, tenant_id: str, request_id: str
) -> AccessRequest | None:
    row = connection.execute(
        "SELECT data FROM access_requests WHERE tenant_id = ? AND request_id = ?",
        (tenant_id, request_id),
    ).fetchone()
    return None if row is None else deserialize_model(row[0], AccessRequest)


def _put_access_request(connection: Any, request: AccessRequest, event: OutboxEvent) -> None:
    connection.execute(
        "INSERT INTO access_requests "
        "(tenant_id, request_id, data, creation_request_data, creation_event_data) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT (tenant_id, request_id) DO UPDATE SET data = excluded.data",
        (request.tenant_id, request.request_id, serialize_model(request),
         serialize_model(request), serialize_model(event)),
    )


def get_access_request(store: Any, tenant_id: str, request_id: str) -> AccessRequest | None:
    with store.read_connection() as connection:
        return _get_access_request(connection, tenant_id, request_id)


def put_access_request(store: Any, request: AccessRequest, event: OutboxEvent) -> None:
    with store.write_transaction() as connection:
        if request.state is not AccessRequestState.PENDING:
            raise Conflict(ErrorCode.ACCESS_REQUEST_CREATION_STATE)
        current = _get_access_request(connection, request.tenant_id, request.request_id)
        if current is not None:
            row = connection.execute(
                "SELECT creation_request_data, creation_event_data FROM access_requests "
                "WHERE tenant_id = ? AND request_id = ?",
                (request.tenant_id, request.request_id)).fetchone()
            if row is None or row[0] != serialize_model(request):
                raise Conflict(ErrorCode.ACCESS_REQUEST_CONFLICT)
            if row[1] != serialize_model(event):
                raise Conflict(ErrorCode.ACCESS_REQUEST_CREATION_CONFLICT)
            return
        store.put_outbox_event(connection, event, request.tenant_id)
        _put_access_request(connection, request, event)


def decide_access_request(store: Any, request: AccessRequest, event: OutboxEvent) -> AccessRequest:
    with store.write_transaction() as connection:
        current = _get_access_request(connection, request.tenant_id, request.request_id)
        if current is None:
            raise Conflict(ErrorCode.ACCESS_REQUEST_STATE_CONFLICT)
        identity = (current.tenant_id, current.request_id, current.user_id)
        if identity != (request.tenant_id, request.request_id, request.user_id):
            raise Conflict(ErrorCode.ACCESS_REQUEST_IDENTITY_CONFLICT)
        if request.state not in {AccessRequestState.APPROVED, AccessRequestState.REJECTED}:
            raise Conflict(ErrorCode.ACCESS_REQUEST_DECISION_STATE)
        if current == request:
            validate_access_request_decision(connection, request, event)
            return request
        if current.state is not AccessRequestState.PENDING:
            raise Conflict(ErrorCode.ACCESS_REQUEST_STATE_CONFLICT)
        store.put_outbox_event(connection, event, request.tenant_id)
        _put_access_request(connection, request, event)
        put_access_request_decision(connection, request, event)
        return request
