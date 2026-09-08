"""Consultas e marcadores raw do plano de controle SQLite."""

from typing import Any

from cnes_domain.control_plane.commands import CompleteJob, FailJob
from cnes_domain.control_plane.entities import Job, ManifestRef, RawManifestRecord, RawResyncState
from cnes_domain.control_plane.enums import JobState
from cnes_domain.control_plane.errors import Conflict, ControlPlaneErrorCode
from cnes_domain.control_plane.queries import (
    AgentRawManifestChainQuery,
    RawManifestByIdQuery,
    RawResyncStateQuery,
)
from cnes_infra.control_plane.sqlite_publication import _build_ancestry
from cnes_infra.control_plane.sqlite_schema import deserialize_model, serialize_model


class SQLiteRawRegistrationQueries:
    """Expõe consultas fortes do registro raw SQLite."""

    def query_raw_manifest_by_id(
        self, query: RawManifestByIdQuery
    ) -> RawManifestRecord | None:
        return _query_raw_manifest_by_id(self, query)

    def query_agent_raw_manifest_chain(
        self, query: AgentRawManifestChainQuery
    ) -> tuple[ManifestRef, ...]:
        return _query_agent_raw_manifest_chain(self, query)

    def query_raw_resync_state(self, query: RawResyncStateQuery) -> RawResyncState | None:
        return _query_raw_resync_state(self, query)


def _legacy_head(
    connection: Any, identity: tuple[str, ...], current_job_id: str
) -> tuple[str, str, str] | None:
    row = connection.execute(
        "SELECT data FROM jobs WHERE tenant_id = ? AND agent_id = ? AND source_type = ? "
        "AND file_subtype = ? AND competencia = ? AND state = ? AND job_id != ? "
        "ORDER BY created_at DESC, job_id DESC LIMIT 1",
        (*identity, JobState.SUCCEEDED.value, current_job_id),
    ).fetchone()
    if row is None:
        return None
    job = deserialize_model(row[0], Job)
    return job.result_manifest_id, job.created_at.isoformat(), job.job_id


def _put_agent_head(
    connection: Any, identity: tuple[str, ...], head: tuple[str, str, str]
) -> None:
    manifest_id, created_at, job_id = head
    connection.execute(
        "INSERT INTO raw_agent_heads (tenant_id, agent_id, source_type, file_subtype, "
        "competencia, job_id, manifest_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (tenant_id, agent_id, source_type, file_subtype, competencia) "
        "DO UPDATE SET job_id = excluded.job_id, manifest_id = excluded.manifest_id, "
        "created_at = excluded.created_at",
        (*identity, job_id, manifest_id, created_at),
    )


def advance_agent_head(connection: Any, job: Job, command: CompleteJob) -> None:
    expected = command.expected_head_manifest_id
    manifest = command.manifest
    identity = (
        manifest.tenant_id, manifest.agent_id, manifest.source_type,
        manifest.file_subtype, manifest.competencia,
    )
    row = connection.execute(
        "SELECT manifest_id, created_at, job_id FROM raw_agent_heads "
        "WHERE tenant_id = ? AND agent_id = ? AND source_type = ? "
        "AND file_subtype = ? AND competencia = ?",
        identity,
    ).fetchone()
    marker = connection.execute(
        "SELECT 1 FROM raw_resync_states WHERE tenant_id = ? AND agent_id = ? "
        "AND source_type = ? AND file_subtype = ? AND competencia = ?",
        identity,
    ).fetchone()
    current = row if row is not None else _legacy_head(connection, identity, job.job_id)
    if expected is not None and (
        marker is not None or current is None or current[0] != expected
    ):
        raise Conflict(ControlPlaneErrorCode.TRANSACTION_CONFLICT)
    ordering = (job.created_at.isoformat(), job.job_id)
    if (
        expected is None
        and marker is None
        and current is not None
        and current[1:] >= ordering
    ):
        if row is None:
            _put_agent_head(connection, identity, current)
        return
    _put_agent_head(connection, identity, (manifest.manifest_id, *ordering))


def validate_resync_rejection(connection: Any, job: Job, command: FailJob) -> None:
    expected_marker = command.expected_resync_marker
    if expected_marker is None:
        return
    identity = (
        job.tenant_id, job.agent_id, job.source_type,
        job.file_subtype, job.competencia,
    )
    marker = connection.execute(
        "SELECT 1 FROM raw_resync_states WHERE tenant_id = ? AND agent_id = ? "
        "AND source_type = ? AND file_subtype = ? AND competencia = ?",
        identity,
    ).fetchone()
    if (marker is not None) != expected_marker:
        raise Conflict(ControlPlaneErrorCode.TRANSACTION_CONFLICT)
    if expected_marker:
        return
    row = connection.execute(
        "SELECT manifest_id, created_at, job_id FROM raw_agent_heads "
        "WHERE tenant_id = ? AND agent_id = ? AND source_type = ? "
        "AND file_subtype = ? AND competencia = ?",
        identity,
    ).fetchone()
    current = row if row is not None else _legacy_head(connection, identity, job.job_id)
    current_id = None if current is None else current[0]
    if current_id != command.expected_head_manifest_id:
        raise Conflict(ControlPlaneErrorCode.TRANSACTION_CONFLICT)


def _query_raw_manifest_by_id(store: Any, query: RawManifestByIdQuery) -> RawManifestRecord | None:
    with store.read_connection() as connection:
        row = connection.execute(
            "SELECT data FROM raw_manifests WHERE tenant_id = ? AND manifest_id = ?",
            (query.tenant_id, query.manifest_id),
        ).fetchone()
    return None if row is None else deserialize_model(row[0], RawManifestRecord)


def _query_agent_raw_manifest_chain(
    store: Any, query: AgentRawManifestChainQuery
) -> tuple[ManifestRef, ...]:
    if query.limit <= 0:
        return ()
    identity = query.identity
    with store.read_connection() as connection:
        row = connection.execute(
            "SELECT raw_manifests.data FROM jobs JOIN raw_manifests ON "
            "raw_manifests.tenant_id = jobs.tenant_id AND "
            "raw_manifests.manifest_id = json_extract(jobs.data, '$.result_manifest_id') "
            "LEFT JOIN raw_agent_heads ON raw_agent_heads.tenant_id = jobs.tenant_id "
            "AND raw_agent_heads.agent_id = jobs.agent_id "
            "AND raw_agent_heads.source_type = jobs.source_type "
            "AND raw_agent_heads.file_subtype = jobs.file_subtype "
            "AND raw_agent_heads.competencia = jobs.competencia WHERE jobs.tenant_id = ? "
            "AND jobs.agent_id = ? AND jobs.source_type = ? AND jobs.file_subtype = ? "
            "AND jobs.competencia = ? AND jobs.state = ? "
            "AND (raw_agent_heads.job_id IS NULL OR jobs.job_id = raw_agent_heads.job_id) "
            "ORDER BY (jobs.job_id = raw_agent_heads.job_id) DESC, "
            "CASE WHEN raw_agent_heads.job_id IS NULL THEN jobs.created_at END DESC, "
            "CASE WHEN raw_agent_heads.job_id IS NULL THEN jobs.job_id END DESC LIMIT 1",
            (
                identity.tenant_id, query.agent_id, identity.source_type,
                identity.file_subtype, identity.competencia, JobState.SUCCEEDED.value,
            ),
        ).fetchone()
        if row is None:
            return ()
        head = deserialize_model(row[0], RawManifestRecord)
        raw_identity = (
            identity.tenant_id, identity.source_type,
            identity.file_subtype, identity.competencia,
        )
        chain = _build_ancestry(connection, raw_identity, head, query.limit)
    if chain is None or len(chain) != head.sequence:
        return ()
    return tuple(
        ManifestRef(manifest_id=item.manifest_id, manifest_key=item.manifest_key)
        for item in chain
    )


def _query_raw_resync_state(store: Any, query: RawResyncStateQuery) -> RawResyncState | None:
    identity = query.identity
    with store.read_connection() as connection:
        row = connection.execute(
            "SELECT data FROM raw_resync_states WHERE tenant_id = ? AND agent_id = ? "
            "AND source_type = ? AND file_subtype = ? AND competencia = ?",
            (
                identity.tenant_id, query.agent_id, identity.source_type,
                identity.file_subtype, identity.competencia,
            ),
        ).fetchone()
    return None if row is None else deserialize_model(row[0], RawResyncState)


def delete_resync_state(connection: Any, manifest: RawManifestRecord) -> None:
    """Remove o marcador de resync da identidade raw."""
    connection.execute(
        "DELETE FROM raw_resync_states WHERE tenant_id = ? AND agent_id = ? "
        "AND source_type = ? AND file_subtype = ? AND competencia = ?",
        (
            manifest.tenant_id, manifest.agent_id, manifest.source_type,
            manifest.file_subtype, manifest.competencia,
        ),
    )


def put_resync_state(store: Any, connection: Any, job: Job) -> None:
    """Cria o primeiro marcador de resync da identidade raw."""
    state = RawResyncState(
        tenant_id=job.tenant_id,
        agent_id=job.agent_id,
        source_type=job.source_type,
        file_subtype=job.file_subtype,
        competencia=job.competencia,
        required_since=store.now(),
    )
    connection.execute(
        "INSERT INTO raw_resync_states "
        "(tenant_id, agent_id, source_type, file_subtype, competencia, data) "
        "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
        (
            state.tenant_id, state.agent_id, state.source_type,
            state.file_subtype, state.competencia, serialize_model(state),
        ),
    )
