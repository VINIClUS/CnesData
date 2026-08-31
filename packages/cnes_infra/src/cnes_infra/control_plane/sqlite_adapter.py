"""SQLite control-plane adapter."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from cnes_domain.control_plane.entities import (
    AccessRequest,
    Agent,
    Job,
    Membership,
    OutboxEvent,
    Run,
    Tenant,
)
from cnes_domain.control_plane.enums import AgentState, JobState
from cnes_domain.control_plane.errors import Conflict, NotFound
from cnes_infra.control_plane import sqlite_claims, sqlite_idempotency, sqlite_publication
from cnes_infra.control_plane.sqlite_schema import (
    _SQLiteWALUnavailable,
    deserialize_model,
    initialize_schema,
    is_network_filesystem,
    serialize_model,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from datetime import datetime

    from cnes_domain.control_plane.commands import (
        BeginIdempotency,
        BindRunDispatch,
        CancelJob,
        ClaimJob,
        ClaimRunUnit,
        CommitRunUnit,
        CompleteJob,
        FailJob,
        FailRunUnit,
        FinalizeRunCancellation,
        FinishRunDispatch,
        IdempotencyOutcome,
        PublishDataset,
        PutRunUnits,
        RenewJobLease,
        ReserveRunDispatch,
        TransitionRun,
    )
    from cnes_domain.control_plane.entities import (
        DatasetPointer,
        DatasetVersion,
        ManifestRef,
        RawManifestRecord,
        RunDispatch,
        RunUnit,
    )

_BUSY_TIMEOUT_MS = 5000
class _SQLiteBusyError(RuntimeError):
    pass


class _SQLiteFilesystemError(RuntimeError):
    pass


def _fetch_one[Model: BaseModel](
    connection: sqlite3.Connection,
    statement: str,
    parameters: tuple[Any, ...],
    model: type[Model],
) -> Model | None:
    row = connection.execute(statement, parameters).fetchone()
    return None if row is None else deserialize_model(row[0], model)


def _fetch_all[Model: BaseModel](
    connection: sqlite3.Connection,
    statement: str,
    parameters: tuple[Any, ...],
    model: type[Model],
) -> tuple[Model, ...]:
    rows = connection.execute(statement, parameters).fetchall()
    return tuple(deserialize_model(row[0], model) for row in rows)


def _is_network_filesystem(path: Path) -> bool:
    return is_network_filesystem(path)


class SQLiteControlPlane:
    """Persiste o plano de controle em um arquivo SQLite local."""

    def __init__(self, database_path: Path, clock: Callable[[], datetime]) -> None:
        self._database_path = Path(database_path)
        self._clock = clock

    def now(self) -> datetime:
        return self._clock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self._database_path,
            timeout=_BUSY_TIMEOUT_MS / 1000,
            isolation_level=None,
        )
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def initialize(self) -> None:
        if _is_network_filesystem(self._database_path):
            raise _SQLiteFilesystemError("sqlite_network_filesystem")
        try:
            initialize_schema(self._connect, self._database_path)
        except _SQLiteWALUnavailable as error:
            raise _SQLiteFilesystemError("sqlite_wal_unavailable") from error
        except (OSError, sqlite3.Error) as error:
            raise _SQLiteFilesystemError("sqlite_filesystem") from error

    @contextmanager
    def read_connection(self) -> Iterator[sqlite3.Connection]:
        try:
            connection = self._connect()
        except sqlite3.Error as error:
            raise _SQLiteFilesystemError("sqlite_filesystem") from error
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        with self.read_connection() as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
                yield connection
                connection.commit()
            except sqlite3.OperationalError as error:
                connection.rollback()
                if "locked" in str(error).lower() or "busy" in str(error).lower():
                    raise _SQLiteBusyError("sqlite_busy") from error
                raise
            except Exception:
                connection.rollback()
                raise

    def get_tenant(self, tenant_id: str) -> Tenant | None:
        with self.read_connection() as connection:
            return _fetch_one(
                connection,
                "SELECT data FROM tenants WHERE tenant_id = ?",
                (tenant_id,),
                Tenant,
            )

    def put_tenant(self, tenant: Tenant) -> None:
        with self.write_transaction() as connection:
            connection.execute(
                "INSERT INTO tenants (tenant_id, data) VALUES (?, ?) "
                "ON CONFLICT (tenant_id) DO UPDATE SET data = excluded.data",
                (tenant.tenant_id, serialize_model(tenant)),
            )

    def get_membership(self, tenant_id: str, user_id: str) -> Membership | None:
        with self.read_connection() as connection:
            return _fetch_one(
                connection,
                "SELECT data FROM memberships WHERE tenant_id = ? AND user_id = ?",
                (tenant_id, user_id),
                Membership,
            )

    def put_membership(self, membership: Membership) -> None:
        with self.write_transaction() as connection:
            connection.execute(
                "INSERT INTO memberships (tenant_id, user_id, data) VALUES (?, ?, ?) "
                "ON CONFLICT (tenant_id, user_id) DO UPDATE SET data = excluded.data",
                (membership.tenant_id, membership.user_id, serialize_model(membership)),
            )

    def get_agent_record(
        self, connection: sqlite3.Connection, tenant_id: str, agent_id: str
    ) -> Agent | None:
        return _fetch_one(
            connection,
            "SELECT data FROM agents WHERE tenant_id = ? AND agent_id = ?",
            (tenant_id, agent_id),
            Agent,
        )

    def get_agent(self, tenant_id: str, agent_id: str) -> Agent | None:
        with self.read_connection() as connection:
            return self.get_agent_record(connection, tenant_id, agent_id)

    def put_agent(self, agent: Agent) -> None:
        with self.write_transaction() as connection:
            connection.execute(
                "INSERT INTO agents (tenant_id, agent_id, state, data) VALUES (?, ?, ?, ?) "
                "ON CONFLICT (tenant_id, agent_id) DO UPDATE SET "
                "state = excluded.state, data = excluded.data",
                (agent.tenant_id, agent.agent_id, agent.state.value, serialize_model(agent)),
            )

    def get_job_record(
        self, connection: sqlite3.Connection, tenant_id: str, job_id: str
    ) -> Job | None:
        return _fetch_one(
            connection,
            "SELECT data FROM jobs WHERE tenant_id = ? AND job_id = ?",
            (tenant_id, job_id),
            Job,
        )

    def get_job(self, tenant_id: str, job_id: str) -> Job | None:
        with self.read_connection() as connection:
            return self.get_job_record(connection, tenant_id, job_id)

    def put_job_record(self, connection: sqlite3.Connection, job: Job) -> None:
        connection.execute(
            "INSERT INTO jobs (tenant_id, job_id, agent_id, source_type, file_subtype, "
            "competencia, state, created_at, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (tenant_id, job_id) DO UPDATE SET agent_id = excluded.agent_id, "
            "source_type = excluded.source_type, file_subtype = excluded.file_subtype, "
            "competencia = excluded.competencia, state = excluded.state, "
            "created_at = excluded.created_at, data = excluded.data",
            (
                job.tenant_id,
                job.job_id,
                job.agent_id,
                job.source_type,
                job.file_subtype,
                job.competencia,
                job.state.value,
                job.created_at.isoformat(),
                serialize_model(job),
            ),
        )

    def put_outbox_event(self, connection: sqlite3.Connection, event: OutboxEvent) -> None:
        current = _fetch_one(
            connection,
            "SELECT data FROM outbox_events WHERE event_id = ?",
            (event.event_id,),
            OutboxEvent,
        )
        if current is not None:
            if current != event:
                raise Conflict("outbox_event_conflict")
            return
        connection.execute(
            "INSERT INTO outbox_events "
            "(event_id, tenant_id, created_at, delivered_at, data) VALUES (?, ?, ?, ?, ?)",
            (
                event.event_id,
                event.tenant_id,
                event.created_at.isoformat(),
                None if event.delivered_at is None else event.delivered_at.isoformat(),
                serialize_model(event),
            ),
        )

    def create_job(self, job: Job, event: OutboxEvent) -> Job:
        with self.write_transaction() as connection:
            current = self.get_job_record(connection, job.tenant_id, job.job_id)
            if current is not None:
                if current != job:
                    raise Conflict("job_conflict")
                return current
            self.put_job_record(connection, job)
            self.put_outbox_event(connection, event)
            return job

    def list_claimable_jobs(
        self, tenant_id: str, agent_id: str, limit: int
    ) -> tuple[Job, ...]:
        with self.read_connection() as connection:
            agent = self.get_agent_record(connection, tenant_id, agent_id)
            if agent is None or agent.state is AgentState.REVOKED:
                return ()
            jobs = _fetch_all(
                connection,
                "SELECT data FROM jobs WHERE tenant_id = ? AND agent_id = ? "
                "ORDER BY created_at, job_id",
                (tenant_id, agent_id),
                Job,
            )
        claimable = [
            job
            for job in jobs
            if job.state in {JobState.PENDING, JobState.FAILED_RETRYABLE}
            or (job.state is JobState.LEASED and job.lease_until <= self.now())
        ]
        return tuple(claimable[:limit])

    def claim_job(self, command: ClaimJob) -> Job | None:
        return sqlite_claims.claim_job(self, command)

    def renew_job_lease(self, command: RenewJobLease) -> Job:
        return sqlite_claims.renew_job_lease(self, command)

    def put_manifest_record(
        self, connection: sqlite3.Connection, manifest: RawManifestRecord) -> None:
        current = _fetch_one(
            connection,
            "SELECT data FROM raw_manifests WHERE tenant_id = ? AND manifest_id = ?",
            (manifest.tenant_id, manifest.manifest_id),
            type(manifest),
        )
        if current is not None:
            if current != manifest:
                raise Conflict("manifest_immutable")
            return
        connection.execute(
            "INSERT INTO raw_manifests (tenant_id, manifest_id, agent_id, source_type, "
            "file_subtype, competencia, created_at, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                manifest.tenant_id,
                manifest.manifest_id,
                manifest.agent_id,
                manifest.source_type,
                manifest.file_subtype,
                manifest.competencia,
                manifest.created_at.isoformat(),
                serialize_model(manifest),
            ),
        )

    def complete_job(self, command: CompleteJob, event: OutboxEvent) -> Job:
        return sqlite_claims.complete_job(self, command, event)

    def fail_job(self, command: FailJob, event: OutboxEvent) -> Job:
        return sqlite_claims.fail_job(self, command, event)

    def pending_outbox(self, limit: int) -> tuple[OutboxEvent, ...]:
        with self.read_connection() as connection:
            return _fetch_all(
                connection,
                "SELECT data FROM outbox_events WHERE delivered_at IS NULL "
                "ORDER BY created_at, event_id LIMIT ?",
                (limit,),
                OutboxEvent,
            )

    def mark_outbox_delivered(self, event_id: str, delivered_at: datetime) -> None:
        with self.write_transaction() as connection:
            event = _fetch_one(
                connection,
                "SELECT data FROM outbox_events WHERE event_id = ?",
                (event_id,),
                OutboxEvent,
            )
            if event is None:
                raise NotFound("outbox_event_missing")
            delivered = event.model_copy(update={"delivered_at": delivered_at})
            connection.execute(
                "UPDATE outbox_events SET delivered_at = ?, data = ? WHERE event_id = ?",
                (delivered_at.isoformat(), serialize_model(delivered), event_id),
            )

    def latest_succeeded_job(self, *args: str, **kwargs: str) -> Job | None:
        values = sqlite_publication.normalize_long_call(
            args, kwargs, sqlite_publication.LATEST_JOB_FIELDS
        )
        return sqlite_publication.latest_succeeded_job(self, values)

    def list_raw_manifest_chain(self, *args: Any, **kwargs: Any) -> tuple[ManifestRef, ...]:
        values = sqlite_publication.normalize_long_call(
            args, kwargs, sqlite_publication.DEPENDENCY_FIELDS, 31
        )
        return sqlite_publication.list_raw_manifest_chain(self, values)

    def cancel_job(self, command: CancelJob, event: OutboxEvent) -> Job:
        return sqlite_claims.cancel_job(self, command, event)

    def put_run(self, run: Run) -> None:
        sqlite_publication.put_run(self, run)

    @staticmethod
    def decode_run(payload: str) -> Run:
        return deserialize_model(payload, Run)

    def get_run_record(
        self, connection: sqlite3.Connection, tenant_id: str, run_id: str
    ) -> Run | None:
        return _fetch_one(
            connection,
            "SELECT data FROM runs WHERE tenant_id = ? AND run_id = ?",
            (tenant_id, run_id),
            Run,
        )

    def put_run_record(self, connection: sqlite3.Connection, run: Run) -> None:
        connection.execute(
            "INSERT INTO runs (tenant_id, run_id, competencia, dataset_name, state, "
            "created_at, data) VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (tenant_id, run_id) DO UPDATE SET competencia = excluded.competencia, "
            "dataset_name = excluded.dataset_name, state = excluded.state, "
            "created_at = excluded.created_at, data = excluded.data",
            (
                run.tenant_id,
                run.run_id,
                run.competencia,
                run.dataset_name,
                run.state.value,
                run.created_at.isoformat(),
                serialize_model(run),
            ),
        )
        connection.execute(
            "DELETE FROM run_dependencies WHERE tenant_id = ? AND run_id = ?",
            (run.tenant_id, run.run_id),
        )
        connection.executemany(
            "INSERT INTO run_dependencies "
            "(tenant_id, run_id, source_type, file_subtype, required) VALUES (?, ?, ?, ?, ?)",
            (
                (run.tenant_id, run.run_id, item.source_type, item.file_subtype, item.required)
                for item in run.dependencies
            ),
        )

    def get_run(self, tenant_id: str, run_id: str) -> Run | None:
        return sqlite_publication.get_run(self, tenant_id, run_id)

    def list_waiting_runs_for_dependency(self, *args: Any, **kwargs: Any) -> tuple[Run, ...]:
        values = sqlite_publication.normalize_long_call(
            args, kwargs, sqlite_publication.DEPENDENCY_FIELDS, 100
        )
        return sqlite_publication.list_waiting_runs(self, values)

    def list_recoverable_runs(self, now: datetime, limit: int = 100) -> tuple[Run, ...]:
        return sqlite_publication.list_recoverable_runs(self, now, limit)

    def transition_run(self, command: TransitionRun, event: OutboxEvent) -> Run:
        return sqlite_publication.transition_run(self, command, event)

    def put_run_units(self, command: PutRunUnits) -> tuple[RunUnit, ...]:
        return sqlite_claims.put_run_units(self, command)

    def list_run_units(self, tenant_id: str, run_id: str) -> tuple[RunUnit, ...]:
        return sqlite_claims.list_run_units(self, tenant_id, run_id)

    def claim_run_unit(self, command: ClaimRunUnit) -> RunUnit | None:
        return sqlite_claims.claim_run_unit(self, command)

    def commit_run_unit(self, command: CommitRunUnit, event: OutboxEvent) -> RunUnit:
        return sqlite_claims.commit_run_unit(self, command, event)

    def fail_run_unit(self, command: FailRunUnit, event: OutboxEvent) -> RunUnit:
        return sqlite_claims.fail_run_unit(self, command, event)

    def finalize_run_cancellation(
        self, command: FinalizeRunCancellation, event: OutboxEvent
    ) -> Run:
        return sqlite_claims.finalize_run_cancellation(self, command, event)

    def reserve_run_dispatch(self, command: ReserveRunDispatch) -> RunDispatch:
        return sqlite_claims.reserve_run_dispatch(self, command)

    def bind_run_dispatch(self, command: BindRunDispatch) -> RunDispatch:
        return sqlite_claims.bind_run_dispatch(self, command)

    def finish_run_dispatch(self, command: FinishRunDispatch) -> RunDispatch:
        return sqlite_claims.finish_run_dispatch(self, command)

    def get_active_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None:
        return sqlite_claims.get_active_run_dispatch(self, tenant_id, run_id)

    def begin_idempotency(self, command: BeginIdempotency) -> IdempotencyOutcome:
        return sqlite_idempotency.begin_idempotency(self, command)

    def publish_dataset(self, command: PublishDataset) -> DatasetPointer:
        return sqlite_publication.publish_dataset(self, command)

    def get_dataset_pointer(self, tenant_id: str, dataset_name: str) -> DatasetPointer | None:
        return sqlite_publication.get_dataset_pointer(self, tenant_id, dataset_name)

    def get_dataset_version(
        self, tenant_id: str, dataset_name: str, version_id: str
    ) -> DatasetVersion | None:
        return sqlite_publication.get_dataset_version(
            self, (tenant_id, dataset_name, version_id)
        )

    def put_access_request(self, request: AccessRequest, event: OutboxEvent) -> None:
        sqlite_publication.put_access_request(self, request, event)

    def get_access_request(self, tenant_id: str, request_id: str) -> AccessRequest | None:
        return sqlite_publication.get_access_request(self, tenant_id, request_id)

    def decide_access_request(
        self, request: AccessRequest, event: OutboxEvent
    ) -> AccessRequest:
        return sqlite_publication.decide_access_request(self, request, event)
