"""SQLite control-plane migrations."""
from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from cnes_domain.control_plane.commands import BindRunDispatch, FinishRunDispatch
from cnes_domain.control_plane.entities import AccessRequest, Job, RunDispatch
from cnes_domain.control_plane.enums import AccessRequestState, DispatchState, JobState
from cnes_infra.control_plane.sqlite_schema import deserialize_model, serialize_model

if TYPE_CHECKING:
    import sqlite3


def _add_bind_response_column(db: sqlite3.Connection) -> None:
    columns = {row[1] for row in db.execute("PRAGMA table_info(run_dispatch_bind_writes)")}
    if "response_data" not in columns:
        db.execute("ALTER TABLE run_dispatch_bind_writes ADD COLUMN response_data TEXT")


def _migrate_bind_responses(db: sqlite3.Connection) -> None:
    _add_bind_response_column(db)
    rows = db.execute(
        "SELECT b.tenant_id, b.run_id, b.dispatch_id, b.command_data, d.data "
        "FROM run_dispatch_bind_writes b LEFT JOIN run_dispatches d "
        "ON d.tenant_id = b.tenant_id AND d.run_id = b.run_id AND d.dispatch_id = b.dispatch_id "
        "WHERE b.response_data IS NULL"
    )
    for tenant_id, run_id, dispatch_id, command_data, data in rows:
        if data is None:
            continue
        command = deserialize_model(command_data, BindRunDispatch)
        dispatch = deserialize_model(data, RunDispatch).model_copy(
            update={
                "state": DispatchState.STARTED,
                "execution_ref": command.execution_ref,
                "lease_until": command.now + timedelta(seconds=command.lease_seconds),
                "terminal_outcome": None,
            }
        )
        db.execute(
            "UPDATE run_dispatch_bind_writes SET response_data = ? WHERE tenant_id = ? "
            "AND run_id = ? AND dispatch_id = ?",
            (serialize_model(dispatch), tenant_id, run_id, dispatch_id),
        )


def _add_finish_response_column(db: sqlite3.Connection) -> None:
    columns = {row[1] for row in db.execute("PRAGMA table_info(run_dispatch_terminal_writes)")}
    if "response_data" not in columns:
        db.execute("ALTER TABLE run_dispatch_terminal_writes ADD COLUMN response_data TEXT")


def _migrate_finish_responses(db: sqlite3.Connection) -> None:
    _add_finish_response_column(db)
    rows = db.execute(
        "SELECT b.tenant_id, b.run_id, b.dispatch_id, b.command_data, d.data "
        "FROM run_dispatch_terminal_writes b LEFT JOIN run_dispatches d "
        "ON d.tenant_id = b.tenant_id AND d.run_id = b.run_id AND d.dispatch_id = b.dispatch_id "
        "WHERE b.response_data IS NULL"
    )
    for tenant_id, run_id, dispatch_id, command_data, data in rows:
        if data is None:
            continue
        command = deserialize_model(command_data, FinishRunDispatch)
        dispatch = deserialize_model(data, RunDispatch).model_copy(
            update={"state": DispatchState.TERMINAL, "terminal_outcome": command.outcome}
        )
        db.execute(
            "UPDATE run_dispatch_terminal_writes SET response_data = ? WHERE tenant_id = ? "
            "AND run_id = ? AND dispatch_id = ?",
            (serialize_model(dispatch), tenant_id, run_id, dispatch_id),
        )


def _migrate_job_snapshot(db: sqlite3.Connection) -> None:
    columns = {row[1] for row in db.execute("PRAGMA table_info(job_creation_writes)")}
    if "job_data" not in columns:
        db.execute("ALTER TABLE job_creation_writes ADD COLUMN job_data TEXT")
    rows = db.execute(
        "SELECT w.tenant_id, w.job_id, j.data FROM job_creation_writes w JOIN jobs j "
        "ON j.tenant_id = w.tenant_id AND j.job_id = w.job_id WHERE w.job_data IS NULL"
    )
    for tenant_id, job_id, data in rows:
        job = deserialize_model(data, Job).model_copy(
            update={
                "state": JobState.PENDING,
                "attempt": 0,
                "fencing_token": 0,
                "lease_owner": None,
                "lease_until": None,
                "result_manifest_id": None,
                "result_manifest_key": None,
                "error_code": None,
            }
        )
        db.execute(
            "UPDATE job_creation_writes SET job_data = ? WHERE tenant_id = ? AND job_id = ?",
            (serialize_model(job), tenant_id, job_id),
        )


def _migrate_access_snapshot(db: sqlite3.Connection) -> None:
    columns = {row[1] for row in db.execute("PRAGMA table_info(access_requests)")}
    if "creation_request_data" not in columns:
        db.execute("ALTER TABLE access_requests ADD COLUMN creation_request_data TEXT")
    if "creation_event_data" not in columns:
        db.execute("ALTER TABLE access_requests ADD COLUMN creation_event_data TEXT")
    rows = db.execute(
        "SELECT tenant_id, request_id, data FROM access_requests "
        "WHERE creation_request_data IS NULL"
    )
    for tenant_id, request_id, data in rows:
        original = deserialize_model(data, AccessRequest).model_copy(
            update={"state": AccessRequestState.PENDING, "decided_by": None, "decided_at": None}
        )
        db.execute(
            "UPDATE access_requests SET creation_request_data = ? WHERE tenant_id = ? "
            "AND request_id = ?",
            (serialize_model(original), tenant_id, request_id),
        )


def _migrate_publication_response(db: sqlite3.Connection) -> None:
    columns = {row[1] for row in db.execute("PRAGMA table_info(dataset_publications)")}
    if "response_data" not in columns:
        db.execute("ALTER TABLE dataset_publications ADD COLUMN response_data TEXT")


def _migrate_run_units(db: sqlite3.Connection) -> None:
    columns = {row[1] for row in db.execute("PRAGMA table_info(runs)")}
    if "unit_registry_data" not in columns:
        db.execute("ALTER TABLE runs ADD COLUMN unit_registry_data TEXT")
    rows = db.execute(
        "SELECT tenant_id, run_id FROM runs WHERE unit_registry_data IS NULL"
    ).fetchall()
    for tenant_id, run_id in rows:
        units = db.execute(
            "SELECT data FROM run_units WHERE tenant_id = ? AND run_id = ? ORDER BY unit_id",
            (tenant_id, run_id),
        ).fetchall()
        if units:
            db.execute(
                "UPDATE runs SET unit_registry_data = ? WHERE tenant_id = ? AND run_id = ?",
                ("\x1e".join(row[0] for row in units), tenant_id, run_id),
            )


def migrate_schema(db: sqlite3.Connection) -> None:
    _migrate_bind_responses(db)
    _migrate_finish_responses(db)
    _migrate_job_snapshot(db)
    _migrate_access_snapshot(db)
    _migrate_publication_response(db)
    _migrate_run_units(db)
