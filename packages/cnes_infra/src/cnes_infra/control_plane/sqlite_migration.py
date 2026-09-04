"""SQLite control-plane migrations."""
from __future__ import annotations

from typing import TYPE_CHECKING

from cnes_domain.control_plane.entities import AccessRequest
from cnes_domain.control_plane.enums import AccessRequestState
from cnes_infra.control_plane.sqlite_schema import deserialize_model, serialize_model

if TYPE_CHECKING:
    import sqlite3


def migrate_schema(db: sqlite3.Connection) -> None:
    bind_columns = {row[1] for row in db.execute("PRAGMA table_info(run_dispatch_bind_writes)")}
    if "response_data" not in bind_columns:
        db.execute("ALTER TABLE run_dispatch_bind_writes ADD COLUMN response_data TEXT")
    db.execute(
        "UPDATE run_dispatch_bind_writes SET response_data = (SELECT data FROM "
        "run_dispatches WHERE run_dispatches.tenant_id = run_dispatch_bind_writes.tenant_id "
        "AND run_dispatches.run_id = run_dispatch_bind_writes.run_id) "
        "WHERE response_data IS NULL"
    )
    job_columns = {row[1] for row in db.execute("PRAGMA table_info(job_creation_writes)")}
    if "job_data" not in job_columns:
        db.execute("ALTER TABLE job_creation_writes ADD COLUMN job_data TEXT")
    db.execute(
        "UPDATE job_creation_writes SET job_data = (SELECT data FROM jobs WHERE "
        "jobs.tenant_id = job_creation_writes.tenant_id "
        "AND jobs.job_id = job_creation_writes.job_id) WHERE job_data IS NULL"
    )
    run_columns = {row[1] for row in db.execute("PRAGMA table_info(runs)")}
    if "unit_registry_data" not in run_columns:
        db.execute("ALTER TABLE runs ADD COLUMN unit_registry_data TEXT")
    access_columns = {row[1] for row in db.execute("PRAGMA table_info(access_requests)")}
    if "creation_request_data" not in access_columns:
        db.execute("ALTER TABLE access_requests ADD COLUMN creation_request_data TEXT")
    for tenant_id, request_id, data in db.execute(
        "SELECT tenant_id, request_id, data FROM access_requests "
        "WHERE creation_request_data IS NULL"
    ):
        original = deserialize_model(data, AccessRequest).model_copy(
            update={"state": AccessRequestState.PENDING, "decided_by": None, "decided_at": None}
        )
        db.execute(
            "UPDATE access_requests SET creation_request_data = ? WHERE "
            "tenant_id = ? AND request_id = ?",
            (serialize_model(original), tenant_id, request_id),
        )
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
