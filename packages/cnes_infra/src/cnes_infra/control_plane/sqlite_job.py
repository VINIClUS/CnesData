"""SQLite job creation validation."""
from __future__ import annotations

from typing import Any

from cnes_domain.control_plane.enums import JobState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.control_plane.sqlite_schema import (
    put_job_creation_write,
    validate_job_creation_replay,
)


def validate_initial_job(job: Any) -> None:
    values = (job.attempt, job.fencing_token, job.lease_owner, job.lease_until)
    results = (job.result_manifest_id, job.result_manifest_key, job.error_code)
    if job.state is not JobState.PENDING or values != (0, 0, None, None) or any(results):
        raise Conflict(ErrorCode.JOB_INITIAL_STATE_INVALID)


def create_job(store: Any, job: Any, event: Any) -> Any:
    with store.write_transaction() as connection:
        current = store.get_job_record(connection, job.tenant_id, job.job_id)
        if current is not None:
            validate_job_creation_replay(connection, job, event)
            return current
        validate_initial_job(job)
        store.put_outbox_event(connection, event, job.tenant_id)
        store.put_job_record(connection, job)
        put_job_creation_write(connection, job, event)
        return job
