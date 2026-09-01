"""SQLite lease and fencing operations."""

from __future__ import annotations

from datetime import timedelta
from hashlib import sha256
from typing import TYPE_CHECKING, Any

from cnes_domain.control_plane.entities import RunDispatch, RunUnit
from cnes_domain.control_plane.enums import (
    AgentState,
    DispatchState,
    JobState,
    RunState,
    RunUnitState,
)
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost
from cnes_domain.control_plane.transitions import transition_run, transition_run_unit
from cnes_infra.control_plane.sqlite_schema import (
    deserialize_model,
    put_job_cancellation,
    put_job_terminal_write,
    put_run_cancellation,
    put_run_dispatch_bind,
    put_run_dispatch_finish,
    put_run_unit_terminal_write,
    serialize_model,
    validate_job_cancellation,
    validate_job_terminal_replay,
    validate_run_cancellation,
    validate_run_dispatch_bind,
    validate_run_dispatch_finish,
    validate_run_dispatch_wave,
    validate_run_unit_terminal_replay,
)

if TYPE_CHECKING:
    from cnes_domain.control_plane.commands import (
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
        PutRunUnits,
        RenewJobLease,
        ReserveRunDispatch,
    )
    from cnes_domain.control_plane.entities import Job, OutboxEvent


def _validate_job_fence(store: Any, connection: Any, command: Any) -> Job:
    job = store.get_job_record(connection, command.tenant_id, command.job_id)
    agent = None if job is None else store.get_agent_record(connection, job.tenant_id, job.agent_id)
    if job is None or job.state is not JobState.LEASED:
        raise LeaseLost("job_not_leased")
    if agent is None or agent.state is AgentState.REVOKED:
        raise LeaseLost("agent_revoked")
    if job.lease_owner != command.owner:
        raise LeaseLost("owner_mismatch")
    if job.fencing_token != command.fencing_token:
        raise FenceRejected("fence_mismatch")
    if job.lease_until is None or job.lease_until <= store.now():
        raise LeaseLost("lease_expired")
    return job


def claim_job(store: Any, command: ClaimJob) -> Job | None:
    with store.write_transaction() as connection:
        job = store.get_job_record(connection, command.tenant_id, command.job_id)
        agent = None if job is None else store.get_agent_record(
            connection, job.tenant_id, job.agent_id)
        if job is None or agent is None or agent.state is AgentState.REVOKED:
            return None
        retryable = job.state in {JobState.PENDING, JobState.FAILED_RETRYABLE}
        expired = job.state is JobState.LEASED and (
            job.lease_until is None or job.lease_until <= command.now)
        if not retryable and not expired:
            return None
        claimed = job.model_copy(
            update={
                "state": JobState.LEASED,
                "attempt": job.attempt + 1,
                "fencing_token": job.fencing_token + 1,
                "lease_owner": command.owner,
                "lease_until": command.now + timedelta(seconds=command.lease_seconds),
            }
        )
        store.put_job_record(connection, claimed)
        return claimed


def renew_job_lease(store: Any, command: RenewJobLease) -> Job:
    with store.write_transaction() as connection:
        job = _validate_job_fence(store, connection, command)
        renewed = job.model_copy(
            update={"lease_until": command.now + timedelta(seconds=command.lease_seconds)}
        )
        store.put_job_record(connection, renewed)
        return renewed


def _validate_manifest_identity(job: Job, manifest: Any) -> None:
    expected = (
        job.tenant_id, job.agent_id, job.source_type,
        job.file_subtype, job.competencia, job.requested_snapshot_mode,
    )
    actual = (
        manifest.tenant_id, manifest.agent_id, manifest.source_type,
        manifest.file_subtype, manifest.competencia, manifest.snapshot_mode,
    )
    if actual != expected:
        raise Conflict("manifest_identity_mismatch")


def complete_job(store: Any, command: CompleteJob, event: OutboxEvent) -> Job:
    with store.write_transaction() as connection:
        job = store.get_job_record(connection, command.tenant_id, command.job_id)
        if job is not None and job.state is JobState.SUCCEEDED:
            validate_job_terminal_replay(connection, job, command, event)
            return job
        job = _validate_job_fence(store, connection, command)
        manifest = command.manifest
        _validate_manifest_identity(job, manifest)
        completed = job.model_copy(
            update={
                "state": JobState.SUCCEEDED,
                "lease_owner": None,
                "lease_until": None,
                "result_manifest_id": manifest.manifest_id,
                "result_manifest_key": manifest.manifest_key,
            }
        )
        store.put_outbox_event(connection, event, command.tenant_id)
        store.put_job_record(connection, completed)
        store.put_manifest_record(connection, manifest)
        put_job_terminal_write(connection, "complete", command, event)
        return completed
def fail_job(store: Any, command: FailJob, event: OutboxEvent) -> Job:
    with store.write_transaction() as connection:
        job = store.get_job_record(connection, command.tenant_id, command.job_id)
        failed_states = {JobState.FAILED_RETRYABLE, JobState.FAILED_FINAL}
        if job is not None and job.state in failed_states:
            validate_job_terminal_replay(connection, job, command, event)
            return job
        job = _validate_job_fence(store, connection, command)
        state = JobState.FAILED_RETRYABLE if command.retryable else JobState.FAILED_FINAL
        failed = job.model_copy(
            update={
                "state": state,
                "lease_owner": None,
                "lease_until": None,
                "error_code": command.error_code,
            }
        )
        store.put_outbox_event(connection, event, command.tenant_id)
        store.put_job_record(connection, failed)
        put_job_terminal_write(connection, "fail", command, event)
        return failed
def cancel_job(store: Any, command: CancelJob, event: OutboxEvent) -> Job:
    with store.write_transaction() as connection:
        job = store.get_job_record(connection, command.tenant_id, command.job_id)
        if job is None:
            raise Conflict("job_missing")
        if job.state is JobState.CANCEL_REQUESTED:
            validate_job_cancellation(connection, command, event)
            return job
        if job.state is not JobState.LEASED:
            raise Conflict("job_not_leased")
        canceled = job.model_copy(update={"state": JobState.CANCEL_REQUESTED})
        store.put_outbox_event(connection, event, command.tenant_id)
        store.put_job_record(connection, canceled)
        put_job_cancellation(connection, command, event)
        return canceled


def _list_run_units(connection: Any, tenant_id: str, run_id: str) -> tuple[RunUnit, ...]:
    rows = connection.execute(
        "SELECT data FROM run_units WHERE tenant_id = ? AND run_id = ? ORDER BY unit_id",
        (tenant_id, run_id),
    ).fetchall()
    return tuple(deserialize_model(row[0], RunUnit) for row in rows)


def list_run_units(store: Any, tenant_id: str, run_id: str) -> tuple[RunUnit, ...]:
    with store.read_connection() as connection:
        return _list_run_units(connection, tenant_id, run_id)


def _put_run_unit(connection: Any, unit: RunUnit) -> None:
    connection.execute(
        "INSERT INTO run_units (tenant_id, run_id, unit_id, state, lease_until, "
        "dispatch_id, data) VALUES (?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (tenant_id, run_id, unit_id) DO UPDATE SET state = excluded.state, "
        "lease_until = excluded.lease_until, dispatch_id = excluded.dispatch_id, "
        "data = excluded.data",
        (
            unit.tenant_id,
            unit.run_id,
            unit.unit_id,
            unit.state.value,
            None if unit.lease_until is None else unit.lease_until.isoformat(),
            unit.dispatch_id,
            serialize_model(unit),
        ),
    )


def put_run_units(store: Any, command: PutRunUnits) -> tuple[RunUnit, ...]:
    with store.write_transaction() as connection:
        run = store.get_run_record(connection, command.tenant_id, command.run_id)
        if run is None or run.state is not command.expected_run_state:
            raise Conflict("run_state_conflict")
        current = _list_run_units(connection, command.tenant_id, command.run_id)
        canonical = tuple(sorted(command.units, key=lambda unit: unit.unit_id))
        if current:
            if current != canonical:
                raise Conflict("units_conflict")
            return current
        for unit in canonical:
            _put_run_unit(connection, unit)
        return canonical


def _get_dispatch(connection: Any, tenant_id: str, run_id: str) -> RunDispatch | None:
    row = connection.execute(
        "SELECT data FROM run_dispatches WHERE tenant_id = ? AND run_id = ?",
        (tenant_id, run_id),
    ).fetchone()
    return None if row is None else deserialize_model(row[0], RunDispatch)


def _put_dispatch(connection: Any, dispatch: RunDispatch) -> None:
    connection.execute(
        "INSERT INTO run_dispatches (tenant_id, run_id, dispatch_id, wave_id, generation, "
        "state, lease_until, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT (tenant_id, run_id) DO UPDATE SET dispatch_id = excluded.dispatch_id, "
        "wave_id = excluded.wave_id, generation = excluded.generation, state = excluded.state, "
        "lease_until = excluded.lease_until, data = excluded.data",
        (
            dispatch.tenant_id,
            dispatch.run_id,
            dispatch.dispatch_id,
            dispatch.wave_id,
            dispatch.generation,
            dispatch.state.value,
            dispatch.lease_until.isoformat(),
            serialize_model(dispatch),
        ),
    )


def _has_live_unit_lease(connection: Any, dispatch: RunDispatch, now: Any) -> bool:
    units = _list_run_units(connection, dispatch.tenant_id, dispatch.run_id)
    return any(
        unit.dispatch_id == dispatch.dispatch_id
        and unit.state is RunUnitState.LEASED
        and unit.lease_until > now
        for unit in units
    )


def reserve_run_dispatch(store: Any, command: ReserveRunDispatch) -> RunDispatch:
    with store.write_transaction() as connection:
        run = store.get_run_record(connection, command.tenant_id, command.run_id)
        if run is None or run.state is not RunState.PROCESSING:
            raise Conflict("parent_not_processing")
        validate_run_dispatch_wave(connection, command)
        current = _get_dispatch(connection, command.tenant_id, command.run_id)
        same_wave = current is not None and current.wave_id == command.wave_id
        replay = (
            same_wave
            and current.lease_until > command.now
            and current.state in {DispatchState.RESERVED, DispatchState.STARTED}
        )
        if replay:
            return current
        live = current and current.state is not DispatchState.TERMINAL
        lease_live = current is not None and current.lease_until > command.now
        if live and (lease_live or _has_live_unit_lease(connection, current, command.now)):
            raise Conflict("dispatch_live")
        generation = 1 if current is None else current.generation + 1
        identity = "\x1f".join(
            (command.tenant_id, command.run_id, command.wave_id, str(generation), *command.unit_ids)
        )
        dispatch = RunDispatch(
            tenant_id=command.tenant_id,
            run_id=command.run_id,
            wave_id=command.wave_id,
            dispatch_id=sha256(identity.encode()).hexdigest()[:16],
            generation=generation,
            unit_ids=command.unit_ids,
            state=DispatchState.RESERVED,
            lease_until=command.now + timedelta(seconds=command.lease_seconds),
        )
        _put_dispatch(connection, dispatch)
        return dispatch


def get_active_run_dispatch(store: Any, tenant_id: str, run_id: str) -> RunDispatch | None:
    with store.read_connection() as connection:
        dispatch = _get_dispatch(connection, tenant_id, run_id)
    if dispatch is None or dispatch.state is DispatchState.TERMINAL:
        return None
    return dispatch if dispatch.lease_until > store.now() else None

def bind_run_dispatch(store: Any, command: BindRunDispatch) -> RunDispatch:
    with store.write_transaction() as connection:
        run = store.get_run_record(connection, command.tenant_id, command.run_id)
        if run is None or run.state is not RunState.PROCESSING:
            raise Conflict("parent_not_processing")
        dispatch = _get_dispatch(connection, command.tenant_id, command.run_id)
        if dispatch is None or dispatch.dispatch_id != command.dispatch_id:
            raise Conflict("dispatch_stale")
        if dispatch.state is DispatchState.STARTED:
            validate_run_dispatch_bind(connection, command)
            return dispatch
        if dispatch.lease_until <= command.now:
            raise Conflict("dispatch_expired")
        if dispatch.state is not DispatchState.RESERVED:
            raise Conflict("dispatch_terminal")
        started = dispatch.model_copy(
            update={
                "state": DispatchState.STARTED,
                "execution_ref": command.execution_ref,
                "lease_until": command.now + timedelta(seconds=command.lease_seconds),
            }
        )
        _put_dispatch(connection, started)
        put_run_dispatch_bind(connection, command)
        return started
def finish_run_dispatch(store: Any, command: FinishRunDispatch) -> RunDispatch:
    with store.write_transaction() as connection:
        dispatch = _get_dispatch(connection, command.tenant_id, command.run_id)
        if dispatch is None or dispatch.dispatch_id != command.dispatch_id:
            raise Conflict("dispatch_stale")
        if dispatch.state is DispatchState.TERMINAL:
            validate_run_dispatch_finish(connection, command)
            return dispatch
        if dispatch.lease_until <= command.finished_at:
            raise Conflict("dispatch_expired")
        finished = dispatch.model_copy(
            update={"state": DispatchState.TERMINAL, "terminal_outcome": command.outcome}
        )
        _put_dispatch(connection, finished)
        put_run_dispatch_finish(connection, command)
        return finished


def claim_run_unit(store: Any, command: ClaimRunUnit) -> RunUnit | None:
    with store.write_transaction() as connection:
        units = _list_run_units(connection, command.tenant_id, command.run_id)
        unit = next((item for item in units if item.unit_id == command.unit_id), None)
        run = store.get_run_record(connection, command.tenant_id, command.run_id)
        dispatch = _get_dispatch(connection, command.tenant_id, command.run_id)
        valid_dispatch = (
            dispatch is not None
            and dispatch.dispatch_id == command.dispatch_id
            and dispatch.state is not DispatchState.TERMINAL
            and dispatch.lease_until > command.now
            and command.unit_id in dispatch.unit_ids
        )
        invalid = unit is None or run is None or run.state is not RunState.PROCESSING
        if invalid or not valid_dispatch:
            return None
        claimable = unit.state in {RunUnitState.PENDING, RunUnitState.FAILED_RETRYABLE}
        claimable |= unit.state is RunUnitState.LEASED and (
            unit.dispatch_id != command.dispatch_id or unit.lease_until <= command.now
        )
        if not claimable:
            return None
        claimed = unit.model_copy(
            update={
                "state": RunUnitState.LEASED,
                "attempt": unit.attempt + 1,
                "fencing_token": unit.fencing_token + 1,
                "lease_owner": command.owner,
                "lease_until": command.now + timedelta(seconds=command.lease_seconds),
                "dispatch_id": command.dispatch_id,
            }
        )
        _put_run_unit(connection, claimed)
        return claimed


def _validate_unit_fence(store: Any, connection: Any, command: Any) -> tuple[RunUnit, Any]:
    units = _list_run_units(connection, command.tenant_id, command.run_id)
    unit = next((item for item in units if item.unit_id == command.unit_id), None)
    run = store.get_run_record(connection, command.tenant_id, command.run_id)
    dispatch = _get_dispatch(connection, command.tenant_id, command.run_id)
    if run is None or run.state is not RunState.PROCESSING:
        raise LeaseLost("parent_not_processing")
    if dispatch is None or dispatch.dispatch_id != command.dispatch_id:
        raise LeaseLost("dispatch_mismatch")
    if dispatch.state not in {DispatchState.RESERVED, DispatchState.STARTED}:
        raise LeaseLost("dispatch_inactive")
    if dispatch.lease_until <= store.now():
        raise LeaseLost("dispatch_expired")
    if unit is None or unit.state is not RunUnitState.LEASED:
        raise LeaseLost("unit_not_leased")
    if unit.dispatch_id != command.dispatch_id:
        raise LeaseLost("unit_dispatch_mismatch")
    if unit.lease_owner != command.owner:
        raise LeaseLost("owner_mismatch")
    if unit.fencing_token != command.fencing_token:
        raise FenceRejected("fence_mismatch")
    if unit.lease_until is None or unit.lease_until <= store.now():
        raise LeaseLost("lease_expired")
    return unit, run


def commit_run_unit(store: Any, command: CommitRunUnit, event: OutboxEvent) -> RunUnit:
    with store.write_transaction() as connection:
        current = next(
            (unit for unit in _list_run_units(connection, command.tenant_id, command.run_id)
             if unit.unit_id == command.unit_id), None
        )
        if current is not None and current.state is RunUnitState.SUCCEEDED:
            return validate_run_unit_terminal_replay(connection, current, command, event)
        unit, run = _validate_unit_fence(store, connection, command)
        completed = transition_run_unit(unit, RunUnitState.SUCCEEDED, run).model_copy(
            update={
                "lease_owner": None,
                "lease_until": None,
                "output_manifests": command.output_manifests,
            }
        )
        store.put_outbox_event(connection, event, command.tenant_id)
        _put_run_unit(connection, completed)
        put_run_unit_terminal_write(connection, "commit", command, event)
        return completed


def fail_run_unit(store: Any, command: FailRunUnit, event: OutboxEvent) -> RunUnit:
    with store.write_transaction() as connection:
        current = next(
            (unit for unit in _list_run_units(connection, command.tenant_id, command.run_id)
             if unit.unit_id == command.unit_id), None
        )
        terminal = {
            RunUnitState.FAILED_RETRYABLE,
            RunUnitState.FAILED_FINAL,
            RunUnitState.SUCCEEDED_DEGRADED,
        }
        if current is not None and current.state in terminal:
            return validate_run_unit_terminal_replay(connection, current, command, event)
        unit, run = _validate_unit_fence(store, connection, command)
        optional = any(
            (item.source_type, item.file_subtype) == (unit.source_type, unit.file_subtype)
            and not item.required
            for item in run.dependencies
        )
        if command.retryable:
            state = RunUnitState.FAILED_RETRYABLE
        elif optional:
            state = RunUnitState.SUCCEEDED_DEGRADED
        else:
            state = RunUnitState.FAILED_FINAL
        failed = unit.model_copy(
            update={"error_code": command.error_code, "lease_owner": None, "lease_until": None}
        )
        failed = transition_run_unit(failed, state, run)
        store.put_outbox_event(connection, event, command.tenant_id)
        _put_run_unit(connection, failed)
        put_run_unit_terminal_write(connection, "fail", command, event)
        if state is RunUnitState.SUCCEEDED_DEGRADED:
            source = f"{unit.source_type}/{unit.file_subtype}"
            missing_sources = tuple(dict.fromkeys((*run.missing_sources, source)))
            updated = run.model_copy(update={"missing_sources": missing_sources})
            store.put_run_record(connection, updated)
        return failed
def finalize_run_cancellation(
    store: Any, command: FinalizeRunCancellation, event: OutboxEvent
) -> Any:
    with store.write_transaction() as connection:
        run = store.get_run_record(connection, command.tenant_id, command.run_id)
        if run is not None and run.state is RunState.CANCELED:
            validate_run_cancellation(connection, command, event)
            return run
        if run is None or run.state is not RunState.CANCEL_REQUESTED:
            raise Conflict("run_not_canceling")
        terminal = {
            RunUnitState.SUCCEEDED, RunUnitState.SUCCEEDED_DEGRADED,
            RunUnitState.FAILED_FINAL, RunUnitState.CANCELED,
        }
        units = _list_run_units(connection, command.tenant_id, command.run_id)
        store.put_outbox_event(connection, event, command.tenant_id)
        for unit in units:
            if unit.state not in terminal:
                canceled = transition_run_unit(unit, RunUnitState.CANCELED, run).model_copy(
                    update={"lease_owner": None, "lease_until": None})
                _put_run_unit(connection, canceled)
        canceled_run = transition_run(run, RunState.CANCELED)
        store.put_run_record(connection, canceled_run)
        put_run_cancellation(connection, command, event)
        return canceled_run
