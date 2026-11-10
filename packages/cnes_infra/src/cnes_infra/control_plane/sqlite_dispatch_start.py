"""Transações SQLite para envio, binding e liquidação do dispatch."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

from cnes_domain.control_plane.entities import Run, RunDispatch, RunUnit
from cnes_domain.control_plane.enums import (
    DispatchOutcome,
    DispatchState,
    RunState,
    RunUnitState,
)
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.control_plane.sqlite_claims import read_dispatch_record, write_dispatch_record
from cnes_infra.control_plane.sqlite_dispatch import (
    put_run_dispatch_bind,
    put_run_dispatch_finish,
    validate_run_dispatch_bind,
    validate_run_dispatch_finish,
)
from cnes_infra.control_plane.sqlite_schema import deserialize_model

if TYPE_CHECKING:
    from cnes_domain.control_plane.commands import (
        BindRunDispatch,
        CancelReservedRunDispatch,
        ClaimRunDispatchStart,
        FinishRunDispatch,
    )


def get_run_dispatch(store: Any, tenant_id: str, run_id: str) -> RunDispatch | None:
    with store.read_connection() as connection:
        return read_dispatch_record(connection, tenant_id, run_id)


def get_active_run_dispatch(store: Any, tenant_id: str, run_id: str) -> RunDispatch | None:
    with store.read_connection() as connection:
        row = connection.execute(
            "SELECT d.data FROM run_dispatches d JOIN runs r "
            "ON r.tenant_id = d.tenant_id AND r.run_id = d.run_id "
            "WHERE d.tenant_id = ? AND d.run_id = ? AND r.state = ?",
            (tenant_id, run_id, RunState.PROCESSING.value),
        ).fetchone()
    dispatch = None if row is None else deserialize_model(row[0], RunDispatch)
    if dispatch is None or dispatch.state is DispatchState.TERMINAL:
        return None
    if dispatch.start_claim is not None:
        return dispatch
    return dispatch if dispatch.lease_until > store.now() else None


_LATE_BIND_RUN_STATES = frozenset(
    {
        RunState.PUBLISHED,
        RunState.PUBLISHED_DEGRADED,
        RunState.FAILED,
        RunState.CANCELED,
    }
)
_COMPLETED_UNIT_STATES = frozenset(
    {RunUnitState.SUCCEEDED, RunUnitState.SUCCEEDED_DEGRADED, RunUnitState.FAILED_FINAL}
)


def _late_bind_proof(
    connection: Any, run: Run, dispatch: RunDispatch, command: Any
) -> bool:
    if run.state not in _LATE_BIND_RUN_STATES:
        return False
    if dispatch.state not in {
        DispatchState.RESERVED,
        DispatchState.STARTING,
        DispatchState.STARTED,
    }:
        return False
    if dispatch.start_claim != command.claim:
        return False
    units = tuple(
        deserialize_model(row[0], RunUnit)
        for row in connection.execute(
            "SELECT data FROM run_units WHERE tenant_id = ? AND run_id = ?",
            (run.tenant_id, run.run_id),
        ).fetchall()
    )
    completed = {
        unit.unit_id
        for unit in units
        if unit.dispatch_id == dispatch.dispatch_id and unit.state in _COMPLETED_UNIT_STATES
    }
    return bool(dispatch.unit_ids) and set(dispatch.unit_ids) <= completed


def bind_run_dispatch(store: Any, command: BindRunDispatch) -> RunDispatch:
    with store.write_transaction() as connection:
        replay = validate_run_dispatch_bind(connection, command)
        if replay is not None:
            return replay
        run = store.get_run_record(connection, command.tenant_id, command.run_id)
        if run is None:
            raise Conflict(ErrorCode.PARENT_NOT_PROCESSING)
        if run.state is not RunState.PROCESSING and run.state not in _LATE_BIND_RUN_STATES:
            raise Conflict(ErrorCode.PARENT_NOT_PROCESSING)
        dispatch = read_dispatch_record(connection, command.tenant_id, command.run_id)
        if dispatch is None or dispatch.dispatch_id != command.dispatch_id:
            raise Conflict(ErrorCode.DISPATCH_STALE)
        if dispatch.lease_until <= command.now and dispatch.start_claim is None:
            raise Conflict(ErrorCode.DISPATCH_EXPIRED)
        if dispatch.state is DispatchState.STARTED:
            if (
                command.claim == dispatch.start_claim
                and command.execution_ref == dispatch.execution_ref
            ):
                return dispatch
            raise Conflict(ErrorCode.DISPATCH_BINDING_CONFLICT)
        if dispatch.state not in {DispatchState.RESERVED, DispatchState.STARTING}:
            raise Conflict(ErrorCode.DISPATCH_TERMINAL)
        if dispatch.start_claim != command.claim:
            raise Conflict(ErrorCode.DISPATCH_BINDING_CONFLICT)
        if run.state is not RunState.PROCESSING and not _late_bind_proof(
            connection, run, dispatch, command
        ):
            raise Conflict(ErrorCode.PARENT_NOT_PROCESSING)
        started = dispatch.model_copy(
            update={
                "state": DispatchState.STARTED,
                "execution_ref": command.execution_ref,
                "lease_until": command.now + timedelta(seconds=command.lease_seconds),
                "start_claim": command.claim,
            }
        )
        write_dispatch_record(connection, started)
        put_run_dispatch_bind(connection, command, started)
        return started


def claim_start(
    store: Any,
    command: ClaimRunDispatchStart,
) -> RunDispatch | None:
    with store.write_transaction() as connection:
        dispatch = read_dispatch_record(connection, command.tenant_id, command.run_id)
        if dispatch is None or dispatch.dispatch_id != command.dispatch_id:
            return None
        if dispatch.state is DispatchState.STARTING:
            return None
        if dispatch.state is not DispatchState.RESERVED:
            return None
        if dispatch.lease_until <= command.now or dispatch.generation != command.claim.generation:
            return None
        claimed = dispatch.model_copy(
            update={"state": DispatchState.STARTING, "start_claim": command.claim}
        )
        write_dispatch_record(connection, claimed)
        return claimed


def cancel_reserved(
    store: Any,
    command: CancelReservedRunDispatch,
) -> RunDispatch | None:
    with store.write_transaction() as connection:
        dispatch = read_dispatch_record(connection, command.tenant_id, command.run_id)
        if dispatch is None or dispatch.dispatch_id != command.dispatch_id:
            return None
        if (
            dispatch.state is not DispatchState.RESERVED
            or dispatch.generation != command.claim.generation
        ):
            return None
        canceled = dispatch.model_copy(
            update={
                "state": DispatchState.TERMINAL,
                "terminal_outcome": DispatchOutcome.CANCELED,
                "canceled_before_send": True,
                "start_claim": command.claim,
            }
        )
        write_dispatch_record(connection, canceled)
        return canceled


def finish_run_dispatch(store: Any, command: FinishRunDispatch) -> RunDispatch:
    with store.write_transaction() as connection:
        replay = validate_run_dispatch_finish(connection, command)
        if replay is not None:
            return replay
        dispatch = read_dispatch_record(connection, command.tenant_id, command.run_id)
        if dispatch is None or dispatch.dispatch_id != command.dispatch_id:
            raise Conflict(ErrorCode.DISPATCH_STALE)
        if dispatch.lease_until <= command.finished_at and dispatch.start_claim is None:
            raise Conflict(ErrorCode.DISPATCH_EXPIRED)
        if dispatch.state is DispatchState.STARTING and command.claim != dispatch.start_claim:
            raise Conflict(ErrorCode.DISPATCH_BINDING_CONFLICT)
        if command.claim != dispatch.start_claim:
            raise Conflict(ErrorCode.DISPATCH_BINDING_CONFLICT)
        finished = dispatch.model_copy(
            update={"state": DispatchState.TERMINAL, "terminal_outcome": command.outcome}
        )
        write_dispatch_record(connection, finished)
        put_run_dispatch_finish(connection, command, finished)
        return finished
