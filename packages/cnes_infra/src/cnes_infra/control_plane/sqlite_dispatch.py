"""SQLite dispatch replay operations."""
from __future__ import annotations

from typing import Any

from cnes_domain.control_plane.entities import RunDispatch
from cnes_domain.control_plane.errors import Conflict
from cnes_infra.control_plane.sqlite_schema import deserialize_model, serialize_model


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


def put_run_dispatch_finish(connection: Any, command: Any, dispatch: Any) -> None:
    connection.execute(
        "INSERT INTO run_dispatch_terminal_writes "
        "(tenant_id, run_id, dispatch_id, command_data, response_data) VALUES (?, ?, ?, ?, ?)",
        (
            command.tenant_id,
            command.run_id,
            command.dispatch_id,
            serialize_model(command),
            serialize_model(dispatch),
        ),
    )


def validate_run_dispatch_finish(connection: Any, command: Any) -> RunDispatch | None:
    row = connection.execute(
        "SELECT command_data, response_data FROM run_dispatch_terminal_writes "
        "WHERE tenant_id = ? AND run_id = ? AND dispatch_id = ?",
        (command.tenant_id, command.run_id, command.dispatch_id),
    ).fetchone()
    if row is None:
        return None
    if row[0] != serialize_model(command):
        raise Conflict("dispatch_finish_conflict")
    return None if row[1] is None else deserialize_model(row[1], RunDispatch)
