"""SQLAlchemy after_cursor_execute listener for N+1 query counting."""
from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import event

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


def install_query_counter(engine: Engine) -> None:

    @event.listens_for(engine, "after_cursor_execute")
    def _count(conn, cursor, statement, params, context, executemany):
        try:
            from central_api.middleware import increment_query_count
            increment_query_count()
        except LookupError:
            pass
