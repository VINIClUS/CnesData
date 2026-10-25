"""Interleaving de escritores reais sem invalidar o lock do SQLite."""

import sqlite3
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import closing
from threading import Event
from typing import Any

import pytest

from packages.cnes_infra.tests.control_plane.outbox_cursor_support import OutboxHarness, SqlBoundary


def _write(sql: str) -> bool:
    return sql.lstrip().upper().startswith(("INSERT", "UPDATE", "REPLACE"))


def _lock_held(harness: OutboxHarness) -> bool:
    path = harness.root / "control.sqlite3"
    with closing(sqlite3.connect(path, timeout=0, isolation_level=None)) as probe:
        try:
            probe.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as error:
            if error.sqlite_errorcode not in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                raise
            return True
        probe.rollback()
    return False


class SQLiteCasInterleaving:
    def __init__(self, harness: OutboxHarness, monkeypatch: pytest.MonkeyPatch) -> None:
        self.harness = harness
        self.monkeypatch = monkeypatch
        self.contender = harness.restart()
        self.lock_observed: list[bool] = []
        self.statements: list[tuple[str, str]] = []

    def compete(self, first: Any, second: Any) -> list[bool]:
        attempted = Event()
        followers: list[Future[bool]] = []
        with ThreadPoolExecutor(max_workers=2) as executor:
            self._instrument_contender(attempted)

            def interleave(sql: str) -> None:
                self.statements.append(("first", sql))
                if not _write(sql) or followers:
                    return
                locked = _lock_held(self.harness)
                self.lock_observed.append(locked)
                followers.append(executor.submit(
                    self.contender.advance_outbox_cursor, second.expected, second.next_cursor
                ))
                if locked:
                    assert attempted.wait(timeout=3), "contender=did_not_reach_sql_boundary"
                    assert not followers[0].done(), "contender=completed_while_writer_holds_lock"
                else:
                    assert followers[0].result(timeout=3) is True

            connect = self.harness.adapter._connect
            self.monkeypatch.setattr(
                self.harness.adapter, "_connect", lambda: SqlBoundary(connect(), interleave)
            )
            leading = executor.submit(
                self.harness.adapter.advance_outbox_cursor, first.expected, first.next_cursor
            )
            result = leading.result(timeout=8)
            assert len(followers) == 1, "interleaving=missing_write_boundary"
            return [result, followers[0].result(timeout=8)]

    def _instrument_contender(self, attempted: Event) -> None:
        connect = self.contender._connect

        def observe(sql: str) -> None:
            self.statements.append(("second", sql))
            if sql.lstrip().upper().startswith("BEGIN") or _write(sql):
                attempted.set()

        self.monkeypatch.setattr(
            self.contender, "_connect", lambda: SqlBoundary(connect(), observe)
        )
