"""Disputas CAS com duas instâncias, conexões de cada thread e releitura nativa."""

import logging
import sqlite3
from collections.abc import Generator
from datetime import UTC, datetime
from threading import Barrier, Event, Thread
from typing import cast

import pytest

from cnes_domain.refresh_tokens import (
    RefreshTokenContext,
    RefreshTokenRecord,
    RefreshTokenUnavailable,
)

from . import test_sqlite_refresh_token_mutations as cases


@pytest.fixture(autouse=True)
def observacoes_sanitizadas(
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> Generator[None]:
    caplog.set_level(logging.DEBUG)
    yield
    captured = capsys.readouterr()
    cases.assert_no_sensitive(caplog.text + captured.out + captured.err)


class Gates:
    def __init__(self) -> None:
        self.barrier: Barrier | None = None
        self.hold_update = False
        self.a_progress = Event()
        self.a_update = Event()
        self.b_started = Event()
        self.b_attempting = Event()
        self.b_acquired = Event()
        self.release = Event()
        self.reservations: list[tuple[bool, bool, bool, int | None]] = []
        self.closed_probes = 0


def assert_write_reserved(boundary: "WorkerDatabase", selected: bool) -> None:
    probe = sqlite3.connect(boundary.path, isolation_level=None, timeout=0)
    try:
        try:
            probe.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as error:
            code = error.sqlite_errorcode
        else:
            code = None
        observation = (
            selected,
            boundary.gates.a_update.is_set(),
            boundary.gates.b_started.is_set(),
            code,
        )
        boundary.gates.reservations.append(observation)
        assert observation[:3] == (False, False, False)
        assert code == sqlite3.SQLITE_BUSY, "write_reservation_missing_before_select"
    finally:
        try:
            probe.rollback()
        finally:
            probe.close()
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                sqlite3.Connection.execute(probe, "SELECT 1")
            boundary.gates.closed_probes += 1


class GatedConnection(cases.ObservedConnection):
    selected = False

    def coordinate_a(self, verb: str) -> None:
        boundary = cast("WorkerDatabase", self.database)
        if boundary.role != "a" or not boundary.gates.hold_update:
            return
        if verb.startswith("SELECT") and not self.selected:
            assert_write_reserved(boundary, self.selected)
        if verb.startswith("UPDATE"):
            assert self.selected
            assert boundary.gates.reservations == [(False, False, False, sqlite3.SQLITE_BUSY)]
            assert boundary.gates.closed_probes == 1
            boundary.gates.a_update.set()
            boundary.gates.a_progress.set()
            assert boundary.gates.release.wait(5), "release_timeout"

    def execute(self, sql: str, parameters: object = (), /) -> sqlite3.Cursor:
        boundary = cast("WorkerDatabase", self.database)
        verb = sql.lstrip().upper()
        self.coordinate_a(verb)
        attempting = boundary.role == "b" and verb.startswith("BEGIN")
        if attempting:
            boundary.gates.b_attempting.set()
        cursor = super().execute(sql, parameters)
        if attempting:
            boundary.gates.b_acquired.set()
        if verb.startswith("SELECT"):
            self.selected = True
        return cursor


class WorkerDatabase(cases.Database):
    def __init__(self, original: cases.Database, role: str, gates: Gates) -> None:
        super().__init__(original.path)
        self.role = role
        self.gates = gates
        self.connection_type = GatedConnection
        self.result: bool | None = None
        self.error: Exception | None = None
        self.done = Event()

    def connect(self) -> cases.ObservedConnection:
        connection = super().connect()
        if self.gates.barrier is not None:
            try:
                self.gates.barrier.wait(timeout=5)
            except Exception:
                connection.close()
                raise
        return connection

    def run(self, operation: str) -> None:
        try:
            self.result = getattr(self.store(), operation)(cases.PLAINTEXT, cases.mutation())
            self.assert_closed()
        except Exception as error:
            self.error = error
        finally:
            for connection in self.connections:
                if not connection.closed:
                    sqlite3.Connection.close(connection)
            self.done.set()
            if self.role == "a":
                self.gates.a_progress.set()


def finish(workers: tuple[WorkerDatabase, WorkerDatabase]) -> None:
    for worker in workers:
        if worker.error is not None:
            raise worker.error
    assert [worker.result for worker in workers].count(True) == 1
    assert [worker.result for worker in workers].count(False) == 1
    assert all(type(worker.result) is bool for worker in workers)


def join(threads: tuple[Thread, Thread], gates: Gates) -> None:
    gates.release.set()
    for thread in threads:
        if thread.ident is not None:
            thread.join(timeout=6)
            assert not thread.is_alive(), "worker_not_joined"


def assert_restart(database: cases.Database, revoked: bool) -> None:
    record = database.store().validate(
        cases.PLAINTEXT,
        RefreshTokenContext(cases.TENANT, cases.NOW),
    )
    if revoked:
        assert record is None
        expected = (*cases.ROW[:7], "2026-01-02T03:04:06.100002+00:00", 8)
    else:
        assert record == RefreshTokenRecord(
            cases.DIGEST,
            "33333333-3333-4333-8333-333333333333",
            cases.TENANT,
            "synthetic-cas-host",
            datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC),
            datetime(2026, 1, 2, 3, 4, 6, 654321, tzinfo=UTC),
            datetime(2026, 1, 2, 3, 4, 6, 100002, tzinfo=UTC),
            None,
            8,
        )
        expected = (*cases.ROW[:6], "2026-01-02T03:04:06.100002+00:00", None, 8)
    assert database.rows() == [expected]
    database.assert_closed()


@pytest.mark.parametrize(
    "operations",
    [("mark_used", "mark_used"), ("revoke", "revoke"), ("mark_used", "revoke")],
)
def test_duas_instancias_disputam_mesma_versao_com_um_vencedor(
    tmp_path,
    operations: tuple[str, str],
) -> None:
    database = cases.Database(tmp_path / "race.sqlite")
    database.seed(cases.ROW)
    gates = Gates()
    gates.barrier = Barrier(2)
    workers = (WorkerDatabase(database, "a", gates), WorkerDatabase(database, "b", gates))
    threads = tuple(
        Thread(target=w.run, args=(op,)) for w, op in zip(workers, operations, strict=True)
    )
    try:
        for thread in threads:
            thread.start()
    finally:
        join(cast("tuple[Thread, Thread]", threads), gates)
    finish(workers)
    winner = operations[0] if workers[0].result is True else operations[1]
    assert_restart(database, winner == "revoke")
    if operations == ("mark_used", "revoke") and winner == "revoke":
        assert database.store().mark_used(cases.PLAINTEXT, cases.mutation(version=8)) is False
        assert database.store().revoke(cases.PLAINTEXT, cases.mutation(version=8)) is False
        assert database.rows() == [(*cases.ROW[:7], "2026-01-02T03:04:06.100002+00:00", 8)]
        database.assert_closed()
    if operations == ("mark_used", "revoke") and winner == "mark_used":
        assert database.store().revoke(cases.PLAINTEXT, cases.mutation(version=8)) is True
        assert database.rows() == [
            (
                *cases.ROW[:6],
                "2026-01-02T03:04:06.100002+00:00",
                "2026-01-02T03:04:06.100002+00:00",
                9,
            )
        ]
        assert (
            database.store().validate(
                cases.PLAINTEXT,
                RefreshTokenContext(cases.TENANT, cases.NOW),
            )
            is None
        )
        assert database.store().mark_used(cases.PLAINTEXT, cases.mutation(version=9)) is False
        assert database.store().revoke(cases.PLAINTEXT, cases.mutation(version=9)) is False
        assert database.rows() == [
            (
                *cases.ROW[:6],
                "2026-01-02T03:04:06.100002+00:00",
                "2026-01-02T03:04:06.100002+00:00",
                9,
            )
        ]
        database.assert_closed()


@pytest.mark.parametrize("journal", ["DELETE", "WAL"])
def test_concorrente_rele_estado_apos_commit_sem_promover_snapshot(tmp_path, journal: str) -> None:
    database = cases.Database(tmp_path / "serialized.sqlite")
    database.seed(cases.ROW)
    statement, expected = {
        "DELETE": ("PRAGMA journal_mode=DELETE", [("delete",)]),
        "WAL": ("PRAGMA journal_mode=WAL", [("wal",)]),
    }[journal]
    assert database.query(statement) == expected
    gates = Gates()
    gates.hold_update = True
    workers = (WorkerDatabase(database, "a", gates), WorkerDatabase(database, "b", gates))
    threads = (
        Thread(target=workers[0].run, args=("mark_used",)),
        Thread(target=workers[1].run, args=("mark_used",)),
    )
    try:
        threads[0].start()
        assert gates.a_progress.wait(5), "first_worker_timeout"
        if workers[0].error is not None:
            raise workers[0].error
        assert gates.reservations == [(False, False, False, sqlite3.SQLITE_BUSY)]
        assert gates.closed_probes == 1
        assert gates.a_update.is_set()
        gates.b_started.set()
        threads[1].start()
        assert gates.b_attempting.wait(5), "second_begin_not_attempted"
        assert not gates.b_acquired.is_set()
    finally:
        join(threads, gates)
    finish(workers)
    assert workers[0].result is True
    assert workers[1].result is False
    assert gates.b_acquired.is_set()
    assert_restart(database, False)


def test_lock_esgotado_e_unavailable_sem_mutacao(tmp_path) -> None:
    database = cases.Database(tmp_path / "locked.sqlite")
    database.seed(cases.ROW)
    database.timeout = 0
    blocker = sqlite3.connect(database.path, isolation_level=None)
    try:
        blocker.execute("BEGIN IMMEDIATE")
        with pytest.raises(RefreshTokenUnavailable) as caught:
            database.store().mark_used(cases.PLAINTEXT, cases.mutation())
        cases.assert_sanitized(caught.value, "")
        assert database.rows() == [cases.ROW]
        assert database.commits == 0
        database.assert_closed()
    finally:
        blocker.rollback()
        blocker.close()
    assert database.store().mark_used(cases.PLAINTEXT, cases.mutation()) is True
    assert_restart(database, False)
