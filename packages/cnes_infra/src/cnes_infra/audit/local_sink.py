"""Sink de auditoria local recuperável em JSONL e Parquet."""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from itertools import groupby
from pathlib import Path
from secrets import token_hex
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq

from cnes_domain.control_plane.entities import OutboxEvent
from cnes_domain.control_plane.errors import Conflict

if TYPE_CHECKING:
    from collections.abc import Iterator

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    log_path TEXT NOT NULL,
    offset INTEGER NOT NULL,
    length INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    batch_path TEXT
)
"""
_PARQUET_SCHEMA = pa.schema(
    [
        pa.field("tenant_id", pa.string()),
        pa.field("event_id", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("aggregate_id", pa.string()),
        pa.field("payload_json", pa.string()),
        pa.field("created_at", pa.string()),
        pa.field("delivered_at", pa.string()),
    ]
)


@dataclass(frozen=True, slots=True)
class _IndexEntry:
    event_id: str
    tenant_id: str
    log_path: str
    offset: int
    length: int
    digest: str


@dataclass(frozen=True, slots=True)
class _PendingAppend:
    event: OutboxEvent
    path: Path
    record: bytes


@dataclass(frozen=True, slots=True)
class _BatchEntry:
    event_id: str
    tenant_id: str
    log_path: str
    offset: int
    length: int


def _canonical_event(event: OutboxEvent) -> bytes:
    return json.dumps(
        event.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
    ).encode()


def _safe_component(value: str) -> str:
    invalid = value in {"", ".", ".."} or any(char in value for char in "#/\\\0")
    if invalid:
        raise ValueError("audit_path=invalid")
    return value


def _log_path(event: OutboxEvent) -> Path:
    tenant = _safe_component(event.tenant_id)
    return Path(
        "audit",
        tenant,
        f"{event.created_at:%Y}",
        f"{event.created_at:%m}",
        f"{event.created_at:%d}",
        "events.jsonl",
    )


def _file_digest(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _batch_group(entry: _BatchEntry) -> tuple[str, str]:
    return entry.tenant_id, entry.log_path


class LocalAuditSink:
    """Entrega ao menos uma vez baseada em ``event_id`` estável, não exactly-once."""

    def __init__(self, root: Path, parquet_batch_size: int = 1000) -> None:
        if parquet_batch_size <= 0:
            raise ValueError("parquet_batch_size=invalid")
        self._root = Path(root).absolute()
        self._audit_root = self._root / "audit"
        self._batch_size = parquet_batch_size
        self._audit_root.mkdir(parents=True, exist_ok=True)
        self._lock_path = self._audit_root / ".sink.lock"
        self._database_path = self._audit_root / "index.sqlite3"
        with self._locked(), self._connect() as database:
            database.execute(_SCHEMA)
            database.commit()
            self._recover(database)
            self._materialize_batches(database)

    @contextmanager
    def _locked(self) -> Iterator[None]:
        with self._lock_path.open("a+b") as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        database = sqlite3.connect(self._database_path)
        try:
            database.execute("PRAGMA synchronous=FULL")
            yield database
        finally:
            database.close()

    def _fault(self, boundary: str) -> None:
        pass

    def _recover(self, database: sqlite3.Connection) -> None:
        for path in sorted(self._audit_root.glob("*/*/*/*/events.jsonl")):
            self._recover_log(database, path)
        database.commit()

    def _recover_log(self, database: sqlite3.Connection, path: Path) -> None:
        relative = path.relative_to(self._root).as_posix()
        row = database.execute(
            "SELECT COALESCE(MAX(offset + length), 0) FROM events WHERE log_path = ?",
            (relative,),
        ).fetchone()
        offset = int(row[0])
        with path.open("r+b") as stream:
            stream.seek(offset)
            while record := stream.readline():
                if not record.endswith(b"\n"):
                    stream.truncate(offset)
                    stream.flush()
                    os.fsync(stream.fileno())
                    break
                event, digest = self._validate_record(relative, record)
                entry = _IndexEntry(
                    event.event_id, event.tenant_id, relative, offset, len(record), digest
                )
                self._index(database, entry)
                offset += len(record)

    def _validate_record(self, relative: str, record: bytes) -> tuple[OutboxEvent, str]:
        body = record.removesuffix(b"\n")
        try:
            event = OutboxEvent.model_validate_json(body)
            valid = _canonical_event(event) == body and _log_path(event).as_posix() == relative
        except (ValueError, TypeError):
            valid = False
        if not valid:
            raise ValueError("audit_record=invalid")
        return event, sha256(body).hexdigest()

    def _index(self, database: sqlite3.Connection, entry: _IndexEntry) -> None:
        existing = database.execute(
            "SELECT 1 FROM events WHERE event_id = ?",
            (entry.event_id,),
        ).fetchone()
        if existing is not None:
            raise Conflict("event_id=immutable")
        database.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, NULL)",
            (
                entry.event_id,
                entry.tenant_id,
                entry.log_path,
                entry.offset,
                entry.length,
                entry.digest,
            ),
        )

    def _existing_matches(
        self, database: sqlite3.Connection, event: OutboxEvent, record: bytes
    ) -> bool | None:
        row = database.execute(
            "SELECT log_path, offset, length FROM events WHERE event_id = ?",
            (event.event_id,),
        ).fetchone()
        if row is None:
            return None
        relative, offset, length = row
        with (self._root / relative).open("rb") as stream:
            stream.seek(offset)
            existing = stream.read(length)
        return existing == record

    def append(self, event: OutboxEvent) -> None:
        """Persiste um evento completo antes de indexá-lo.

        Args:
            event: Evento validado com identidade estável.
        """
        relative = _log_path(event).as_posix()
        body = _canonical_event(event)
        record = body + b"\n"
        with self._locked(), self._connect() as database:
            self._recover(database)
            existing = self._existing_matches(database, event, record)
            if existing is not None:
                if not existing:
                    raise Conflict("event_id=immutable")
                self._materialize_batches(database)
                return
            path = self._root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            self._append_record(database, _PendingAppend(event, path, record))
            self._materialize_batches(database)

    def _append_record(
        self, database: sqlite3.Connection, pending: _PendingAppend
    ) -> None:
        with pending.path.open("a+b") as stream:
            stream.seek(0, os.SEEK_END)
            offset = stream.tell()
            stream.write(pending.record)
            self._fault("after_file_write")
            stream.flush()
            os.fsync(stream.fileno())
            self._fault("after_file_fsync")
        digest = sha256(pending.record.removesuffix(b"\n")).hexdigest()
        entry = _IndexEntry(
            pending.event.event_id,
            pending.event.tenant_id,
            pending.path.relative_to(self._root).as_posix(),
            offset,
            len(pending.record),
            digest,
        )
        self._index(database, entry)
        self._fault("before_index_commit")
        database.commit()
        self._fault("after_index_commit")

    def _materialize_batches(self, database: sqlite3.Connection) -> None:
        rows = database.execute(
            "SELECT event_id, tenant_id, log_path, offset, length FROM events "
            "WHERE batch_path IS NULL ORDER BY tenant_id, log_path, offset"
        ).fetchall()
        entries = [_BatchEntry(*row) for row in rows]
        for _, grouped in groupby(entries, key=_batch_group):
            pending = list(grouped)
            while len(pending) >= self._batch_size:
                batch, pending = pending[: self._batch_size], pending[self._batch_size :]
                self._materialize_batch(database, batch)

    def _materialize_batch(
        self, database: sqlite3.Connection, entries: list[_BatchEntry]
    ) -> None:
        records = [
            self._read_record(entry.log_path, entry.offset, entry.length)
            for entry in entries
        ]
        digest = sha256(b"".join(records)).hexdigest()
        relative = Path(entries[0].log_path).parent / f"batch-{digest}.parquet"
        table = self._parquet_table(records)
        self._publish_parquet(self._root / relative, table)
        database.executemany(
            "UPDATE events SET batch_path = ? WHERE event_id = ?",
            [(relative.as_posix(), entry.event_id) for entry in entries],
        )
        database.commit()

    def _read_record(self, relative: str, offset: int, length: int) -> bytes:
        with (self._root / relative).open("rb") as stream:
            stream.seek(offset)
            record = stream.read(length)
        if len(record) != length or not record.endswith(b"\n"):
            raise ValueError("audit_record=missing")
        return record

    @staticmethod
    def _parquet_table(records: list[bytes]) -> pa.Table:
        serialized = [json.loads(record) for record in records]
        rows = [
            {
                "tenant_id": item["tenant_id"],
                "event_id": item["event_id"],
                "event_type": item["event_type"],
                "aggregate_id": item["aggregate_id"],
                "payload_json": json.dumps(
                    item["payload"], sort_keys=True, separators=(",", ":")
                ),
                "created_at": item["created_at"],
                "delivered_at": item["delivered_at"],
            }
            for item in serialized
        ]
        return pa.Table.from_pylist(rows, schema=_PARQUET_SCHEMA)

    def _publish_parquet(self, destination: Path, table: pa.Table) -> None:
        temporary = destination.with_name(f".{destination.name}.{token_hex(8)}.tmp")
        try:
            with temporary.open("xb") as stream:
                pq.write_table(table, stream)
                stream.flush()
                os.fsync(stream.fileno())
            expected = _file_digest(temporary)
            try:
                os.link(temporary, destination)
            except FileExistsError as error:
                if _file_digest(destination) != expected:
                    raise Conflict("batch=immutable") from error
            self._fsync_directory(destination.parent)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
