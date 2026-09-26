"""Armazenamento local imutável para Windows, serializado por SQLite."""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from secrets import token_hex
from typing import TYPE_CHECKING

from cnes_domain.control_plane.errors import Conflict
from cnes_domain.ports.object_store import ObjectStat
from cnes_infra.object_store._common import require_digest, stream_with_digest, validate_key

if TYPE_CHECKING:
    from collections.abc import Iterator
    from contextlib import AbstractContextManager as ContextManager
    from typing import BinaryIO


class WindowsFilesystemObjectStore:
    def __init__(self, root: str | Path) -> None:
        self._objects = Path(root).absolute() / ".cnes-object-store-internal" / "objects"
        self._objects.mkdir(parents=True, exist_ok=True)
        self._lock_path = self._objects.parent / "lock.sqlite3"

    @staticmethod
    def _identity(key: str) -> tuple[str, str]:
        valid = validate_key(key)
        return valid, sha256(valid.encode()).hexdigest()

    @contextmanager
    def _write_lock(self) -> Iterator[None]:
        database = sqlite3.connect(self._lock_path, timeout=30)
        try:
            database.execute("BEGIN IMMEDIATE")
            yield
            database.commit()
        finally:
            database.close()

    @staticmethod
    def _stat_path(key: str, path: Path) -> ObjectStat:
        with path.open("rb") as stream:
            size, digest = stream_with_digest(stream)
        return ObjectStat(key=key, size_bytes=size, sha256=digest)

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        valid, digest = self._identity(key)
        destination = self._objects / digest
        with self._write_lock():
            temporary = self._objects / f".cnes-object-store-{digest}-{token_hex(16)}.tmp"
            try:
                with temporary.open("xb") as stream:
                    size, actual = stream_with_digest(body, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
                require_digest(actual, expected_sha256)
                try:
                    os.link(temporary, destination)
                except FileExistsError as error:
                    existing = self._stat_path(valid, destination)
                    if existing.sha256 != actual:
                        raise Conflict("object=immutable") from error
                    return existing
                return ObjectStat(key=valid, size_bytes=size, sha256=actual)
            finally:
                temporary.unlink(missing_ok=True)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        _, digest = self._identity(key)
        return (self._objects / digest).open("rb")

    def stat(self, key: str) -> ObjectStat | None:
        valid, digest = self._identity(key)
        try:
            return self._stat_path(valid, self._objects / digest)
        except FileNotFoundError:
            return None

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        validate_key(destination_key)
        with self.open(source_key) as source:
            return self.put(destination_key, source, expected_sha256)

    def delete(self, key: str) -> None:
        _, digest = self._identity(key)
        with self._write_lock():
            (self._objects / digest).unlink(missing_ok=True)
