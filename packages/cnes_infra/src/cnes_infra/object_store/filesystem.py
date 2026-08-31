"""Object store imutável sobre filesystem POSIX."""

from __future__ import annotations

import errno
import fcntl
import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING

from cnes_domain.control_plane.errors import Conflict
from cnes_domain.ports.object_store import ObjectStat
from cnes_infra.object_store._common import require_digest, stream_with_digest, validate_key

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager as ContextManager
    from typing import BinaryIO

_TEMP_PREFIX = ".cnes-object-store-"


class _RecoveryKind(Enum):
    ABANDONED = "abandoned"
    LINKED = "linked"
    LOSING = "losing"


@dataclass(frozen=True, slots=True)
class _StagedFile:
    path: Path
    size: int
    digest: str


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class FilesystemObjectStore:
    def __init__(
        self, root: str | Path, fault_injector: Callable[[str], None] | None = None
    ) -> None:
        self._root = Path(root)
        self._fault_injector = fault_injector
        self._root.mkdir(parents=True, exist_ok=True)

    def _fault(self, boundary: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(boundary)

    def _path(self, key: str) -> Path:
        return self._root / validate_key(key)

    @staticmethod
    def _stat(key: str, path: Path) -> ObjectStat:
        with path.open("rb") as stream:
            size, digest = stream_with_digest(stream)
        return ObjectStat(key=key, size_bytes=size, sha256=digest)

    @staticmethod
    def _namespace(key: str) -> str:
        return f"{_TEMP_PREFIX}{sha256(key.encode()).hexdigest()}"

    @contextmanager
    def _destination_lock(self, destination: Path, namespace: str) -> Iterator[None]:
        lock_path = destination.parent / f"{namespace}.lock"
        with lock_path.open("a+b") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    @staticmethod
    def _remove_temporary(temporary: Path, directory: Path) -> None:
        temporary.unlink(missing_ok=True)
        _fsync_directory(directory)

    @staticmethod
    def _classify_recovery(temporary: Path, destination: Path) -> _RecoveryKind:
        if not destination.exists():
            return _RecoveryKind.ABANDONED
        if os.path.samestat(temporary.stat(), destination.stat()):
            return _RecoveryKind.LINKED
        if destination.is_file():
            return _RecoveryKind.LOSING
        raise Conflict("invalid_destination")

    def _recover(self, destination: Path, namespace: str) -> None:
        recoveries: set[_RecoveryKind] = set()
        for temporary in destination.parent.glob(f"{namespace}-*.tmp"):
            recoveries.add(self._classify_recovery(temporary, destination))
            temporary.unlink(missing_ok=True)
        if recoveries:
            _fsync_directory(destination.parent)

    def _stage(
        self, destination: Path, namespace: str, body: BinaryIO, expected_sha256: str
    ) -> _StagedFile:
        descriptor, name = tempfile.mkstemp(
            dir=destination.parent, prefix=f"{namespace}-", suffix=".tmp"
        )
        temporary = Path(name)
        try:
            self._fault("temporary_created")
            with os.fdopen(descriptor, "wb") as stream:
                descriptor = -1
                size, digest = stream_with_digest(body, stream)
                stream.flush()
                os.fsync(stream.fileno())
            self._fault("file_fsynced")
            try:
                require_digest(digest, expected_sha256)
            except ValueError:
                self._remove_temporary(temporary, destination.parent)
                raise
            return _StagedFile(path=temporary, size=size, digest=digest)
        except Exception:
            if descriptor >= 0:
                os.close(descriptor)
            raise

    def _link(self, key: str, destination: Path, staged: _StagedFile) -> tuple[ObjectStat, bool]:
        try:
            os.link(staged.path, destination)
        except OSError as error:
            if error.errno != errno.EEXIST:
                self._remove_temporary(staged.path, destination.parent)
                raise
            existing = self._stat(key, destination)
            self._remove_temporary(staged.path, destination.parent)
            if existing.sha256 != staged.digest:
                raise Conflict("immutable_object") from error
            return existing, False
        return ObjectStat(key=key, size_bytes=staged.size, sha256=staged.digest), True

    def _publish(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        destination = self._path(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        namespace = self._namespace(key)
        with self._destination_lock(destination, namespace):
            self._recover(destination, namespace)
            staged = self._stage(destination, namespace, body, expected_sha256)
            stat, linked = self._link(key, destination, staged)
            if linked:
                self._fault("destination_linked")
                _fsync_directory(destination.parent)
                self._fault("directory_fsynced")
            staged.path.unlink(missing_ok=True)
            self._fault("temporary_unlinked")
            _fsync_directory(destination.parent)
            self._fault("directory_final_fsynced")
            return stat

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        return self._publish(key, body, expected_sha256)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        return self._path(key).open("rb")

    def stat(self, key: str) -> ObjectStat | None:
        path = self._path(key)
        return self._stat(key, path) if path.exists() else None

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        validate_key(source_key)
        validate_key(destination_key)
        with self.open(source_key) as source:
            return self.put(destination_key, source, expected_sha256)

    def delete(self, key: str) -> None:
        destination = self._path(key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        namespace = self._namespace(key)
        with self._destination_lock(destination, namespace):
            self._recover(destination, namespace)
            if destination.exists():
                destination.unlink()
                _fsync_directory(destination.parent)
