"""Object store imutável sobre filesystem POSIX."""

from __future__ import annotations

import ctypes
import errno
import fcntl
import os
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from hashlib import sha256
from pathlib import Path
from secrets import token_hex
from stat import S_ISLNK, S_ISREG
from typing import TYPE_CHECKING

from cnes_domain.control_plane.errors import Conflict
from cnes_domain.ports.object_store import ObjectStat
from cnes_infra.object_store._common import require_digest, stream_with_digest, validate_key

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager as ContextManager
    from typing import BinaryIO

_TEMP_PREFIX = ".cnes-object-store-"
_HEX_DIGITS = frozenset("0123456789abcdef")
_OWNER_XATTR = "user.cnes_object_store_destination"
_MISSING_XATTR_ERRNOS = frozenset({errno.ENODATA, errno.ENOENT})
_AT_FDCWD = -100
_AT_EMPTY_PATH = 0x1000
_LINKAT = ctypes.CDLL(None, use_errno=True).linkat
_LINKAT.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_int)
_LINKAT.restype = ctypes.c_int


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


def _mkdir_durable(directory: Path) -> None:
    missing: list[Path] = []
    current = directory
    while not current.exists():
        missing.append(current)
        current = current.parent
    for path in reversed(missing):
        path.mkdir(exist_ok=True)
        _fsync_directory(path.parent)


def _reject_symlink_components(root: Path, path: Path) -> None:
    current = root
    for part in path.relative_to(root).parts:
        current /= part
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            return
        if S_ISLNK(mode):
            raise ValueError("object_path=symlink")


def _link_descriptor(descriptor: int, destination: Path) -> None:
    result = _LINKAT(descriptor, b"", _AT_FDCWD, os.fsencode(destination), _AT_EMPTY_PATH)
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), destination)


def _temporary_identity(path: Path) -> tuple[str, str] | None:
    namespace, _, token = path.name.removesuffix(".tmp").rpartition("-")
    digest = namespace.removeprefix(_TEMP_PREFIX)
    if len(digest) != 64 or not token:
        return None
    return (namespace, token) if set(digest) <= _HEX_DIGITS else None


def _temporary_owner(path: Path) -> tuple[str, str] | None:
    try:
        if not S_ISREG(path.lstat().st_mode):
            return None
        value = os.getxattr(path, _OWNER_XATTR, follow_symlinks=False)
    except OSError as error:
        if error.errno in _MISSING_XATTR_ERRNOS:
            return None
        raise
    try:
        ownership = value.decode().split("\0", maxsplit=1)
    except UnicodeDecodeError:
        return None
    return (ownership[0], ownership[1]) if len(ownership) == 2 else None


class FilesystemObjectStore:
    def __init__(
        self, root: str | Path, fault_injector: Callable[[str], None] | None = None
    ) -> None:
        self._root = Path(root)
        self._fault_injector = fault_injector
        _mkdir_durable(self._root)
        self._recover_startup()

    def _fault(self, boundary: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(boundary)

    def _path(self, key: str) -> Path:
        path = self._root / validate_key(key)
        _reject_symlink_components(self._root, path)
        return path

    @staticmethod
    def _stat(key: str, path: Path) -> ObjectStat:
        with path.open("rb") as stream:
            size, digest = stream_with_digest(stream)
        return ObjectStat(key=key, size_bytes=size, sha256=digest)

    @staticmethod
    def _namespace(key: str) -> str:
        return f"{_TEMP_PREFIX}{sha256(key.encode()).hexdigest()}"

    @contextmanager
    def _namespace_lock(
        self, directory: Path, namespace: str, *, blocking: bool = True
    ) -> Iterator[bool]:
        lock_path = directory / f"{namespace}.lock"
        with lock_path.open("a+b") as lock:
            flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
            try:
                fcntl.flock(lock.fileno(), flags)
            except BlockingIOError:
                yield False
                return
            try:
                yield True
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    @contextmanager
    def _destination_lock(self, destination: Path, namespace: str) -> Iterator[None]:
        with self._namespace_lock(destination.parent, namespace):
            yield

    def _owned_destination(self, temporary: Path, namespace: str) -> Path | None:
        identity = _temporary_identity(temporary)
        ownership = _temporary_owner(temporary)
        if identity is None or ownership is None or identity[1] != ownership[1]:
            return None
        owner = ownership[0]
        try:
            destination = self._path(owner)
        except ValueError:
            return None
        if destination == temporary or destination.parent != temporary.parent:
            return None
        return destination if self._namespace(owner) == namespace else None

    def _recover_startup(self) -> None:
        for temporary in self._root.rglob(f"{_TEMP_PREFIX}*.tmp"):
            identity = _temporary_identity(temporary)
            if identity is None:
                continue
            namespace = identity[0]
            destination = self._owned_destination(temporary, namespace)
            if destination is None:
                continue
            with self._namespace_lock(temporary.parent, namespace, blocking=False) as acquired:
                if not acquired:
                    continue
                self._recover(destination, namespace)

    @staticmethod
    def _remove_temporary(temporary: Path, directory: Path) -> None:
        temporary.unlink(missing_ok=True)
        _fsync_directory(directory)

    @staticmethod
    def _classify_recovery(temporary: Path, destination: Path) -> _RecoveryKind:
        if not destination.exists():
            return _RecoveryKind.ABANDONED
        if os.path.samestat(temporary.lstat(), destination.stat()):
            return _RecoveryKind.LINKED
        if destination.is_file():
            return _RecoveryKind.LOSING
        raise Conflict("destination=invalid")

    def _recover(self, destination: Path, namespace: str) -> None:
        recoveries: set[_RecoveryKind] = set()
        for temporary in destination.parent.glob(f"{namespace}-*.tmp"):
            if self._owned_destination(temporary, namespace) != destination:
                continue
            recoveries.add(self._classify_recovery(temporary, destination))
            temporary.unlink(missing_ok=True)
        if recoveries:
            _fsync_directory(destination.parent)

    def _mark_temporary(self, descriptor: int, destination: Path, token: str) -> None:
        key = destination.relative_to(self._root).as_posix()
        ownership = f"{key}\0{token}".encode()
        os.setxattr(descriptor, _OWNER_XATTR, ownership)
        os.fsync(descriptor)

    def _stage(
        self, destination: Path, namespace: str, body: BinaryIO, expected_sha256: str
    ) -> _StagedFile:
        descriptor = os.open(destination.parent, os.O_RDWR | os.O_TMPFILE, 0o600)
        token = token_hex(16)
        temporary = destination.parent / f"{namespace}-{token}.tmp"
        try:
            self._fault("temporary_created_before_ownership")
            self._mark_temporary(descriptor, destination, token)
            _link_descriptor(descriptor, temporary)
            _fsync_directory(destination.parent)
            self._fault("temporary_created")
            try:
                stream = os.fdopen(descriptor, "wb")
                descriptor = -1
                with stream:
                    size, digest = stream_with_digest(body, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
            except Exception:
                self._remove_temporary(temporary, destination.parent)
                raise
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
                raise Conflict("object=immutable") from error
            return existing, False
        return ObjectStat(key=key, size_bytes=staged.size, sha256=staged.digest), True

    def _publish(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        destination = self._path(key)
        _mkdir_durable(destination.parent)
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
        _mkdir_durable(destination.parent)
        namespace = self._namespace(key)
        with self._destination_lock(destination, namespace):
            self._recover(destination, namespace)
            if destination.exists():
                destination.unlink()
                _fsync_directory(destination.parent)
