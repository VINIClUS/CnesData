"""Object store imutável sobre filesystem POSIX."""

from __future__ import annotations

import ctypes
import errno
import fcntl
import os
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from secrets import token_hex
from stat import S_ISREG
from typing import TYPE_CHECKING

from cnes_domain.control_plane.errors import Conflict
from cnes_domain.ports.object_store import ObjectStat
from cnes_infra.object_store._common import require_digest, stream_with_digest, validate_key

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager as ContextManager
    from typing import BinaryIO

_LAYOUT = ".cnes-object-store-internal"
_OBJECTS = "objects"
_LOCKS = "locks"
_TEMP_PREFIX = ".cnes-object-store-"
_HEX_DIGITS = frozenset("0123456789abcdef")
_OWNER_XATTR = "user.cnes_object_store_destination"
_MISSING_XATTR_ERRNOS = frozenset({errno.ENODATA, errno.ENOENT})
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
_AT_EMPTY_PATH = 0x1000
_LINKAT = ctypes.CDLL(None, use_errno=True).linkat
_LINKAT.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_int)
_LINKAT.restype = ctypes.c_int


@dataclass(frozen=True, slots=True)
class _Layout:
    objects: int
    locks: int


@dataclass(frozen=True, slots=True)
class _StagedFile:
    name: str
    size: int
    digest: str


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, _DIRECTORY_FLAGS)
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


def _open_or_create_directory(parent: int, name: str) -> int:
    created = False
    try:
        os.mkdir(name, dir_fd=parent)
        created = True
    except FileExistsError:
        pass
    descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent)
    if created:
        os.fsync(parent)
    return descriptor


def _link_descriptor(descriptor: int, directory: int, name: str) -> None:
    result = _LINKAT(descriptor, b"", directory, os.fsencode(name), _AT_EMPTY_PATH)
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), name)


def _temporary_identity(name: str) -> tuple[str, str] | None:
    namespace, _, token = name.removesuffix(".tmp").rpartition("-")
    digest = namespace.removeprefix(_TEMP_PREFIX)
    if len(digest) != 64 or not token:
        return None
    return (namespace, token) if set(digest) <= _HEX_DIGITS else None


def _temporary_owner(descriptor: int) -> tuple[str, str] | None:
    try:
        value = os.getxattr(descriptor, _OWNER_XATTR)
    except OSError as error:
        if error.errno in _MISSING_XATTR_ERRNOS:
            return None
        raise
    try:
        ownership = value.decode().split("\0", maxsplit=1)
    except UnicodeDecodeError:
        return None
    return (ownership[0], ownership[1]) if len(ownership) == 2 else None


def _open_candidate(objects: int, name: str) -> int | None:
    try:
        return os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=objects)
    except OSError as error:
        if error.errno in {errno.ENOENT, errno.ELOOP}:
            return None
        raise


class FilesystemObjectStore:
    def __init__(
        self, root: str | Path, fault_injector: Callable[[str], None] | None = None
    ) -> None:
        self._root = Path(root)
        self._fault_injector = fault_injector
        _mkdir_durable(self._root)
        self._ensure_layout()
        self._recover_startup()

    def _ensure_layout(self) -> None:
        root = os.open(self._root, _DIRECTORY_FLAGS)
        try:
            internal = _open_or_create_directory(root, _LAYOUT)
            try:
                objects = _open_or_create_directory(internal, _OBJECTS)
                os.close(objects)
                locks = _open_or_create_directory(internal, _LOCKS)
                os.close(locks)
            finally:
                os.close(internal)
        finally:
            os.close(root)

    @contextmanager
    def _layout(self) -> Iterator[_Layout]:
        with ExitStack() as stack:
            root = os.open(self._root, _DIRECTORY_FLAGS)
            stack.callback(os.close, root)
            internal = os.open(_LAYOUT, _DIRECTORY_FLAGS, dir_fd=root)
            stack.callback(os.close, internal)
            objects = os.open(_OBJECTS, _DIRECTORY_FLAGS, dir_fd=internal)
            stack.callback(os.close, objects)
            locks = os.open(_LOCKS, _DIRECTORY_FLAGS, dir_fd=internal)
            stack.callback(os.close, locks)
            yield _Layout(objects=objects, locks=locks)

    def _fault(self, boundary: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(boundary)

    @staticmethod
    def _identity(key: str) -> tuple[str, str]:
        valid_key = validate_key(key)
        digest = sha256(valid_key.encode()).hexdigest()
        return valid_key, digest

    @staticmethod
    def _stat_descriptor(key: str, descriptor: int) -> ObjectStat:
        with os.fdopen(descriptor, "rb") as stream:
            size, digest = stream_with_digest(stream)
        return ObjectStat(key=key, size_bytes=size, sha256=digest)

    @classmethod
    def _stat_at(cls, key: str, directory: int, name: str) -> ObjectStat:
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
        return cls._stat_descriptor(key, descriptor)

    @contextmanager
    def _namespace_lock(self, locks: int, digest: str, *, blocking: bool = True) -> Iterator[bool]:
        descriptor = os.open(
            f"{digest}.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600, dir_fd=locks
        )
        try:
            flags = fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB
            try:
                fcntl.flock(descriptor, flags)
            except BlockingIOError:
                yield False
                return
            try:
                yield True
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def _owned_temporary(
        self, objects: int, name: str, key: str, digest: str
    ) -> tuple[int, os.stat_result] | None:
        identity = _temporary_identity(name)
        if identity is None or identity[0] != f"{_TEMP_PREFIX}{digest}":
            return None
        descriptor = _open_candidate(objects, name)
        if descriptor is None:
            return None
        metadata = os.fstat(descriptor)
        owner = _temporary_owner(descriptor) if S_ISREG(metadata.st_mode) else None
        if owner != (key, identity[1]):
            os.close(descriptor)
            return None
        return descriptor, metadata

    def _recover_startup(self) -> None:
        with self._layout() as layout:
            for name in os.listdir(layout.objects):
                identity = _temporary_identity(name)
                if identity is None:
                    continue
                candidate = self._startup_owner(layout.objects, name, identity)
                if candidate is None:
                    continue
                key, digest = candidate
                with self._namespace_lock(layout.locks, digest, blocking=False) as acquired:
                    if acquired:
                        self._recover(layout.objects, key, digest)

    def _startup_owner(
        self, objects: int, name: str, identity: tuple[str, str]
    ) -> tuple[str, str] | None:
        digest = identity[0].removeprefix(_TEMP_PREFIX)
        descriptor = _open_candidate(objects, name)
        if descriptor is None:
            return None
        try:
            owner = _temporary_owner(descriptor)
        finally:
            os.close(descriptor)
        if owner is None or owner[1] != identity[1]:
            return None
        try:
            key, expected = self._identity(owner[0])
        except ValueError:
            return None
        return (key, digest) if expected == digest else None

    @staticmethod
    def _remove_temporary(objects: int, name: str) -> None:
        try:
            os.unlink(name, dir_fd=objects)
        except FileNotFoundError:
            return
        os.fsync(objects)

    @staticmethod
    def _classify_recovery(objects: int, temporary: os.stat_result, digest: str) -> None:
        try:
            destination = os.open(digest, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=objects)
        except FileNotFoundError:
            return
        try:
            metadata = os.fstat(destination)
        finally:
            os.close(destination)
        if os.path.samestat(temporary, metadata):
            return
        if S_ISREG(metadata.st_mode):
            return
        raise Conflict("destination=invalid")

    def _recover(self, objects: int, key: str, digest: str) -> None:
        namespace = f"{_TEMP_PREFIX}{digest}"
        for name in os.listdir(objects):
            if not name.startswith(f"{namespace}-"):
                continue
            owned = self._owned_temporary(objects, name, key, digest)
            if owned is None:
                continue
            descriptor, metadata = owned
            try:
                self._classify_recovery(objects, metadata, digest)
                self._remove_temporary(objects, name)
            finally:
                os.close(descriptor)

    def _mark_temporary(self, descriptor: int, key: str, token: str) -> None:
        os.setxattr(descriptor, _OWNER_XATTR, f"{key}\0{token}".encode())
        os.fsync(descriptor)

    def _stage(
        self, objects: int, identity: tuple[str, str], body: BinaryIO, expected_sha256: str
    ) -> _StagedFile:
        key, digest = identity
        descriptor = os.open(".", os.O_RDWR | os.O_TMPFILE, 0o600, dir_fd=objects)
        token = token_hex(16)
        name = f"{_TEMP_PREFIX}{digest}-{token}.tmp"
        try:
            self._fault("temporary_created_before_ownership")
            self._mark_temporary(descriptor, key, token)
            _link_descriptor(descriptor, objects, name)
            try:
                os.fsync(objects)
            except OSError:
                self._remove_temporary(objects, name)
                raise
            self._fault("temporary_created")
            try:
                stream = os.fdopen(descriptor, "wb")
                descriptor = -1
                with stream:
                    size, content_digest = stream_with_digest(body, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
            except Exception:
                self._remove_temporary(objects, name)
                raise
            self._fault("file_fsynced")
            try:
                require_digest(content_digest, expected_sha256)
            except ValueError:
                self._remove_temporary(objects, name)
                raise
            return _StagedFile(name=name, size=size, digest=content_digest)
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def _link(
        self, key: str, objects: int, digest: str, staged: _StagedFile
    ) -> tuple[ObjectStat, bool]:
        try:
            os.link(
                staged.name,
                digest,
                src_dir_fd=objects,
                dst_dir_fd=objects,
                follow_symlinks=False,
            )
        except OSError as error:
            if error.errno != errno.EEXIST:
                self._remove_temporary(objects, staged.name)
                raise
            existing = self._stat_at(key, objects, digest)
            self._remove_temporary(objects, staged.name)
            if existing.sha256 != staged.digest:
                raise Conflict("object=immutable") from error
            return existing, False
        return ObjectStat(key=key, size_bytes=staged.size, sha256=staged.digest), True

    def _publish(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        valid_key, digest = self._identity(key)
        with self._layout() as layout, self._namespace_lock(layout.locks, digest):
            self._recover(layout.objects, valid_key, digest)
            staged = self._stage(layout.objects, (valid_key, digest), body, expected_sha256)
            stat, linked = self._link(valid_key, layout.objects, digest, staged)
            if linked:
                self._fault("destination_linked")
                os.fsync(layout.objects)
                self._fault("directory_fsynced")
            self._remove_temporary(layout.objects, staged.name)
            self._fault("temporary_unlinked")
            os.fsync(layout.objects)
            self._fault("directory_final_fsynced")
            return stat

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        return self._publish(key, body, expected_sha256)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        _, digest = self._identity(key)
        with self._layout() as layout:
            descriptor = os.open(digest, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=layout.objects)
        return os.fdopen(descriptor, "rb")

    def stat(self, key: str) -> ObjectStat | None:
        valid_key, digest = self._identity(key)
        with self._layout() as layout:
            try:
                return self._stat_at(valid_key, layout.objects, digest)
            except FileNotFoundError:
                return None

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        validate_key(source_key)
        validate_key(destination_key)
        with self.open(source_key) as source:
            return self.put(destination_key, source, expected_sha256)

    def delete(self, key: str) -> None:
        valid_key, digest = self._identity(key)
        with self._layout() as layout, self._namespace_lock(layout.locks, digest):
            self._recover(layout.objects, valid_key, digest)
            try:
                descriptor = os.open(digest, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=layout.objects)
                os.close(descriptor)
                os.unlink(digest, dir_fd=layout.objects)
            except FileNotFoundError:
                return
            os.fsync(layout.objects)
