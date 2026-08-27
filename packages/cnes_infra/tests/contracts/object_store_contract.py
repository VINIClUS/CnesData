"""Casos reutilizáveis de conformidade do armazenamento de objetos."""

from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from hashlib import sha256
from io import BytesIO
from typing import Any

from cnes_domain.control_plane.errors import Conflict
from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort
from packages.cnes_infra.tests.contracts.clock import MutableClock

_Runner = Callable[[Any, MutableClock], None]
_KEY = "raw/354130/CNES/2026-07/snapshot/data.parquet"


@dataclass(frozen=True, slots=True)
class ObjectStoreCase:
    """Executa uma invariável contra um adapter de objetos."""

    name: str
    _runner: _Runner = field(repr=False, compare=False)

    def run(self, adapter: Any, clock: MutableClock) -> None:
        """Executa o caso e identifica qualquer falha pelo nome."""
        try:
            assert isinstance(adapter, ObjectStorePort)
            self._runner(adapter, clock)
        except Exception as error:
            raise AssertionError(f"case={self.name}") from error


def _digest(body: bytes) -> str:
    return sha256(body).hexdigest()


def _expect_rejection(action: Callable[[], Any]) -> None:
    try:
        action()
    except Exception:
        return
    raise AssertionError("rejection_required")


def _open(adapter: Any, key: str) -> None:
    with adapter.open(key):
        pass


def _case_roundtrip(adapter: Any, clock: MutableClock) -> None:
    body = b"cnes-contract\x00\xff"
    expected_hash = _digest(body)
    stat = adapter.put(_KEY, BytesIO(body), expected_hash)
    assert (stat.key, stat.size_bytes, stat.sha256) == (_KEY, len(body), expected_hash)
    with adapter.open(_KEY) as stream:
        assert stream.read() == body
    assert adapter.stat(_KEY) == stat
    assert adapter.put(_KEY, BytesIO(body), expected_hash) == stat


def _case_immutability(adapter: Any, clock: MutableClock) -> None:
    original = b"original"
    adapter.put(_KEY, BytesIO(original), _digest(original))
    _expect_rejection(lambda: adapter.put(_KEY, BytesIO(b"changed"), _digest(b"changed")))
    _expect_rejection(lambda: adapter.put("raw/hash-error", BytesIO(original), _digest(b"wrong")))
    with adapter.open(_KEY) as stream:
        assert stream.read() == original
    assert adapter.stat("raw/hash-error") is None


def _case_safe_keys(adapter: Any, clock: MutableClock) -> None:
    invalid_keys = ("", "/absolute", "a/../b", "a/./b", "a//b", "a\\b")
    adapter.put("valid/key", BytesIO(b"x"), _digest(b"x"))
    for key in invalid_keys:
        _expect_rejection(lambda key=key: adapter.put(key, BytesIO(b"x"), _digest(b"x")))
        _expect_rejection(lambda key=key: _open(adapter, key))
        _expect_rejection(lambda key=key: adapter.stat(key))
        _expect_rejection(lambda key=key: adapter.delete(key))
        _expect_rejection(lambda key=key: adapter.promote(key, "valid/key", _digest(b"x")))
        _expect_rejection(lambda key=key: adapter.promote("valid/key", key, _digest(b"x")))


def _case_promote(adapter: Any, clock: MutableClock) -> None:
    source = "staging/upload"
    destination = "raw/promoted"
    body = b"promoted-content"
    expected_hash = _digest(body)
    source_stat = adapter.put(source, BytesIO(body), expected_hash)
    promoted = adapter.promote(source, destination, expected_hash)
    assert (promoted.key, promoted.size_bytes, promoted.sha256) == (
        destination, len(body), expected_hash
    )
    assert adapter.stat(destination) == promoted
    assert adapter.stat(source) == source_stat
    with adapter.open(source) as stream:
        assert stream.read() == body
    assert adapter.promote(source, destination, expected_hash) == promoted
    conflict = b"conflicting"
    adapter.put("staging/conflict", BytesIO(conflict), _digest(conflict))
    _expect_rejection(lambda: adapter.promote("staging/conflict", destination, _digest(conflict)))
    _expect_rejection(lambda: adapter.promote(source, "raw/bad-hash", _digest(b"wrong")))
    assert adapter.stat("raw/bad-hash") is None
    with adapter.open(destination) as stream:
        assert stream.read() == body


def _case_delete(adapter: Any, clock: MutableClock) -> None:
    body = b"temporary"
    adapter.put(_KEY, BytesIO(body), _digest(body))
    adapter.delete(_KEY)
    assert adapter.stat(_KEY) is None


def object_store_cases() -> tuple[ObjectStoreCase, ...]:
    """Retorna o catálogo estável de invariáveis do armazenamento."""
    return (
        ObjectStoreCase("roundtrip", _case_roundtrip),
        ObjectStoreCase("immutability", _case_immutability),
        ObjectStoreCase("safe_keys", _case_safe_keys),
        ObjectStoreCase("promote", _case_promote),
        ObjectStoreCase("delete", _case_delete),
    )


class _MemoryObjectStore:
    def __init__(self, mutation: str | None = None) -> None:
        self.mutation = mutation
        self.objects: dict[str, bytes] = {}

    def _validate_key(self, key: str) -> None:
        if self.mutation == "safe_keys" and key == "":
            return
        invalid = not key or key.startswith("/") or "\\" in key or "//" in key
        if invalid or any(part in {"", ".", ".."} for part in key.split("/")):
            raise ValueError("invalid_key")

    @staticmethod
    def _stat(key: str, body: bytes) -> ObjectStat:
        return ObjectStat(key=key, size_bytes=len(body), sha256=sha256(body).hexdigest())

    def put(self, key: str, body: Any, expected_sha256: str) -> ObjectStat:
        self._validate_key(key)
        content = body.read()
        digest = sha256(content).hexdigest()
        if digest != expected_sha256:
            if self.mutation == "immutability":
                self.objects[key] = content
            raise ValueError("hash_mismatch")
        current = self.objects.get(key)
        if current is not None and current != content:
            if self.mutation == "immutability":
                self.objects[key] = content
            raise Conflict("immutable_object")
        self.objects[key] = content
        stat = self._stat(key, content)
        if self.mutation == "roundtrip":
            return ObjectStat(key=key, size_bytes=0, sha256=stat.sha256)
        return stat

    @contextmanager
    def open(self, key: str):
        self._validate_key(key)
        if key not in self.objects:
            raise FileNotFoundError(key)
        yield BytesIO(self.objects[key])

    def stat(self, key: str) -> ObjectStat | None:
        self._validate_key(key)
        body = self.objects.get(key)
        return None if body is None else self._stat(key, body)

    def delete(self, key: str) -> None:
        self._validate_key(key)
        if self.mutation == "delete":
            return
        self.objects.pop(key, None)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        self._validate_key(source_key)
        self._validate_key(destination_key)
        source = self.objects.get(source_key)
        if source is None or sha256(source).hexdigest() != expected_sha256:
            if self.mutation == "promote":
                self.objects[destination_key] = source or b""
            raise ValueError("source_hash_mismatch")
        current = self.objects.get(destination_key)
        if current is not None and current != source:
            if self.mutation == "promote":
                self.objects[destination_key] = source
            raise Conflict("immutable_destination")
        self.objects[destination_key] = source
        return self._stat(destination_key, source)
