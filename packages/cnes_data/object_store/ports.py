"""Porta de armazenamento imutável de objetos."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from contextlib import AbstractContextManager as ContextManager
    from typing import BinaryIO


@dataclass(frozen=True, slots=True)
class ObjectStat:
    key: str
    size_bytes: int
    sha256: str


@runtime_checkable
class ObjectStorePort(Protocol):
    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat: ...
    def open(self, key: str) -> ContextManager[BinaryIO]: ...
    def stat(self, key: str) -> ObjectStat | None: ...
    def promote(
        self, source_key: str, destination_key: str, expected_sha256: str
    ) -> ObjectStat: ...
    def delete(self, key: str) -> None: ...
