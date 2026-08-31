"""Validação e digest compartilhados por object stores."""

from __future__ import annotations

from hashlib import sha256
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import BinaryIO

_CHUNK_SIZE = 1024 * 1024


def validate_key(key: str) -> str:
    parts = key.split("/")
    if not key or key.startswith("/") or "\\" in key:
        raise ValueError("object_key=invalid")
    if any(part in {"", ".", ".."} for part in parts):
        raise ValueError("object_key=invalid")
    return key


def stream_with_digest(source: BinaryIO, destination: BinaryIO | None = None) -> tuple[int, str]:
    digest = sha256()
    size = 0
    while chunk := source.read(_CHUNK_SIZE):
        digest.update(chunk)
        size += len(chunk)
        if destination is not None:
            destination.write(chunk)
    return size, digest.hexdigest()


def require_digest(actual: str, expected: str) -> None:
    if actual != expected:
        raise ValueError("sha256=mismatch")
