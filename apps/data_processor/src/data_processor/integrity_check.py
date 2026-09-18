"""Recompute SHA-256 over downloaded Parquet bytes; reject mismatch."""
from __future__ import annotations

import hashlib


class IntegrityError(RuntimeError):
    """Parquet sha256 does not match what edge agent reported."""


def verify_parquet(path: str, expected_sha256: str | None) -> bool:
    """Read Parquet at path, hash it, compare to expected.

    expected_sha256: 64-char hex from RegisterJob payload. None → skip.
    Returns True on match (or when expected_sha256 is None).
    Raises IntegrityError on mismatch.
    Raises FileNotFoundError if the file doesn't exist.
    """
    if expected_sha256 is None:
        with open(path, "rb"):
            pass
        return True
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    computed = h.hexdigest()
    if computed != expected_sha256:
        raise IntegrityError(
            f"sha256_mismatch expected={expected_sha256} got={computed}"
        )
    return True
