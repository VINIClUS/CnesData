"""Canonical private close snapshot and commitment helpers."""
from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_snapshot(value: Any) -> bytes:
    """Encode a private 0/1 snapshot deterministically."""
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def snapshot_sha256(value: Any) -> str:
    """Hash a canonical private snapshot without exposing its contents."""
    return hashlib.sha256(canonical_snapshot(value)).hexdigest()


def public_membership_commitment(value: Any) -> str:
    """Hash public dispatch resolutions separately from private identities."""
    return snapshot_sha256(value)
