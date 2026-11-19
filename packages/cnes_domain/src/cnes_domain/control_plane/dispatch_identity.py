"""Identidade do dispatch e digest do request de start, sem dependência de pydantic."""

from __future__ import annotations

import json
import re
from hashlib import sha256

_LOWER_HEX_16 = re.compile(r"^[0-9a-f]{16}$")


def require_dispatch_id(value: str, name: str) -> str:
    """Raises: ValueError: `value` fora de hex16 minúsculo."""
    if not _LOWER_HEX_16.fullmatch(value):
        raise ValueError(f"invalid_{name}")
    return value


def request_json_sha256(value: object) -> str:
    """Return the canonical digest of a JSON start request."""
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return sha256(encoded).hexdigest()
