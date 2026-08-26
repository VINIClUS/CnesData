"""Canonical manifest validation helpers."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime  # noqa: TC003
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cnes_contracts.manifests.outputs import OutputManifest
    from cnes_contracts.manifests.raw import RawManifest

_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_COMPETENCIA_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"


def _validate_utc(value: datetime) -> datetime:
    if value.utcoffset() is None or value.utcoffset().total_seconds() != 0:
        raise ValueError("datetime_utc_required")
    return value


def manifest_sha256(model: RawManifest | OutputManifest) -> str:
    payload = model.model_dump_json(exclude_none=False, by_alias=False).encode()
    return hashlib.sha256(payload).hexdigest()


def _wire_value(value: object) -> str:
    return str(value.value) if isinstance(value, Enum) else str(value)


def _segments(object_key: str) -> tuple[str, ...]:
    if object_key.startswith("/") or object_key.endswith("/"):
        raise ValueError("object_key_invalid")
    parts = tuple(object_key.split("/"))
    if any(not _SAFE_SEGMENT.fullmatch(part) for part in parts):
        raise ValueError("object_key_invalid")
    return parts


def _expected_segments(manifest: RawManifest | OutputManifest) -> tuple[str, ...]:
    layer = _wire_value(getattr(manifest, "layer", "raw"))
    tenant_id = manifest.tenant_id
    competencia = manifest.competencia
    if layer == "raw":
        return (
            layer,
            tenant_id,
            _wire_value(manifest.source_type),
            competencia,
            manifest.snapshot_id,
        )
    if layer == "normalized":
        return (
            layer,
            tenant_id,
            _wire_value(manifest.source_type),
            competencia,
            manifest.run_id,
        )
    if layer == "reconciliation":
        return (layer, tenant_id, competencia, manifest.run_id)
    if layer == "serving":
        return (layer, tenant_id, manifest.run_id)
    raise ValueError("object_key_layer")


def validate_object_key(manifest: RawManifest | OutputManifest) -> None:
    parts = _segments(manifest.object_key)
    expected = _expected_segments(manifest)
    if len(parts) != len(expected) + 1:
        raise ValueError("object_key_layout")
    if parts[:-1] != expected:
        raise ValueError("object_key_identity")
    if expected[0] == "serving" and not parts[-1].endswith(".json"):
        raise ValueError("object_key_invalid")
