"""resources.scope do mode-canary derivado do tenant do secret e do owner do keeper."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from scripts.promotion.mode_canary_seed import SeedBinding, build_mode_seed
from scripts.promotion.promote_mode_record import PUBLISHED_EVENT_TYPE

_ROOT: Final = Path(__file__).resolve().parents[2]
FIXTURE_DIRECTORY: Final = _ROOT / "tests/production/fixtures/mode_canary"
_RUN_PREFIXES: Final = ("unit", "recovery-a", "recovery-b")
_SEED_MODES: Final = (("unit",), ("recovery-a", "recovery-b"))
_BUCKET: Final = "scope"
_EPOCH: Final = datetime(2026, 1, 1, tzinfo=UTC)


def _fixtures() -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in FIXTURE_DIRECTORY.iterdir()}


def _raw_keys(tenant_id: str, owner_sha256: str, fixtures: dict[str, bytes]) -> set[str]:
    keys: set[str] = set()
    for prefixes in _SEED_MODES:
        runs = tuple(f"{prefix}-{owner_sha256[:16]}" for prefix in prefixes)
        seed = build_mode_seed(SeedBinding(tenant_id, runs, owner_sha256, _EPOCH, _BUCKET),
                               fixtures)
        keys.update(obj.key for obj in seed.objects)
    return keys


def _audit_key(tenant_id: str, run_id: str) -> str:
    event_id = f"{PUBLISHED_EVENT_TYPE}:{tenant_id}:{run_id}"
    return f"audit/.event-id/{hashlib.sha256(event_id.encode()).hexdigest()}.json"


def mode_scope(tenant_id: str, owner_sha256: str) -> dict[str, Any]:
    """Returns: scope fechado do input mode_canary para o owner do keeper.

    Args:
        tenant_id: `prod-canary-<32 hex>` do secret.
        owner_sha256: hash do lease do keeper.

    Raises: ContractError: tenant ou owner fora do contrato do seed.
    """
    runs = sorted(f"{prefix}-{owner_sha256[:16]}" for prefix in _RUN_PREFIXES)
    return {
        "tenant_id": tenant_id,
        "run_ids": runs,
        "raw_object_keys": sorted(_raw_keys(tenant_id, owner_sha256, _fixtures())),
        "output_object_keys": [f"outputs/{tenant_id}/{run}/manifest.json" for run in runs],
        "audit_object_keys": sorted(_audit_key(tenant_id, run) for run in runs),
    }
