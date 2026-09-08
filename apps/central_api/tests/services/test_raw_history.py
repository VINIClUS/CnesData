from datetime import UTC, datetime
from hashlib import sha256

import pytest

from central_api.services.raw_ingestion import _valid_chain
from cnes_contracts import RawManifest, SnapshotMode, SourceType, manifest_sha256


def _manifest(mode: SnapshotMode = SnapshotMode.FULL) -> RawManifest:
    delta = mode is SnapshotMode.DELTA
    snapshot_id = "delta-2" if delta else "base"
    return RawManifest(
        manifest_version=1, manifest_id=f"manifest-{snapshot_id}", tenant_id="354130",
        source_type=SourceType.CNES_LOCAL, file_subtype="CNES_VINCULO",
        competencia="2026-07", agent_id="agent-1", agent_version="1.0",
        schema_version="v1", snapshot_mode=mode, snapshot_id=snapshot_id,
        base_snapshot_id="base" if delta else None, sequence=2 if delta else 1,
        previous_manifest_sha256="a" * 64 if delta else None,
        object_sha256=sha256(b"parquet").hexdigest(), row_count=1,
        size_bytes=7,
        object_key=f"raw/354130/CNES_LOCAL/2026-07/{snapshot_id}/data.parquet",
        created_at=datetime(2026, 7, 15, 12, tzinfo=UTC),
    )


@pytest.mark.parametrize("corruption", ["identity", "schema"])
def test_cadeia_historica_rejeita_identidade_ou_schema_descontinuo(
    corruption: str,
) -> None:
    expected = _manifest()
    base = expected.model_copy(
        update={"file_subtype": "divergent"} if corruption == "identity" else {}
    )
    updates = {
        "previous_manifest_sha256": manifest_sha256(base),
        "file_subtype": base.file_subtype,
    }
    if corruption == "schema":
        updates["schema_version"] = "divergent"
    delta = _manifest(SnapshotMode.DELTA).model_copy(update=updates)

    assert not _valid_chain((base, delta), "agent-1", expected)
