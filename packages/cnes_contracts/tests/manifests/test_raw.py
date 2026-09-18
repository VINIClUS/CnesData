"""Testes dos manifestos raw."""

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType

HASH = "a" * 64


def raw_values() -> dict[str, object]:
    return {
        "manifest_version": 1,
        "manifest_id": "manifest-1",
        "tenant_id": "354130",
        "source_type": SourceType.CNES_LOCAL,
        "file_subtype": "CNES_VINCULO",
        "competencia": "2026-07",
        "agent_id": "agent-1",
        "agent_version": "1.2.3",
        "schema_version": "cnes-raw-v1",
        "snapshot_mode": SnapshotMode.FULL,
        "snapshot_id": "snapshot-1",
        "base_snapshot_id": None,
        "sequence": 1,
        "previous_manifest_sha256": None,
        "object_sha256": HASH,
        "row_count": 10,
        "size_bytes": 100,
        "object_key": ("raw/354130/CNES_LOCAL/2026-07/snapshot-1/cnes_vinculo.parquet"),
        "created_at": datetime(2026, 7, 1, tzinfo=UTC),
    }


def test_manifesto_raw_full_valido_e_imutavel():
    manifest = RawManifest.model_validate(raw_values())

    assert manifest.sequence == 1
    with pytest.raises(ValidationError):
        manifest.sequence = 2


def test_manifesto_raw_rejeita_campos_extras_e_tipos_coagidos():
    with pytest.raises(ValidationError):
        RawManifest.model_validate({**raw_values(), "unknown": True})
    with pytest.raises(ValidationError):
        RawManifest.model_validate({**raw_values(), "row_count": "10"})


def test_manifesto_raw_rejeita_versao_diferente():
    with pytest.raises(ValidationError):
        RawManifest.model_validate({**raw_values(), "manifest_version": 2})


def test_manifesto_raw_aceita_enums_wire_em_json():
    payload = RawManifest.model_validate(raw_values()).model_dump_json()

    manifest = RawManifest.model_validate_json(payload)

    assert manifest.source_type is SourceType.CNES_LOCAL
    assert manifest.snapshot_mode is SnapshotMode.FULL


@pytest.mark.parametrize("field", ["object_sha256", "previous_manifest_sha256"])
@pytest.mark.parametrize("value", ["A" * 64, "a" * 63, "g" * 64])
def test_manifesto_raw_rejeita_hash_invalido(field: str, value: str):
    values = raw_values()
    values.update(
        snapshot_mode=SnapshotMode.DELTA,
        base_snapshot_id="snapshot-1",
        sequence=2,
        previous_manifest_sha256=HASH,
    )
    values[field] = value

    with pytest.raises(ValidationError):
        RawManifest.model_validate(values)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"sequence": 2}, "full_chain_invalid"),
        ({"base_snapshot_id": "snapshot-0"}, "full_chain_invalid"),
        ({"previous_manifest_sha256": HASH}, "full_chain_invalid"),
        (
            {
                "snapshot_mode": SnapshotMode.DELTA,
                "sequence": 2,
                "previous_manifest_sha256": HASH,
            },
            "delta_chain_required",
        ),
        (
            {
                "snapshot_mode": SnapshotMode.DELTA,
                "base_snapshot_id": "snapshot-1",
                "sequence": 2,
            },
            "delta_chain_required",
        ),
        (
            {
                "snapshot_mode": SnapshotMode.DELTA,
                "base_snapshot_id": "snapshot-1",
                "sequence": 1,
                "previous_manifest_sha256": HASH,
            },
            "delta_chain_required",
        ),
    ],
)
def test_manifesto_raw_valida_cadeia(changes: dict[str, object], message: str):
    with pytest.raises(ValidationError, match=message):
        RawManifest.model_validate({**raw_values(), **changes})


@pytest.mark.parametrize(("field", "value"), [("size_bytes", 0), ("row_count", -1)])
def test_manifesto_raw_valida_cardinalidade(field: str, value: int):
    with pytest.raises(ValidationError):
        RawManifest.model_validate({**raw_values(), field: value})


@pytest.mark.parametrize(
    "created_at",
    [
        datetime(2026, 7, 1, tzinfo=UTC).replace(tzinfo=None),
        datetime(2026, 7, 1, tzinfo=timezone(timedelta(hours=-3))),
    ],
)
def test_manifesto_raw_exige_timestamp_utc(created_at: datetime):
    with pytest.raises(ValidationError, match="datetime_utc_required"):
        RawManifest.model_validate({**raw_values(), "created_at": created_at})


def test_manifesto_raw_rejeita_object_key_nao_canonica():
    with pytest.raises(ValidationError, match="object_key"):
        RawManifest.model_validate({**raw_values(), "object_key": "raw/outro/arquivo.parquet"})
