"""Testes das validações comuns de manifestos."""

import hashlib
from datetime import UTC, datetime

import pytest

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType
from cnes_contracts.manifests.validation import manifest_sha256, validate_object_key


def raw_manifest() -> RawManifest:
    return RawManifest(
        manifest_version=1,
        manifest_id="manifest-1",
        tenant_id="354130",
        source_type=SourceType.CNES_LOCAL,
        file_subtype="CNES_VINCULO",
        competencia="2026-07",
        agent_id="agent-1",
        agent_version="1",
        schema_version="cnes-raw-v1",
        snapshot_mode=SnapshotMode.FULL,
        snapshot_id="snapshot-1",
        base_snapshot_id=None,
        sequence=1,
        previous_manifest_sha256=None,
        object_sha256="a" * 64,
        row_count=1,
        size_bytes=1,
        object_key="raw/354130/CNES_LOCAL/2026-07/snapshot-1/data.parquet",
        created_at=datetime(2026, 7, 1, tzinfo=UTC),
    )


def output_manifest(layer: str) -> OutputManifest:
    source_type = SourceType.CNES_LOCAL if layer == "normalized" else None
    keys = {
        "normalized": "normalized/354130/CNES_LOCAL/2026-07/run-1/data.parquet",
        "reconciliation": "reconciliation/354130/2026-07/run-1/data.parquet",
        "serving": "serving/354130/run-1/overview.json",
    }
    return OutputManifest.model_validate(
        {
            "manifest_version": 1,
            "manifest_id": f"manifest-{layer}",
            "tenant_id": "354130",
            "layer": layer,
            "source_type": source_type,
            "competencia": "2026-07",
            "run_id": "run-1",
            "unit_id": "unit-1",
            "attempt": 1,
            "schema_version": "schema-v1",
            "object_key": keys[layer],
            "object_sha256": "b" * 64,
            "row_count": 1,
            "created_at": datetime(2026, 7, 2, tzinfo=UTC),
        }
    )


def test_manifest_hash_usa_serializacao_canonica():
    manifest = raw_manifest()

    expected = hashlib.sha256(
        manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    ).hexdigest()

    assert manifest_sha256(manifest) == expected


@pytest.mark.parametrize("layer", ["normalized", "reconciliation", "serving"])
def test_validate_object_key_aceita_layouts_de_saida(layer: str):
    validate_object_key(output_manifest(layer))


def test_validate_object_key_aceita_layout_raw():
    validate_object_key(raw_manifest())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("tenant_id", "outro"),
        ("source_type", SourceType.SIHD),
        ("competencia", "2026-08"),
        ("snapshot_id", "snapshot-2"),
    ],
)
def test_validate_object_key_rejeita_identidade_raw_incorreta(field: str, value: object):
    with pytest.raises(ValueError, match="object_key_identity"):
        validate_object_key(raw_manifest().model_copy(update={field: value}))


@pytest.mark.parametrize(
    ("layer", "field", "value"),
    [
        ("normalized", "tenant_id", "outro"),
        ("normalized", "source_type", SourceType.SIHD),
        ("normalized", "competencia", "2026-08"),
        ("normalized", "run_id", "run-2"),
        ("reconciliation", "tenant_id", "outro"),
        ("reconciliation", "competencia", "2026-08"),
        ("reconciliation", "run_id", "run-2"),
        ("serving", "tenant_id", "outro"),
        ("serving", "run_id", "run-2"),
    ],
)
def test_validate_object_key_rejeita_identidade_output_incorreta(
    layer: str, field: str, value: object
):
    with pytest.raises(ValueError, match="object_key_identity"):
        validate_object_key(output_manifest(layer).model_copy(update={field: value}))


@pytest.mark.parametrize(
    "key",
    [
        "raw/354130/CNES_LOCAL/2026-07/snapshot-1/../secret",
        "/raw/354130/CNES_LOCAL/2026-07/snapshot-1/data.parquet",
        "raw//CNES_LOCAL/2026-07/snapshot-1/data.parquet",
        "raw/354130/CNES_LOCAL/2026-07/snapshot-1/folder/data.parquet",
        "raw/354130/CNES_LOCAL/2026-07/snapshot-1/.hidden",
    ],
)
def test_validate_object_key_rejeita_traversal_e_leaf_insegura(key: str):
    manifest = raw_manifest().model_copy(update={"object_key": key})
    with pytest.raises(ValueError, match="object_key"):
        validate_object_key(manifest)


def test_validate_object_key_rejeita_camada_desconhecida():
    manifest = output_manifest("normalized").model_copy(update={"layer": "unknown"})
    with pytest.raises(ValueError, match="object_key_layer"):
        validate_object_key(manifest)


def test_output_serving_rejeita_leaf_sem_extensao_json():
    manifest = output_manifest("serving")
    values = {**manifest.model_dump(), "object_key": "serving/354130/run-1/overview.parquet"}

    with pytest.raises(ValueError, match="object_key_invalid"):
        OutputManifest.model_validate(values)
