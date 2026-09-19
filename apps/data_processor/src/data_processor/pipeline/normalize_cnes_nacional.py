"""Stage function: normaliza CNES_NACIONAL a partir do manifesto raw FULL unico."""

from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.processing import NormalizeResult
from cnes_contracts.manifests.raw import SnapshotMode, SourceType

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import NormalizeRequest
    from cnes_contracts.manifests.raw import RawManifest
    from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort

_SOURCE_COLUMNS = (
    "CPF", "CNS", "NOME_PROFISSIONAL", "NOME_SOCIAL", "SEXO", "CBO", "CNES",
    "TIPO_VINCULO", "SUS", "CH_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS",
    "CH_HOSPITALAR", "FONTE",
)
_PROVENANCE_COLUMNS = (
    "_source_manifest_id", "_source_snapshot_id", "_source_type", "_normalized_at",
)
_NORMALIZED_COLUMNS = (*_SOURCE_COLUMNS, *_PROVENANCE_COLUMNS)
_NATURAL_KEY = ("CPF", "CNS", "CNES", "CBO")
_SCHEMA_VERSION = "cnes-normalized-v1"


def normalize_cnes_nacional(request: NormalizeRequest, store: ObjectStorePort) -> NormalizeResult:
    """Le o manifesto raw FULL unico de CNES_NACIONAL e publica o Parquet normalizado.

    Args:
        request: exatamente um raw_manifest CNES_NACIONAL FULL e um target_key.
        store: porta de objetos genérica (não passa por AttemptObjectStore).

    Returns:
        NormalizeResult com um único OutputManifest, schema cnes-normalized-v1.

    Raises:
        ValueError: manifesto raw fora do shape esperado, mais de um target_key,
            ou objeto não encontrado após put.
    """
    manifest = _single_full_manifest(request)
    if len(request.target_keys) != 1:
        raise ValueError("target_keys_must_be_single")
    frame = _read_frame(store, manifest).select(list(_SOURCE_COLUMNS))
    normalized = _with_provenance(frame, request, manifest)
    normalized = normalized.sort(list(_NATURAL_KEY), nulls_last=True, maintain_order=True)
    normalized = normalized.select(list(_NORMALIZED_COLUMNS))
    output = _write_and_build_manifest(normalized, request, manifest, store)
    return NormalizeResult(manifests=(output,))


def _single_full_manifest(request: NormalizeRequest) -> RawManifest:
    manifests = request.raw_manifests
    if len(manifests) != 1:
        raise ValueError("unexpected_raw_manifest_shape")
    manifest = manifests[0]
    if manifest.source_type is not SourceType.CNES_NACIONAL:
        raise ValueError("unexpected_raw_manifest_shape")
    if manifest.snapshot_mode is not SnapshotMode.FULL:
        raise ValueError("unexpected_raw_manifest_shape")
    return manifest


def _read_frame(store: ObjectStorePort, manifest: RawManifest) -> pl.DataFrame:
    with store.open(manifest.object_key) as handle:
        return pl.read_parquet(handle)


def _with_provenance(
    frame: pl.DataFrame, request: NormalizeRequest, manifest: RawManifest
) -> pl.DataFrame:
    return frame.with_columns(
        pl.lit(manifest.manifest_id).alias("_source_manifest_id"),
        pl.lit(manifest.snapshot_id).alias("_source_snapshot_id"),
        pl.lit(request.source_type.value).alias("_source_type"),
        pl.lit(request.normalized_at.isoformat()).alias("_normalized_at"),
    )


def _serialize(frame: pl.DataFrame) -> bytes:
    output = BytesIO()
    frame.write_parquet(
        output, compression="zstd", compression_level=3, statistics=True, row_group_size=64_000,
    )
    return output.getvalue()


def _persist(store: ObjectStorePort, target_key: str, frame: pl.DataFrame) -> ObjectStat:
    payload = _serialize(frame)
    digest = sha256(payload).hexdigest()
    store.put(target_key, BytesIO(payload), digest)
    stat = store.stat(target_key)
    if stat is None:
        raise ValueError(f"output_not_found key={target_key}")
    return stat


def _write_and_build_manifest(
    frame: pl.DataFrame,
    request: NormalizeRequest,
    manifest: RawManifest,
    store: ObjectStorePort,
) -> OutputManifest:
    target_key = request.target_keys[0]
    stat = _persist(store, target_key, frame)
    return OutputManifest(
        manifest_version=1,
        manifest_id=f"normalized-{request.run_id}-{request.unit_id}-{request.attempt}",
        tenant_id=request.tenant_id,
        layer="normalized",
        source_type=request.source_type,
        competencia=manifest.competencia,
        run_id=request.run_id,
        unit_id=request.unit_id,
        attempt=request.attempt,
        schema_version=_SCHEMA_VERSION,
        object_key=target_key,
        object_sha256=stat.sha256,
        row_count=frame.height,
        created_at=request.normalized_at,
    )
