"""Stage function: normaliza CNES_LOCAL a partir da cadeia raw FULL+DELTA."""

from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.processing import NormalizeResult
from data_processor.adapters.cnes_local_adapter import _MAP_PROFISSIONAL_RAW, _normalizar_nfkd
from data_processor.pipeline.delta_reconstruction import reconstruct_from_deltas

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import NormalizeRequest
    from cnes_contracts.manifests.raw import RawManifest
    from cnes_domain.ports.object_store import ObjectStorePort

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
_FRAME_SCHEMA: dict[str, type[pl.DataType]] = {
    "CPF": pl.String, "CNS": pl.String, "NOME_PROFISSIONAL": pl.String,
    "NOME_SOCIAL": pl.String, "SEXO": pl.String, "CBO": pl.String, "CNES": pl.String,
    "TIPO_VINCULO": pl.String, "SUS": pl.String, "CH_TOTAL": pl.Int64,
    "CH_AMBULATORIAL": pl.Int64, "CH_OUTRAS": pl.Int64, "CH_HOSPITALAR": pl.Int64,
    "FONTE": pl.String,
}


def normalize_cnes_local(request: NormalizeRequest, store: ObjectStorePort) -> NormalizeResult:
    """Reconstroi o estado atual de CNES_LOCAL e publica o Parquet normalizado.

    Args:
        request: cadeia raw FULL+DELTA e um único target_key.
        store: porta de objetos genérica (não passa por AttemptObjectStore).

    Returns:
        NormalizeResult com um único OutputManifest, schema cnes-normalized-v1.

    Raises:
        ValueError: mais de um target_key, ou objeto não encontrado após put.
    """
    if len(request.target_keys) != 1:
        raise ValueError("target_keys_must_be_single")
    manifests = request.raw_manifests
    base_manifest = manifests[0]
    frames = [_canonicalize(_read_frame(store, manifest)) for manifest in manifests]
    current = reconstruct_from_deltas(frames[0], frames[1:], _NATURAL_KEY)
    normalized = _with_provenance(current, request, base_manifest)
    normalized = normalized.sort(list(_NATURAL_KEY), nulls_last=True, maintain_order=True)
    normalized = normalized.select(list(_NORMALIZED_COLUMNS))
    manifest = _write_and_build_manifest(normalized, request, base_manifest, store)
    return NormalizeResult(manifests=(manifest,))


def _read_frame(store: ObjectStorePort, manifest: RawManifest) -> pl.DataFrame:
    with store.open(manifest.object_key) as handle:
        return pl.read_parquet(handle)


def _materialize_missing_columns(frame: pl.DataFrame) -> pl.DataFrame:
    missing = [name for name in _SOURCE_COLUMNS if name not in frame.columns]
    if not missing:
        return frame
    return frame.with_columns(
        [pl.lit(None).cast(_FRAME_SCHEMA[name]).alias(name) for name in missing]
    )


def _canonicalize(frame: pl.DataFrame) -> pl.DataFrame:
    frame = frame.rename({k: v for k, v in _MAP_PROFISSIONAL_RAW.items() if k in frame.columns})
    frame = _materialize_missing_columns(frame)
    frame = frame.with_columns(
        [pl.col(name).cast(dtype) for name, dtype in _FRAME_SCHEMA.items()]
    )
    frame = frame.with_columns(
        pl.col("CPF").str.strip_chars(),
        pl.col("CNS").str.strip_chars(),
        pl.col("SEXO").str.strip_chars(),
        pl.col("SUS").str.strip_chars(),
        pl.col("TIPO_VINCULO").str.strip_chars(),
        pl.col("CNES").str.strip_chars().str.pad_start(7, "0"),
        pl.col("CBO").str.strip_chars().str.pad_start(6, "0"),
        _normalizar_nfkd(pl.col("NOME_PROFISSIONAL")).alias("NOME_PROFISSIONAL"),
        _normalizar_nfkd(pl.col("NOME_SOCIAL")).alias("NOME_SOCIAL"),
        pl.lit("LOCAL").alias("FONTE"),
    )
    columns = [*_SOURCE_COLUMNS, *(["_op"] if "_op" in frame.columns else [])]
    return frame.select(columns)


def _with_provenance(
    frame: pl.DataFrame, request: NormalizeRequest, base_manifest: RawManifest
) -> pl.DataFrame:
    return frame.with_columns(
        pl.lit(base_manifest.manifest_id).alias("_source_manifest_id"),
        pl.lit(base_manifest.snapshot_id).alias("_source_snapshot_id"),
        pl.lit(request.source_type.value).alias("_source_type"),
        pl.lit(request.normalized_at.isoformat()).alias("_normalized_at"),
    )


def _serialize(frame: pl.DataFrame) -> bytes:
    output = BytesIO()
    frame.write_parquet(
        output, compression="zstd", compression_level=3, statistics=True, row_group_size=64_000,
    )
    return output.getvalue()


def _write_and_build_manifest(
    frame: pl.DataFrame,
    request: NormalizeRequest,
    base_manifest: RawManifest,
    store: ObjectStorePort,
) -> OutputManifest:
    target_key = request.target_keys[0]
    payload = _serialize(frame)
    digest = sha256(payload).hexdigest()
    store.put(target_key, BytesIO(payload), digest)
    stat = store.stat(target_key)
    if stat is None:
        raise ValueError(f"output_not_found key={target_key}")
    return OutputManifest(
        manifest_version=1,
        manifest_id=f"normalized-{request.run_id}-{request.unit_id}-{request.attempt}",
        tenant_id=request.tenant_id,
        layer="normalized",
        source_type=request.source_type,
        competencia=base_manifest.competencia,
        run_id=request.run_id,
        unit_id=request.unit_id,
        attempt=request.attempt,
        schema_version=_SCHEMA_VERSION,
        object_key=target_key,
        object_sha256=stat.sha256,
        row_count=frame.height,
        created_at=request.normalized_at,
    )
