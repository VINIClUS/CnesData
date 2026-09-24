"""Stage function: normaliza uma unidade SIA_LOCAL (um subtipo) em dados e qualidade."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.processing import NormalizeResult
from cnes_contracts.manifests.validation import manifest_sha256
from data_processor.adapters.sia_adapter import canonicalize_apa, canonicalize_bpi
from data_processor.sources.sia.contract import (
    QualityCheck,
    provenance_metadata,
    read_raw,
    resolve_normalize_targets,
    serialize_parquet,
    split_quality,
    with_source_row,
    write_verified,
)
from data_processor.sources.sia.reference_data import REFERENCE_SUBTYPES, normalize_reference

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import NormalizeRequest
    from cnes_contracts.manifests.raw import RawManifest
    from cnes_domain.ports.object_store import ObjectStorePort

logger = logging.getLogger(__name__)

SCHEMA_VERSIONS = {
    "SIA_APA": "sia-apa-normalized-v1",
    "SIA_BPI": "sia-bpi-normalized-v1",
    "SIA_BPIHST": "sia-bpi-normalized-v1",
    "DIM_SIGTAP": "sia-reference-sigtap-v1",
    "DIM_MUNICIPIO": "sia-reference-municipio-v1",
}
QUALITY_SCHEMA_VERSION = "sia-quality-v1"
_RAW_IDENTITY = "_raw_identity"


@dataclass(frozen=True, slots=True)
class _Output:
    key: str
    frame: pl.DataFrame
    schema_version: str


def normalize_sia(request: NormalizeRequest, store: ObjectStorePort) -> NormalizeResult:
    """Normaliza um subtipo SIA; toda linha raw termina em dados ou em qualidade.

    Args:
        request: uma cadeia FULL de um subtipo de SIA_DEPENDENCIES e seu par de target_keys.
        store: porta de objetos genérica.

    Returns:
        NormalizeResult com os manifests de dados e qualidade, ordenados por object_key.

    Raises:
        SiaContractError: request fora do contrato, hash raw divergente ou put não verificado.
        ValueError: schema raw fora do contrato Edge.
    """
    targets = resolve_normalize_targets(request)
    raw_manifest = request.raw_manifests[0]
    raw = read_raw(store, raw_manifest)
    data, quality = _split(targets.subtype, raw, raw_manifest.competencia)
    raw_hash = manifest_sha256(raw_manifest)
    outputs = (
        _Output(targets.data_key, data, SCHEMA_VERSIONS[targets.subtype]),
        _Output(targets.quality_key, quality, QUALITY_SCHEMA_VERSION),
    )
    manifests = [_persist(store, request, output, raw_hash) for output in outputs]
    logger.info(
        "sia_normalized subtype=%s input=%d rows=%d quality=%d",
        targets.subtype, raw.height, data.height, quality.height,
    )
    return NormalizeResult(manifests=tuple(sorted(manifests, key=lambda item: item.object_key)))


def _split(
    subtype: str, raw: pl.DataFrame, competencia: str
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if subtype in REFERENCE_SUBTYPES:
        return normalize_reference(subtype, raw, competencia)
    canonical = canonicalize_apa(raw) if subtype == "SIA_APA" else canonicalize_bpi(raw, subtype)
    identity = raw.select(pl.struct(pl.all()).alias(_RAW_IDENTITY))
    indexed = with_source_row(canonical).hstack(identity)
    data, quality = split_quality(indexed, _fact_checks(competencia), (_RAW_IDENTITY,))
    return data.drop(_RAW_IDENTITY), quality


def _fact_checks(competencia: str) -> tuple[QualityCheck, ...]:
    no_detail = pl.lit(None, dtype=pl.String)
    return (
        QualityCheck(
            "competencia_divergente",
            pl.col("competencia").ne_missing(pl.lit(competencia)),
            pl.lit("competencia=") + pl.col("competencia").fill_null(""),
        ),
        QualityCheck("cnes_ausente", pl.col("cnes").is_null(), no_detail),
        QualityCheck("procedimento_ausente", pl.col("cod_procedimento").is_null(), no_detail),
    )


def _with_provenance(
    frame: pl.DataFrame, manifest: RawManifest, raw_hash: str, request: NormalizeRequest
) -> pl.DataFrame:
    return frame.with_columns(
        pl.lit(manifest.file_subtype).alias("_source_subtype"),
        pl.lit(manifest.manifest_id).alias("_source_manifest_id"),
        pl.lit(raw_hash).alias("_source_manifest_sha256"),
        pl.lit(request.normalized_at.isoformat()).alias("_normalized_at"),
    )


def _persist(
    store: ObjectStorePort, request: NormalizeRequest, output: _Output, raw_hash: str
) -> OutputManifest:
    raw_manifest = request.raw_manifests[0]
    frame = _with_provenance(output.frame, raw_manifest, raw_hash, request)
    payload = serialize_parquet(frame, provenance_metadata(output.schema_version, {raw_hash}))
    stat = write_verified(store, output.key, payload)
    stem = output.key.rsplit("/", 1)[-1].removesuffix(".parquet")
    return OutputManifest(
        manifest_version=1,
        manifest_id=f"normalized-{request.run_id}-{request.unit_id}-{request.attempt}-{stem}",
        tenant_id=request.tenant_id,
        layer="normalized",
        source_type=request.source_type,
        competencia=raw_manifest.competencia,
        run_id=request.run_id,
        unit_id=request.unit_id,
        attempt=request.attempt,
        schema_version=output.schema_version,
        object_key=output.key,
        object_sha256=stat.sha256,
        row_count=frame.height,
        created_at=request.normalized_at,
    )
