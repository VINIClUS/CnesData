"""Stage function: normaliza uma cadeia raw BPA_C ou BPA_I sem lookup SQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.processing import NormalizeResult
from cnes_contracts.manifests.raw import SourceType
from data_processor.adapters.bpa_adapter import (
    RAW_ROW_KEY,
    canonicalize,
    prepare_raw,
    quality_issues,
    with_record_ids,
)
from data_processor.pipeline.delta_reconstruction import reconstruct_from_deltas
from data_processor.sources.bpa.contract import (
    BPA_LAYOUT,
    PROVENANCE_SCHEMA,
    leaf,
    persist,
    read_parquet,
    serialize_parquet,
)

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import NormalizeRequest
    from cnes_contracts.manifests.raw import RawManifest
    from cnes_domain.orchestration.source_catalog import SubtypeLayout
    from cnes_domain.ports.object_store import ObjectStorePort

_DATA_SCHEMA_VERSION = "bpa-normalized-v1"
_QUALITY_SCHEMA_VERSION = "bpa-quality-v1"


def normalize_bpa(request: NormalizeRequest, store: ObjectStorePort) -> NormalizeResult:
    """Normaliza uma única cadeia BPA_C ou BPA_I no par dados + qualidade.

    Args:
        request: cadeia raw de um só subtipo BPA_MAG e o par ordenado de target_keys.
        store: porta de objetos genérica.

    Returns:
        NormalizeResult com os manifests de dados e de qualidade, nessa ordem.

    Raises:
        ValueError: source_type, subtipo ou target_keys fora do contrato BPA,
            ou artefato não verificado após put.
    """
    layout = _subtype_layout(request)
    data_key, quality_key = _target_pair(request, layout)
    base = request.raw_manifests[0]
    frames = [prepare_raw(read_parquet(store, item.object_key)) for item in request.raw_manifests]
    current = reconstruct_from_deltas(frames[0], frames[1:], RAW_ROW_KEY)
    keyed = with_record_ids(current, layout.file_subtype)
    issues = quality_issues(keyed, layout.file_subtype, base.competencia)
    rows = canonicalize(keyed, layout.file_subtype, base.competencia, issues["source_record_id"])
    rows = _with_provenance(rows, request, base)
    manifests = (
        _write(store, request, (data_key, "data", _DATA_SCHEMA_VERSION), rows),
        _write(store, request, (quality_key, "quality", _QUALITY_SCHEMA_VERSION), issues),
    )
    return NormalizeResult(manifests=manifests)


def _subtype_layout(request: NormalizeRequest) -> SubtypeLayout:
    if request.source_type is not SourceType.BPA_MAG:
        raise ValueError(f"bpa_source_type_invalido source_type={request.source_type.value}")
    subtypes = sorted({item.file_subtype for item in request.raw_manifests})
    if len(subtypes) != 1:
        raise ValueError(f"bpa_subtipo_misto subtypes={','.join(subtypes)}")
    for layout in BPA_LAYOUT.normalized:
        if layout.file_subtype == subtypes[0]:
            return layout
    raise ValueError(f"bpa_subtipo_nao_declarado subtype={subtypes[0]}")


def _target_pair(request: NormalizeRequest, layout: SubtypeLayout) -> tuple[str, str]:
    by_leaf = {leaf(key): key for key in request.target_keys}
    if sorted(by_leaf) != sorted(layout.normalized_filenames):
        raise ValueError(f"bpa_target_keys_invalidos subtype={layout.file_subtype}")
    data_name, quality_name = layout.normalized_filenames
    return by_leaf[data_name], by_leaf[quality_name]


def _with_provenance(
    frame: pl.DataFrame, request: NormalizeRequest, base: RawManifest
) -> pl.DataFrame:
    return frame.with_columns(
        pl.lit(base.manifest_id).alias("_source_manifest_id"),
        pl.lit(base.snapshot_id).alias("_source_snapshot_id"),
        pl.lit(request.source_type.value).alias("_source_type"),
        pl.lit(request.normalized_at.isoformat()).alias("_normalized_at"),
    ).cast(PROVENANCE_SCHEMA)


def _write(
    store: ObjectStorePort,
    request: NormalizeRequest,
    spec: tuple[str, str, str],
    frame: pl.DataFrame,
) -> OutputManifest:
    key, role, schema_version = spec
    stat = persist(store, key, serialize_parquet(frame))
    return OutputManifest(
        manifest_version=1,
        manifest_id=f"normalized-{request.run_id}-{request.unit_id}-{request.attempt}-{role}",
        tenant_id=request.tenant_id,
        layer="normalized",
        source_type=request.source_type,
        competencia=request.raw_manifests[0].competencia,
        run_id=request.run_id,
        unit_id=request.unit_id,
        attempt=request.attempt,
        schema_version=schema_version,
        object_key=key,
        object_sha256=stat.sha256,
        row_count=frame.height,
        created_at=request.normalized_at,
    )
