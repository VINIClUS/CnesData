"""Stage function: reconcilia BPA_C e BPA_I por chave natural de produção."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.processing import ReconcileResult
from data_processor.sources.bpa.contract import (
    DATA_FILENAMES,
    QUALITY_FILENAMES,
    leaf,
    persist,
    read_parquet,
    serialize_parquet,
)

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import ReconcileRequest
    from cnes_domain.ports.object_store import ObjectStorePort

_GROUP_KEY = ("competencia", "cnes", "sigtap", "cbo", "file_subtype")
_RECONCILIATION_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "tenant_id": pl.String, "competencia": pl.String, "cnes": pl.String, "sigtap": pl.String,
    "cbo": pl.String, "file_subtype": pl.String, "linhas": pl.Int64, "linhas_aceitas": pl.Int64,
    "qtd_apresentada": pl.Int64, "qtd_aceita": pl.Int64,
    "source_record_ids": pl.List(pl.String), "source_manifest_ids": pl.List(pl.String),
}
_DIVERGENCE_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "tenant_id": pl.String, "competencia": pl.String, "file_subtype": pl.String,
    "cnes": pl.String, "source_record_id": pl.String, "field": pl.String, "code": pl.String,
    "raw_value": pl.String, "source_manifest_id": pl.String,
}
_RECONCILIATION_SCHEMA_VERSION = "bpa-reconciliation-v1"
_DIVERGENCE_SCHEMA_VERSION = "bpa-divergence-v1"


def reconcile_bpa(request: ReconcileRequest, store: ObjectStorePort) -> ReconcileResult:
    """Agrupa a produção BPA por chave natural registrando apresentada vs aceita.

    Args:
        request: os quatro manifests normalizados BPA (dados e qualidade de C e I).
        store: porta de objetos genérica.

    Returns:
        ReconcileResult com manifests de reconciliação/divergência e KPIs inteiros.

    Raises:
        ValueError: manifests normalizados incompletos ou artefato não verificado.
    """
    by_leaf = _normalized_by_leaf(request)
    data = pl.concat([read_parquet(store, by_leaf[name]) for name in DATA_FILENAMES])
    issues = pl.concat([read_parquet(store, by_leaf[name]) for name in QUALITY_FILENAMES])
    reconciled = _aggregate(data, request.tenant_id)
    divergences = _divergences(data, issues, request)
    return ReconcileResult(
        reconciliation_manifest=_write(
            store, request,
            (request.reconciliation_key, "reconciliation", _RECONCILIATION_SCHEMA_VERSION),
            reconciled,
        ),
        divergence_manifest=_write(
            store, request,
            (request.divergence_key, "divergence", _DIVERGENCE_SCHEMA_VERSION),
            divergences,
        ),
        kpis=_kpis(data, reconciled, divergences),
    )


def _normalized_by_leaf(request: ReconcileRequest) -> dict[str, OutputManifest]:
    by_leaf = {leaf(item.object_key): item for item in request.normalized_manifests}
    expected = {*DATA_FILENAMES, *QUALITY_FILENAMES}
    if set(by_leaf) != expected or len(request.normalized_manifests) != len(expected):
        raise ValueError(f"bpa_normalized_incompleto leaves={','.join(sorted(by_leaf))}")
    return by_leaf


def _aggregate(data: pl.DataFrame, tenant_id: str) -> pl.DataFrame:
    quantity = pl.col("quantidade").fill_null(0)
    grouped = data.group_by(list(_GROUP_KEY)).agg(
        pl.len().alias("linhas"),
        pl.col("valido").sum().alias("linhas_aceitas"),
        quantity.sum().alias("qtd_apresentada"),
        pl.when(pl.col("valido")).then(quantity).otherwise(0).sum().alias("qtd_aceita"),
        pl.col("source_record_id").sort().alias("source_record_ids"),
        pl.col("_source_manifest_id").unique().sort().alias("source_manifest_ids"),
    )
    grouped = grouped.with_columns(pl.lit(tenant_id).alias("tenant_id"))
    return (
        grouped.select(list(_RECONCILIATION_SCHEMA))
        .cast(_RECONCILIATION_SCHEMA)
        .sort(list(_GROUP_KEY), nulls_last=True)
    )


def _divergences(
    data: pl.DataFrame, issues: pl.DataFrame, request: ReconcileRequest
) -> pl.DataFrame:
    origin = data.select(
        "source_record_id", "cnes", pl.col("_source_manifest_id").alias("source_manifest_id"),
    )
    joined = issues.join(origin, on="source_record_id", how="left")
    joined = joined.with_columns(
        pl.lit(request.tenant_id).alias("tenant_id"),
        pl.lit(request.competencia).alias("competencia"),
    )
    return (
        joined.select(list(_DIVERGENCE_SCHEMA))
        .cast(_DIVERGENCE_SCHEMA)
        .sort(["file_subtype", "source_record_id", "field", "code"])
    )


def _kpis(
    data: pl.DataFrame, reconciled: pl.DataFrame, divergences: pl.DataFrame
) -> dict[str, int]:
    raw = {
        "linhas_bpa_c": data.filter(pl.col("file_subtype") == "BPA_C").height,
        "linhas_bpa_i": data.filter(pl.col("file_subtype") == "BPA_I").height,
        "linhas_aceitas": reconciled["linhas_aceitas"].sum(),
        "linhas_com_divergencia": divergences["source_record_id"].n_unique(),
        "divergencias": divergences.height,
        "qtd_apresentada": reconciled["qtd_apresentada"].sum(),
        "qtd_aceita": reconciled["qtd_aceita"].sum(),
        "grupos_reconciliados": reconciled.height,
    }
    return {key: int(value) for key, value in raw.items()}


def _write(
    store: ObjectStorePort,
    request: ReconcileRequest,
    spec: tuple[str, str, str],
    frame: pl.DataFrame,
) -> OutputManifest:
    key, prefix, schema_version = spec
    stat = persist(store, key, serialize_parquet(frame))
    return OutputManifest(
        manifest_version=1,
        manifest_id=f"{prefix}-{request.run_id}-{request.unit_id}-{request.attempt}",
        tenant_id=request.tenant_id,
        layer="reconciliation",
        source_type=None,
        competencia=request.competencia,
        run_id=request.run_id,
        unit_id=request.unit_id,
        attempt=request.attempt,
        schema_version=schema_version,
        object_key=key,
        object_sha256=stat.sha256,
        row_count=frame.height,
        created_at=request.reconciled_at,
    )
