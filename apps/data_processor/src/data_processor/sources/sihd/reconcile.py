"""Stage function: reconcilia internacoes e procedimentos SIHD em totais e divergencias."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.processing import ReconcileResult
from cnes_contracts.manifests.raw import SourceType
from data_processor.sources.sihd.contract import (
    DIVERGENCE_SCHEMA_VERSION,
    RECONCILIATION_SCHEMA_VERSION,
    SIHD_LAYOUT,
)
from data_processor.sources.sihd.normalize import persist_verified, serialize_parquet

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import ReconcileRequest
    from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort

_GROUP = ("CNES", "PROCEDIMENTO", "COMPETENCIA")
_AIH_ID = "AIH_ID"
_AIH_JOIN = ("COMPETENCIA", _AIH_ID)
_SEQUENCE_COLUMNS = {"internacoes": "SEQ", "procedimentos": "SEQ_PRINC"}
_RECONCILIATION_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "CNES": pl.String, "PROCEDIMENTO": pl.String, "COMPETENCIA": pl.String,
    "AIH_IDS": pl.List(pl.String), "aih_count": pl.Int64, "procedimento_qtd": pl.Int64,
    "valor_centavos": pl.Int64, "dt_internacao_min": pl.Date, "dt_saida_max": pl.Date,
    "_source_manifest_ids": pl.List(pl.String),
}
_DIVERGENCE_SCHEMA: dict[str, type[pl.DataType]] = {
    "kind": pl.String, "SIHD_KEY": pl.String, "field": pl.String, "value": pl.String,
    "source_manifest_id": pl.String,
}
_ROLES = {
    SIHD_LAYOUT.normalized[0].normalized_filenames[0]: "internacoes",
    SIHD_LAYOUT.normalized[0].normalized_filenames[1]: "quality_internacao",
    SIHD_LAYOUT.normalized[1].normalized_filenames[0]: "procedimentos",
    SIHD_LAYOUT.normalized[1].normalized_filenames[1]: "quality_proc_aih",
}


def reconcile_sihd(request: ReconcileRequest, store: ObjectStorePort) -> ReconcileResult:
    """Agrega procedimentos por CNES/procedimento/competencia e lista divergencias.

    Args:
        request: os quatro manifests normalizados SIHD da competencia.
        store: porta de objetos generica.

    Returns:
        ReconcileResult com manifests de reconciliacao e divergencia e KPIs inteiros.

    Raises:
        ValueError: manifests fora do layout SIHD ou saida nao verificada apos put.
    """
    manifests = _split_manifests(request.normalized_manifests)
    frames = {
        role: _with_aih_id(_read_frame(store, manifest), role)
        for role, manifest in manifests.items()
    }
    ids = {role: manifest.manifest_id for role, manifest in manifests.items()}
    totals = _totals(frames["procedimentos"], frames["internacoes"], ids)
    divergences = _divergences(frames, ids)
    quality_count = frames["quality_internacao"].height + frames["quality_proc_aih"].height
    kpis = {
        "internacao_count": frames["internacoes"].height,
        "procedimento_count": frames["procedimentos"].height,
        "valor_total_centavos": int(totals["valor_centavos"].sum()),
        "reconciled_row_count": totals.height,
        "divergence_count": divergences.height - quality_count,
        "quality_issue_count": quality_count,
    }
    return ReconcileResult(
        reconciliation_manifest=_persist(
            store, request, (request.reconciliation_key, "reconciliation"), totals
        ),
        divergence_manifest=_persist(
            store, request, (request.divergence_key, "divergence"), divergences
        ),
        kpis=kpis,
    )


def _split_manifests(manifests: tuple[OutputManifest, ...]) -> dict[str, OutputManifest]:
    by_role: dict[str, OutputManifest] = {}
    for manifest in manifests:
        if manifest.source_type is not SourceType.SIHD:
            raise ValueError(f"unexpected_source_type manifest_id={manifest.manifest_id}")
        role = _ROLES.get(manifest.object_key.rsplit("/", 1)[-1])
        if role is not None:
            by_role[role] = manifest
    if len(manifests) != len(_ROLES) or len(by_role) != len(_ROLES):
        raise ValueError(f"normalized_manifests_incomplete count={len(manifests)}")
    return by_role


def _read_frame(store: ObjectStorePort, manifest: OutputManifest) -> pl.DataFrame:
    with store.open(manifest.object_key) as handle:
        return pl.read_parquet(handle)


def _with_aih_id(frame: pl.DataFrame, role: str) -> pl.DataFrame:
    sequence = _SEQUENCE_COLUMNS.get(role)
    if sequence is None:
        return frame
    parts = [pl.col("OE_GESTOR"), pl.col(sequence).cast(pl.String)]
    return frame.with_columns(pl.concat_str(parts, separator=".").alias(_AIH_ID))


def _totals(
    procedimentos: pl.DataFrame, internacoes: pl.DataFrame, ids: dict[str, str]
) -> pl.DataFrame:
    periods = internacoes.group_by(list(_AIH_JOIN)).agg(
        pl.col("DT_INTERNACAO").min(), pl.col("DT_SAIDA").max()
    )
    identified = procedimentos.filter(pl.col(_AIH_ID).is_not_null())
    grouped = (
        identified.join(periods, on=list(_AIH_JOIN), how="left")
        .group_by(list(_GROUP))
        .agg(
            pl.col(_AIH_ID).drop_nulls().unique().sort().alias("AIH_IDS"),
            pl.col(_AIH_ID).drop_nulls().n_unique().alias("aih_count"),
            pl.col("QTD").sum().alias("procedimento_qtd"),
            pl.col("VALOR_CENTAVOS").sum().alias("valor_centavos"),
            pl.col("DT_INTERNACAO").min().alias("dt_internacao_min"),
            pl.col("DT_SAIDA").max().alias("dt_saida_max"),
        )
    )
    source_ids = sorted((ids["internacoes"], ids["procedimentos"]))
    grouped = grouped.with_columns(
        pl.Series("_source_manifest_ids", [source_ids] * grouped.height, pl.List(pl.String))
    )
    columns = [pl.col(name).cast(dtype) for name, dtype in _RECONCILIATION_SCHEMA.items()]
    return grouped.select(columns).sort(list(_GROUP), nulls_last=True)


def _divergence_select(kind: pl.Expr, field: pl.Expr, value: pl.Expr, source: str) -> list[pl.Expr]:
    return [
        kind.alias("kind"), pl.col("SIHD_KEY"), field.alias("field"),
        value.cast(pl.String).alias("value"), pl.lit(source).alias("source_manifest_id"),
    ]


def _orphans(
    frame: pl.DataFrame, other: pl.DataFrame, kind: str, source: str
) -> pl.DataFrame:
    keys = other.select(list(_AIH_JOIN)).unique()
    missing = frame.join(keys, on=list(_AIH_JOIN), how="anti")
    return missing.select(
        _divergence_select(pl.lit(kind), pl.lit(_AIH_ID), pl.col(_AIH_ID), source)
    )


def _cnes_mismatches(
    procedimentos: pl.DataFrame, internacoes: pl.DataFrame, source: str
) -> pl.DataFrame:
    aih_cnes = internacoes.group_by(list(_AIH_JOIN)).agg(
        pl.col("CNES").drop_nulls().sort().first().alias("_AIH_CNES")
    )
    mismatched = procedimentos.join(aih_cnes, on=list(_AIH_JOIN), how="inner").filter(
        pl.col("CNES").is_not_null()
        & pl.col("_AIH_CNES").is_not_null()
        & (pl.col("CNES") != pl.col("_AIH_CNES"))
    )
    value = pl.concat_str([pl.col("CNES"), pl.col("_AIH_CNES")], separator="|")
    return mismatched.select(
        _divergence_select(pl.lit("cnes_divergente"), pl.lit("CNES"), value, source)
    )


def _quality_rows(quality: pl.DataFrame, source: str) -> pl.DataFrame:
    kind = pl.concat_str([pl.lit("quality:"), pl.col("issue_code")])
    return quality.select(_divergence_select(kind, pl.col("field"), pl.col("value"), source))


def _divergences(frames: dict[str, pl.DataFrame], ids: dict[str, str]) -> pl.DataFrame:
    internacoes = frames["internacoes"]
    procedimentos = frames["procedimentos"]
    parts = [
        _cnes_mismatches(procedimentos, internacoes, ids["procedimentos"]),
        _orphans(internacoes, procedimentos, "internacao_sem_proc", ids["internacoes"]),
        _orphans(procedimentos, internacoes, "proc_sem_internacao", ids["procedimentos"]),
        _quality_rows(frames["quality_internacao"], ids["quality_internacao"]),
        _quality_rows(frames["quality_proc_aih"], ids["quality_proc_aih"]),
    ]
    typed = [part.cast(_DIVERGENCE_SCHEMA) for part in parts]
    combined = pl.concat([pl.DataFrame(schema=_DIVERGENCE_SCHEMA), *typed])
    return combined.sort(["kind", "SIHD_KEY", "field"], nulls_last=True)


def _persist(
    store: ObjectStorePort,
    request: ReconcileRequest,
    target: tuple[str, str],
    frame: pl.DataFrame,
) -> OutputManifest:
    target_key, prefix = target
    stat = persist_verified(store, target_key, serialize_parquet(frame))
    schema_version = (
        RECONCILIATION_SCHEMA_VERSION if prefix == "reconciliation" else DIVERGENCE_SCHEMA_VERSION
    )
    return _output_manifest(request, (target_key, prefix, schema_version), stat, frame.height)


def _output_manifest(
    request: ReconcileRequest, spec: tuple[str, str, str], stat: ObjectStat, row_count: int
) -> OutputManifest:
    target_key, prefix, schema_version = spec
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
        object_key=target_key,
        object_sha256=stat.sha256,
        row_count=row_count,
        created_at=request.reconciled_at,
    )
