"""Stage function: reconcilia APA, BPI e BPIHST por chave natural contra o SIGTAP."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import polars as pl
import polars.selectors as cs

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.processing import ReconcileResult
from data_processor.sources.sia.contract import (
    PROVENANCE_METADATA_KEY,
    SIA_LAYOUT,
    provenance_metadata,
    read_output,
    resolve_reconcile_inputs,
    serialize_parquet,
    write_verified,
)

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import ReconcileRequest
    from cnes_domain.ports.object_store import ObjectStorePort

logger = logging.getLogger(__name__)

type Frames = dict[str, tuple[pl.DataFrame, dict[str, str]]]

KPIS_METADATA_KEY = "sia_kpis"
FONTES = (("apa.parquet", "SIA_APA"), ("bpi.parquet", "SIA_BPI"), ("bpihst.parquet", "SIA_BPIHST"))
UNKNOWN_PROCEDURE = "procedimento_desconhecido"
EMPTY_SIGTAP = "referencia_sigtap_vazia"
BPI_HISTORY_DUPLICATE = "candidato_duplicado_bpi_bpihst"
DIVERGENCE_TYPES = (BPI_HISTORY_DUPLICATE, EMPTY_SIGTAP, UNKNOWN_PROCEDURE)
_RECONCILIATION_SCHEMA_VERSION = "sia-reconciliation-v1"
_DIVERGENCE_SCHEMA_VERSION = "sia-divergence-v1"
_KEY = ("competencia", "cnes", "cod_procedimento", "fonte")
_RECONCILIATION_COLUMNS = (
    *_KEY, "descricao_procedimento", "linhas", "quantidade", "valor_aprovado_cents",
)
_DIVERGENCE_COLUMNS = (*_KEY, "tipo", "linhas", "referencias")
_REFERENCES = pl.List(pl.String)
_DIVERGENCE_SCHEMA = {
    "competencia": pl.String, "cnes": pl.String, "cod_procedimento": pl.String,
    "fonte": pl.String, "tipo": pl.String, "linhas": pl.Int64, "referencias": _REFERENCES,
}


def reconcile_sia(request: ReconcileRequest, store: ObjectStorePort) -> ReconcileResult:
    """Agrega produção SIA por competência/CNES/procedimento/fonte, sem precedência entre fontes.

    Args:
        request: os dez manifests normalizados SIA_LOCAL e os dois destinos do layout.
        store: porta de objetos genérica.

    Returns:
        ReconcileResult com Parquets de reconciliação e divergências e KPIs inteiros.

    Raises:
        SiaContractError: conjunto normalizado incompleto, hash divergente ou put não verificado.
    """
    frames = {
        leaf: read_output(store, manifest)
        for leaf, manifest in resolve_reconcile_inputs(request).items()
    }
    procedures = _procedures(frames["reference_sigtap.parquet"][0])
    reconciled = _aggregate(_facts(frames), procedures)
    divergences = pl.concat([
        _unknown_procedures(reconciled, procedures.height), _history_duplicates(frames)
    ])
    divergences = divergences.sort(
        [*_KEY, "tipo", pl.col("referencias").list.join(",")], nulls_last=True
    ).select(_DIVERGENCE_COLUMNS)
    kpis = _kpis(frames, reconciled, divergences)
    raw_hashes = _raw_hashes(frames)
    reconciliation_metadata = {
        **provenance_metadata(_RECONCILIATION_SCHEMA_VERSION, raw_hashes),
        KPIS_METADATA_KEY: json.dumps(kpis, sort_keys=True),
    }
    reconciliation_manifest = _persist(
        store, request, (request.reconciliation_key, reconciled), reconciliation_metadata
    )
    divergence_manifest = _persist(
        store, request, (request.divergence_key, divergences),
        provenance_metadata(_DIVERGENCE_SCHEMA_VERSION, raw_hashes),
    )
    logger.info(
        "sia_reconciled rows=%d divergences=%d", reconciled.height, divergences.height
    )
    return ReconcileResult(
        reconciliation_manifest=reconciliation_manifest,
        divergence_manifest=divergence_manifest,
        kpis=kpis,
    )


def _facts(frames: Frames) -> pl.DataFrame:
    parts = []
    for leaf, fonte in FONTES:
        frame = frames[leaf][0]
        value = (
            pl.col("valor_aprovado_cents")
            if "valor_aprovado_cents" in frame.columns
            else pl.lit(None, dtype=pl.Int64)
        )
        parts.append(frame.select(
            "competencia", "cnes", "cod_procedimento",
            pl.lit(fonte).alias("fonte"),
            "quantidade",
            value.alias("valor_aprovado_cents"),
        ))
    return pl.concat(parts)


def _procedures(sigtap: pl.DataFrame) -> pl.DataFrame:
    return sigtap.select(
        "cod_procedimento", pl.col("descricao").alias("descricao_procedimento")
    )


def _aggregate(facts: pl.DataFrame, procedures: pl.DataFrame) -> pl.DataFrame:
    aggregated = facts.group_by(list(_KEY)).agg(
        pl.len().cast(pl.Int64).alias("linhas"),
        pl.col("quantidade").sum(),
        pl.col("valor_aprovado_cents").sum(),
    )
    reported_value = pl.col("fonte") == "SIA_APA"
    return (
        aggregated.with_columns(
            pl.when(reported_value).then(pl.col("valor_aprovado_cents")).alias(
                "valor_aprovado_cents"
            )
        )
        .join(procedures, on="cod_procedimento", how="left")
        .sort(list(_KEY))
        .select(_RECONCILIATION_COLUMNS)
    )


def _unknown_procedures(reconciled: pl.DataFrame, reference_size: int) -> pl.DataFrame:
    unknown = reconciled.filter(pl.col("descricao_procedimento").is_null())
    if reference_size == 0 and unknown.height:
        # An empty SIGTAP slot (absent source) cannot prove any code unknown: report the
        # missing reference once instead of flagging every production row.
        return pl.DataFrame(
            {
                "competencia": [reconciled["competencia"][0]], "cnes": [None],
                "cod_procedimento": [None], "fonte": [None], "tipo": [EMPTY_SIGTAP],
                "linhas": [int(unknown["linhas"].sum())], "referencias": [None],
            },
            schema=_DIVERGENCE_SCHEMA,
        )
    return unknown.select(
        *_KEY,
        pl.lit(UNKNOWN_PROCEDURE).alias("tipo"),
        "linhas",
        pl.lit(None, dtype=_REFERENCES).alias("referencias"),
    )


def _history_duplicates(frames: Frames) -> pl.DataFrame:
    current = frames["bpi.parquet"][0]
    history = frames["bpihst.parquet"][0]
    canonical = [name for name in current.columns if not name.startswith("_")]
    pairs = current.select(*canonical, "_source_row").join(
        history.select(*canonical, "_source_row"),
        on=canonical, how="inner", nulls_equal=True, suffix="_historico",
    )
    return pairs.select(
        "competencia", "cnes", "cod_procedimento",
        pl.lit("SIA_BPI+SIA_BPIHST").alias("fonte"),
        pl.lit(BPI_HISTORY_DUPLICATE).alias("tipo"),
        pl.lit(1, dtype=pl.Int64).alias("linhas"),
        pl.concat_list(
            pl.format("SIA_BPI:{}", "_source_row"),
            pl.format("SIA_BPIHST:{}", "_source_row_historico"),
        ).alias("referencias"),
    )


def _kpis(frames: Frames, reconciled: pl.DataFrame, divergences: pl.DataFrame) -> dict[str, int]:
    kpis: dict[str, int] = {}
    for item in SIA_LAYOUT.normalized:
        data_leaf, quality_leaf = item.normalized_filenames
        slug = item.file_subtype.lower()
        kpis[f"linhas_{slug}"] = frames[data_leaf][0].height
        kpis[f"qualidade_{slug}"] = frames[quality_leaf][0].height
    kpis["datas_invalidas_normalizadas"] = sum(
        int(frames[leaf][0].select(pl.any_horizontal(cs.ends_with("_invalida")).sum()).item())
        for leaf, _ in FONTES
    )
    kpis["quantidade_total"] = int(reconciled["quantidade"].sum())
    kpis["valor_aprovado_cents_total"] = int(reconciled["valor_aprovado_cents"].sum())
    for tipo in DIVERGENCE_TYPES:
        kpis[tipo] = divergences.filter(pl.col("tipo") == tipo).height
    return kpis


def _raw_hashes(frames: Frames) -> set[str]:
    return {
        digest
        for _, metadata in frames.values()
        for digest in json.loads(metadata[PROVENANCE_METADATA_KEY])
    }


def _persist(
    store: ObjectStorePort,
    request: ReconcileRequest,
    target: tuple[str, pl.DataFrame],
    metadata: dict[str, str],
) -> OutputManifest:
    key, frame = target
    stat = write_verified(store, key, serialize_parquet(frame, metadata))
    stem = key.rsplit("/", 1)[-1].removesuffix(".parquet")
    return OutputManifest(
        manifest_version=1,
        manifest_id=f"reconciliation-{request.run_id}-{request.unit_id}-{request.attempt}-{stem}",
        tenant_id=request.tenant_id,
        layer="reconciliation",
        source_type=None,
        competencia=request.competencia,
        run_id=request.run_id,
        unit_id=request.unit_id,
        attempt=request.attempt,
        schema_version=metadata["schema_version"],
        object_key=key,
        object_sha256=stat.sha256,
        row_count=frame.height,
        created_at=request.reconciled_at,
    )
