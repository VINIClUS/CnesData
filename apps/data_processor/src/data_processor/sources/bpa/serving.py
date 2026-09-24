"""Stage function: materializa os documentos serving BPA agregados e sem PII."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.outputs import OutputManifest, ServingDocument
from cnes_contracts.manifests.processing import MaterializeResult
from data_processor.sources.bpa.contract import (
    BPA_DEPENDENCIES,
    BPA_LAYOUT,
    leaf,
    persist,
    read_parquet,
)

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import MaterializeRequest
    from cnes_domain.ports.object_store import ObjectStorePort

type Payload = dict[str, object]

_SCHEMA_VERSION = "bpa-serving-v1"
_DATASET = "bpa"
_MAX_ESTABELECIMENTOS = 500
_TOTALS = ("linhas", "linhas_aceitas", "qtd_apresentada", "qtd_aceita")


def materialize_bpa(request: MaterializeRequest, store: ObjectStorePort) -> MaterializeResult:
    """Materializa `overview` e `by-establishment` a partir da reconciliação BPA.

    Args:
        request: manifests de reconciliação/divergência e as chaves serving do layout.
        store: porta de objetos genérica.

    Returns:
        MaterializeResult com manifests e documentos ordenados por nome.

    Raises:
        ValueError: target_keys fora do layout BPA ou artefato não verificado.
    """
    keys = _target_keys(request)
    reconciled = read_parquet(store, request.reconciliation_manifest.object_key)
    divergences = read_parquet(store, request.divergence_manifest.object_key)
    payloads: dict[str, Payload] = {
        "by-establishment": _by_establishment(request, reconciled, divergences),
        "overview": _overview(request, reconciled, divergences),
    }
    manifests: list[OutputManifest] = []
    documents: list[ServingDocument] = []
    for name in sorted(payloads):
        document = ServingDocument(
            schema_version=_SCHEMA_VERSION, document_name=name, tenant_id=request.tenant_id,
            run_id=request.run_id, generated_at=request.generated_at, payload=payloads[name],
        )
        manifests.append(_write(store, request, keys[name], document))
        documents.append(document)
    return MaterializeResult(manifests=tuple(manifests), documents=tuple(documents))


def _target_keys(request: MaterializeRequest) -> dict[str, str]:
    by_name = {leaf(key).removesuffix(".json"): key for key in request.target_keys}
    if sorted(by_name) != sorted(BPA_LAYOUT.serving_documents):
        raise ValueError(f"bpa_serving_target_keys_invalidos names={','.join(sorted(by_name))}")
    return by_name


def _totals(frame: pl.DataFrame) -> Payload:
    return {name: int(frame[name].sum()) for name in _TOTALS}


def _overview(
    request: MaterializeRequest, reconciled: pl.DataFrame, divergences: pl.DataFrame
) -> Payload:
    kpis = {
        **_totals(reconciled),
        "estabelecimentos": int(reconciled["cnes"].drop_nulls().n_unique()),
        "procedimentos": int(reconciled["sigtap"].drop_nulls().n_unique()),
        "divergencias": divergences.height,
        "registros_com_divergencia": int(divergences["source_record_id"].n_unique()),
    }
    by_subtype = [
        {"file_subtype": dependency.file_subtype,
         **_totals(reconciled.filter(pl.col("file_subtype") == dependency.file_subtype))}
        for dependency in BPA_DEPENDENCIES
    ]
    counts = divergences.group_by("code").len().sort("code")
    return {
        "dataset": _DATASET,
        "competencia": request.competencia,
        "missing_sources": list(request.missing_sources),
        "kpis": kpis,
        "por_subtipo": by_subtype,
        "divergence_counts": {code: int(total) for code, total in counts.iter_rows()},
    }


def _by_establishment(
    request: MaterializeRequest, reconciled: pl.DataFrame, divergences: pl.DataFrame
) -> Payload:
    per_cnes = reconciled.group_by("cnes").agg(
        *[pl.col(name).sum() for name in _TOTALS],
        pl.col("sigtap").drop_nulls().n_unique().alias("procedimentos"),
    )
    issues = divergences.group_by("cnes").agg(pl.len().alias("divergencias"))
    rows = (
        per_cnes.join(issues, on="cnes", how="left", nulls_equal=True)
        .with_columns(pl.col("divergencias").fill_null(0))
        .sort(["qtd_apresentada", "cnes"], descending=[True, False], nulls_last=True)
        .cast(dict.fromkeys((*_TOTALS, "procedimentos", "divergencias"), pl.Int64))
    )
    valid = rows.filter(pl.col("cnes").is_not_null())
    return {
        "dataset": _DATASET,
        "competencia": request.competencia,
        "limite": _MAX_ESTABELECIMENTOS,
        "total_estabelecimentos": valid.height,
        "truncado": valid.height > _MAX_ESTABELECIMENTOS,
        "estabelecimentos": valid.head(_MAX_ESTABELECIMENTOS).to_dicts(),
        "sem_cnes_valido": _without_cnes(rows),
    }


def _without_cnes(rows: pl.DataFrame) -> Payload:
    missing = rows.filter(pl.col("cnes").is_null())
    return {name: int(missing[name].sum()) for name in (*_TOTALS, "divergencias")}


def _render(document: ServingDocument) -> bytes:
    envelope = {
        "schema_version": document.schema_version,
        "tenant_id": document.tenant_id,
        "run_id": document.run_id,
        "generated_at": document.generated_at.isoformat().replace("+00:00", "Z"),
    }
    body = {**envelope, **document.payload}
    return (json.dumps(body, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _write(
    store: ObjectStorePort, request: MaterializeRequest, key: str, document: ServingDocument
) -> OutputManifest:
    stat = persist(store, key, _render(document))
    return OutputManifest(
        manifest_version=1,
        manifest_id=(
            f"serving-{request.run_id}-{request.unit_id}-{request.attempt}"
            f"-{document.document_name}"
        ),
        tenant_id=request.tenant_id,
        layer="serving",
        source_type=None,
        competencia=request.competencia,
        run_id=request.run_id,
        unit_id=request.unit_id,
        attempt=request.attempt,
        schema_version=_SCHEMA_VERSION,
        object_key=key,
        object_sha256=stat.sha256,
        row_count=1,
        created_at=request.generated_at,
    )
