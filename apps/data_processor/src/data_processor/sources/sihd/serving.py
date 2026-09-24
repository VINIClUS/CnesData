"""Stage function: materializa o overview SIHD agregado, sem PII."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.outputs import OutputManifest, ServingDocument
from cnes_contracts.manifests.processing import MaterializeResult
from data_processor.pipeline.materialize_cnes import _render
from data_processor.sources.sihd.contract import (
    PII_DENY_LIST,
    SERVING_SCHEMA_VERSION,
    SIHD_DEFINITION,
    SIHD_LAYOUT,
)
from data_processor.sources.sihd.normalize import persist_verified

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import MaterializeRequest
    from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort

type Row = dict[str, object]

_DOCUMENT_NAME = SIHD_LAYOUT.serving_documents[0]
_TARGET_LEAF = f"{_DOCUMENT_NAME}.json"
_DENIED = frozenset(name.lower() for name in PII_DENY_LIST)
_QUALITY_PREFIX = "quality:"


def materialize_sihd(request: MaterializeRequest, store: ObjectStorePort) -> MaterializeResult:
    """Materializa overview.json com totais, periodo e resumo de divergencias.

    Args:
        request: manifests de reconciliacao/divergencia e exatamente um target_key.
        store: porta de objetos generica.

    Returns:
        MaterializeResult com um OutputManifest e o ServingDocument overview.

    Raises:
        ValueError: target_key fora do layout, campo de PII no payload ou saida nao verificada.
    """
    target_key = _target_key(request)
    reconciled = _read_frame(store, request.reconciliation_manifest)
    divergences = _read_frame(store, request.divergence_manifest)
    payload = _payload(request, reconciled.to_dicts(), divergences["kind"].to_list())
    _assert_no_pii(payload)
    document = ServingDocument(
        schema_version=SERVING_SCHEMA_VERSION,
        document_name=_DOCUMENT_NAME,
        tenant_id=request.tenant_id,
        run_id=request.run_id,
        generated_at=request.generated_at,
        payload=payload,
    )
    stat = persist_verified(store, target_key, _render(document))
    return MaterializeResult(
        manifests=(_output_manifest(request, target_key, stat),), documents=(document,)
    )


def _assert_no_pii(value: object) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower() in _DENIED:
                raise ValueError(f"pii_field_in_serving field={key}")
            _assert_no_pii(child)
    elif isinstance(value, list):
        for child in value:
            _assert_no_pii(child)


def _target_key(request: MaterializeRequest) -> str:
    if len(request.target_keys) != 1:
        raise ValueError(f"target_keys_must_be_single count={len(request.target_keys)}")
    key = request.target_keys[0]
    if key.rsplit("/", 1)[-1] != _TARGET_LEAF:
        raise ValueError(f"unexpected_target_key key={key}")
    return key


def _read_frame(store: ObjectStorePort, manifest: OutputManifest) -> pl.DataFrame:
    with store.open(manifest.object_key) as handle:
        return pl.read_parquet(handle)


def _payload(request: MaterializeRequest, rows: list[Row], kinds: list[str]) -> dict[str, object]:
    divergence_counts = _count(kinds)
    quality = sum(n for kind, n in divergence_counts.items() if kind.startswith(_QUALITY_PREFIX))
    totals = _bucket(rows)
    return {
        "dataset": SIHD_DEFINITION.pipeline_id,
        "competencia": request.competencia,
        "totais": {
            "aih_com_procedimento": totals["aih_count"],
            "procedimento_qtd": totals["procedimento_qtd"],
            "valor_total_centavos": totals["valor_centavos"],
            "internacao_sem_procedimento": divergence_counts.get("internacao_sem_proc", 0),
            "linhas_qualidade": quality,
        },
        "periodo": _period(rows),
        "por_cnes": _grouped(rows, "CNES", "cnes"),
        "por_procedimento": _grouped(rows, "PROCEDIMENTO", "procedimento"),
        "divergencias": divergence_counts,
        "missing_sources": list(request.missing_sources),
    }


def _count(kinds: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for kind in kinds:
        counts[kind] = counts.get(kind, 0) + 1
    return dict(sorted(counts.items()))


def _bucket(rows: list[Row]) -> dict[str, int]:
    aihs = {aih for row in rows for aih in row["NUM_AIHS"]}
    return {
        "aih_count": len(aihs),
        "procedimento_qtd": sum(int(row["procedimento_qtd"]) for row in rows),
        "valor_centavos": sum(int(row["valor_centavos"]) for row in rows),
    }


def _grouped(rows: list[Row], column: str, label: str) -> list[dict[str, object]]:
    groups: dict[object, list[Row]] = {}
    for row in rows:
        groups.setdefault(row[column], []).append(row)
    ordered = sorted(groups.items(), key=lambda item: (item[0] is None, str(item[0])))
    return [{label: key, **_bucket(members)} for key, members in ordered]


def _period(rows: list[Row]) -> dict[str, str | None]:
    starts = [row["dt_internacao_min"] for row in rows if row["dt_internacao_min"] is not None]
    ends = [row["dt_saida_max"] for row in rows if row["dt_saida_max"] is not None]
    return {
        "dt_internacao_min": min(starts).isoformat() if starts else None,
        "dt_saida_max": max(ends).isoformat() if ends else None,
    }


def _output_manifest(
    request: MaterializeRequest, target_key: str, stat: ObjectStat
) -> OutputManifest:
    return OutputManifest(
        manifest_version=1,
        manifest_id=f"serving-{request.run_id}-{request.unit_id}-{request.attempt}",
        tenant_id=request.tenant_id,
        layer="serving",
        source_type=None,
        competencia=request.competencia,
        run_id=request.run_id,
        unit_id=request.unit_id,
        attempt=request.attempt,
        schema_version=SERVING_SCHEMA_VERSION,
        object_key=target_key,
        object_sha256=stat.sha256,
        row_count=1,
        created_at=request.generated_at,
    )
