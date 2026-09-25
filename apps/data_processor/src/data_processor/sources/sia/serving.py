"""Stage function: materializa os documentos de serving SIA (overview e by-establishment)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.outputs import OutputManifest, ServingDocument
from cnes_contracts.manifests.processing import MaterializeResult
from data_processor.sources.sia.contract import read_output, resolve_serving_targets, write_verified
from data_processor.sources.sia.reconcile import DIVERGENCE_TYPES, FONTES, KPIS_METADATA_KEY

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import MaterializeRequest
    from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort

_DATASET = "sia"


def materialize_sia(request: MaterializeRequest, store: ObjectStorePort) -> MaterializeResult:
    """Materializa apenas agregados SIA, sem identificadores de paciente ou profissional.

    Args:
        request: manifests de reconciliação/divergência e as chaves dos dois documentos.
        store: porta de objetos genérica.

    Returns:
        MaterializeResult com `by-establishment` e `overview`, em ordem de nome.

    Raises:
        SiaContractError: target_keys fora do layout, hash divergente ou put não verificado.
    """
    targets = resolve_serving_targets(request)
    reconciled, metadata = read_output(store, request.reconciliation_manifest)
    divergences, _ = read_output(store, request.divergence_manifest)
    payloads = {
        "overview": _overview(
            request, reconciled, divergences, json.loads(metadata[KPIS_METADATA_KEY])
        ),
        "by-establishment": _by_establishment(request, reconciled, divergences),
    }
    documents = tuple(
        ServingDocument(
            schema_version=f"{_DATASET}-{name}-v1",
            document_name=name,
            tenant_id=request.tenant_id,
            run_id=request.run_id,
            generated_at=request.generated_at,
            payload=payloads[name],
        )
        for name in sorted(targets)
    )
    manifests = tuple(
        _output_manifest(
            request,
            document,
            write_verified(store, targets[document.document_name], _render(document)),
        )
        for document in documents
    )
    return MaterializeResult(manifests=manifests, documents=documents)


def _totals(frame: pl.DataFrame) -> dict[str, object]:
    totals: dict[str, object] = {}
    for _, fonte in FONTES:
        part = frame.filter(pl.col("fonte") == fonte)
        totals[fonte] = {
            "linhas": int(part["linhas"].sum()),
            "quantidade": int(part["quantidade"].sum()),
            "valor_aprovado_cents": (
                int(part["valor_aprovado_cents"].sum()) if fonte == "SIA_APA" else None
            ),
        }
    return totals


def _divergence_counts(divergences: pl.DataFrame) -> dict[str, object]:
    return {
        tipo: divergences.filter(pl.col("tipo") == tipo).height
        for tipo in DIVERGENCE_TYPES
    }


def _overview(
    request: MaterializeRequest,
    reconciled: pl.DataFrame,
    divergences: pl.DataFrame,
    kpis: dict[str, int],
) -> dict[str, object]:
    return {
        "dataset": _DATASET,
        "competencia": request.competencia,
        "totais_por_fonte": _totals(reconciled),
        "divergencias": _divergence_counts(divergences),
        "kpis": kpis,
        "missing_sources": list(request.missing_sources),
    }


def _by_establishment(
    request: MaterializeRequest, reconciled: pl.DataFrame, divergences: pl.DataFrame
) -> dict[str, object]:
    establishments = [
        {
            "cnes": cnes,
            "totais_por_fonte": _totals(reconciled.filter(pl.col("cnes") == cnes)),
            "divergencias": _divergence_counts(divergences.filter(pl.col("cnes") == cnes)),
        }
        for cnes in sorted(reconciled["cnes"].unique().to_list())
    ]
    return {
        "dataset": _DATASET,
        "competencia": request.competencia,
        "estabelecimentos": establishments,
    }


def _render(document: ServingDocument) -> bytes:
    envelope = {
        "schema_version": document.schema_version,
        "tenant_id": document.tenant_id,
        "run_id": document.run_id,
        "generated_at": document.generated_at.isoformat().replace("+00:00", "Z"),
    }
    body = {**envelope, **document.payload}
    return (json.dumps(body, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _output_manifest(
    request: MaterializeRequest, document: ServingDocument, stat: ObjectStat
) -> OutputManifest:
    return OutputManifest(
        manifest_version=1,
        manifest_id=(
            f"serving-{request.run_id}-{request.unit_id}-{request.attempt}-{document.document_name}"
        ),
        tenant_id=request.tenant_id,
        layer="serving",
        source_type=None,
        competencia=request.competencia,
        run_id=request.run_id,
        unit_id=request.unit_id,
        attempt=request.attempt,
        schema_version=document.schema_version,
        object_key=stat.key,
        object_sha256=stat.sha256,
        row_count=1,
        created_at=request.generated_at,
    )
