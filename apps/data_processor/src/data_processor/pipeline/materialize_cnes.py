"""Stage function: materializa o documento de serving CNES agregado."""

from __future__ import annotations

import json
from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.outputs import OutputManifest, ServingDocument
from cnes_contracts.manifests.processing import MaterializeResult

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import MaterializeRequest
    from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort

_SCHEMA_VERSION = "cnes-serving-v1"
_DOCUMENT_NAME = "overview"
_TARGET_LEAF = f"{_DOCUMENT_NAME}.json"
_MISSING_NATIONAL = "CNES_NACIONAL"


def materialize_cnes(request: MaterializeRequest, store: ObjectStorePort) -> MaterializeResult:
    """Materializa o documento de serving agregado da competencia reconciliada.

    Args:
        request: manifestos reconciliation/divergence e exatamente um target_key.
        store: porta de objetos generica (nao passa por AttemptObjectStore).

    Returns:
        MaterializeResult com um unico OutputManifest e ServingDocument.

    Raises:
        ValueError: target_key fora do shape esperado, ids de origem ambiguos,
            ou objeto nao encontrado apos put.
    """
    target_key = _target_key(request)
    reconciled = _read_frame(store, request.reconciliation_manifest)
    divergences = _read_frame(store, request.divergence_manifest)
    payload = _payload(request, reconciled, divergences)
    document = ServingDocument(
        schema_version=_SCHEMA_VERSION,
        document_name=_DOCUMENT_NAME,
        tenant_id=request.tenant_id,
        run_id=request.run_id,
        generated_at=request.generated_at,
        payload=payload,
    )
    stat = _persist(store, target_key, _render(document))
    manifest = _output_manifest(request, target_key, stat)
    return MaterializeResult(manifests=(manifest,), documents=(document,))


def _target_key(request: MaterializeRequest) -> str:
    if len(request.target_keys) != 1:
        raise ValueError("target_keys_must_be_single")
    key = request.target_keys[0]
    if key.rsplit("/", 1)[-1] != _TARGET_LEAF:
        raise ValueError("unexpected_target_key")
    return key


def _read_frame(store: ObjectStorePort, manifest: OutputManifest) -> pl.DataFrame:
    with store.open(manifest.object_key) as handle:
        return pl.read_parquet(handle)


def _payload(
    request: MaterializeRequest, reconciled: pl.DataFrame, divergences: pl.DataFrame
) -> dict[str, object]:
    return {
        "competencia": request.competencia,
        "kpis": _kpis(reconciled, divergences, request.missing_sources),
        "divergence_counts": _divergence_counts(divergences),
        "missing_sources": list(request.missing_sources),
    }


def _kpis(
    reconciled: pl.DataFrame, divergences: pl.DataFrame, missing_sources: tuple[str, ...]
) -> dict[str, int]:
    ids = reconciled["_source_manifest_ids"].to_list()
    matches = [item for item in ids if len(item) == 2]
    singles = [item[0] for item in ids if len(item) == 1]
    local_only = _local_only_count(singles, matches, missing_sources)
    identities = {a or b for a, b in reconciled.select("CNS", "CPF").iter_rows()}
    raw = {
        "match_count": len(matches),
        "local_only_count": local_only,
        "national_only_count": len(singles) - local_only,
        "conflict_count": divergences["natural_key"].n_unique(),
        "reconciled_row_count": reconciled.height,
        "active_professional_count": len(identities),
    }
    return {key: int(value) for key, value in raw.items()}


def _local_only_count(
    singles: list[str], matches: list[list[str]], missing_sources: tuple[str, ...]
) -> int:
    if _MISSING_NATIONAL in missing_sources:
        return len(singles)
    if not matches:
        raise ValueError("source_manifest_ids_ambiguous")
    local_id = matches[0][0]
    return sum(1 for item in singles if item == local_id)


def _divergence_counts(divergences: pl.DataFrame) -> dict[str, int]:
    counts: dict[str, int] = {}
    for name in divergences["field"].to_list():
        counts[name] = counts.get(name, 0) + 1
    return counts


def _render(document: ServingDocument) -> bytes:
    envelope = {
        "schema_version": document.schema_version,
        "tenant_id": document.tenant_id,
        "run_id": document.run_id,
        "generated_at": document.generated_at.isoformat().replace("+00:00", "Z"),
    }
    body = {**envelope, **document.payload}
    return (json.dumps(body, indent=2, ensure_ascii=False) + "\n").encode("utf-8")


def _persist(store: ObjectStorePort, target_key: str, payload: bytes) -> ObjectStat:
    digest = sha256(payload).hexdigest()
    store.put(target_key, BytesIO(payload), digest)
    stat = store.stat(target_key)
    if stat is None:
        raise ValueError(f"output_not_found key={target_key}")
    return stat


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
        schema_version=_SCHEMA_VERSION,
        object_key=target_key,
        object_sha256=stat.sha256,
        row_count=1,
        created_at=request.generated_at,
    )
