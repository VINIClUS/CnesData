"""Stage function: reconcilia CNES_LOCAL e CNES_NACIONAL num Parquet Gold unico."""

from __future__ import annotations

from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.processing import ReconcileResult
from cnes_contracts.manifests.raw import SourceType

if TYPE_CHECKING:
    from cnes_contracts.manifests.processing import ReconcileRequest
    from cnes_domain.ports.object_store import ObjectStat, ObjectStorePort

type Row = dict[str, object]
type NaturalKey = tuple[str, str, str, str]
type ManifestIds = tuple[str, str | None]

_SOURCE_COLUMNS = (
    "CPF", "CNS", "NOME_PROFISSIONAL", "NOME_SOCIAL", "SEXO", "CBO", "CNES",
    "TIPO_VINCULO", "SUS", "CH_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS",
    "CH_HOSPITALAR", "FONTE",
)
_MERGED_COLUMNS = _SOURCE_COLUMNS[:-1]
_RECONCILED_FIELDS = tuple(
    item for item in _SOURCE_COLUMNS if item not in {"FONTE", "CNS", "CNES", "CBO"}
)
_SORT_FIELDS = ("CPF", "CNS", "CNES", "CBO")
_GOLD_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "CPF": pl.String, "CNS": pl.String, "NOME_PROFISSIONAL": pl.String,
    "NOME_SOCIAL": pl.String, "SEXO": pl.String, "CBO": pl.String, "CNES": pl.String,
    "TIPO_VINCULO": pl.String, "SUS": pl.String, "CH_TOTAL": pl.Int64,
    "CH_AMBULATORIAL": pl.Int64, "CH_OUTRAS": pl.Int64, "CH_HOSPITALAR": pl.Int64,
    "COMPETENCIA": pl.String, "_source_manifest_ids": pl.List(pl.String),
}
_DIVERGENCE_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "natural_key": pl.Struct(
        {"identity": pl.String, "CNES": pl.String, "CBO": pl.String, "COMPETENCIA": pl.String}
    ),
    "field": pl.String,
    "local_value": pl.String,
    "national_value": pl.String,
    "selected_value": pl.String,
    "selected_source": pl.String,
    "source_manifest_ids": pl.List(pl.String),
}
_RECONCILIATION_SCHEMA_VERSION = "cnes-reconciliation-v1"
_DIVERGENCE_SCHEMA_VERSION = "cnes-divergence-v1"
_SELECTED_SOURCE = "LOCAL"


def reconcile_cnes(request: ReconcileRequest, store: ObjectStorePort) -> ReconcileResult:
    """Reconcilia CNES_LOCAL e CNES_NACIONAL de uma competencia num Parquet Gold unico.

    Args:
        request: exatamente um normalized_manifest CNES_LOCAL e zero ou um CNES_NACIONAL.
        store: porta de objetos generica (nao passa por AttemptObjectStore).

    Returns:
        ReconcileResult com os dois OutputManifest e os 6 KPIs inteiros.

    Raises:
        ValueError: manifesto normalizado fora do shape esperado, linha sem identidade,
            ou objeto nao encontrado apos put.
    """
    local_manifest, national_manifest = _split_manifests(request.normalized_manifests)
    local = _rows_by_key(store, local_manifest, request.competencia)
    national = (
        _rows_by_key(store, national_manifest, request.competencia)
        if national_manifest is not None
        else {}
    )
    manifest_ids = _manifest_ids(local_manifest, national_manifest)
    rows = _reconciled_rows(local, national, request.competencia, manifest_ids)
    divergences = _divergence_rows(local, national, manifest_ids)
    kpis = _kpis(local, national, rows, divergences)
    reconciliation_manifest = _persist_reconciliation(store, request, rows)
    divergence_manifest = _persist_divergences(store, request, divergences)
    return ReconcileResult(
        reconciliation_manifest=reconciliation_manifest,
        divergence_manifest=divergence_manifest,
        kpis=kpis,
    )


def _split_manifests(
    manifests: tuple[OutputManifest, ...],
) -> tuple[OutputManifest, OutputManifest | None]:
    local: OutputManifest | None = None
    national: OutputManifest | None = None
    for manifest in manifests:
        if manifest.source_type is SourceType.CNES_LOCAL and local is None:
            local = manifest
        elif manifest.source_type is SourceType.CNES_NACIONAL and national is None:
            national = manifest
        else:
            raise ValueError("unexpected_source_type")
    if local is None:
        raise ValueError("unexpected_source_type")
    return local, national


def _manifest_ids(
    local_manifest: OutputManifest, national_manifest: OutputManifest | None
) -> ManifestIds:
    national_id = national_manifest.manifest_id if national_manifest is not None else None
    return local_manifest.manifest_id, national_id


def _read_frame(store: ObjectStorePort, manifest: OutputManifest) -> pl.DataFrame:
    with store.open(manifest.object_key) as handle:
        return pl.read_parquet(handle)


def _rows_by_key(
    store: ObjectStorePort, manifest: OutputManifest, competencia: str
) -> dict[NaturalKey, Row]:
    frame = _read_frame(store, manifest).select(list(_SOURCE_COLUMNS))
    return {_natural_key(row, competencia): row for row in frame.iter_rows(named=True)}


def _natural_key(row: Row, competencia: str) -> NaturalKey:
    identity = row["CNS"] or row["CPF"]
    if not isinstance(identity, str):
        raise ValueError("identity_missing")
    return identity, str(row["CNES"]), str(row["CBO"]), competencia


def _sort_key(row: Row) -> tuple[str, ...]:
    return tuple(str(row[item] or "") for item in _SORT_FIELDS)


def _merge_row(
    local: Row | None, national: Row | None, competencia: str, manifest_ids: ManifestIds
) -> Row:
    row: Row = {}
    for column in _MERGED_COLUMNS:
        local_value = local[column] if local is not None else None
        national_value = national[column] if national is not None else None
        row[column] = local_value if local_value is not None else national_value
    row["COMPETENCIA"] = competencia
    row["_source_manifest_ids"] = _present_manifest_ids(local, national, manifest_ids)
    return row


def _present_manifest_ids(
    local: Row | None, national: Row | None, manifest_ids: ManifestIds
) -> list[str]:
    local_id, national_id = manifest_ids
    candidates = ((local is not None, local_id), (national is not None, national_id))
    return [manifest_id for present, manifest_id in candidates if present]


def _reconciled_rows(
    local: dict[NaturalKey, Row],
    national: dict[NaturalKey, Row],
    competencia: str,
    manifest_ids: ManifestIds,
) -> list[Row]:
    rows = [
        _merge_row(local.get(key), national.get(key), competencia, manifest_ids)
        for key in local.keys() | national.keys()
    ]
    return sorted(rows, key=_sort_key)


def _natural_key_payload(key: NaturalKey) -> dict[str, str]:
    identity, cnes, cbo, competencia = key
    return {"identity": identity, "CNES": cnes, "CBO": cbo, "COMPETENCIA": competencia}


def _divergence_rows(
    local: dict[NaturalKey, Row], national: dict[NaturalKey, Row], manifest_ids: ManifestIds
) -> list[dict[str, object]]:
    divergences: list[dict[str, object]] = []
    for key in sorted(local.keys() & national.keys()):
        divergences.extend(_field_divergences(key, local[key], national[key], manifest_ids))
    return divergences


def _field_divergences(
    key: NaturalKey, local_row: Row, national_row: Row, manifest_ids: ManifestIds
) -> list[dict[str, object]]:
    local_id, national_id = manifest_ids
    divergences: list[dict[str, object]] = []
    for field in _RECONCILED_FIELDS:
        local_value = local_row[field]
        national_value = national_row[field]
        if local_value is None or national_value is None or local_value == national_value:
            continue
        divergences.append({
            "natural_key": _natural_key_payload(key),
            "field": field,
            "local_value": local_value,
            "national_value": national_value,
            "selected_value": local_value,
            "selected_source": _SELECTED_SOURCE,
            "source_manifest_ids": [local_id, national_id],
        })
    return divergences


def _divergence_key(item: dict[str, object]) -> NaturalKey:
    payload = item["natural_key"]
    return payload["identity"], payload["CNES"], payload["CBO"], payload["COMPETENCIA"]


def _kpis(
    local: dict[NaturalKey, Row],
    national: dict[NaturalKey, Row],
    rows: list[Row],
    divergences: list[dict[str, object]],
) -> dict[str, int]:
    conflict_keys = {_divergence_key(item) for item in divergences}
    identities = {row["CNS"] or row["CPF"] for row in rows}
    return {
        "match_count": len(local.keys() & national.keys()),
        "local_only_count": len(local.keys() - national.keys()),
        "national_only_count": len(national.keys() - local.keys()),
        "conflict_count": len(conflict_keys),
        "reconciled_row_count": len(rows),
        "active_professional_count": len(identities),
    }


def _serialize(frame: pl.DataFrame) -> bytes:
    output = BytesIO()
    frame.write_parquet(
        output, compression="zstd", compression_level=3, statistics=True, row_group_size=64_000,
    )
    return output.getvalue()


def _persist(store: ObjectStorePort, target_key: str, frame: pl.DataFrame) -> ObjectStat:
    payload = _serialize(frame)
    digest = sha256(payload).hexdigest()
    store.put(target_key, BytesIO(payload), digest)
    stat = store.stat(target_key)
    if stat is None:
        raise ValueError(f"output_not_found key={target_key}")
    return stat


def _output_manifest(
    request: ReconcileRequest, spec: tuple[str, str, str], stat: ObjectStat, row_count: int
) -> OutputManifest:
    target_key, manifest_prefix, schema_version = spec
    return OutputManifest(
        manifest_version=1,
        manifest_id=f"{manifest_prefix}-{request.run_id}-{request.unit_id}-{request.attempt}",
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


def _persist_reconciliation(
    store: ObjectStorePort, request: ReconcileRequest, rows: list[Row]
) -> OutputManifest:
    frame = pl.DataFrame(rows, schema=_GOLD_SCHEMA)
    stat = _persist(store, request.reconciliation_key, frame)
    spec = (request.reconciliation_key, "reconciliation", _RECONCILIATION_SCHEMA_VERSION)
    return _output_manifest(request, spec, stat, frame.height)


def _persist_divergences(
    store: ObjectStorePort, request: ReconcileRequest, divergences: list[dict[str, object]]
) -> OutputManifest:
    frame = _divergence_frame(divergences)
    stat = _persist(store, request.divergence_key, frame)
    spec = (request.divergence_key, "divergence", _DIVERGENCE_SCHEMA_VERSION)
    return _output_manifest(request, spec, stat, frame.height)


def _divergence_frame(divergences: list[dict[str, object]]) -> pl.DataFrame:
    rows = [
        {
            "natural_key": item["natural_key"],
            "field": item["field"],
            "local_value": str(item["local_value"]),
            "national_value": str(item["national_value"]),
            "selected_value": str(item["selected_value"]),
            "selected_source": item["selected_source"],
            "source_manifest_ids": item["source_manifest_ids"],
        }
        for item in divergences
    ]
    return pl.DataFrame(rows, schema=_DIVERGENCE_SCHEMA)
