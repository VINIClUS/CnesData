"""Compara o vertical slice CNES contra a fixture congelada CND-002, campo a campo.

CLI em `scripts/compare_cnes_data_plane_cli.py`; este módulo é a biblioteca.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from typing import TYPE_CHECKING, Any

import polars as pl

from cnes_contracts.manifests.processing import (
    MaterializeRequest,
    NormalizeRequest,
    ReconcileRequest,
)
from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType
from data_processor.pipeline.materialize_cnes import materialize_cnes
from data_processor.pipeline.normalize_cnes_local import normalize_cnes_local
from data_processor.pipeline.normalize_cnes_nacional import normalize_cnes_nacional
from data_processor.pipeline.reconcile_cnes import reconcile_cnes
from scripts.verify_data_plane_fixtures import (
    GOLD_COLUMNS,
    GOLD_FILE,
    LOCAL_MANIFEST_ID,
    MANIFEST_FILE,
    NATIONAL_MANIFEST_ID,
    SERVING_FILE,
)

if TYPE_CHECKING:
    from pathlib import Path

    from cnes_contracts.manifests.outputs import OutputManifest
    from cnes_contracts.manifests.processing import NormalizeResult, ReconcileResult
    from cnes_domain.ports.object_store import ObjectStorePort

LOGGER = logging.getLogger(__name__)

DIVERGENCE_FILE = "cnes-divergence-v1.json"
KPIS_FILE = "kpis.json"
NORMALIZED_IDS_FILE = "normalized-manifest-ids.json"
_DEFAULT_VOLATILE_FIELDS = ("run_id", "generated_at")
_DIVERGENCE_VALUE_FIELDS = ("local_value", "national_value", "selected_value")
_DIVERGENCE_FIELDS = (
    "local_value", "national_value", "selected_value", "selected_source", "source_manifest_ids",
)
_NORMALIZED_ID_PATTERN = re.compile(r"^normalized-.+$")
_SLICE_TIMESTAMP = datetime(2026, 1, 31, 23, 59, 59, tzinfo=UTC)

RULE_MANIFEST_IDS_NORMALIZED_LAYER = "manifest_ids_are_normalized_layer"
RULE_DIVERGENCE_VALUES_ARE_STRINGS = "divergence_values_are_strings"

# CND-054 scope amendment (#177): as duas regras abaixo cobrem diferenças que existem
# porque `pipeline/**` (CND-050..053) é forbidden path desta issue — não podem ser
# corrigidas aqui. Cada uma é restrita ao(s) campo(s) nomeado(s), com checagem exata.
APPROVED_RULES: frozenset[str] = frozenset({
    RULE_MANIFEST_IDS_NORMALIZED_LAYER,
    RULE_DIVERGENCE_VALUES_ARE_STRINGS,
})


@dataclass(frozen=True, slots=True)
class ComparisonDifference:
    layer: str
    key: str
    field: str
    expected: object
    actual: object
    rule: str = ""


class UnexplainedDifference(Exception):
    """Uma diferença sem approved rule correspondente."""


def require_explained(
    differences: tuple[ComparisonDifference, ...], approved_rules: frozenset[str]
) -> None:
    """Levanta na primeira diferença cujo `rule` não está em `approved_rules`.

    Args: differences: saída de `compare_outputs`. approved_rules: ver `APPROVED_RULES`.
    Raises: UnexplainedDifference: diferença sem regra aprovada correspondente.
    """
    for difference in differences:
        if difference.rule not in approved_rules:
            raise UnexplainedDifference(
                f"layer={difference.layer} key={difference.key} field={difference.field}"
            )


def volatile_normalized(payload: dict[str, object], volatile_fields: tuple[str, ...]) -> dict:
    """Substitui campos voláteis por um sentinela fixo, preservando a ordem das chaves."""
    return {
        key: ("<volatile>" if key in volatile_fields else value) for key, value in payload.items()
    }


def _read_manifest(root: Path) -> dict[str, Any]:
    manifest_path = root / MANIFEST_FILE
    if not manifest_path.is_file():
        return {"volatile_fields": list(_DEFAULT_VOLATILE_FIELDS)}
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _gold_key(row: dict[str, Any]) -> tuple[object, ...]:
    return (row["CNS"] or row["CPF"], row["CNES"], row["CBO"], row["COMPETENCIA"])


def _divergence_key(item: dict[str, Any]) -> tuple[object, ...]:
    natural_key = item["natural_key"]
    return (
        natural_key["identity"], natural_key["CNES"], natural_key["CBO"],
        natural_key["COMPETENCIA"], item["field"],
    )


@dataclass(frozen=True, slots=True)
class _RuleContext:
    layer: str
    run_ids: tuple[str, str | None] | None


def _manifest_ids_rule(
    expected: object, actual: object, run_ids: tuple[str, str | None] | None
) -> str:
    if not isinstance(expected, list) or not isinstance(actual, list):
        return ""
    if len(expected) != len(actual):
        return ""
    if run_ids is None:
        shaped = all(
            isinstance(item, str) and _NORMALIZED_ID_PATTERN.fullmatch(item) for item in actual
        )
        return RULE_MANIFEST_IDS_NORMALIZED_LAYER if shaped else ""
    substitution = {LOCAL_MANIFEST_ID: run_ids[0], NATIONAL_MANIFEST_ID: run_ids[1]}
    expected_actual = [substitution.get(item) for item in expected]
    return RULE_MANIFEST_IDS_NORMALIZED_LAYER if expected_actual == actual else ""


def _divergence_value_rule(expected: object, actual: object) -> str:
    if isinstance(actual, str) and str(expected) == actual:
        return RULE_DIVERGENCE_VALUES_ARE_STRINGS
    return ""


def _field_rule(context: _RuleContext, field: str, expected: object, actual: object) -> str:
    is_gold = context.layer == "gold"
    manifest_id_field = "_source_manifest_ids" if is_gold else "source_manifest_ids"
    if field == manifest_id_field:
        return _manifest_ids_rule(expected, actual, context.run_ids)
    if not is_gold and field in _DIVERGENCE_VALUE_FIELDS:
        return _divergence_value_rule(expected, actual)
    return ""


def _row_diffs(
    context: _RuleContext,
    rows: tuple[dict[tuple, dict], dict[tuple, dict]],
    fields: tuple[str, ...],
) -> list[ComparisonDifference]:
    layer = context.layer
    expected_rows, actual_rows = rows
    differences: list[ComparisonDifference] = []
    for key in sorted(expected_rows.keys() | actual_rows.keys(), key=str):
        expected_row, actual_row = expected_rows.get(key), actual_rows.get(key)
        if expected_row is None or actual_row is None:
            differences.append(
                ComparisonDifference(layer, str(key), "__row__", expected_row, actual_row)
            )
            continue
        for field in fields:
            expected_value, actual_value = expected_row.get(field), actual_row.get(field)
            if expected_value != actual_value:
                rule = _field_rule(context, field, expected_value, actual_value)
                differences.append(
                    ComparisonDifference(layer, str(key), field, expected_value, actual_value, rule)
                )
    return differences


def _compare_gold(
    expected_path: Path, actual_path: Path, run_ids: tuple[str, str | None] | None
) -> list[ComparisonDifference]:
    expected_frame = pl.read_parquet(expected_path)
    actual_frame = pl.read_parquet(actual_path)
    same_columns = list(expected_frame.columns) == list(actual_frame.columns)
    if not same_columns or expected_frame.schema != actual_frame.schema:
        return [
            ComparisonDifference(
                "gold", "", "__schema__", str(expected_frame.schema), str(actual_frame.schema)
            )
        ]
    expected_rows = {_gold_key(row): row for row in expected_frame.to_dicts()}
    actual_rows = {_gold_key(row): row for row in actual_frame.to_dicts()}
    context = _RuleContext("gold", run_ids)
    return _row_diffs(context, (expected_rows, actual_rows), GOLD_COLUMNS)


def _compare_divergences(
    expected_path: Path, actual_path: Path, run_ids: tuple[str, str | None] | None
) -> list[ComparisonDifference]:
    expected_items = json.loads(expected_path.read_text(encoding="utf-8"))
    actual_items = json.loads(actual_path.read_text(encoding="utf-8"))
    expected_rows = {_divergence_key(item): item for item in expected_items}
    actual_rows = {_divergence_key(item): item for item in actual_items}
    context = _RuleContext("divergences", run_ids)
    return _row_diffs(context, (expected_rows, actual_rows), _DIVERGENCE_FIELDS)


def _read_run_ids(actual: Path) -> tuple[str, str | None] | None:
    path = actual / NORMALIZED_IDS_FILE
    if not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload["local"], payload["national"]


def _compare_serving(
    expected_path: Path, actual_path: Path, volatile_fields: tuple[str, ...]
) -> list[ComparisonDifference]:
    expected_doc = json.loads(expected_path.read_text(encoding="utf-8"))
    actual_doc = json.loads(actual_path.read_text(encoding="utf-8"))
    if list(expected_doc) != list(actual_doc):
        return [
            ComparisonDifference("serving", "", "__order__", list(expected_doc), list(actual_doc))
        ]
    expected_norm = volatile_normalized(expected_doc, volatile_fields)
    actual_norm = volatile_normalized(actual_doc, volatile_fields)
    return [
        ComparisonDifference("serving", "", field, expected_norm[field], actual_norm.get(field))
        for field in expected_norm
        if expected_norm[field] != actual_norm.get(field)
    ]


def _compare_kpis(expected_path: Path, actual_path: Path) -> list[ComparisonDifference]:
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    actual = json.loads(actual_path.read_text(encoding="utf-8"))
    return [
        ComparisonDifference("kpis", "", field, expected.get(field), actual.get(field))
        for field in sorted(set(expected) | set(actual))
        if expected.get(field) != actual.get(field)
    ]


def compare_outputs(expected: Path, actual: Path) -> tuple[ComparisonDifference, ...]:
    """Compara, campo a campo e sem tolerância, o output do vertical slice CNES.

    Args: expected/actual: diretórios com `cnes-gold-v2.parquet`, `cnes-divergence-v1.json`,
        `cnes-serving-v1.json`, `kpis.json` e (opcional, em `expected`) `fixture-manifest.json`.
    Returns: tupla de `ComparisonDifference`; vazia quando não há nenhuma diferença.
    """
    manifest = _read_manifest(expected)
    volatile_fields = tuple(manifest.get("volatile_fields", _DEFAULT_VOLATILE_FIELDS))
    run_ids = _read_run_ids(actual)
    differences: list[ComparisonDifference] = []
    differences.extend(_compare_gold(expected / GOLD_FILE, actual / GOLD_FILE, run_ids))
    divergence_paths = (expected / DIVERGENCE_FILE, actual / DIVERGENCE_FILE)
    differences.extend(_compare_divergences(*divergence_paths, run_ids))
    serving_paths = (expected / SERVING_FILE, actual / SERVING_FILE)
    differences.extend(_compare_serving(*serving_paths, volatile_fields))
    differences.extend(_compare_kpis(expected / KPIS_FILE, actual / KPIS_FILE))
    return tuple(differences)


# --- Driver: roda normalize+reconcile+materialize sobre o dataset da fixture CND-002 ---


@dataclass(frozen=True, slots=True)
class _RawSource:
    filename: str
    manifest_id: str
    source_type: SourceType


@dataclass(frozen=True, slots=True)
class _SliceContext:
    root: Path
    manifest: dict[str, Any]
    run_id: str

    @property
    def tenant_id(self) -> str:
        return self.manifest["tenant_id"]

    @property
    def competencia(self) -> str:
        return self.manifest["competencia"]


@dataclass(frozen=True, slots=True)
class SliceArtifacts:
    reconciliation_manifest: OutputManifest
    divergence_manifest: OutputManifest
    serving_manifest: OutputManifest
    kpis: dict[str, int]
    local_manifest_id: str
    national_manifest_id: str | None


def _load_raw_manifest(
    source: _RawSource, context: _SliceContext, store: ObjectStorePort
) -> RawManifest:
    body = (context.root / source.filename).read_bytes()
    digest = sha256(body).hexdigest()
    key = (
        f"raw/{context.tenant_id}/{source.source_type.value}/{context.competencia}/"
        f"{source.manifest_id}/data.parquet"
    )
    store.put(key, BytesIO(body), digest)
    file_meta = context.manifest["files"][source.filename]
    return RawManifest(
        manifest_version=1, manifest_id=source.manifest_id, tenant_id=context.tenant_id,
        source_type=source.source_type, file_subtype="CNES_VINCULO",
        competencia=context.competencia,
        agent_id="compare-cnes-data-plane", agent_version="1.0.0",
        schema_version=file_meta["schema_version"], snapshot_mode=SnapshotMode.FULL,
        snapshot_id=source.manifest_id, base_snapshot_id=None, sequence=1,
        previous_manifest_sha256=None, object_sha256=digest,
        row_count=file_meta["row_count"], size_bytes=len(body), object_key=key,
        created_at=_SLICE_TIMESTAMP,
    )


def _normalize_request(
    raw_manifest: RawManifest, context: _SliceContext, unit_id: str
) -> NormalizeRequest:
    target = (
        f"normalized/{context.tenant_id}/{raw_manifest.source_type.value}/"
        f"{context.competencia}/{context.run_id}/data.parquet"
    )
    return NormalizeRequest(
        tenant_id=context.tenant_id, run_id=context.run_id, unit_id=unit_id, attempt=1,
        source_type=raw_manifest.source_type, raw_manifests=(raw_manifest,),
        target_keys=(target,), normalized_at=_SLICE_TIMESTAMP,
    )


def _reconcile_request(
    local: NormalizeResult, national: NormalizeResult, context: _SliceContext
) -> ReconcileRequest:
    base = f"reconciliation/{context.tenant_id}/{context.competencia}/{context.run_id}"
    return ReconcileRequest(
        tenant_id=context.tenant_id, competencia=context.competencia, run_id=context.run_id,
        unit_id="unit-reconcile", attempt=1,
        normalized_manifests=(local.manifests[0], national.manifests[0]),
        reconciliation_key=f"{base}/gold.parquet", divergence_key=f"{base}/divergences.parquet",
        reconciled_at=_SLICE_TIMESTAMP,
    )


def _materialize_request(reconciled: ReconcileResult, context: _SliceContext) -> MaterializeRequest:
    return MaterializeRequest(
        tenant_id=context.tenant_id, competencia=context.competencia, run_id=context.run_id,
        unit_id="unit-materialize", attempt=1,
        reconciliation_manifest=reconciled.reconciliation_manifest,
        divergence_manifest=reconciled.divergence_manifest, missing_sources=(),
        target_keys=(f"serving/{context.tenant_id}/{context.run_id}/overview.json",),
        generated_at=_SLICE_TIMESTAMP,
    )


def run_vertical_slice(fixture_root: Path, store: ObjectStorePort, run_id: str) -> SliceArtifacts:
    """Roda normalize(local+nacional) -> reconcile -> materialize sobre a fixture CND-002.

    Args: fixture_root: diretório `docs/fixtures/data-plane` (ou cópia). store: porta de
        objetos alvo. run_id: identificador de run — não afeta nenhum valor comparado.
    Returns: SliceArtifacts com os manifests finais e os KPIs.
    """
    context = _SliceContext(fixture_root, _read_manifest(fixture_root), run_id)
    local_source = _RawSource(
        "cnes-local-v1.parquet", "fixture-cnes-local-v1", SourceType.CNES_LOCAL
    )
    national_source = _RawSource(
        "cnes-nacional-v1.parquet", "fixture-cnes-nacional-v1", SourceType.CNES_NACIONAL
    )
    local_raw = _load_raw_manifest(local_source, context, store)
    national_raw = _load_raw_manifest(national_source, context, store)
    local = normalize_cnes_local(_normalize_request(local_raw, context, "unit-local"), store)
    national_request = _normalize_request(national_raw, context, "unit-nacional")
    national = normalize_cnes_nacional(national_request, store)
    reconciled = reconcile_cnes(_reconcile_request(local, national, context), store)
    served = materialize_cnes(_materialize_request(reconciled, context), store)
    return SliceArtifacts(
        reconciliation_manifest=reconciled.reconciliation_manifest,
        divergence_manifest=reconciled.divergence_manifest,
        serving_manifest=served.manifests[0],
        kpis=reconciled.kpis,
        local_manifest_id=local.manifests[0].manifest_id,
        national_manifest_id=national.manifests[0].manifest_id,
    )


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def materialize_expected_directory(fixture_root: Path, output_dir: Path) -> None:
    """Copia o golden congelado para `output_dir` no layout esperado por `compare_outputs`."""
    manifest = _read_manifest(fixture_root)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / GOLD_FILE).write_bytes((fixture_root / GOLD_FILE).read_bytes())
    (output_dir / SERVING_FILE).write_bytes((fixture_root / SERVING_FILE).read_bytes())
    (output_dir / MANIFEST_FILE).write_bytes((fixture_root / MANIFEST_FILE).read_bytes())
    _write_json(output_dir / DIVERGENCE_FILE, manifest["divergences"])
    _write_json(output_dir / KPIS_FILE, manifest["kpis"])


def _copy_object(store: ObjectStorePort, key: str, destination: Path) -> None:
    with store.open(key) as handle:
        destination.write_bytes(handle.read())


def _read_object_frame(store: ObjectStorePort, key: str) -> pl.DataFrame:
    with store.open(key) as handle:
        return pl.read_parquet(handle)


def materialize_actual_directory(
    store: ObjectStorePort, artifacts: SliceArtifacts, output_dir: Path
) -> None:
    """Extrai a saída real do pipeline de `store` para `output_dir` no layout comparável."""
    output_dir.mkdir(parents=True, exist_ok=True)
    _copy_object(store, artifacts.reconciliation_manifest.object_key, output_dir / GOLD_FILE)
    _copy_object(store, artifacts.serving_manifest.object_key, output_dir / SERVING_FILE)
    divergence_frame = _read_object_frame(store, artifacts.divergence_manifest.object_key)
    _write_json(output_dir / DIVERGENCE_FILE, divergence_frame.to_dicts())
    _write_json(output_dir / KPIS_FILE, artifacts.kpis)
    run_ids = {"local": artifacts.local_manifest_id, "national": artifacts.national_manifest_id}
    _write_json(output_dir / NORMALIZED_IDS_FILE, run_ids)
