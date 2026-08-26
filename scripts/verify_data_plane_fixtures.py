"""Verifica as fixtures golden do data plane."""

import argparse
import hashlib
import json
import logging
import re
from collections.abc import Mapping, Sequence
from pathlib import Path

import polars as pl

type JsonValue = None | bool | int | float | str | list[JsonValue] | dict[str, JsonValue]
type JsonMapping = Mapping[str, JsonValue]
type Row = dict[str, object]
type NaturalKey = tuple[str, str, str, str]

LOGGER = logging.getLogger(__name__)
MANIFEST_FILE = "fixture-manifest.json"
LOCAL_FILE = "cnes-local-v1.parquet"
NATIONAL_FILE = "cnes-nacional-v1.parquet"
GOLD_FILE = "cnes-gold-v2.parquet"
SERVING_FILE = "cnes-serving-v1.json"
PARQUET_FILES = (LOCAL_FILE, NATIONAL_FILE, GOLD_FILE)
ARTIFACT_FILES = frozenset((*PARQUET_FILES, SERVING_FILE))
EXPECTED_FILES = frozenset((*ARTIFACT_FILES, MANIFEST_FILE))
LOCAL_MANIFEST_ID = "fixture-cnes-local-v1"
NATIONAL_MANIFEST_ID = "fixture-cnes-nacional-v1"
SOURCE_MANIFEST_IDS = [LOCAL_MANIFEST_ID, NATIONAL_MANIFEST_ID]
SOURCE_COLUMNS = (
    "CPF",
    "CNS",
    "NOME_PROFISSIONAL",
    "NOME_SOCIAL",
    "SEXO",
    "CBO",
    "CNES",
    "TIPO_VINCULO",
    "SUS",
    "CH_TOTAL",
    "CH_AMBULATORIAL",
    "CH_OUTRAS",
    "CH_HOSPITALAR",
    "FONTE",
)
GOLD_COLUMNS = (*SOURCE_COLUMNS[:-1], "COMPETENCIA", "_source_manifest_ids")
RECONCILED_FIELDS = tuple(
    field for field in SOURCE_COLUMNS if field not in {"FONTE", "CNS", "CNES", "CBO"}
)
SORT_FIELDS = ("CPF", "CNS", "CNES", "CBO")
SERVING_FIELDS = (
    "schema_version",
    "tenant_id",
    "run_id",
    "generated_at",
    "competencia",
    "kpis",
    "divergence_counts",
    "missing_sources",
)
EXPECTED_NATURAL_KEY = {
    "identity": {"primary": "CNS", "fallback": "CPF"},
    "dimensions": ["CNES", "CBO", "COMPETENCIA"],
}
EXPECTED_PRECEDENCE = {
    "order": ["LOCAL", "NACIONAL"],
    "rule": "LOCAL_NON_NULL_THEN_NACIONAL",
}
EXPECTED_PARQUET_OPTIONS = {
    "compression": "zstd",
    "compression_level": 3,
    "statistics": True,
    "row_group_size": 5,
}
EXPECTED_SCHEMA_VERSIONS = {
    LOCAL_FILE: "cnes-profissional-v1",
    NATIONAL_FILE: "cnes-profissional-v1",
    GOLD_FILE: "cnes-gold-v2",
    SERVING_FILE: "cnes-serving-v1",
}
EXPECTED_MANIFEST_IDS = {
    LOCAL_FILE: LOCAL_MANIFEST_ID,
    NATIONAL_FILE: NATIONAL_MANIFEST_ID,
}
CPF_PATTERN = re.compile(r"90000000\d{3}")
CNS_PATTERN = re.compile(r"999000000000\d{3}")
NAME_PATTERN = re.compile(r"PROFISSIONAL TESTE \d{3}")


class FixtureError(Exception):
    """Indica violação no contrato das fixtures."""


def _fail(field: str, detail: str = "invalid") -> None:
    raise FixtureError(f"field={field} status={detail}")


def _mapping(value: JsonValue, field: str) -> JsonMapping:
    if not isinstance(value, Mapping):
        _fail(field)
    return value


def _value(mapping: JsonMapping, key: str, field: str) -> JsonValue:
    if key not in mapping:
        _fail(field, "missing")
    return mapping[key]


def _verify_manifest_contract(manifest: JsonMapping) -> None:
    expectations: tuple[tuple[str, JsonValue], ...] = (
        ("fixture_version", 1),
        ("tenant_id", "354130"),
        ("competencia", "2026-01"),
        ("natural_key", EXPECTED_NATURAL_KEY),
        ("precedence", EXPECTED_PRECEDENCE),
        ("volatile_fields", ["run_id", "generated_at"]),
        ("parquet_options", EXPECTED_PARQUET_OPTIONS),
    )
    for field, expected in expectations:
        if _value(manifest, field, field) != expected:
            _fail(field)


def _verify_file_set(root: Path, files: JsonMapping) -> None:
    actual = frozenset(path.name for path in root.iterdir() if path.is_file())
    missing = EXPECTED_FILES - actual
    if missing:
        raise FixtureError(f"file={sorted(missing)[0]} status=missing")
    if actual != EXPECTED_FILES or frozenset(files) != ARTIFACT_FILES:
        _fail("files", "set_mismatch")


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _schema_entries(schema: Mapping[str, pl.DataType]) -> list[JsonValue]:
    return [{"name": name, "dtype": str(dtype)} for name, dtype in schema.items()]


def _json_schema_entries(payload: JsonMapping) -> list[JsonValue]:
    type_names = {
        str: "String",
        dict: "Object",
        list: "Array",
        int: "Integer",
        bool: "Boolean",
    }
    return [
        {"name": name, "dtype": type_names.get(type(value), "Unknown")}
        for name, value in payload.items()
    ]


def _verify_metadata(filename: str, metadata: JsonMapping) -> None:
    if _value(metadata, "schema_version", "schema_version") != EXPECTED_SCHEMA_VERSIONS[filename]:
        _fail("schema_version")
    expected_id = EXPECTED_MANIFEST_IDS.get(filename)
    if expected_id is not None and _value(metadata, "manifest_id", "manifest_id") != expected_id:
        _fail("manifest_id")


def _verify_artifact(root: Path, filename: str, metadata: JsonMapping) -> pl.DataFrame | None:
    path = root / filename
    if not path.is_file():
        raise FixtureError(f"file={filename} status=missing")
    if _sha256(path) != _value(metadata, "sha256", "sha256"):
        raise FixtureError(f"file={filename} field=sha256")
    _verify_metadata(filename, metadata)
    if filename == SERVING_FILE:
        return None
    frame = pl.read_parquet(path)
    if frame.height != _value(metadata, "row_count", "row_count"):
        raise FixtureError(f"file={filename} field=row_count")
    if _schema_entries(pl.read_parquet_schema(path)) != _value(metadata, "schema", "schema"):
        raise FixtureError(f"file={filename} field=schema")
    return frame


def _read_artifacts(root: Path, files: JsonMapping) -> dict[str, pl.DataFrame]:
    frames = {}
    for filename in (*PARQUET_FILES, SERVING_FILE):
        metadata = _mapping(_value(files, filename, "files"), f"files.{filename}")
        frame = _verify_artifact(root, filename, metadata)
        if frame is not None:
            frames[filename] = frame
    return frames


def _matches(value: object, pattern: re.Pattern[str]) -> bool:
    return value is None or (isinstance(value, str) and pattern.fullmatch(value) is not None)


def _verify_personal_fields(row: Row) -> None:
    checks = (
        ("CPF", CPF_PATTERN),
        ("CNS", CNS_PATTERN),
        ("NOME_PROFISSIONAL", NAME_PATTERN),
        ("NOME_SOCIAL", NAME_PATTERN),
    )
    for field, pattern in checks:
        if not _matches(row[field], pattern):
            _fail(field, "non_synthetic")


def _verify_privacy(frames: Sequence[pl.DataFrame]) -> None:
    for frame in frames:
        for row in frame.iter_rows(named=True):
            _verify_personal_fields(row)


def _natural_key(row: Row, competencia: str) -> NaturalKey:
    identity = row["CNS"] or row["CPF"]
    if not isinstance(identity, str):
        _fail("natural_key", "identity_missing")
    return identity, str(row["CNES"]), str(row["CBO"]), competencia


def _rows_by_key(frame: pl.DataFrame, competencia: str) -> dict[NaturalKey, Row]:
    result = {}
    for row in frame.iter_rows(named=True):
        key = _natural_key(row, competencia)
        if key in result:
            _fail("natural_key", "duplicate")
        result[key] = row
    return result


def _sort_key(row: Row) -> tuple[str, ...]:
    return tuple(str(row[field] or "") for field in SORT_FIELDS)


def _verify_source(frame: pl.DataFrame, source: str, competencia: str) -> None:
    if tuple(frame.columns) != SOURCE_COLUMNS or set(frame["FONTE"]) != {source}:
        _fail("source")
    rows = frame.iter_rows(named=True)
    if list(rows) != sorted(frame.iter_rows(named=True), key=_sort_key):
        _fail("ordering")
    _rows_by_key(frame, competencia)


def _merge_row(local: Row | None, national: Row | None, competencia: str) -> Row:
    preferred = local or national
    if preferred is None:
        _fail("gold", "source_missing")
    row = {}
    for field in SOURCE_COLUMNS[:-1]:
        local_value = local[field] if local is not None else None
        national_value = national[field] if national is not None else None
        row[field] = local_value if local_value is not None else national_value
    row["COMPETENCIA"] = competencia
    row["_source_manifest_ids"] = [
        manifest_id
        for present, manifest_id in (
            (local is not None, LOCAL_MANIFEST_ID),
            (national is not None, NATIONAL_MANIFEST_ID),
        )
        if present
    ]
    return row


def _reconciled_rows(
    local: dict[NaturalKey, Row],
    national: dict[NaturalKey, Row],
    competencia: str,
) -> list[Row]:
    rows = [
        _merge_row(local.get(key), national.get(key), competencia)
        for key in local.keys() | national.keys()
    ]
    return sorted(rows, key=_sort_key)


def _natural_key_payload(key: NaturalKey) -> dict[str, JsonValue]:
    identity, cnes, cbo, competencia = key
    return {
        "identity": identity,
        "CNES": cnes,
        "CBO": cbo,
        "COMPETENCIA": competencia,
    }


def _derived_divergences(
    local: dict[NaturalKey, Row], national: dict[NaturalKey, Row]
) -> list[JsonValue]:
    divergences: list[JsonValue] = []
    for key in sorted(local.keys() & national.keys()):
        for field in RECONCILED_FIELDS:
            local_value = local[key][field]
            national_value = national[key][field]
            if local_value is None or national_value is None or local_value == national_value:
                continue
            divergences.append(
                {
                    "natural_key": _natural_key_payload(key),
                    "field": field,
                    "local_value": local_value,
                    "national_value": national_value,
                    "selected_value": local_value,
                    "selected_source": "LOCAL",
                    "source_manifest_ids": SOURCE_MANIFEST_IDS,
                }
            )
    return divergences


def _derived_kpis(
    local: dict[NaturalKey, Row],
    national: dict[NaturalKey, Row],
    rows: Sequence[Row],
    divergences: Sequence[JsonValue],
) -> dict[str, JsonValue]:
    conflict_keys = {
        json.dumps(_mapping(item, "divergences")["natural_key"], sort_keys=True)
        for item in divergences
    }
    identities = {row["CNS"] or row["CPF"] for row in rows}
    return {
        "match_count": len(local.keys() & national.keys()),
        "local_only_count": len(local.keys() - national.keys()),
        "national_only_count": len(national.keys() - local.keys()),
        "conflict_count": len(conflict_keys),
        "reconciled_row_count": len(rows),
        "active_professional_count": len(identities),
    }


def _verify_reconciliation(
    frames: Mapping[str, pl.DataFrame], manifest: JsonMapping
) -> dict[str, JsonValue]:
    competencia = str(manifest["competencia"])
    local_frame = frames[LOCAL_FILE]
    national_frame = frames[NATIONAL_FILE]
    _verify_source(local_frame, "LOCAL", competencia)
    _verify_source(national_frame, "NACIONAL", competencia)
    local = _rows_by_key(local_frame, competencia)
    national = _rows_by_key(national_frame, competencia)
    rows = _reconciled_rows(local, national, competencia)
    gold = frames[GOLD_FILE]
    if tuple(gold.columns) != GOLD_COLUMNS or gold.to_dicts() != rows:
        _fail("gold")
    divergences = _derived_divergences(local, national)
    if manifest["divergences"] != divergences:
        _fail("divergences")
    kpis = _derived_kpis(local, national, rows, divergences)
    if manifest["kpis"] != kpis:
        _fail("kpis")
    return kpis


def _verify_serving(root: Path, files: JsonMapping, kpis: JsonValue) -> None:
    payload = _mapping(json.loads((root / SERVING_FILE).read_text(encoding="utf-8")), "serving")
    if tuple(payload) != SERVING_FIELDS:
        _fail("serving_fields")
    metadata = _mapping(files[SERVING_FILE], f"files.{SERVING_FILE}")
    if _value(metadata, "row_count", "row_count") != 1:
        _fail("row_count")
    if _json_schema_entries(payload) != _value(metadata, "schema", "schema"):
        _fail("schema")
    expected = {
        "schema_version": "cnes-serving-v1",
        "tenant_id": "354130",
        "competencia": "2026-01",
        "kpis": kpis,
        "divergence_counts": {"NOME_PROFISSIONAL": 1, "CH_TOTAL": 1},
        "missing_sources": [],
    }
    for field, value in expected.items():
        if payload[field] != value:
            _fail("serving")


def verify_fixture_set(root: Path, manifest: Mapping[str, JsonValue]) -> None:
    """Valida integridade, privacidade e reconciliação das fixtures.

    Args: root: Diretório das fixtures; manifest: contrato carregado.
    Raises: FixtureError: Quando qualquer contrato for violado.
    """
    _verify_manifest_contract(manifest)
    files = _mapping(_value(manifest, "files", "files"), "files")
    _verify_file_set(root, files)
    frames = _read_artifacts(root, files)
    _verify_privacy(tuple(frames.values()))
    kpis = _verify_reconciliation(frames, manifest)
    _verify_serving(root, files, kpis)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    return parser.parse_args()


def main() -> int:
    """Executa o verificador e retorna o status do processo."""
    args = _parse_args()
    try:
        manifest = json.loads((args.root / MANIFEST_FILE).read_text(encoding="utf-8"))
        verify_fixture_set(args.root, _mapping(manifest, "manifest"))
    except Exception as error:
        LOGGER.error("fixture_verify status=invalid root=%s error=%s", args.root, error)
        return 1
    LOGGER.info("fixture_verify status=valid root=%s", args.root)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(main())
