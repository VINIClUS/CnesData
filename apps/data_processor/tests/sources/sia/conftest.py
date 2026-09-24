"""Harness SIA: object store em memória, fixtures congeladas e requests por estágio."""

from __future__ import annotations

import hashlib
import json
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest

from cnes_contracts.manifests.processing import (
    MaterializeRequest,
    NormalizeRequest,
    ReconcileRequest,
)
from cnes_contracts.manifests.raw import RawManifest, SourceType
from cnes_domain.ports.object_store import ObjectStat
from data_processor.sources.sia.contract import SIA_LAYOUT
from data_processor.sources.sia.normalize import normalize_sia
from data_processor.sources.sia.reconcile import reconcile_sia

if TYPE_CHECKING:
    from collections.abc import BinaryIO
    from contextlib import AbstractContextManager as ContextManager

    from cnes_contracts.manifests.outputs import OutputManifest
    from cnes_contracts.manifests.processing import ReconcileResult

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "sia"
TENANT = "354130"
COMPETENCIA = "2026-01"
RUN_ID = "run-sia-1"
NOW = datetime(2026, 2, 1, 4, tzinfo=UTC)
SUBTYPES = tuple(item.file_subtype for item in SIA_LAYOUT.normalized)

_DATETIME = pl.Datetime("ns", "UTC")
_APA_DTYPES = {
    "apa_cmp": pl.String, "apa_cnes": pl.String, "apa_cnspct": pl.String,
    "apa_cnsexe": pl.String, "apa_proc": pl.String, "apa_cbo": pl.String,
    "apa_cid": pl.String, "apa_dtini": _DATETIME, "apa_dtfin": _DATETIME,
    "apa_qtapr": pl.Int32, "apa_vlapr": pl.Int64,
}
_BPI_DTYPES = {
    "bpi_cmp": pl.String, "bpi_cnes": pl.String, "bpi_cnspac": pl.String,
    "bpi_cnsmed": pl.String, "bpi_cbo": pl.String, "bpi_proc": pl.String,
    "bpi_cid": pl.String, "bpi_dtaten": _DATETIME, "bpi_qt": pl.Int32,
    "bpi_folha": pl.Int16, "bpi_seq": pl.Int16,
}
RAW_DTYPES: dict[str, dict[str, Any]] = {
    "SIA_APA": _APA_DTYPES,
    "SIA_BPI": _BPI_DTYPES,
    "SIA_BPIHST": _BPI_DTYPES,
    "DIM_SIGTAP": dict.fromkeys(
        ("co_procedimento", "no_procedimento", "tp_complexidade", "co_financiamento",
         "dt_competencia"),
        pl.String,
    ),
    "DIM_MUNICIPIO": {
        "coduf": pl.String, "codmunic": pl.String, "nome": pl.String,
        "condic": pl.String, "tetopab": pl.Int64, "calcpab": pl.String,
    },
}


@dataclass
class FakeObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        data = body.read()
        digest = hashlib.sha256(data).hexdigest()
        if digest != expected_sha256:
            raise ValueError(f"sha256_mismatch key={key}")
        self.objects[key] = data
        return ObjectStat(key=key, size_bytes=len(data), sha256=digest)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        return nullcontext(BytesIO(self.objects[key]))

    def stat(self, key: str) -> ObjectStat | None:
        data = self.objects.get(key)
        if data is None:
            return None
        return ObjectStat(key=key, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        data = self.objects.pop(source_key)
        self.objects[destination_key] = data
        return ObjectStat(key=destination_key, size_bytes=len(data), sha256=expected_sha256)


def load_fixture(name: str) -> Any:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def frame_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return [
        {name: value.isoformat() if isinstance(value, date) else value
         for name, value in row.items() if name != "_source_manifest_sha256"}
        for row in frame.iter_rows(named=True)
    ]


def raw_frame(subtype: str, rows: list[dict[str, Any]]) -> pl.DataFrame:
    dtypes = RAW_DTYPES[subtype]
    parsed = [
        {name: _parse(value, dtypes[name]) for name, value in row.items()} for row in rows
    ]
    return pl.DataFrame(parsed, schema=dtypes)


def _parse(value: Any, dtype: Any) -> Any:
    if dtype == _DATETIME and value is not None:
        return datetime.fromisoformat(value)
    return value


@dataclass
class SiaHarness:
    store: FakeObjectStore
    raws: list[RawManifest] = field(default_factory=list)
    load_fixture = staticmethod(load_fixture)
    raw_frame = staticmethod(raw_frame)
    frame_rows = staticmethod(frame_rows)

    def read_frame(self, key: str) -> pl.DataFrame:
        return pl.read_parquet(BytesIO(self.store.objects[key]))

    def put_raw(self, subtype: str, frame: pl.DataFrame) -> RawManifest:
        template = next(m for m in load_fixture("raw_manifests.json")
                        if m["file_subtype"] == subtype)
        output = BytesIO()
        frame.write_parquet(output)
        payload = output.getvalue()
        digest = hashlib.sha256(payload).hexdigest()
        self.store.put(template["object_key"], BytesIO(payload), digest)
        body = {**template, "object_sha256": digest, "row_count": frame.height,
                "size_bytes": len(payload)}
        raw = RawManifest.model_validate_json(json.dumps(body))
        self.raws.append(raw)
        return raw

    def normalize_request(self, raw: RawManifest, **overrides: Any) -> NormalizeRequest:
        layout = next(item for item in SIA_LAYOUT.normalized
                      if item.file_subtype == raw.file_subtype)
        prefix = f"normalized/{TENANT}/SIA_LOCAL/{COMPETENCIA}/{RUN_ID}"
        fields: dict[str, Any] = {
            "tenant_id": TENANT, "run_id": RUN_ID, "unit_id": f"unit-{raw.file_subtype}",
            "attempt": 1, "source_type": SourceType.SIA_LOCAL, "raw_manifests": (raw,),
            "target_keys": tuple(f"{prefix}/{name}" for name in layout.normalized_filenames),
            "normalized_at": NOW,
        }
        return NormalizeRequest(**{**fields, **overrides})

    def normalize_fixture(self, subtype: str, rows: list[dict[str, Any]] | None = None):
        source = load_fixture("raw_rows.json")[subtype] if rows is None else rows
        raw = self.put_raw(subtype, raw_frame(subtype, source))
        return normalize_sia(self.normalize_request(raw), self.store)

    def normalize_all(
        self, rows_by_subtype: dict[str, list[dict[str, Any]]] | None = None
    ) -> tuple[OutputManifest, ...]:
        overrides = rows_by_subtype or {}
        return tuple(
            manifest
            for subtype in SUBTYPES
            for manifest in self.normalize_fixture(subtype, overrides.get(subtype)).manifests
        )

    def reconcile_request(self, manifests: tuple[OutputManifest, ...]) -> ReconcileRequest:
        prefix = f"reconciliation/{TENANT}/{COMPETENCIA}/{RUN_ID}"
        return ReconcileRequest(
            tenant_id=TENANT, competencia=COMPETENCIA, run_id=RUN_ID, unit_id="unit-reconcile",
            attempt=1, normalized_manifests=manifests,
            reconciliation_key=f"{prefix}/{SIA_LAYOUT.reconciliation_filename}",
            divergence_key=f"{prefix}/{SIA_LAYOUT.divergence_filename}",
            reconciled_at=NOW,
        )

    def reconcile_all(
        self, rows_by_subtype: dict[str, list[dict[str, Any]]] | None = None
    ) -> ReconcileResult:
        request = self.reconcile_request(self.normalize_all(rows_by_subtype))
        return reconcile_sia(request, self.store)

    def materialize_request(self, result: ReconcileResult, **overrides: Any) -> MaterializeRequest:
        fields: dict[str, Any] = {
            "tenant_id": TENANT, "competencia": COMPETENCIA, "run_id": RUN_ID,
            "unit_id": "unit-materialize", "attempt": 1,
            "reconciliation_manifest": result.reconciliation_manifest,
            "divergence_manifest": result.divergence_manifest, "missing_sources": (),
            "target_keys": tuple(
                f"serving/{TENANT}/{RUN_ID}/{name}.json" for name in SIA_LAYOUT.serving_documents
            ),
            "generated_at": NOW,
        }
        return MaterializeRequest(**{**fields, **overrides})


@pytest.fixture
def sia() -> SiaHarness:
    return SiaHarness(FakeObjectStore())
