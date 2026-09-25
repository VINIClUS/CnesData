"""Helpers compartilhados dos testes SIHD: object store fake e fixtures congeladas."""

from __future__ import annotations

import hashlib
import json
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from cnes_contracts.manifests.processing import NormalizeRequest, ReconcileRequest
from cnes_contracts.manifests.raw import RawManifest, SourceType
from cnes_domain.ports.object_store import ObjectStat

if TYPE_CHECKING:
    from contextlib import AbstractContextManager as ContextManager
    from typing import BinaryIO

    from cnes_contracts.manifests.outputs import OutputManifest

FIXTURES_DIR = Path(__file__).resolve().parents[2] / "fixtures" / "sihd"
TENANT = "354130"
COMPETENCIA = "2026-01"
RUN_ID = "run-1"
NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)
LEAVES = {
    "SIHD_INTERNACAO": ("internacoes.parquet", "quality_issues_internacao.parquet"),
    "SIHD_PROC_AIH": ("procedimentos_aih.parquet", "quality_issues_proc_aih.parquet"),
}
UNIT_IDS = {"SIHD_INTERNACAO": "unit-internacao", "SIHD_PROC_AIH": "unit-proc-aih"}


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


@dataclass
class BlindStatStore(FakeObjectStore):
    def stat(self, key: str) -> ObjectStat | None:
        return None


@dataclass
class TamperedStatStore(FakeObjectStore):
    def stat(self, key: str) -> ObjectStat | None:
        stat = super().stat(key)
        assert stat is not None
        return ObjectStat(key=key, size_bytes=stat.size_bytes, sha256="0" * 64)


def load_json(name: str) -> object:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def raw_spec(subtype: str) -> dict[str, object]:
    return dict(load_json("raw_manifests.json")["manifests"][subtype])


def raw_rows(subtype: str) -> list[dict[str, object]]:
    return load_json(str(raw_spec(subtype)["rows_file"]))


def serialize(frame: pl.DataFrame) -> bytes:
    output = BytesIO()
    frame.write_parquet(output, compression="zstd", compression_level=3)
    return output.getvalue()


def put_raw(
    store: FakeObjectStore, spec: dict[str, object], frame: pl.DataFrame
) -> RawManifest:
    payload = serialize(frame)
    fields = {key: value for key, value in spec.items() if key != "rows_file"}
    fields.update(
        object_sha256=hashlib.sha256(payload).hexdigest(),
        size_bytes=len(payload),
        row_count=frame.height,
    )
    store.objects[str(fields["object_key"])] = payload
    return RawManifest.model_validate_json(json.dumps(fields))


def target_keys(subtype: str, competencia: str = COMPETENCIA) -> tuple[str, ...]:
    prefix = f"normalized/{TENANT}/SIHD/{competencia}/{RUN_ID}"
    return tuple(f"{prefix}/{leaf}" for leaf in LEAVES[subtype])


def normalize_request(
    manifests: tuple[RawManifest, ...], subtype: str, keys: tuple[str, ...] | None = None
) -> NormalizeRequest:
    return NormalizeRequest(
        tenant_id=TENANT,
        run_id=RUN_ID,
        unit_id=UNIT_IDS[subtype],
        attempt=1,
        source_type=SourceType.SIHD,
        raw_manifests=manifests,
        target_keys=keys if keys is not None else target_keys(subtype),
        normalized_at=NOW,
    )


def fixture_request(store: FakeObjectStore, subtype: str) -> NormalizeRequest:
    manifest = put_raw(store, raw_spec(subtype), pl.DataFrame(raw_rows(subtype)))
    return normalize_request((manifest,), subtype)


def read_parquet(store: FakeObjectStore, key: str) -> pl.DataFrame:
    return pl.read_parquet(BytesIO(store.objects[key]))


def json_rows(frame: pl.DataFrame) -> list[dict[str, object]]:
    rendered = frame.with_columns(pl.col(pl.Date).dt.to_string("%Y-%m-%d"))
    return rendered.to_dicts()


def normalize_all(store: FakeObjectStore) -> tuple[OutputManifest, ...]:
    from data_processor.sources.sihd.normalize import normalize_sihd

    manifests: list[OutputManifest] = []
    for subtype in LEAVES:
        manifests.extend(normalize_sihd(fixture_request(store, subtype), store).manifests)
    return tuple(manifests)


def reconcile_request(manifests: tuple[OutputManifest, ...]) -> ReconcileRequest:
    prefix = f"reconciliation/{TENANT}/{COMPETENCIA}/{RUN_ID}"
    return ReconcileRequest(
        tenant_id=TENANT,
        competencia=COMPETENCIA,
        run_id=RUN_ID,
        unit_id="unit-reconcile",
        attempt=1,
        normalized_manifests=manifests,
        reconciliation_key=f"{prefix}/sihd.parquet",
        divergence_key=f"{prefix}/sihd_divergences.parquet",
        reconciled_at=NOW,
    )
