"""Testes de reconcile_bpa: agrupamento por chave natural sem colapsar proveniência."""

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
import pytest

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.processing import ReconcileRequest
from cnes_contracts.manifests.raw import SourceType
from cnes_domain.ports.object_store import ObjectStat
from data_processor.adapters.bpa_adapter import NORMALIZED_SCHEMA, QUALITY_SCHEMA
from data_processor.sources.bpa.contract import PROVENANCE_SCHEMA
from data_processor.sources.bpa.reconcile import reconcile_bpa

if TYPE_CHECKING:
    from contextlib import AbstractContextManager as ContextManager
    from typing import BinaryIO

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "bpa"
_TENANT = "354130"
_COMPETENCIA = "2026-08"
_RUN_ID = "fixture-bpa-run-v1"
_NOW = datetime(2026, 9, 2, 13, 0, tzinfo=UTC)
_NORMALIZED = f"normalized/{_TENANT}/BPA_MAG/{_COMPETENCIA}/{_RUN_ID}"
_RECONCILIATION_KEY = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/bpa.parquet"
_DIVERGENCE_KEY = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/bpa_divergences.parquet"
_INPUTS = {
    "bpa_c.parquet": ("BPA_C", "rows"),
    "quality_issues_bpa_c.parquet": ("BPA_C", "quality_issues"),
    "bpa_i.parquet": ("BPA_I", "rows"),
    "quality_issues_bpa_i.parquet": ("BPA_I", "quality_issues"),
}


@dataclass
class _FakeObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        payload = body.read()
        self.objects[key] = payload
        return ObjectStat(key=key, size_bytes=len(payload), sha256=expected_sha256)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        return nullcontext(BytesIO(self.objects[key]))

    def stat(self, key: str) -> ObjectStat | None:
        payload = self.objects.get(key)
        if payload is None:
            return None
        return ObjectStat(key=key, size_bytes=len(payload),
                          sha256=hashlib.sha256(payload).hexdigest())

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        raise NotImplementedError


def _frame(rows: list[dict[str, object]], kind: str) -> pl.DataFrame:
    if kind == "quality_issues":
        return pl.DataFrame(rows, schema=QUALITY_SCHEMA)
    schema = {**NORMALIZED_SCHEMA, **PROVENANCE_SCHEMA, "data_atendimento": pl.String}
    frame = pl.DataFrame(rows, schema=schema)
    return frame.with_columns(pl.col("data_atendimento").str.to_date())


def _manifest(leaf: str, payload: bytes, row_count: int) -> OutputManifest:
    return OutputManifest(
        manifest_version=1, manifest_id=f"normalized-{leaf}", tenant_id=_TENANT,
        layer="normalized", source_type=SourceType.BPA_MAG, competencia=_COMPETENCIA,
        run_id=_RUN_ID, unit_id="unit-normalize", attempt=1, schema_version="bpa-normalized-v1",
        object_key=f"{_NORMALIZED}/{leaf}", object_sha256=hashlib.sha256(payload).hexdigest(),
        row_count=row_count, created_at=_NOW,
    )


def _normalized_inputs(store: _FakeObjectStore) -> tuple[OutputManifest, ...]:
    expected = json.loads((_FIXTURES / "expected_normalized.json").read_text(encoding="utf-8"))
    manifests = []
    for leaf, (file_subtype, kind) in _INPUTS.items():
        frame = _frame(expected[file_subtype][kind], kind)
        output = BytesIO()
        frame.write_parquet(output)
        payload = output.getvalue()
        store.put(f"{_NORMALIZED}/{leaf}", BytesIO(payload), hashlib.sha256(payload).hexdigest())
        manifests.append(_manifest(leaf, payload, frame.height))
    return tuple(manifests)


def _request(manifests: tuple[OutputManifest, ...]) -> ReconcileRequest:
    return ReconcileRequest(
        tenant_id=_TENANT, competencia=_COMPETENCIA, run_id=_RUN_ID, unit_id="unit-reconcile",
        attempt=1, normalized_manifests=manifests, reconciliation_key=_RECONCILIATION_KEY,
        divergence_key=_DIVERGENCE_KEY, reconciled_at=_NOW,
    )


def _reconcile() -> tuple[_FakeObjectStore, object]:
    store = _FakeObjectStore()
    result = reconcile_bpa(_request(_normalized_inputs(store)), store)
    return store, result


def test_agrupa_por_chave_natural_com_quantidade_apresentada_e_aceita() -> None:
    store, _ = _reconcile()

    frame = pl.read_parquet(BytesIO(store.objects[_RECONCILIATION_KEY]))

    summary = frame.select(
        "cnes", "sigtap", "cbo", "file_subtype",
        "linhas", "linhas_aceitas", "qtd_apresentada", "qtd_aceita",
    ).rows()
    assert summary == [
        ("2269481", "0101010010", "223505", "BPA_C", 1, 1, 3, 3),
        ("2269481", "0301010056", "225125", "BPA_C", 2, 2, 15, 15),
        ("2269481", "0301010072", "225125", "BPA_I", 3, 2, 3, 2),
        ("7654321", "0301010056", "225125", "BPA_C", 2, 0, 4, 0),
        ("7654321", "0301010072", "2231F9", "BPA_I", 1, 0, 1, 0),
        ("7654321", "0301010072", None, "BPA_I", 1, 0, 2, 0),
        ("7654321", None, "225125", "BPA_C", 1, 0, 2, 0),
        (None, "0301010056", "225125", "BPA_C", 1, 0, 7, 0),
    ]
    assert set(frame["tenant_id"]) == {_TENANT}
    assert set(frame["competencia"]) == {_COMPETENCIA}


def test_preserva_proveniencia_de_cada_linha_fonte() -> None:
    store, _ = _reconcile()
    expected = json.loads((_FIXTURES / "expected_normalized.json").read_text(encoding="utf-8"))
    source_ids = sorted(
        row["source_record_id"] for item in expected.values() for row in item["rows"]
    )

    frame = pl.read_parquet(BytesIO(store.objects[_RECONCILIATION_KEY]))

    assert sorted(frame["source_record_ids"].explode().to_list()) == source_ids
    assert set(frame["source_manifest_ids"].explode()) == {
        "fixture-bpa-c-v1", "fixture-bpa-i-v1",
    }


def test_divergencias_vem_das_quality_issues_com_cnes() -> None:
    store, _ = _reconcile()

    frame = pl.read_parquet(BytesIO(store.objects[_DIVERGENCE_KEY]))

    assert frame.height == 11
    assert frame.group_by("code").len().sort("code").rows() == [
        ("cbo_invalido", 1), ("cid_invalido", 1), ("cnes_invalido", 1),
        ("cns_profissional_ausente", 1), ("competencia_divergente", 1),
        ("data_atendimento_ausente", 1), ("data_atendimento_invalida", 1), ("origem_divergente", 1),
        ("quantidade_invalida", 1), ("registro_duplicado", 1), ("sigtap_invalido", 1),
    ]
    assert frame.filter(pl.col("code") == "sigtap_invalido")["cnes"].to_list() == ["7654321"]


def test_kpis_de_reconciliacao_sao_exatos() -> None:
    _, result = _reconcile()

    assert result.kpis == {
        "linhas_bpa_c": 7, "linhas_bpa_i": 5, "linhas_aceitas": 5,
        "linhas_com_divergencia": 7, "divergencias": 11, "qtd_apresentada": 37,
        "qtd_aceita": 20, "grupos_reconciliados": 8,
    }
    assert result.reconciliation_manifest.row_count == 8
    assert result.divergence_manifest.row_count == 11
    assert result.reconciliation_manifest.schema_version == "bpa-reconciliation-v1"
    assert result.divergence_manifest.schema_version == "bpa-divergence-v1"


def test_bytes_de_reconciliacao_sao_idempotentes() -> None:
    first, _ = _reconcile()
    second, _ = _reconcile()

    assert first.objects[_RECONCILIATION_KEY] == second.objects[_RECONCILIATION_KEY]
    assert first.objects[_DIVERGENCE_KEY] == second.objects[_DIVERGENCE_KEY]


def test_exige_os_quatro_manifests_normalizados() -> None:
    store = _FakeObjectStore()
    manifests = _normalized_inputs(store)
    incomplete = tuple(item for item in manifests if not item.object_key.endswith("bpa_i.parquet"))

    with pytest.raises(ValueError, match="bpa_normalized_incompleto"):
        reconcile_bpa(_request(incomplete), store)


def test_rejeita_normalizado_divergente_do_sha256_do_manifesto() -> None:
    store = _FakeObjectStore()
    manifests = _normalized_inputs(store)
    store.objects[f"{_NORMALIZED}/bpa_c.parquet"] = store.objects[f"{_NORMALIZED}/bpa_i.parquet"]

    with pytest.raises(ValueError, match="input_sha256_mismatch"):
        reconcile_bpa(_request(manifests), store)
