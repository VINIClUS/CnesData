"""Testes de reconcile_cnes: precedência, divergência, KPIs e artefatos gold."""

from __future__ import annotations

import hashlib
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
from data_processor.pipeline.reconcile_cnes import _divergence_rows, _rows_by_key, reconcile_cnes

if TYPE_CHECKING:
    from contextlib import AbstractContextManager as ContextManager
    from typing import BinaryIO

    from cnes_domain.ports.object_store import ObjectStat


@dataclass
class _FakeObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        from cnes_domain.ports.object_store import ObjectStat as _ObjectStat

        payload = body.read()
        self.objects[key] = payload
        return _ObjectStat(key=key, size_bytes=len(payload), sha256=expected_sha256)

    def open(self, key: str) -> ContextManager[BinaryIO]:
        return nullcontext(BytesIO(self.objects[key]))

    def stat(self, key: str) -> ObjectStat | None:
        from cnes_domain.ports.object_store import ObjectStat as _ObjectStat

        payload = self.objects.get(key)
        if payload is None:
            return None
        digest = hashlib.sha256(payload).hexdigest()
        return _ObjectStat(key=key, size_bytes=len(payload), sha256=digest)

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        raise NotImplementedError


@dataclass
class _BlindStatStore(_FakeObjectStore):
    def stat(self, key: str) -> ObjectStat | None:
        return None


_TENANT = "354130"
_COMPETENCIA = "2026-01"
_RUN_ID = "run-1"
_UNIT_ID = "unit-1"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)
_RECONCILIATION_KEY = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/reconciled.parquet"
_DIVERGENCE_KEY = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/divergences.parquet"
_FIXTURES_DIR = Path(__file__).resolve().parents[4] / "docs" / "fixtures" / "data-plane"

_NORMALIZED_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "CPF": pl.String, "CNS": pl.String, "NOME_PROFISSIONAL": pl.String,
    "NOME_SOCIAL": pl.String, "SEXO": pl.String, "CBO": pl.String, "CNES": pl.String,
    "TIPO_VINCULO": pl.String, "SUS": pl.String, "CH_TOTAL": pl.Int64,
    "CH_AMBULATORIAL": pl.Int64, "CH_OUTRAS": pl.Int64, "CH_HOSPITALAR": pl.Int64,
    "FONTE": pl.String, "_source_manifest_id": pl.String, "_source_snapshot_id": pl.String,
    "_source_type": pl.String, "_normalized_at": pl.String,
}


def _row(
    cpf: str | None, cns: str | None, cbo: str = "225125", cnes: str = "1234567",
    nome: str = "PROFISSIONAL TESTE 001", fonte: str = "LOCAL", **overrides: object,
) -> dict[str, object]:
    base: dict[str, object] = {
        "CPF": cpf, "CNS": cns, "NOME_PROFISSIONAL": nome, "NOME_SOCIAL": None,
        "SEXO": "F", "CBO": cbo, "CNES": cnes, "TIPO_VINCULO": "01", "SUS": "S",
        "CH_TOTAL": 40, "CH_AMBULATORIAL": 20, "CH_OUTRAS": 0, "CH_HOSPITALAR": 20,
        "FONTE": fonte, "_source_manifest_id": "src-1", "_source_snapshot_id": "snap-1",
        "_source_type": fonte, "_normalized_at": _NOW.isoformat(),
    }
    base.update(overrides)
    return base


def _fixture_rows(filename: str) -> list[dict[str, object]]:
    rows = pl.read_parquet(_FIXTURES_DIR / filename).to_dicts()
    for row in rows:
        row["_source_manifest_id"] = "src-1"
        row["_source_snapshot_id"] = "snap-1"
        row["_source_type"] = row["FONTE"]
        row["_normalized_at"] = _NOW.isoformat()
    return rows


def _serialize_normalized(rows: list[dict[str, object]]) -> bytes:
    output = BytesIO()
    frame = pl.DataFrame(rows, schema=_NORMALIZED_SCHEMA)
    frame.write_parquet(output, compression="zstd", compression_level=3)
    return output.getvalue()


def _normalized_manifest(
    store: _FakeObjectStore, rows: list[dict[str, object]], source_type: SourceType, unit_id: str,
) -> OutputManifest:
    key = f"normalized/{_TENANT}/{source_type.value}/{_COMPETENCIA}/{_RUN_ID}/{unit_id}.parquet"
    payload = _serialize_normalized(rows)
    digest = hashlib.sha256(payload).hexdigest()
    store.put(key, BytesIO(payload), digest)
    return OutputManifest(
        manifest_version=1,
        manifest_id=f"normalized-{_RUN_ID}-{unit_id}-1",
        tenant_id=_TENANT,
        layer="normalized",
        source_type=source_type,
        competencia=_COMPETENCIA,
        run_id=_RUN_ID,
        unit_id=unit_id,
        attempt=1,
        schema_version="cnes-normalized-v1",
        object_key=key,
        object_sha256=digest,
        row_count=len(rows),
        created_at=_NOW,
    )


def _fixture_manifests(store: _FakeObjectStore) -> tuple[OutputManifest, OutputManifest]:
    local_rows = _fixture_rows("cnes-local-v1.parquet")
    nacional_rows = _fixture_rows("cnes-nacional-v1.parquet")
    local = _normalized_manifest(store, local_rows, SourceType.CNES_LOCAL, "unit-local")
    national = _normalized_manifest(store, nacional_rows, SourceType.CNES_NACIONAL, "unit-nacional")
    return local, national


def _request(
    normalized_manifests: tuple[OutputManifest, ...],
    reconciliation_key: str = _RECONCILIATION_KEY,
    divergence_key: str = _DIVERGENCE_KEY,
) -> ReconcileRequest:
    return ReconcileRequest(
        tenant_id=_TENANT, competencia=_COMPETENCIA, run_id=_RUN_ID, unit_id=_UNIT_ID,
        attempt=1, normalized_manifests=normalized_manifests,
        reconciliation_key=reconciliation_key, divergence_key=divergence_key,
        reconciled_at=_NOW,
    )


def test_precedencia_local_nao_nulo_vence() -> None:
    store = _FakeObjectStore()
    local_manifest = _normalized_manifest(
        store,
        [
            _row(
                "90000000003", "999000000000003", cbo="223505", nome="PROFISSIONAL TESTE 003",
                CH_OUTRAS=None,
            )
        ],
        SourceType.CNES_LOCAL, "unit-local",
    )
    national_manifest = _normalized_manifest(
        store,
        [
            _row(
                "90000000003", "999000000000003", cbo="223505", nome="PROFISSIONAL TESTE 003",
                CH_OUTRAS=8, fonte="NACIONAL",
            )
        ],
        SourceType.CNES_NACIONAL, "unit-nacional",
    )
    request = _request((local_manifest, national_manifest))

    result = reconcile_cnes(request, store)

    reconciled = pl.read_parquet(BytesIO(store.objects[_RECONCILIATION_KEY]))
    assert reconciled.to_dicts()[0]["CH_OUTRAS"] == 8
    divergences = pl.read_parquet(BytesIO(store.objects[_DIVERGENCE_KEY]))
    assert divergences.height == 0
    assert result.kpis["conflict_count"] == 0


def test_divergencia_tem_shape_da_fixture() -> None:
    store = _FakeObjectStore()
    local, national = _fixture_manifests(store)
    local_rows = _rows_by_key(store, local, _COMPETENCIA)
    national_rows = _rows_by_key(store, national, _COMPETENCIA)

    manifest_ids = (local.manifest_id, national.manifest_id)
    divergences = _divergence_rows(local_rows, national_rows, manifest_ids)

    assert len(divergences) == 2
    by_field = {item["field"]: item for item in divergences}
    assert set(by_field) == {"NOME_PROFISSIONAL", "CH_TOTAL"}
    assert by_field["NOME_PROFISSIONAL"]["local_value"] == "PROFISSIONAL TESTE 002"
    assert by_field["NOME_PROFISSIONAL"]["national_value"] == "PROFISSIONAL TESTE 102"
    assert by_field["CH_TOTAL"]["local_value"] == 40
    assert by_field["CH_TOTAL"]["national_value"] == 30
    for item in divergences:
        assert item["selected_source"] == "LOCAL"
        assert item["selected_value"] == item["local_value"]
        assert item["natural_key"] == {
            "identity": "999000000000002", "CNES": "1234567",
            "CBO": "225125", "COMPETENCIA": _COMPETENCIA,
        }
        assert item["source_manifest_ids"] == [local.manifest_id, national.manifest_id]


def test_parquet_de_divergencia_tem_colunas_e_tipos() -> None:
    store = _FakeObjectStore()
    local, national = _fixture_manifests(store)
    request = _request((local, national))

    reconcile_cnes(request, store)

    frame = pl.read_parquet(BytesIO(store.objects[_DIVERGENCE_KEY]))
    assert frame.columns == [
        "natural_key", "field", "local_value", "national_value",
        "selected_value", "selected_source", "source_manifest_ids",
    ]
    assert frame.schema["field"] == pl.String
    assert frame.schema["local_value"] == pl.String
    assert isinstance(frame.schema["natural_key"], pl.Struct)
    assert {item.name for item in frame.schema["natural_key"].fields} == {
        "identity", "CNES", "CBO", "COMPETENCIA",
    }
    assert frame.schema["source_manifest_ids"] == pl.List(pl.String)


def test_kpis_batem_com_fixture_travada() -> None:
    store = _FakeObjectStore()
    local, national = _fixture_manifests(store)
    request = _request((local, national))

    result = reconcile_cnes(request, store)

    assert result.kpis == {
        "match_count": 3, "local_only_count": 2, "national_only_count": 2,
        "conflict_count": 1, "reconciled_row_count": 7, "active_professional_count": 6,
    }
    assert all(type(value) is int for value in result.kpis.values())


def test_rejeita_fonte_nacional_duplicada_ou_desconhecida() -> None:
    store = _FakeObjectStore()
    local = _normalized_manifest(
        store, [_row("90000000001", "999000000000001")], SourceType.CNES_LOCAL, "unit-local",
    )
    national_a = _normalized_manifest(
        store, [_row("90000000002", "999000000000002", fonte="NACIONAL")],
        SourceType.CNES_NACIONAL, "unit-nacional-a",
    )
    national_b = _normalized_manifest(
        store, [_row("90000000003", "999000000000003", fonte="NACIONAL")],
        SourceType.CNES_NACIONAL, "unit-nacional-b",
    )
    duplicate_request = _request((local, national_a, national_b))
    with pytest.raises(ValueError, match="unexpected_source_type"):
        reconcile_cnes(duplicate_request, store)

    sihd = _normalized_manifest(
        store, [_row("90000000004", "999000000000004")], SourceType.SIHD, "unit-sihd",
    )
    unknown_request = _request((local, sihd))
    with pytest.raises(ValueError, match="unexpected_source_type"):
        reconcile_cnes(unknown_request, store)


def test_rejeita_ausencia_de_fonte_local() -> None:
    store = _FakeObjectStore()
    national = _normalized_manifest(
        store, [_row("90000000001", "999000000000001", fonte="NACIONAL")],
        SourceType.CNES_NACIONAL, "unit-nacional",
    )
    request = _request((national,))

    with pytest.raises(ValueError, match="unexpected_source_type"):
        reconcile_cnes(request, store)


def test_colunas_do_reconciliado_batem_com_cnes_gold_v2() -> None:
    store = _FakeObjectStore()
    local, national = _fixture_manifests(store)
    request = _request((local, national))

    reconcile_cnes(request, store)

    reconciled = pl.read_parquet(BytesIO(store.objects[_RECONCILIATION_KEY]))
    gold = pl.read_parquet(_FIXTURES_DIR / "cnes-gold-v2.parquet")
    assert reconciled.columns == gold.columns
    assert reconciled.schema == gold.schema


def test_reconciliado_reproduz_valores_e_ordem_do_cnes_gold_v2() -> None:
    store = _FakeObjectStore()
    local, national = _fixture_manifests(store)
    request = _request((local, national))

    reconcile_cnes(request, store)

    reconciled = pl.read_parquet(BytesIO(store.objects[_RECONCILIATION_KEY]))
    gold = pl.read_parquet(_FIXTURES_DIR / "cnes-gold-v2.parquet")
    assert reconciled.drop("_source_manifest_ids").equals(gold.drop("_source_manifest_ids"))


def test_reconcilia_sem_fonte_nacional() -> None:
    store = _FakeObjectStore()
    local = _normalized_manifest(
        store,
        [_row("90000000001", "999000000000001", cbo="225125", nome="PROFISSIONAL TESTE 001")],
        SourceType.CNES_LOCAL, "unit-local",
    )
    request = _request((local,))

    result = reconcile_cnes(request, store)

    assert result.kpis == {
        "match_count": 0, "local_only_count": 1, "national_only_count": 0,
        "conflict_count": 0, "reconciled_row_count": 1, "active_professional_count": 1,
    }
    reconciled = pl.read_parquet(BytesIO(store.objects[_RECONCILIATION_KEY]))
    assert reconciled.to_dicts()[0]["_source_manifest_ids"] == [local.manifest_id]
    divergences = pl.read_parquet(BytesIO(store.objects[_DIVERGENCE_KEY]))
    assert divergences.height == 0


def test_rejeita_linha_sem_cns_e_cpf() -> None:
    store = _FakeObjectStore()
    local = _normalized_manifest(
        store, [_row(None, None, cbo="225125", nome="PROFISSIONAL TESTE 001")],
        SourceType.CNES_LOCAL, "unit-local",
    )
    request = _request((local,))

    with pytest.raises(ValueError, match="identity_missing"):
        reconcile_cnes(request, store)


def test_falha_quando_objeto_escrito_nao_e_encontrado() -> None:
    store = _BlindStatStore()
    local = _normalized_manifest(
        store, [_row("90000000001", "999000000000001")], SourceType.CNES_LOCAL, "unit-local",
    )
    request = _request((local,))

    with pytest.raises(ValueError, match="output_not_found"):
        reconcile_cnes(request, store)


def test_execucoes_repetidas_produzem_o_mesmo_sha256() -> None:
    store_a = _FakeObjectStore()
    store_b = _FakeObjectStore()
    local_a, national_a = _fixture_manifests(store_a)
    local_b, national_b = _fixture_manifests(store_b)

    result_a = reconcile_cnes(_request((local_a, national_a)), store_a)
    result_b = reconcile_cnes(_request((local_b, national_b)), store_b)

    assert (
        result_a.reconciliation_manifest.object_sha256
        == result_b.reconciliation_manifest.object_sha256
    )
    assert result_a.divergence_manifest.object_sha256 == result_b.divergence_manifest.object_sha256
