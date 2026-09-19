"""Testes de materialize_cnes: agregação de KPIs, privacidade e artefato serving."""

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
from cnes_contracts.manifests.processing import MaterializeRequest
from data_processor.pipeline.materialize_cnes import materialize_cnes
from data_processor.pipeline.reconcile_cnes import _DIVERGENCE_SCHEMA
from scripts.verify_data_plane_fixtures import (
    CNS_PATTERN,
    CPF_PATTERN,
    NAME_PATTERN,
    SERVING_FIELDS,
)

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
_RUN_ID = "fixture-cnes-run-v1"
_UNIT_ID = "unit-materialize"
_GENERATED_AT = datetime(2026, 1, 31, 23, 59, 59, tzinfo=UTC)
_RECONCILIATION_KEY = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/reconciled.parquet"
_DIVERGENCE_KEY = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/divergences.parquet"
_TARGET_KEY = f"serving/{_TENANT}/{_RUN_ID}/overview.json"
_FIXTURES_DIR = Path(__file__).resolve().parents[4] / "docs" / "fixtures" / "data-plane"


def _put_frame(store: _FakeObjectStore, key: str, frame: pl.DataFrame) -> str:
    output = BytesIO()
    frame.write_parquet(output, compression="zstd", compression_level=3)
    payload = output.getvalue()
    digest = hashlib.sha256(payload).hexdigest()
    store.put(key, BytesIO(payload), digest)
    return digest


def _reconciliation_manifest(key: str, digest: str, row_count: int) -> OutputManifest:
    return OutputManifest(
        manifest_version=1,
        manifest_id=f"reconciliation-{key.rsplit('/', 1)[-1]}",
        tenant_id=_TENANT,
        layer="reconciliation",
        source_type=None,
        competencia=_COMPETENCIA,
        run_id=_RUN_ID,
        unit_id=_UNIT_ID,
        attempt=1,
        schema_version="cnes-reconciliation-v1",
        object_key=key,
        object_sha256=digest,
        row_count=row_count,
        created_at=_GENERATED_AT,
    )


def _divergence_manifest(key: str, digest: str, row_count: int) -> OutputManifest:
    return OutputManifest(
        manifest_version=1,
        manifest_id=f"divergence-{key.rsplit('/', 1)[-1]}",
        tenant_id=_TENANT,
        layer="reconciliation",
        source_type=None,
        competencia=_COMPETENCIA,
        run_id=_RUN_ID,
        unit_id=_UNIT_ID,
        attempt=1,
        schema_version="cnes-divergence-v1",
        object_key=key,
        object_sha256=digest,
        row_count=row_count,
        created_at=_GENERATED_AT,
    )


def _divergence_frame() -> pl.DataFrame:
    manifest = json.loads((_FIXTURES_DIR / "fixture-manifest.json").read_text(encoding="utf-8"))
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
        for item in manifest["divergences"]
    ]
    return pl.DataFrame(rows, schema=_DIVERGENCE_SCHEMA)


def _request(
    *,
    reconciliation_key: str = _RECONCILIATION_KEY,
    divergence_key: str = _DIVERGENCE_KEY,
    missing_sources: tuple[str, ...] = (),
    target_keys: tuple[str, ...] = (_TARGET_KEY,),
) -> MaterializeRequest:
    return MaterializeRequest(
        tenant_id=_TENANT,
        competencia=_COMPETENCIA,
        run_id=_RUN_ID,
        unit_id=_UNIT_ID,
        attempt=1,
        reconciliation_manifest=_reconciliation_manifest(reconciliation_key, "0" * 64, 0),
        divergence_manifest=_divergence_manifest(divergence_key, "0" * 64, 0),
        missing_sources=missing_sources,
        target_keys=target_keys,
        generated_at=_GENERATED_AT,
    )


def _fixture_request(store: _FakeObjectStore) -> MaterializeRequest:
    gold = pl.read_parquet(_FIXTURES_DIR / "cnes-gold-v2.parquet")
    gold_digest = _put_frame(store, _RECONCILIATION_KEY, gold)
    divergences = _divergence_frame()
    divergence_digest = _put_frame(store, _DIVERGENCE_KEY, divergences)
    return MaterializeRequest(
        tenant_id=_TENANT,
        competencia=_COMPETENCIA,
        run_id=_RUN_ID,
        unit_id=_UNIT_ID,
        attempt=1,
        reconciliation_manifest=_reconciliation_manifest(
            _RECONCILIATION_KEY, gold_digest, gold.height
        ),
        divergence_manifest=_divergence_manifest(
            _DIVERGENCE_KEY, divergence_digest, divergences.height
        ),
        missing_sources=(),
        target_keys=(_TARGET_KEY,),
        generated_at=_GENERATED_AT,
    )


def test_documento_reproduz_a_fixture_congelada() -> None:
    store = _FakeObjectStore()
    request = _fixture_request(store)

    materialize_cnes(request, store)

    written = store.objects[_TARGET_KEY]
    expected = (_FIXTURES_DIR / "cnes-serving-v1.json").read_bytes()
    assert written == expected


def test_payload_tem_exatamente_quatro_campos() -> None:
    store = _FakeObjectStore()
    request = _fixture_request(store)

    result = materialize_cnes(request, store)

    assert set(result.documents[0].payload) == {
        "competencia",
        "kpis",
        "divergence_counts",
        "missing_sources",
    }


def test_documento_serializado_segue_ordem_serving_fields() -> None:
    store = _FakeObjectStore()
    request = _fixture_request(store)

    materialize_cnes(request, store)

    written = json.loads(store.objects[_TARGET_KEY])
    fixture = json.loads((_FIXTURES_DIR / "cnes-serving-v1.json").read_bytes())
    assert tuple(written) == SERVING_FIELDS
    assert tuple(written) == tuple(fixture)


def test_nenhum_valor_pessoal_no_documento() -> None:
    store = _FakeObjectStore()
    request = _fixture_request(store)

    materialize_cnes(request, store)

    rendered = store.objects[_TARGET_KEY].decode("utf-8")
    assert CPF_PATTERN.search(rendered) is None
    assert CNS_PATTERN.search(rendered) is None
    assert NAME_PATTERN.search(rendered) is None


def test_missing_sources_reflete_o_request() -> None:
    store = _FakeObjectStore()
    gold = pl.read_parquet(_FIXTURES_DIR / "cnes-gold-v2.parquet")
    local_only = gold.filter(pl.col("_source_manifest_ids").list.len() == 1).filter(
        pl.col("_source_manifest_ids").list.get(0) == "fixture-cnes-local-v1"
    )
    gold_digest = _put_frame(store, _RECONCILIATION_KEY, local_only)
    empty_divergences = pl.DataFrame([], schema=_DIVERGENCE_SCHEMA)
    divergence_digest = _put_frame(store, _DIVERGENCE_KEY, empty_divergences)
    request = MaterializeRequest(
        tenant_id=_TENANT,
        competencia=_COMPETENCIA,
        run_id=_RUN_ID,
        unit_id=_UNIT_ID,
        attempt=1,
        reconciliation_manifest=_reconciliation_manifest(
            _RECONCILIATION_KEY, gold_digest, local_only.height
        ),
        divergence_manifest=_divergence_manifest(_DIVERGENCE_KEY, divergence_digest, 0),
        missing_sources=("CNES_NACIONAL",),
        target_keys=(_TARGET_KEY,),
        generated_at=_GENERATED_AT,
    )

    result = materialize_cnes(request, store)

    payload = result.documents[0].payload
    assert payload["missing_sources"] == ["CNES_NACIONAL"]
    assert payload["kpis"]["match_count"] == 0
    assert payload["kpis"]["national_only_count"] == 0
    assert payload["kpis"]["local_only_count"] == local_only.height


def test_json_serializado_e_deterministico() -> None:
    store_a = _FakeObjectStore()
    store_b = _FakeObjectStore()
    request_a = _fixture_request(store_a)
    request_b = _fixture_request(store_b)

    result_a = materialize_cnes(request_a, store_a)
    result_b = materialize_cnes(request_b, store_b)

    assert result_a.manifests[0].object_sha256 == result_b.manifests[0].object_sha256


def test_manifest_de_serving_tem_layer_e_row_count() -> None:
    store = _FakeObjectStore()
    request = _fixture_request(store)

    result = materialize_cnes(request, store)

    manifest = result.manifests[0]
    document = result.documents[0]
    assert manifest.layer == "serving"
    assert manifest.source_type is None
    assert manifest.row_count == 1
    assert manifest.object_key == _TARGET_KEY
    assert manifest.schema_version == "cnes-serving-v1"
    assert document.document_name == "overview"
    assert "document_name" not in json.loads(store.objects[_TARGET_KEY])


def test_rejeita_target_keys_fora_do_padrao() -> None:
    store = _FakeObjectStore()
    store.objects[_RECONCILIATION_KEY] = b""
    store.objects[_DIVERGENCE_KEY] = b""
    request = _request(target_keys=(f"serving/{_TENANT}/{_RUN_ID}/wrong.json",))

    with pytest.raises(ValueError, match="unexpected_target_key"):
        materialize_cnes(request, store)


def test_rejeita_multiplos_target_keys() -> None:
    store = _FakeObjectStore()
    store.objects[_RECONCILIATION_KEY] = b""
    store.objects[_DIVERGENCE_KEY] = b""
    request = _request(
        target_keys=(
            _TARGET_KEY,
            f"serving/{_TENANT}/{_RUN_ID}/extra.json",
        )
    )

    with pytest.raises(ValueError, match="target_keys_must_be_single"):
        materialize_cnes(request, store)


def test_falha_quando_objeto_escrito_nao_e_encontrado() -> None:
    store = _BlindStatStore()
    request = _fixture_request(store)

    with pytest.raises(ValueError, match="output_not_found"):
        materialize_cnes(request, store)


def test_rejeita_ids_de_origem_ambiguos() -> None:
    store = _FakeObjectStore()
    gold = pl.DataFrame(
        [
            {
                "CPF": "90000000001", "CNS": "999000000000001", "NOME_PROFISSIONAL": "A",
                "NOME_SOCIAL": None, "SEXO": "F", "CBO": "225125", "CNES": "1234567",
                "TIPO_VINCULO": "01", "SUS": "S", "CH_TOTAL": 40, "CH_AMBULATORIAL": 20,
                "CH_OUTRAS": 0, "CH_HOSPITALAR": 20, "COMPETENCIA": _COMPETENCIA,
                "_source_manifest_ids": ["manifest-a"],
            },
            {
                "CPF": "90000000002", "CNS": "999000000000002", "NOME_PROFISSIONAL": "B",
                "NOME_SOCIAL": None, "SEXO": "F", "CBO": "225125", "CNES": "1234567",
                "TIPO_VINCULO": "01", "SUS": "S", "CH_TOTAL": 40, "CH_AMBULATORIAL": 20,
                "CH_OUTRAS": 0, "CH_HOSPITALAR": 20, "COMPETENCIA": _COMPETENCIA,
                "_source_manifest_ids": ["manifest-b"],
            },
        ],
    )
    gold_digest = _put_frame(store, _RECONCILIATION_KEY, gold)
    empty_divergences = pl.DataFrame([], schema=_DIVERGENCE_SCHEMA)
    divergence_digest = _put_frame(store, _DIVERGENCE_KEY, empty_divergences)
    request = MaterializeRequest(
        tenant_id=_TENANT,
        competencia=_COMPETENCIA,
        run_id=_RUN_ID,
        unit_id=_UNIT_ID,
        attempt=1,
        reconciliation_manifest=_reconciliation_manifest(
            _RECONCILIATION_KEY, gold_digest, gold.height
        ),
        divergence_manifest=_divergence_manifest(_DIVERGENCE_KEY, divergence_digest, 0),
        missing_sources=(),
        target_keys=(_TARGET_KEY,),
        generated_at=_GENERATED_AT,
    )

    with pytest.raises(ValueError, match="source_manifest_ids_ambiguous"):
        materialize_cnes(request, store)
