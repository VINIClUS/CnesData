"""Testes de normalize_bpa: chaves naturais, cadeia por subtipo e qualidade explícita."""

from __future__ import annotations

import hashlib
import json
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest
from pydantic import ValidationError

from cnes_contracts.manifests.processing import NormalizeRequest
from cnes_contracts.manifests.raw import RawManifest, SourceType
from cnes_domain.ports.object_store import ObjectStat
from data_processor.sources.bpa.contract import BPA_DEPENDENCIES, BPA_LAYOUT
from data_processor.sources.bpa.normalize import normalize_bpa

if TYPE_CHECKING:
    from contextlib import AbstractContextManager as ContextManager
    from typing import BinaryIO

_FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "bpa"
_TENANT = "354130"
_COMPETENCIA = "2026-08"
_RUN_ID = "fixture-bpa-run-v1"
_NOW = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
_PREFIX = f"normalized/{_TENANT}/BPA_MAG/{_COMPETENCIA}/{_RUN_ID}"
_PII_COLUMNS = frozenset({
    "prd_cnsmed", "prd_cnspac", "prd_cpf_pcnte", "prd_nmpac", "prd_dtnasc",
    "nu_cns_prof", "nu_cns_pac", "nu_cpf_pac", "cns", "cpf",
})


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


@dataclass
class _BlindStatStore(_FakeObjectStore):
    def stat(self, key: str) -> ObjectStat | None:
        return None


@dataclass
class _CorruptStatStore(_FakeObjectStore):
    def stat(self, key: str) -> ObjectStat | None:
        return ObjectStat(key=key, size_bytes=1, sha256="0" * 64)


def _load(name: str) -> object:
    return json.loads((_FIXTURES / name).read_text(encoding="utf-8"))


def _raw_payload(rows: list[dict[str, object]]) -> bytes:
    output = BytesIO()
    frame = pl.DataFrame(rows, schema_overrides={"prd_qt_p": pl.Float64})
    frame.write_parquet(output, compression="zstd", compression_level=3)
    return output.getvalue()


def _put_raw(store: _FakeObjectStore, file_subtype: str,
             rows: list[dict[str, object]] | None = None) -> RawManifest:
    template = _load(f"raw_manifest_{file_subtype.lower()}.json")
    if rows is None:
        rows = _load("raw_rows.json")[file_subtype]
    payload = _raw_payload(rows)
    digest = hashlib.sha256(payload).hexdigest()
    store.put(template["object_key"], BytesIO(payload), digest)
    merged = {**template, "object_sha256": digest, "size_bytes": len(payload),
              "row_count": len(rows)}
    return RawManifest.model_validate_json(json.dumps(merged))


def _target_keys(file_subtype: str) -> tuple[str, ...]:
    layout = next(item for item in BPA_LAYOUT.normalized if item.file_subtype == file_subtype)
    return tuple(f"{_PREFIX}/{name}" for name in layout.normalized_filenames)


def _request(raw: tuple[RawManifest, ...], target_keys: tuple[str, ...],
             unit_id: str = "unit-bpa-c") -> NormalizeRequest:
    return NormalizeRequest(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id, attempt=1,
        source_type=SourceType.BPA_MAG, raw_manifests=raw, target_keys=target_keys,
        normalized_at=_NOW,
    )


def _read(store: _FakeObjectStore, key: str) -> list[dict[str, object]]:
    rows = pl.read_parquet(BytesIO(store.objects[key])).to_dicts()
    return [
        {name: value.isoformat() if isinstance(value, date) else value
         for name, value in row.items()}
        for row in rows
    ]


def _normalize(file_subtype: str) -> tuple[_FakeObjectStore, object]:
    store = _FakeObjectStore()
    raw = _put_raw(store, file_subtype)
    unit_id = f"unit-{file_subtype.lower().replace('_', '-')}"
    result = normalize_bpa(_request((raw,), _target_keys(file_subtype), unit_id), store)
    return store, result


def test_layout_bpa_tem_um_par_de_saidas_por_dependencia() -> None:
    assert [(item.source_type, item.file_subtype) for item in BPA_LAYOUT.normalized] == [
        (item.source_type, item.file_subtype) for item in BPA_DEPENDENCIES
    ]
    assert [item.normalized_filenames for item in BPA_LAYOUT.normalized] == [
        ("bpa_c.parquet", "quality_issues_bpa_c.parquet"),
        ("bpa_i.parquet", "quality_issues_bpa_i.parquet"),
    ]


def test_fixtures_bpa_batem_com_hashes_congelados() -> None:
    manifest = _load("fixture-manifest.json")
    for name, expected in manifest["files"].items():
        payload = (_FIXTURES / name).read_bytes()
        assert hashlib.sha256(payload).hexdigest() == expected["sha256"], name
        assert len(payload) == expected["bytes"], name
    raw_rows = _load("raw_rows.json")
    assert {key: len(rows) for key, rows in raw_rows.items()} == manifest["row_counts"]


@pytest.mark.parametrize("file_subtype", ["BPA_C", "BPA_I"])
def test_normaliza_bpa_c_e_bpa_i_sem_chaves_sql(file_subtype: str) -> None:
    store, result = _normalize(file_subtype)
    expected = _load("expected_normalized.json")[file_subtype]
    data_key, quality_key = _target_keys(file_subtype)

    assert _read(store, data_key) == expected["rows"]
    assert _read(store, quality_key) == expected["quality_issues"]
    assert not any(name.startswith("sk_") for name in expected["rows"][0])
    assert [item.object_key for item in result.manifests] == [data_key, quality_key]
    assert [item.row_count for item in result.manifests] == [
        len(expected["rows"]), len(expected["quality_issues"]),
    ]
    assert [item.schema_version for item in result.manifests] == [
        "bpa-normalized-v1", "bpa-quality-v1",
    ]
    for item in result.manifests:
        assert item.object_sha256 == hashlib.sha256(store.objects[item.object_key]).hexdigest()
        assert item.source_type is SourceType.BPA_MAG


@pytest.mark.parametrize("file_subtype", ["BPA_C", "BPA_I"])
def test_nenhuma_linha_fonte_e_pulada(file_subtype: str) -> None:
    store, _ = _normalize(file_subtype)
    raw_rows = _load("raw_rows.json")[file_subtype]

    rows = _read(store, _target_keys(file_subtype)[0])

    assert len(rows) == len(raw_rows)
    assert len({row["source_record_id"] for row in rows}) == len(rows)


def test_normalizado_nao_carrega_cns_nem_cpf() -> None:
    store, _ = _normalize("BPA_I")

    columns = set(pl.read_parquet(BytesIO(store.objects[_target_keys("BPA_I")[0]])).columns)

    assert not columns & _PII_COLUMNS
    payload = store.objects[_target_keys("BPA_I")[0]]
    assert b"999000000000001" not in payload
    assert b"PACIENTE TESTE" not in payload


def test_preserva_linha_com_referencia_desconhecida_como_divergencia() -> None:
    store, _ = _normalize("BPA_C")
    data_key, quality_key = _target_keys("BPA_C")

    rows = _read(store, data_key)
    issues = _read(store, quality_key)

    invalid = [row for row in rows if row["sigtap"] is None]
    assert len(invalid) == 1
    assert invalid[0]["valido"] is False
    assert invalid[0]["quantidade"] == 2
    record_id = invalid[0]["source_record_id"]
    matching = [item for item in issues if item["source_record_id"] == record_id]
    assert matching == [{
        "source_record_id": record_id, "file_subtype": "BPA_C",
        "field": "sigtap", "code": "sigtap_invalido", "raw_value": "03010100",
    }]


def test_source_record_id_e_deterministico_entre_execucoes() -> None:
    first, _ = _normalize("BPA_I")
    second, _ = _normalize("BPA_I")

    assert first.objects == second.objects


def test_rejeita_cadeia_com_subtipo_nao_declarado() -> None:
    store = _FakeObjectStore()
    raw = _put_raw(store, "BPA_C")
    foreign = raw.model_copy(update={"file_subtype": "BPA_X"})

    with pytest.raises(ValueError, match="bpa_subtipo_nao_declarado"):
        normalize_bpa(_request((foreign,), _target_keys("BPA_C")), store)


def test_rejeita_unidade_com_subtipo_misto() -> None:
    store = _FakeObjectStore()
    raw_c = _put_raw(store, "BPA_C")
    raw_i = _put_raw(store, "BPA_I")

    with pytest.raises(ValueError, match="bpa_subtipo_misto"):
        normalize_bpa(_request((raw_c, raw_i), _target_keys("BPA_C")), store)


def test_rejeita_source_type_diferente_de_bpa_mag() -> None:
    store = _FakeObjectStore()
    raw = _put_raw(store, "BPA_C")
    template = raw.model_dump(mode="json")
    template.update(source_type="SIHD",
                    object_key=f"raw/{_TENANT}/SIHD/{_COMPETENCIA}/fixture-bpa-c-v1/data.parquet")
    sihd = RawManifest.model_validate_json(json.dumps(template))
    keys = tuple(
        key.replace("/BPA_MAG/", "/SIHD/") for key in _target_keys("BPA_C")
    )
    request = NormalizeRequest(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id="unit-bpa-c", attempt=1,
        source_type=SourceType.SIHD, raw_manifests=(sihd,), target_keys=keys,
        normalized_at=_NOW,
    )

    with pytest.raises(ValueError, match="bpa_source_type_invalido"):
        normalize_bpa(request, store)


def test_rejeita_target_keys_de_outro_subtipo() -> None:
    store = _FakeObjectStore()
    raw = _put_raw(store, "BPA_C")

    with pytest.raises(ValueError, match="bpa_target_keys_invalidos"):
        normalize_bpa(_request((raw,), _target_keys("BPA_I")), store)


def test_rejeita_cadeia_cross_tenant_na_construcao_do_request() -> None:
    store = _FakeObjectStore()
    raw = _put_raw(store, "BPA_C")
    template = raw.model_dump(mode="json")
    template.update(tenant_id="355030",
                    object_key=f"raw/355030/BPA_MAG/{_COMPETENCIA}/fixture-bpa-c-v1/data.parquet")
    foreign = RawManifest.model_validate_json(json.dumps(template))

    with pytest.raises(ValidationError, match="raw_identity"):
        _request((foreign,), _target_keys("BPA_C"))


def test_rejeita_competencia_mista_na_construcao_do_request() -> None:
    store = _FakeObjectStore()
    raw_c = _put_raw(store, "BPA_C")
    template = _put_raw(store, "BPA_I").model_dump(mode="json")
    template.update(competencia="2026-07",
                    object_key=f"raw/{_TENANT}/BPA_MAG/2026-07/fixture-bpa-i-v1/data.parquet")
    other = RawManifest.model_validate_json(json.dumps(template))

    with pytest.raises(ValidationError, match="raw_identity"):
        _request((raw_c, other), _target_keys("BPA_C"))


def test_subtipo_sem_linhas_gera_parquet_vazio_verificado() -> None:
    store = _FakeObjectStore()
    raw = _put_raw(store, "BPA_I", rows=[])

    result = normalize_bpa(_request((raw,), _target_keys("BPA_I"), "unit-bpa-i"), store)

    assert [item.row_count for item in result.manifests] == [0, 0]
    for item in result.manifests:
        assert store.stat(item.object_key).sha256 == item.object_sha256
    frame = pl.read_parquet(BytesIO(store.objects[_target_keys("BPA_I")[0]]))
    assert frame.height == 0
    assert "source_record_id" in frame.columns


def test_aplica_delta_sobre_a_chave_de_linha() -> None:
    store = _FakeObjectStore()
    full = _put_raw(store, "BPA_C")
    changed = {**_load("raw_rows.json")["BPA_C"][0], "prd_qt_p": 12.0, "_op": "U"}
    payload = _raw_payload([changed])
    key = f"raw/{_TENANT}/BPA_MAG/{_COMPETENCIA}/fixture-bpa-c-d1/data.parquet"
    store.put(key, BytesIO(payload), hashlib.sha256(payload).hexdigest())
    delta = RawManifest.model_validate_json(json.dumps({
        **full.model_dump(mode="json"), "manifest_id": "fixture-bpa-c-d1",
        "snapshot_mode": "DELTA", "snapshot_id": "fixture-bpa-c-d1",
        "base_snapshot_id": full.snapshot_id, "sequence": 2,
        "previous_manifest_sha256": _manifest_sha(full), "row_count": 1,
        "object_key": key, "object_sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
    }))

    normalize_bpa(_request((full, delta), _target_keys("BPA_C")), store)

    rows = _read(store, _target_keys("BPA_C")[0])
    assert len(rows) == 7
    updated = next(row for row in rows if row["quantidade"] == 12)
    assert updated["_source_manifest_id"] == "fixture-bpa-c-d1"
    assert updated["_source_snapshot_id"] == "fixture-bpa-c-d1"
    untouched = [row for row in rows if row is not updated]
    assert {row["_source_manifest_id"] for row in untouched} == {"fixture-bpa-c-v1"}


def test_falha_quando_objeto_nao_aparece_apos_put() -> None:
    store = _BlindStatStore()
    raw = _put_raw(store, "BPA_C")

    with pytest.raises(ValueError, match="output_not_found"):
        normalize_bpa(_request((raw,), _target_keys("BPA_C")), store)


def test_falha_quando_sha256_relido_diverge() -> None:
    store = _CorruptStatStore()
    raw = _put_raw(store, "BPA_C")

    with pytest.raises(ValueError, match="output_sha256_mismatch"):
        normalize_bpa(_request((raw,), _target_keys("BPA_C")), store)


def _manifest_sha(manifest: RawManifest) -> str:
    from cnes_contracts.manifests.validation import manifest_sha256

    return manifest_sha256(manifest)


def test_rejeita_objeto_raw_divergente_do_sha256_do_manifesto() -> None:
    store = _FakeObjectStore()
    raw = _put_raw(store, "BPA_C")
    store.objects[raw.object_key] = _raw_payload(_load("raw_rows.json")["BPA_C"][:1])

    with pytest.raises(ValueError, match="input_sha256_mismatch"):
        normalize_bpa(_request((raw,), _target_keys("BPA_C")), store)


@pytest.mark.parametrize(
    ("op", "expected"), [("X", "op=X"), (None, "op=None"), ("omit", "op=missing")]
)
def test_rejeita_delta_com_operacao_cdc_invalida(op: str | None, expected: str) -> None:
    store = _FakeObjectStore()
    full = _put_raw(store, "BPA_C")
    changed = dict(_load("raw_rows.json")["BPA_C"][0])
    if op != "omit":
        changed["_op"] = op
    payload = _raw_payload([changed])
    key = f"raw/{_TENANT}/BPA_MAG/{_COMPETENCIA}/fixture-bpa-c-d1/data.parquet"
    store.put(key, BytesIO(payload), hashlib.sha256(payload).hexdigest())
    delta = RawManifest.model_validate_json(json.dumps({
        **full.model_dump(mode="json"), "manifest_id": "fixture-bpa-c-d1",
        "snapshot_mode": "DELTA", "snapshot_id": "fixture-bpa-c-d1",
        "base_snapshot_id": full.snapshot_id, "sequence": 2,
        "previous_manifest_sha256": _manifest_sha(full), "row_count": 1,
        "object_key": key, "object_sha256": hashlib.sha256(payload).hexdigest(),
        "size_bytes": len(payload),
    }))

    with pytest.raises(ValueError, match=f"invalid_cdc_op {expected}"):
        normalize_bpa(_request((full, delta), _target_keys("BPA_C")), store)


def test_normaliza_raw_parquet_gzip_emitido_pelo_edge() -> None:
    import gzip

    plain_store, _ = _normalize("BPA_C")
    store = _FakeObjectStore()
    template = _load("raw_manifest_bpa_c.json")
    payload = gzip.compress(_raw_payload(_load("raw_rows.json")["BPA_C"]), mtime=0)
    digest = hashlib.sha256(payload).hexdigest()
    store.put(template["object_key"], BytesIO(payload), digest)
    raw = RawManifest.model_validate_json(json.dumps(
        {**template, "object_sha256": digest, "size_bytes": len(payload)}
    ))

    normalize_bpa(_request((raw,), _target_keys("BPA_C"), "unit-bpa-c"), store)

    data_key = _target_keys("BPA_C")[0]
    assert _read(store, data_key) == _read(plain_store, data_key)
