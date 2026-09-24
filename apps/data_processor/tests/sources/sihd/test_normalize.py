"""TDD de normalize_sihd: chave estavel, qualidade explicita, PII fora, fixtures congeladas."""

from __future__ import annotations

import hashlib
import json

import polars as pl
import pytest

from apps.data_processor.tests.sources.sihd import (
    FIXTURES_DIR,
    NOW,
    RUN_ID,
    TENANT,
    BlindStatStore,
    FakeObjectStore,
    TamperedStatStore,
    fixture_request,
    json_rows,
    load_json,
    normalize_request,
    put_raw,
    raw_rows,
    raw_spec,
    read_parquet,
    target_keys,
)
from cnes_contracts.manifests.processing import NormalizeRequest
from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType
from cnes_contracts.manifests.validation import manifest_sha256
from data_processor.sources.sihd.contract import (
    PII_DENY_LIST,
    SIHD_LAYOUT,
    SUBTYPE_FILES,
)
from data_processor.sources.sihd.normalize import normalize_sihd

_SUBTYPES = ("SIHD_INTERNACAO", "SIHD_PROC_AIH")


def _normalized(subtype: str) -> tuple[FakeObjectStore, dict[str, pl.DataFrame]]:
    store = FakeObjectStore()
    result = normalize_sihd(fixture_request(store, subtype), store)
    frames = {
        manifest.object_key.rsplit("/", 1)[-1]: read_parquet(store, manifest.object_key)
        for manifest in result.manifests
    }
    return store, frames


def test_fixture_manifest_trava_hash_de_raw_e_expected() -> None:
    frozen = load_json("fixture-manifest.json")
    assert set(frozen["files"]) == {
        path.name for path in FIXTURES_DIR.glob("*.json") if path.name != "fixture-manifest.json"
    }
    for name, digest in frozen["files"].items():
        content = (FIXTURES_DIR / name).read_bytes().replace(b"\r\n", b"\n")
        assert hashlib.sha256(content).hexdigest() == digest, name


def test_layout_sihd_tem_um_par_de_arquivos_por_dependencia() -> None:
    assert SUBTYPE_FILES == {
        "SIHD_INTERNACAO": ("internacoes.parquet", "quality_issues_internacao.parquet"),
        "SIHD_PROC_AIH": ("procedimentos_aih.parquet", "quality_issues_proc_aih.parquet"),
    }
    assert len(SIHD_LAYOUT.normalized) == len(SUBTYPE_FILES)


@pytest.mark.parametrize("subtype", _SUBTYPES)
def test_normaliza_sihd_com_chave_estavel_e_proveniencia(subtype: str) -> None:
    _, frames = _normalized(subtype)
    expected = load_json("expected_normalized.json")
    for leaf in SUBTYPE_FILES[subtype]:
        assert json_rows(frames[leaf]) == expected[leaf], leaf


@pytest.mark.parametrize("subtype", _SUBTYPES)
def test_normalizacao_e_deterministica_em_bytes(subtype: str) -> None:
    first, _ = _normalized(subtype)
    second, _ = _normalized(subtype)
    for key in target_keys(subtype):
        assert first.objects[key] == second.objects[key]


def test_registra_codigo_referencia_desconhecido_sem_descartar_linha() -> None:
    _, frames = _normalized("SIHD_PROC_AIH")
    data = frames["procedimentos_aih.parquet"]
    quality = frames["quality_issues_proc_aih.parquet"]
    assert data.height == len(raw_rows("SIHD_PROC_AIH"))
    unknown = quality.filter(pl.col("issue_code") == "codigo_desconhecido")
    assert unknown.select("field", "value").rows() == [("PROCEDIMENTO", "04110100")]
    assert unknown["SIHD_KEY"][0] in data["SIHD_KEY"].to_list()


@pytest.mark.parametrize("subtype", _SUBTYPES)
def test_normalizado_nao_carrega_pii(subtype: str) -> None:
    _, frames = _normalized(subtype)
    denied = {name.lower() for name in PII_DENY_LIST}
    pii_values = {
        str(row[name]) for row in raw_rows(subtype) for name in row
        if name in PII_DENY_LIST and row[name] is not None and len(str(row[name])) > 2
    }
    assert pii_values
    for frame in frames.values():
        assert not denied & {column.lower() for column in frame.columns}
        rendered = json.dumps(json_rows(frame))
        assert not [value for value in pii_values if value in rendered]


@pytest.mark.parametrize("subtype", _SUBTYPES)
def test_manifests_verificados_e_ordenados_por_chave(subtype: str) -> None:
    store = FakeObjectStore()
    result = normalize_sihd(fixture_request(store, subtype), store)
    assert tuple(item.object_key for item in result.manifests) == target_keys(subtype)
    assert len({item.manifest_id for item in result.manifests}) == 2
    for manifest in result.manifests:
        assert manifest.object_sha256 == store.stat(manifest.object_key).sha256
        assert manifest.row_count == read_parquet(store, manifest.object_key).height


def test_subtipo_sem_linhas_gera_parquet_vazio_e_manifest() -> None:
    store = FakeObjectStore()
    manifest = put_raw(store, raw_spec("SIHD_PROC_AIH"), pl.DataFrame())
    result = normalize_sihd(normalize_request((manifest,), "SIHD_PROC_AIH"), store)
    assert [item.row_count for item in result.manifests] == [0, 0]
    data = read_parquet(store, result.manifests[0].object_key)
    assert data.height == 0
    assert "VALOR_CENTAVOS" in data.columns


def test_rejeita_manifesto_sihd_de_competencia_divergente() -> None:
    store = FakeObjectStore()
    rows = raw_rows("SIHD_INTERNACAO")
    rows[1]["AH_CMPT"] = "202512"
    manifest = put_raw(store, raw_spec("SIHD_INTERNACAO"), pl.DataFrame(rows))
    with pytest.raises(ValueError, match="competencia_divergente"):
        normalize_sihd(normalize_request((manifest,), "SIHD_INTERNACAO"), store)
    assert not any(key.startswith("normalized/") for key in store.objects)


def test_rejeita_subtipo_fora_das_dependencias() -> None:
    store = FakeObjectStore()
    spec = raw_spec("SIHD_PROC_AIH") | {"file_subtype": "SIHD_DESCONHECIDO"}
    manifest = put_raw(store, spec, pl.DataFrame(raw_rows("SIHD_PROC_AIH")))
    request = normalize_request((manifest,), "SIHD_PROC_AIH")
    with pytest.raises(ValueError, match="unexpected_file_subtype"):
        normalize_sihd(request, store)


def test_rejeita_unidade_com_mais_de_um_subtipo() -> None:
    store = FakeObjectStore()
    internacao = fixture_request(store, "SIHD_INTERNACAO").raw_manifests
    proc = fixture_request(store, "SIHD_PROC_AIH").raw_manifests
    request = normalize_request((*internacao, *proc), "SIHD_PROC_AIH")
    with pytest.raises(ValueError, match="single_subtype_required"):
        normalize_sihd(request, store)


def test_rejeita_target_keys_de_outro_subtipo() -> None:
    store = FakeObjectStore()
    manifests = fixture_request(store, "SIHD_PROC_AIH").raw_manifests
    request = normalize_request(manifests, "SIHD_PROC_AIH", target_keys("SIHD_INTERNACAO"))
    with pytest.raises(ValueError, match="unexpected_target_keys"):
        normalize_sihd(request, store)


def test_rejeita_source_type_diferente_de_sihd() -> None:
    store = FakeObjectStore()
    spec = raw_spec("SIHD_PROC_AIH") | {
        "source_type": "BPA_MAG",
        "object_key": "raw/354130/BPA_MAG/2026-01/snap-proc-aih-1/proc_aih.parquet",
    }
    manifest = put_raw(store, spec, pl.DataFrame(raw_rows("SIHD_PROC_AIH")))
    keys = tuple(key.replace("/SIHD/", "/BPA_MAG/") for key in target_keys("SIHD_PROC_AIH"))
    request = NormalizeRequest(
        tenant_id=TENANT, run_id=RUN_ID, unit_id="unit-proc-aih", attempt=1,
        source_type=SourceType.BPA_MAG, raw_manifests=(manifest,), target_keys=keys,
        normalized_at=NOW,
    )
    with pytest.raises(ValueError, match="unexpected_source_type"):
        normalize_sihd(request, store)


def test_rejeita_chave_sihd_duplicada_em_vez_de_colapsar() -> None:
    store = FakeObjectStore()
    rows = raw_rows("SIHD_PROC_AIH")
    manifest = put_raw(store, raw_spec("SIHD_PROC_AIH"), pl.DataFrame([*rows, rows[0]]))
    with pytest.raises(ValueError, match="duplicate_sihd_key"):
        normalize_sihd(normalize_request((manifest,), "SIHD_PROC_AIH"), store)


@pytest.mark.parametrize(
    ("store_type", "message"),
    [(BlindStatStore, "output_not_found"), (TamperedStatStore, "output_sha256_mismatch")],
)
def test_rejeita_saida_nao_verificada_apos_put(
    store_type: type[FakeObjectStore], message: str
) -> None:
    store = store_type()
    with pytest.raises(ValueError, match=message):
        normalize_sihd(fixture_request(store, "SIHD_PROC_AIH"), store)


def test_aplica_cadeia_full_mais_delta_sem_ler_so_a_base() -> None:
    store = FakeObjectStore()
    spec = raw_spec("SIHD_INTERNACAO")
    rows = raw_rows("SIHD_INTERNACAO")
    full = put_raw(store, spec, pl.DataFrame(rows))
    updated = rows[1] | {"AH_SITUACAO": "0", "_op": "U"}
    deleted = rows[3] | {"_op": "D"}
    delta_spec = spec | {
        "manifest_id": "raw-sihd-internacao-2",
        "snapshot_mode": SnapshotMode.DELTA.value,
        "snapshot_id": "snap-internacao-2",
        "base_snapshot_id": full.snapshot_id,
        "sequence": 2,
        "previous_manifest_sha256": manifest_sha256(full),
        "object_key": "raw/354130/SIHD/2026-01/snap-internacao-2/internacao.parquet",
    }
    delta = put_raw(store, delta_spec, pl.DataFrame([updated, deleted]))
    result = normalize_sihd(normalize_request((full, delta), "SIHD_INTERNACAO"), store)
    data = read_parquet(store, result.manifests[0].object_key)
    quality = read_parquet(store, result.manifests[1].object_key)
    assert data["SEQ"].to_list() == [1, 2, 3]
    assert data["SITUACAO"].to_list() == ["0", "0", "0"]
    assert quality.height == 0


def _delta_spec(full: RawManifest) -> dict[str, object]:
    return raw_spec("SIHD_INTERNACAO") | {
        "manifest_id": "raw-sihd-internacao-2",
        "snapshot_mode": SnapshotMode.DELTA.value,
        "snapshot_id": "snap-internacao-2",
        "base_snapshot_id": full.snapshot_id,
        "sequence": 2,
        "previous_manifest_sha256": manifest_sha256(full),
        "object_key": "raw/354130/SIHD/2026-01/snap-internacao-2/internacao.parquet",
    }


def test_delta_de_um_gestor_preserva_mesmo_seq_de_outro_gestor() -> None:
    store = FakeObjectStore()
    rows = raw_rows("SIHD_INTERNACAO")
    other = rows[0] | {"AH_OE_GESTOR": "3541300009", "AH_NUM_AIH": "3526100000055"}
    full = put_raw(store, raw_spec("SIHD_INTERNACAO"), pl.DataFrame([*rows, other]))
    delta = put_raw(store, _delta_spec(full), pl.DataFrame([rows[0] | {"_op": "U"}]))
    result = normalize_sihd(normalize_request((full, delta), "SIHD_INTERNACAO"), store)
    data = read_parquet(store, result.manifests[0].object_key)
    assert data.filter(pl.col("SEQ") == 1)["OE_GESTOR"].sort().to_list() == [
        "3541300000", "3541300009",
    ]


@pytest.mark.parametrize("op", ["X", None])
def test_rejeita_delta_com_operacao_cdc_invalida(op: str | None) -> None:
    store = FakeObjectStore()
    rows = raw_rows("SIHD_INTERNACAO")
    full = put_raw(store, raw_spec("SIHD_INTERNACAO"), pl.DataFrame(rows))
    delta_rows = [rows[0] | {"_op": "U"}, rows[1] | {"_op": op}]
    delta = put_raw(store, _delta_spec(full), pl.DataFrame(delta_rows))
    with pytest.raises(ValueError, match="invalid_cdc_op"):
        normalize_sihd(normalize_request((full, delta), "SIHD_INTERNACAO"), store)


def test_rejeita_delta_sem_coluna_de_operacao() -> None:
    store = FakeObjectStore()
    rows = raw_rows("SIHD_INTERNACAO")
    full = put_raw(store, raw_spec("SIHD_INTERNACAO"), pl.DataFrame(rows))
    delta = put_raw(store, _delta_spec(full), pl.DataFrame(rows[:1]))
    with pytest.raises(ValueError, match="invalid_cdc_op op=missing"):
        normalize_sihd(normalize_request((full, delta), "SIHD_INTERNACAO"), store)
