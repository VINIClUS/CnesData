"""TDD de normalize_sia: cinco subtipos, contabilidade exata e contrato de unidade."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from io import BytesIO

import polars as pl
import pytest
from pydantic import ValidationError

from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType
from cnes_contracts.manifests.validation import manifest_sha256
from cnes_domain.orchestration.source_definitions import sia as package_definition
from cnes_domain.ports.object_store import ObjectStat
from data_processor.sources.sia import contract
from data_processor.sources.sia.contract import (
    PROVENANCE_METADATA_KEY,
    SIA_LAYOUT,
    SiaContractError,
)
from data_processor.sources.sia.normalize import (
    QUALITY_SCHEMA_VERSION,
    SCHEMA_VERSIONS,
    normalize_sia,
)

from .conftest import FIXTURES_DIR, FakeObjectStore, SiaHarness

_SUBTYPES = [item.file_subtype for item in SIA_LAYOUT.normalized]
_FIXTURE_FILES = (
    "raw_manifests.json", "raw_rows.json", "expected_normalized.json", "expected_serving.json",
)


@dataclass
class _BlindStatStore(FakeObjectStore):
    def stat(self, key: str) -> ObjectStat | None:
        return None


@dataclass
class _LyingStatStore(FakeObjectStore):
    def stat(self, key: str) -> ObjectStat | None:
        return ObjectStat(key=key, size_bytes=1, sha256="0" * 64)


def _layout(subtype: str):
    return next(item for item in SIA_LAYOUT.normalized if item.file_subtype == subtype)


def _outputs(sia: SiaHarness, subtype: str, rows=None) -> tuple[pl.DataFrame, pl.DataFrame]:
    result = sia.normalize_fixture(subtype, rows)
    data_leaf, quality_leaf = _layout(subtype).normalized_filenames
    by_leaf = {manifest.object_key.rsplit("/", 1)[-1]: manifest for manifest in result.manifests}
    return (
        sia.read_frame(by_leaf[data_leaf].object_key),
        sia.read_frame(by_leaf[quality_leaf].object_key),
    )


def test_fixtures_congeladas_conferem_hash_e_contagem(sia: SiaHarness) -> None:
    frozen = sia.load_fixture("fixture-manifest.json")["files"]
    for name in _FIXTURE_FILES:
        digest = hashlib.sha256((FIXTURES_DIR / name).read_bytes()).hexdigest()
        assert digest == frozen[name]["sha256"], name
    raw_counts = {key: len(rows) for key, rows in sia.load_fixture("raw_rows.json").items()}
    assert raw_counts == frozen["raw_rows.json"]["row_counts"]


def test_contrato_do_app_reexporta_definicao_do_pacote_sem_duplicar() -> None:
    assert contract.SIA_LAYOUT is package_definition.SIA_LAYOUT
    assert contract.SIA_DEPENDENCIES is package_definition.SIA_DEPENDENCIES
    assert contract.SIA_DEFINITION is package_definition.SIA_DEFINITION
    assert len(SIA_LAYOUT.normalized) == len(package_definition.SIA_DEPENDENCIES)


@pytest.mark.parametrize("subtype", _SUBTYPES)
def test_normaliza_subtipo_igual_a_fixture_congelada(sia: SiaHarness, subtype: str) -> None:
    data, quality = _outputs(sia, subtype)

    expected = sia.load_fixture("expected_normalized.json")[subtype]
    assert sia.frame_rows(data) == expected["data"]
    assert sia.frame_rows(quality) == expected["quality"]


@pytest.mark.parametrize("subtype", _SUBTYPES)
def test_contabilidade_linhas_normalizadas_mais_qualidade_igual_entrada(
    sia: SiaHarness, subtype: str
) -> None:
    data, quality = _outputs(sia, subtype)

    entrada = len(sia.load_fixture("raw_rows.json")[subtype])
    assert data.height + quality.height == entrada
    assert set(quality["disposicao"].to_list()) <= {"rejeitada", "duplicata"}
    assert set(data["_source_row"]).isdisjoint(quality["_source_row"])


@pytest.mark.parametrize("subtype", _SUBTYPES)
def test_manifests_seguem_layout_por_folha_e_registram_hash_do_raw(
    sia: SiaHarness, subtype: str
) -> None:
    result = sia.normalize_fixture(subtype)
    raw_hash = manifest_sha256(sia.raws[-1])

    keys = [manifest.object_key for manifest in result.manifests]
    assert keys == sorted(keys)
    assert {key.rsplit("/", 1)[-1] for key in keys} == set(_layout(subtype).normalized_filenames)
    for manifest in result.manifests:
        payload = sia.store.objects[manifest.object_key]
        assert manifest.source_type is SourceType.SIA_LOCAL
        assert manifest.object_sha256 == hashlib.sha256(payload).hexdigest()
        assert manifest.row_count == pl.read_parquet(BytesIO(payload)).height
        assert manifest.schema_version in {SCHEMA_VERSIONS[subtype], QUALITY_SCHEMA_VERSION}
        metadata = pl.read_parquet_metadata(BytesIO(payload))
        assert json.loads(metadata[PROVENANCE_METADATA_KEY]) == [raw_hash]
        assert metadata["schema_version"] == manifest.schema_version


def test_normalizado_carrega_hash_do_raw_manifest_por_linha(sia: SiaHarness) -> None:
    data, quality = _outputs(sia, "SIA_APA")

    raw_hash = manifest_sha256(sia.raws[-1])
    assert set(data["_source_manifest_sha256"]) == {raw_hash}
    assert set(quality["_source_manifest_sha256"]) == {raw_hash}


def test_dbf_ausente_vira_parquet_zero_row_verificado(sia: SiaHarness) -> None:
    result = sia.normalize_fixture("SIA_BPIHST", [])

    assert [manifest.row_count for manifest in result.manifests] == [0, 0]
    data = sia.read_frame(result.manifests[0].object_key)
    assert data.height == 0
    assert "cod_procedimento" in data.columns
    metadata = pl.read_parquet_metadata(BytesIO(sia.store.objects[result.manifests[0].object_key]))
    assert json.loads(metadata[PROVENANCE_METADATA_KEY]) == [manifest_sha256(sia.raws[-1])]


def test_data_malformada_vira_null_com_flag_sem_retirar_linha(sia: SiaHarness) -> None:
    data, _ = _outputs(sia, "SIA_APA")

    row = data.filter(pl.col("_source_row") == 6).row(0, named=True)
    assert row["dt_inicio"] is None
    assert row["dt_fim"] is None
    assert row["dt_inicio_invalida"] is True
    assert row["dt_fim_invalida"] is True


def test_numericos_ja_clampados_pelo_edge_sao_preservados(sia: SiaHarness) -> None:
    data, _ = _outputs(sia, "SIA_APA")

    row = data.filter(pl.col("_source_row") == 1).row(0, named=True)
    assert row["quantidade"] == 2**31 - 1
    assert row["valor_aprovado_cents"] == 9_000_000_000


def test_linha_de_outra_competencia_vira_qualidade_rejeitada(sia: SiaHarness) -> None:
    _, quality = _outputs(sia, "SIA_BPIHST")

    assert quality.select("_source_row", "issue_code", "disposicao", "detalhe").rows() == [
        (2, "competencia_divergente", "rejeitada", "competencia=2025-12"),
    ]


@pytest.mark.parametrize(
    ("subtype", "column"), [("SIA_APA", "apa_cnspct"), ("SIA_BPI", "bpi_cnspac")]
)
def test_normalizado_nao_carrega_cns_do_paciente(
    sia: SiaHarness, subtype: str, column: str
) -> None:
    data, quality = _outputs(sia, subtype)

    patients = {row[column] for row in sia.load_fixture("raw_rows.json")[subtype]}
    for frame in (data, quality):
        assert not any("pac" in name or "pct" in name for name in frame.columns)
        values = {
            value for name in frame.columns if frame.schema[name] == pl.String
            for value in frame[name].to_list()
        }
        assert values.isdisjoint(patients)


@pytest.mark.parametrize("subtype", _SUBTYPES)
def test_grava_somente_as_duas_chaves_alvo(sia: SiaHarness, subtype: str) -> None:
    raw = sia.put_raw(subtype, sia.raw_frame(subtype, []))
    request = sia.normalize_request(raw)
    before = set(sia.store.objects)

    normalize_sia(request, sia.store)

    assert set(sia.store.objects) - before == set(request.target_keys)


def test_bytes_normalizados_sao_idempotentes(sia: SiaHarness) -> None:
    rows = sia.load_fixture("raw_rows.json")["SIA_APA"]
    raw = sia.put_raw("SIA_APA", sia.raw_frame("SIA_APA", rows))
    first = normalize_sia(sia.normalize_request(raw), sia.store)
    second = normalize_sia(sia.normalize_request(raw), sia.store)

    assert first == second


def test_rejeita_manifesto_de_competencia_cruzada(sia: SiaHarness) -> None:
    raw = sia.put_raw("SIA_APA", sia.raw_frame("SIA_APA", []))
    foreign = "normalized/354130/SIA_LOCAL/2025-12/run-sia-1"

    with pytest.raises(ValidationError):
        sia.normalize_request(raw, target_keys=(f"{foreign}/apa.parquet",
                                                f"{foreign}/quality_issues_sia_apa.parquet"))


def test_rejeita_unidade_com_subtipo_misto(sia: SiaHarness) -> None:
    apa = sia.put_raw("SIA_APA", sia.raw_frame("SIA_APA", []))
    bpi = sia.put_raw("SIA_BPI", sia.raw_frame("SIA_BPI", []))

    with pytest.raises(SiaContractError, match="sia_subtype_mixed subtypes=SIA_APA,SIA_BPI"):
        normalize_sia(sia.normalize_request(apa, raw_manifests=(apa, bpi)), sia.store)


def test_rejeita_subtipo_fora_das_dependencias(sia: SiaHarness) -> None:
    raw = sia.put_raw("SIA_APA", sia.raw_frame("SIA_APA", []))
    unknown = raw.model_copy(update={"file_subtype": "SIA_PRD"})

    with pytest.raises(SiaContractError, match="sia_subtype_unknown subtype=SIA_PRD"):
        normalize_sia(sia.normalize_request(raw, raw_manifests=(unknown,)), sia.store)


def test_rejeita_fonte_diferente_de_sia_local(sia: SiaHarness) -> None:
    raw = sia.put_raw("SIA_APA", sia.raw_frame("SIA_APA", []))
    request = sia.normalize_request(raw).model_copy(update={"source_type": SourceType.SIHD})

    with pytest.raises(SiaContractError, match="sia_source_type_invalid source_type=SIHD"):
        normalize_sia(request, sia.store)


def test_rejeita_cadeia_delta(sia: SiaHarness) -> None:
    full = sia.put_raw("SIA_APA", sia.raw_frame("SIA_APA", []))
    delta = RawManifest.model_validate_json(json.dumps({
        **json.loads(full.model_dump_json()),
        "manifest_id": "raw-sia-apa-2", "snapshot_mode": SnapshotMode.DELTA.value,
        "snapshot_id": "sia-apa-full-1", "base_snapshot_id": "sia-apa-full-1", "sequence": 2,
        "previous_manifest_sha256": manifest_sha256(full),
    }))

    with pytest.raises(SiaContractError, match="sia_delta_unsupported subtype=SIA_APA"):
        normalize_sia(sia.normalize_request(full, raw_manifests=(full, delta)), sia.store)


def test_rejeita_target_keys_fora_do_layout(sia: SiaHarness) -> None:
    raw = sia.put_raw("SIA_APA", sia.raw_frame("SIA_APA", []))
    prefix = "normalized/354130/SIA_LOCAL/2026-01/run-sia-1"
    request = sia.normalize_request(
        raw, target_keys=(f"{prefix}/apa.parquet", f"{prefix}/quality_issues_sia_bpi.parquet")
    )

    with pytest.raises(SiaContractError, match="sia_target_keys_invalid subtype=SIA_APA"):
        normalize_sia(request, sia.store)


def test_rejeita_raw_com_sha256_divergente_do_manifest(sia: SiaHarness) -> None:
    raw = sia.put_raw("SIA_APA", sia.raw_frame("SIA_APA", []))
    sia.store.objects[raw.object_key] += b"x"

    with pytest.raises(SiaContractError, match="input_sha256_mismatch"):
        normalize_sia(sia.normalize_request(raw), sia.store)


def test_rejeita_raw_com_schema_fora_do_contrato_edge(sia: SiaHarness) -> None:
    frame = sia.raw_frame("SIA_BPI", []).drop("bpi_proc")
    raw = sia.put_raw("SIA_BPI", frame)

    with pytest.raises(ValueError, match="sia_schema_invalid subtype=SIA_BPI column=bpi_proc"):
        normalize_sia(sia.normalize_request(raw), sia.store)


@pytest.mark.parametrize(
    ("store_type", "message"),
    [(_BlindStatStore, "output_not_found"), (_LyingStatStore, "output_sha256_mismatch")],
)
def test_falha_quando_put_nao_e_verificado_por_stat(store_type, message: str) -> None:
    sia = SiaHarness(store_type())
    raw = sia.put_raw("DIM_SIGTAP", sia.raw_frame("DIM_SIGTAP", []))

    with pytest.raises(SiaContractError, match=message):
        normalize_sia(sia.normalize_request(raw), sia.store)
