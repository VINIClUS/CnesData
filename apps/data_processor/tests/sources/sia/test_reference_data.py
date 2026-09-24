"""TDD das referências SIA: S_CDN/CADMUN viram Parquet versionado, sem SQL."""

from __future__ import annotations

import ast
import inspect
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest

from data_processor.adapters import sia_dim_sync
from data_processor.sources.sia import reference_data
from data_processor.sources.sia.contract import SiaContractError
from data_processor.sources.sia.reference_data import normalize_reference

if TYPE_CHECKING:
    from .conftest import SiaHarness

_SQL_MODULES = ("sqlalchemy", "psycopg", "psycopg2")


def _imported_modules(module: object) -> set[str]:
    tree = ast.parse(Path(inspect.getfile(module)).read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)
    return names


@pytest.mark.parametrize("module", [sia_dim_sync, reference_data])
def test_adapter_de_referencia_nao_importa_sql(module: object) -> None:
    imported = _imported_modules(module)

    assert not any(name.split(".")[0] in _SQL_MODULES for name in imported)


@pytest.mark.parametrize(
    "function",
    [
        sia_dim_sync.build_reference_sigtap,
        sia_dim_sync.build_reference_municipio,
        normalize_reference,
    ],
)
def test_adapter_de_referencia_nao_aceita_engine(function: object) -> None:
    parameters = inspect.signature(function).parameters

    assert all("engine" not in name.lower() for name in parameters)
    assert all("Engine" not in str(item.annotation) for item in parameters.values())


def test_sigtap_segue_layout_oficial_tb_procedimento(sia: SiaHarness) -> None:
    raw = sia.raw_frame("DIM_SIGTAP", sia.load_fixture("raw_rows.json")["DIM_SIGTAP"])

    data, _ = normalize_reference("DIM_SIGTAP", raw, "2026-01")

    assert data.columns == [
        "_source_row", "cod_procedimento", "descricao", "complexidade", "financiamento",
        "competencia_sigtap",
    ]
    assert data.drop("_source_row").rows() == [
        ("0301010072", "CONSULTA MEDICA EM ATENÇÃO ESPECIALIZADA", "2", "06", "2026-01"),
        ("0301010064", "CONSULTA MEDICA EM ATENÇÃO PRIMÁRIA", "1", "01", "2026-01"),
    ]


def test_sigtap_retira_codigo_fora_de_10_digitos_e_duplicado(sia: SiaHarness) -> None:
    raw = sia.raw_frame("DIM_SIGTAP", sia.load_fixture("raw_rows.json")["DIM_SIGTAP"])

    data, quality = normalize_reference("DIM_SIGTAP", raw, "2026-01")

    assert data.height + quality.height == raw.height
    assert quality.rows() == [
        (1, "linha_duplicada", "duplicata", "primeira_linha=0"),
        (3, "codigo_procedimento_invalido", "rejeitada", "co_procedimento=03010100"),
        (4, "competencia_divergente", "rejeitada", "competencia=2026-09"),
    ]


def test_municipio_deriva_ibge6_e_ibge7_dos_layouts_de_4_6_e_7_digitos(sia: SiaHarness) -> None:
    raw = sia.raw_frame("DIM_MUNICIPIO", sia.load_fixture("raw_rows.json")["DIM_MUNICIPIO"])

    data, _ = normalize_reference("DIM_MUNICIPIO", raw, "2026-01")

    assert data.columns == ["_source_row", "ibge6", "ibge7", "uf", "nome"]
    assert data.select("ibge6", "ibge7").rows() == [
        ("354130", "3541307"),
        ("354140", "3541406"),
        ("355030", "3550308"),
    ]


def test_municipio_retira_codigo_invalido_digito_errado_e_duplicado(sia: SiaHarness) -> None:
    raw = sia.raw_frame("DIM_MUNICIPIO", sia.load_fixture("raw_rows.json")["DIM_MUNICIPIO"])

    data, quality = normalize_reference("DIM_MUNICIPIO", raw, "2026-01")

    assert data.height + quality.height == raw.height
    assert quality.rows() == [
        (3, "codigo_municipio_invalido", "rejeitada", "codmunic=35503"),
        (4, "linha_duplicada", "duplicata", "primeira_linha=0"),
        (5, "codigo_municipio_invalido", "rejeitada", "codmunic=3541300"),
    ]


def test_municipio_com_uf_nao_numerica_e_rejeitado() -> None:
    raw = pl.DataFrame({"coduf": ["SP"], "codmunic": ["4130"], "nome": ["X"]})

    data, quality = normalize_reference("DIM_MUNICIPIO", raw, "2026-01")

    assert data.height == 0
    assert quality["issue_code"].to_list() == ["codigo_municipio_invalido"]


def test_referencia_vazia_gera_frames_vazios_tipados(sia: SiaHarness) -> None:
    data, quality = normalize_reference("DIM_SIGTAP", sia.raw_frame("DIM_SIGTAP", []), "2026-01")

    assert data.height == quality.height == 0
    assert quality.schema == pl.Schema({
        "_source_row": pl.Int64, "issue_code": pl.String,
        "disposicao": pl.String, "detalhe": pl.String,
    })


def test_rejeita_referencia_com_schema_fora_do_contrato() -> None:
    raw = pl.DataFrame({"co_procedimento": [301010072], "no_procedimento": ["X"]})

    with pytest.raises(
        ValueError, match="sia_schema_invalid subtype=DIM_SIGTAP column=co_procedimento"
    ):
        normalize_reference("DIM_SIGTAP", raw, "2026-01")


def test_rejeita_subtipo_de_referencia_desconhecido() -> None:
    with pytest.raises(SiaContractError, match="sia_reference_unknown subtype=DIM_CID"):
        normalize_reference("DIM_CID", pl.DataFrame(), "2026-01")


@pytest.mark.parametrize(
    ("subtype", "data_leaf", "schema_version"),
    [
        ("DIM_SIGTAP", "reference_sigtap.parquet", "sia-reference-sigtap-v1"),
        ("DIM_MUNICIPIO", "reference_municipio.parquet", "sia-reference-municipio-v1"),
    ],
)
def test_referencia_vira_parquet_versionado_com_alvos_do_layout(
    sia: SiaHarness, subtype: str, data_leaf: str, schema_version: str
) -> None:
    result = sia.normalize_fixture(subtype)

    quality_manifest, data_manifest = result.manifests
    assert quality_manifest.object_key.endswith(f"quality_issues_{subtype.lower()}.parquet")
    assert data_manifest.object_key.endswith(f"/{data_leaf}")
    assert data_manifest.schema_version == schema_version
    assert quality_manifest.schema_version == "sia-quality-v1"
