"""Testes do row_mapper."""

import polars as pl
import pytest

from cnes_domain.processing.row_mapper import (
    _nan_to_none,
    extrair_fonte,
    mapear_estabelecimentos,
    mapear_profissionais,
    mapear_vinculos,
)
from cnes_domain.tenant import set_tenant_id


@pytest.fixture(autouse=True)
def _tenant():
    set_tenant_id("355030")


@pytest.fixture
def df_prof() -> pl.DataFrame:
    return pl.DataFrame({
        "CPF": ["11122233344", "11122233344", "55566677788"],
        "CNS": ["700000000000001", "700000000000001", "700000000000002"],
        "NOME_PROFISSIONAL": ["ANA", "ANA", "JOAO"],
        "SEXO": ["F", "F", "M"],
        "FONTE": ["LOCAL", "LOCAL", "LOCAL"],
    })


@pytest.fixture
def df_vinc() -> pl.DataFrame:
    return pl.DataFrame({
        "CPF": ["11122233344"],
        "CNES": ["0985333"],
        "CBO": ["225142"],
        "TIPO_VINCULO": ["010101"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [40],
        "CH_OUTRAS": [0],
        "CH_HOSPITALAR": [0],
        "FONTE": ["LOCAL"],
    })


@pytest.fixture
def df_estab() -> pl.DataFrame:
    return pl.DataFrame({
        "CNES": ["0985333", "0985334"],
        "NOME_FANTASIA": ["UBS A", "UBS B"],
        "TIPO_UNIDADE": ["02", "01"],
        "CNPJ_MANTENEDORA": ["55293427000117", "55293427000117"],
        "NATUREZA_JURIDICA": ["1023", "1023"],
        "VINCULO_SUS": ["S", "N"],
        "FONTE": ["LOCAL", "LOCAL"],
    })


def test_mapear_profissionais_dedup_cpf(df_prof: pl.DataFrame):
    rows = mapear_profissionais(df_prof)
    cpfs = [r["cpf"] for r in rows]
    assert len(cpfs) == 2
    assert "11122233344" in cpfs
    assert "55566677788" in cpfs


def test_mapear_profissionais_tenant_id_injetado(df_prof: pl.DataFrame):
    rows = mapear_profissionais(df_prof)
    assert all(r["tenant_id"] == "355030" for r in rows)


def test_mapear_profissionais_fonte_jsonb(df_prof: pl.DataFrame):
    rows = mapear_profissionais(df_prof)
    assert all(r["fontes"] == {"LOCAL": True} for r in rows)


def test_mapear_vinculos_sus_s_true(df_vinc: pl.DataFrame):
    rows = mapear_vinculos("2025-01", df_vinc)
    assert rows[0]["sus"] is True


def test_mapear_vinculos_sus_n_false(df_vinc: pl.DataFrame):
    modified = df_vinc.with_columns(pl.lit("N").alias("SUS"))
    rows = mapear_vinculos("2025-01", modified)
    assert rows[0]["sus"] is False


def test_mapear_vinculos_sus_none_none(df_vinc: pl.DataFrame):
    modified = df_vinc.with_columns(pl.lit(None).cast(pl.Utf8).alias("SUS"))
    rows = mapear_vinculos("2025-01", modified)
    assert rows[0]["sus"] is None


def test_mapear_vinculos_competencia_injetada(df_vinc: pl.DataFrame):
    rows = mapear_vinculos("2025-01", df_vinc)
    assert rows[0]["competencia"] == "2025-01"


def test_mapear_estabelecimentos_vinculo_sus_booleano(
    df_estab: pl.DataFrame,
):
    rows = mapear_estabelecimentos(df_estab)
    by_cnes = {r["cnes"]: r for r in rows}
    assert by_cnes["0985333"]["vinculo_sus"] is True
    assert by_cnes["0985334"]["vinculo_sus"] is False


def test_extrair_fonte_mista_levanta_erro():
    df = pl.DataFrame({"FONTE": ["LOCAL", "NACIONAL"]})
    with pytest.raises(ValueError, match="fonte_mista"):
        extrair_fonte(df)


def test_extrair_fonte_vazio_levanta_erro():
    df = pl.DataFrame({"FONTE": []})
    with pytest.raises(ValueError, match="dataframe_vazio"):
        extrair_fonte(df)


def test_nan_convertido_para_none():
    rows = [{"a": 1, "b": float("nan"), "c": "ok"}]
    result = _nan_to_none(rows)
    assert result[0]["a"] == 1
    assert result[0]["b"] is None
    assert result[0]["c"] == "ok"
