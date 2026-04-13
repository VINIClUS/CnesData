import pandera as pa
import polars as pl
import pytest
from cnes_domain.contracts.schemas import EstabelecimentoContract, ProfissionalContract


def _df_prof_valido() -> pl.DataFrame:
    return pl.DataFrame({
        "CNS": ["123456789012345"],
        "CPF": ["12345678901"],
        "NOME_PROFISSIONAL": ["Ana Silva"],
        "SEXO": ["F"],
        "CBO": ["515105"],
        "CNES": ["1234567"],
        "TIPO_VINCULO": ["30"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [20],
        "CH_OUTRAS": [10],
        "CH_HOSPITALAR": [10],
        "FONTE": ["LOCAL"],
    })


def _df_estab_valido() -> pl.DataFrame:
    return pl.DataFrame({
        "CNES": ["1234567"],
        "NOME_FANTASIA": ["UBS Centro"],
        "TIPO_UNIDADE": ["01"],
        "CNPJ_MANTENEDORA": ["55293427000117"],
        "NATUREZA_JURIDICA": ["1023"],
        "COD_MUNICIPIO": ["354130"],
        "VINCULO_SUS": ["S"],
        "FONTE": ["LOCAL"],
    })


def test_profissional_valido_passa():
    ProfissionalContract.validate(_df_prof_valido().to_pandas())


def test_profissional_cpf_nulo_nacional_passa():
    df = _df_prof_valido().clone()
    df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("CPF"))
    df = df.with_columns(pl.lit("NACIONAL").alias("FONTE"))
    ProfissionalContract.validate(df.to_pandas())


def test_profissional_coluna_ausente_levanta_schema_error():
    df = _df_prof_valido().drop("CNS")
    with pytest.raises(pa.errors.SchemaError):
        ProfissionalContract.validate(df.to_pandas())


def test_profissional_fonte_invalida_levanta_schema_error():
    df = _df_prof_valido().clone()
    df = df.with_columns(pl.lit("DESCONHECIDA").alias("FONTE"))
    with pytest.raises(pa.errors.SchemaError):
        ProfissionalContract.validate(df.to_pandas())


def test_profissional_sus_invalido_levanta_schema_error():
    df = _df_prof_valido().clone()
    df = df.with_columns(pl.lit("X").alias("SUS"))
    with pytest.raises(pa.errors.SchemaError):
        ProfissionalContract.validate(df.to_pandas())


def test_estabelecimento_valido_passa():
    EstabelecimentoContract.validate(_df_estab_valido().to_pandas())


def test_estabelecimento_coluna_ausente_levanta_schema_error():
    df = _df_estab_valido().drop("CNES")
    with pytest.raises(pa.errors.SchemaError):
        EstabelecimentoContract.validate(df.to_pandas())


def test_estabelecimento_fonte_invalida_levanta_schema_error():
    df = _df_estab_valido().clone()
    df = df.with_columns(pl.lit("ERRADO").alias("FONTE"))
    with pytest.raises(pa.errors.SchemaError):
        EstabelecimentoContract.validate(df.to_pandas())


def test_estabelecimento_vinculo_sus_invalido_levanta_schema_error():
    df = _df_estab_valido().clone()
    df = df.with_columns(pl.lit("Z").alias("VINCULO_SUS"))
    with pytest.raises(pa.errors.SchemaError):
        EstabelecimentoContract.validate(df.to_pandas())


def test_profissional_contract_aceita_sexo_nullable():
    df = _df_prof_valido().clone()
    df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("SEXO"))
    ProfissionalContract.validate(df.to_pandas(), lazy=False)
