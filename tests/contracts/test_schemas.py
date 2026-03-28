import pandas as pd
import pandera as pa
import pytest

from contracts.schemas import EstabelecimentoContract, ProfissionalContract


def _df_prof_valido() -> pd.DataFrame:
    return pd.DataFrame({
        "CNS": ["123456789012345"],
        "CPF": ["12345678901"],
        "NOME_PROFISSIONAL": ["Ana Silva"],
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


def _df_estab_valido() -> pd.DataFrame:
    return pd.DataFrame({
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
    ProfissionalContract.validate(_df_prof_valido())


def test_profissional_cpf_nulo_nacional_passa():
    df = _df_prof_valido().copy()
    df["CPF"] = None
    df["FONTE"] = "NACIONAL"
    ProfissionalContract.validate(df)


def test_profissional_coluna_ausente_levanta_schema_error():
    df = _df_prof_valido().drop(columns=["CNS"])
    with pytest.raises(pa.errors.SchemaError):
        ProfissionalContract.validate(df)


def test_profissional_fonte_invalida_levanta_schema_error():
    df = _df_prof_valido().copy()
    df["FONTE"] = "DESCONHECIDA"
    with pytest.raises(pa.errors.SchemaError):
        ProfissionalContract.validate(df)


def test_profissional_sus_invalido_levanta_schema_error():
    df = _df_prof_valido().copy()
    df["SUS"] = "X"
    with pytest.raises(pa.errors.SchemaError):
        ProfissionalContract.validate(df)


def test_estabelecimento_valido_passa():
    EstabelecimentoContract.validate(_df_estab_valido())


def test_estabelecimento_coluna_ausente_levanta_schema_error():
    df = _df_estab_valido().drop(columns=["CNES"])
    with pytest.raises(pa.errors.SchemaError):
        EstabelecimentoContract.validate(df)


def test_estabelecimento_fonte_invalida_levanta_schema_error():
    df = _df_estab_valido().copy()
    df["FONTE"] = "ERRADO"
    with pytest.raises(pa.errors.SchemaError):
        EstabelecimentoContract.validate(df)


def test_estabelecimento_vinculo_sus_invalido_levanta_schema_error():
    df = _df_estab_valido().copy()
    df["VINCULO_SUS"] = "Z"
    with pytest.raises(pa.errors.SchemaError):
        EstabelecimentoContract.validate(df)
