"""Pandera DataFrameModel contracts para os schemas canônicos de ingestão."""
import pandera as pa
from pandera.typing import Series


class ProfissionalContract(pa.DataFrameModel):
    """Contrato para SCHEMA_PROFISSIONAL — local e nacional."""

    CNS: Series[str]
    CPF: Series[str] = pa.Field(nullable=True)
    NOME_PROFISSIONAL: Series[str]
    CBO: Series[str]
    CNES: Series[str]
    TIPO_VINCULO: Series[str]
    SUS: Series[str] = pa.Field(isin=["S", "N"])
    CH_TOTAL: Series[int]
    CH_AMBULATORIAL: Series[int]
    CH_OUTRAS: Series[int]
    CH_HOSPITALAR: Series[int]
    FONTE: Series[str] = pa.Field(isin=["LOCAL", "NACIONAL"])

    class Config:
        strict = False
        coerce = False


class EstabelecimentoContract(pa.DataFrameModel):
    """Contrato para SCHEMA_ESTABELECIMENTO — local e nacional."""

    CNES: Series[str]
    NOME_FANTASIA: Series[str] = pa.Field(nullable=True)
    TIPO_UNIDADE: Series[str]
    CNPJ_MANTENEDORA: Series[str] = pa.Field(nullable=True)
    NATUREZA_JURIDICA: Series[str] = pa.Field(nullable=True)
    COD_MUNICIPIO: Series[str]
    VINCULO_SUS: Series[str] = pa.Field(isin=["S", "N"], nullable=True)
    FONTE: Series[str] = pa.Field(isin=["LOCAL", "NACIONAL"])

    class Config:
        strict = False
        coerce = False
