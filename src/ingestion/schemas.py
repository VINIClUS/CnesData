"""Schemas padronizados da camada de ingestão.

Todos os repositórios devem retornar DataFrames com estas colunas exatas.
São o contrato de dados entre ingestão e análise — nenhum módulo de análise
deve conhecer nomes de colunas brutos de Firebird ou BigQuery.
"""

from typing import Final

SCHEMA_ESTABELECIMENTO: Final[tuple[str, ...]] = (
    "CNES",               # str — código CNES 7 dígitos (PK de cross-check)
    "NOME_FANTASIA",      # str (None na fonte nacional — indisponível no BigQuery)
    "TIPO_UNIDADE",       # str — código do tipo de unidade
    "CNPJ_MANTENEDORA",   # str — 14 dígitos (None quando indisponível)
    "NATUREZA_JURIDICA",  # str (None quando indisponível)
    "COD_MUNICIPIO",      # str — 6 dígitos (padrão local)
    "VINCULO_SUS",        # str — "S"/"N"
    "FONTE",              # str — "LOCAL" ou "NACIONAL"
)

SCHEMA_PROFISSIONAL: Final[tuple[str, ...]] = (
    "CNS",                # str — Cartão Nacional de Saúde 15 dígitos (PK de cross-check)
    "CPF",                # str — 11 dígitos (None na fonte nacional — indisponível no BigQuery)
    "NOME_PROFISSIONAL",  # str
    "SEXO",               # str — M/F (None na fonte nacional — indisponível no BigQuery)
    "CBO",                # str — 6 dígitos
    "CNES",               # str — estabelecimento vinculado
    "TIPO_VINCULO",       # str — IND_VINC / tipo_vinculo
    "SUS",                # str — "S"/"N"
    "CH_TOTAL",           # int — carga horária total
    "CH_AMBULATORIAL",    # int
    "CH_OUTRAS",          # int
    "CH_HOSPITALAR",      # int
    "FONTE",              # str — "LOCAL" ou "NACIONAL"
)

SCHEMA_EQUIPE: Final[tuple[str, ...]] = (
    "INE",            # str — Identificador Nacional da Equipe
    "NOME_EQUIPE",    # str
    "TIPO_EQUIPE",    # str — código do tipo
    "CNES",           # str — estabelecimento da equipe
    "COD_MUNICIPIO",  # str
    "FONTE",          # str — "LOCAL" ou "NACIONAL"
)
