"""Cliente de ingestão de planilhas de RH (folha de pagamento e ponto eletrônico)."""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_EXTENSOES_SUPORTADAS: frozenset[str] = frozenset({".xlsx", ".csv"})

COLUNAS_OBRIGATORIAS_FOLHA: frozenset[str] = frozenset({"CPF", "NOME", "STATUS"})
COLUNAS_OBRIGATORIAS_PONTO: frozenset[str] = frozenset({"CPF", "NOME", "STATUS"})


class HrSchemaError(Exception):
    """Schema do arquivo de RH inválido ou extensão não suportada."""


def carregar_folha(caminho: Path) -> pd.DataFrame:
    """Carrega e valida planilha de folha de pagamento.

    Args:
        caminho: Caminho para arquivo .xlsx ou .csv.

    Returns:
        DataFrame com CPF normalizado (sem pontuação).

    Raises:
        HrSchemaError: Extensão não suportada ou colunas obrigatórias ausentes.
    """
    df = _ler_arquivo(caminho)
    _validar_schema(df, COLUNAS_OBRIGATORIAS_FOLHA, caminho.name)
    resultado = _normalizar_cpf(df.copy())
    _logar_cpf_invalido(resultado, caminho.name)
    logger.info("carregar_folha arquivo=%s rows=%d", caminho.name, len(resultado))
    return resultado


def carregar_ponto(caminho: Path) -> pd.DataFrame:
    """Carrega e valida planilha de ponto eletrônico.

    Args:
        caminho: Caminho para arquivo .xlsx ou .csv.

    Returns:
        DataFrame com CPF normalizado (sem pontuação).

    Raises:
        HrSchemaError: Extensão não suportada ou colunas obrigatórias ausentes.
    """
    df = _ler_arquivo(caminho)
    _validar_schema(df, COLUNAS_OBRIGATORIAS_PONTO, caminho.name)
    resultado = _normalizar_cpf(df.copy())
    _logar_cpf_invalido(resultado, caminho.name)
    logger.info("carregar_ponto arquivo=%s rows=%d", caminho.name, len(resultado))
    return resultado


def _ler_arquivo(caminho: Path) -> pd.DataFrame:
    extensao = caminho.suffix.lower()
    if extensao not in _EXTENSOES_SUPORTADAS:
        raise HrSchemaError(
            f"extensão não suportada arquivo={caminho.name} extensao={extensao}"
        )
    if extensao == ".xlsx":
        return pd.read_excel(caminho)
    return pd.read_csv(caminho)


def _validar_schema(
    df: pd.DataFrame,
    colunas_obrigatorias: frozenset[str],
    fonte: str,
) -> None:
    ausentes = colunas_obrigatorias - set(df.columns)
    if ausentes:
        raise HrSchemaError(
            f"colunas_ausentes={sorted(ausentes)} fonte={fonte}"
        )


def _normalizar_cpf(df: pd.DataFrame) -> pd.DataFrame:
    df["CPF"] = (
        df["CPF"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace("-", "", regex=False)
        .str.strip()
        .where(df["CPF"].notna(), other=None)
    )
    return df


def _logar_cpf_invalido(df: pd.DataFrame, fonte: str) -> None:
    invalidos = df[
        df["CPF"].isna() | (df["CPF"].astype(str).str.len() != 11)
    ]
    for idx in invalidos.index:
        logger.warning("cpf_invalido fonte=%s idx=%d", fonte, idx)
