"""Cliente de ingestão de planilhas de RH (folha de pagamento e ponto eletrônico)."""

import csv
import logging
from collections.abc import Iterator
from pathlib import Path

import pandas as pd

_ENCODING_CHAIN: tuple[str, ...] = ("utf-8-sig", "utf-8", "cp1252")

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


def _detectar_encoding(caminho: Path) -> str:
    for enc in _ENCODING_CHAIN:
        try:
            with open(caminho, encoding=enc) as f:
                f.read(512)
            return enc
        except UnicodeDecodeError:
            continue
    return "cp1252"


def _linhas_limpas(caminho: Path) -> Iterator[str]:
    """Gera linhas com null bytes removidos e encoding detectado automaticamente."""
    enc = _detectar_encoding(caminho)
    with open(caminho, encoding=enc, errors="replace", newline="") as f:
        for linha in f:
            yield linha.replace("\x00", "")


def _ler_arquivo(caminho: Path) -> pd.DataFrame:
    extensao = caminho.suffix.lower()
    if extensao not in _EXTENSOES_SUPORTADAS:
        raise HrSchemaError(
            f"extensão não suportada arquivo={caminho.name} extensao={extensao}"
        )
    if extensao == ".xlsx":
        return pd.read_excel(caminho)
    reader = csv.DictReader(_linhas_limpas(caminho))
    rows = list(reader)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


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
