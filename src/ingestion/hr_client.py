"""Cliente de ingestão de planilhas de RH (folha de pagamento e ponto eletrônico)."""

import csv
import logging
from collections.abc import Iterator
from pathlib import Path

import polars as pl

_ENCODING_CHAIN: tuple[str, ...] = ("utf-8-sig", "utf-8", "cp1252")

logger = logging.getLogger(__name__)

_EXTENSOES_SUPORTADAS: frozenset[str] = frozenset({".xlsx", ".csv"})

COLUNAS_OBRIGATORIAS_FOLHA: frozenset[str] = frozenset(
    {"CPF", "NOME", "STATUS"},
)
COLUNAS_OBRIGATORIAS_PONTO: frozenset[str] = frozenset(
    {"CPF", "NOME", "STATUS"},
)


class HrSchemaError(Exception):
    """Schema do arquivo de RH inválido ou extensão não suportada."""


def carregar_folha(caminho: Path) -> pl.DataFrame:
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
    resultado = _normalizar_cpf(df.clone())
    _logar_cpf_invalido(resultado, caminho.name)
    logger.info(
        "carregar_folha arquivo=%s rows=%d", caminho.name, len(resultado),
    )
    return resultado


def carregar_ponto(caminho: Path) -> pl.DataFrame:
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
    resultado = _normalizar_cpf(df.clone())
    _logar_cpf_invalido(resultado, caminho.name)
    logger.info(
        "carregar_ponto arquivo=%s rows=%d", caminho.name, len(resultado),
    )
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
    enc = _detectar_encoding(caminho)
    with open(caminho, encoding=enc, errors="replace", newline="") as f:
        for linha in f:
            yield linha.replace("\x00", "")


def _ler_arquivo(caminho: Path) -> pl.DataFrame:
    extensao = caminho.suffix.lower()
    if extensao not in _EXTENSOES_SUPORTADAS:
        raise HrSchemaError(
            f"extensão não suportada arquivo={caminho.name} "
            f"extensao={extensao}"
        )
    if extensao == ".xlsx":
        return pl.read_excel(caminho)
    reader = csv.DictReader(_linhas_limpas(caminho))
    rows = list(reader)
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def _validar_schema(
    df: pl.DataFrame,
    colunas_obrigatorias: frozenset[str],
    fonte: str,
) -> None:
    ausentes = colunas_obrigatorias - set(df.columns)
    if ausentes:
        raise HrSchemaError(
            f"colunas_ausentes={sorted(ausentes)} fonte={fonte}"
        )


def _normalizar_cpf(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("CPF")
        .cast(pl.Utf8)
        .str.replace_all(r"\.", "")
        .str.replace_all("-", "")
        .str.strip_chars()
    )


def _logar_cpf_invalido(df: pl.DataFrame, fonte: str) -> None:
    invalidos = df.filter(
        pl.col("CPF").is_null()
        | (pl.col("CPF").cast(pl.Utf8).str.len_chars() != 11)
    )
    for i in range(len(invalidos)):
        logger.warning("cpf_invalido fonte=%s idx=%d", fonte, i)
