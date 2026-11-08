"""Camada de Processamento: Transformação e Validação."""

from typing import Final

import polars as pl

VALOR_SEM_EQUIPE: Final[str] = "SEM EQUIPE VINCULADA"
VALOR_SEM_INE: Final[str] = "-"

ALERTA_ATIVO_SEM_CH: Final[str] = "ATIVO_SEM_CH"
ALERTA_CH_OK: Final[str] = "OK"

_COLUNAS_TEXTO: Final[tuple[str, ...]] = (
    "CPF", "CNS", "NOME_PROFISSIONAL", "CBO", "CNES", "ESTABELECIMENTO",
    "NOME_SOCIAL", "SEXO", "TIPO_VINCULO", "SUS",
)

_MAPEAMENTO_NULOS_EQUIPE: Final[dict[str, str]] = {
    "NOME_EQUIPE": VALOR_SEM_EQUIPE,
    "INE": VALOR_SEM_INE,
    "TIPO_EQUIPE": VALOR_SEM_INE,
}

_SENTINELAS_NULO: set[str] = {"", "None", "nan", "NaN", "NaT"}


def _aplicar_rq002_validar_cpf(df: pl.DataFrame) -> pl.DataFrame:
    """RQ-002: Remove registros com CPF nulo ou comprimento != 11.

    Args:
        df: DataFrame com coluna CPF (Utf8, já stripada).

    Returns:
        DataFrame sem registros com CPF inválido.
    """
    if "CPF" not in df.columns:
        return df
    mascara_invalido = (
        pl.col("CPF").is_in(_SENTINELAS_NULO)
        | (pl.col("CPF").str.len_chars() != 11)
    )
    return df.filter(~mascara_invalido)


def _aplicar_rq003_flag_carga_horaria(df: pl.DataFrame) -> pl.DataFrame:
    """RQ-003: Sinaliza vínculos com carga horária total zero.

    Args:
        df: DataFrame com coluna numérica CH_TOTAL.

    Returns:
        DataFrame com nova coluna ALERTA_STATUS_CH.
    """
    df = df.with_columns(
        pl.when(pl.col("CH_TOTAL") == 0)
        .then(pl.lit(ALERTA_ATIVO_SEM_CH))
        .otherwise(pl.lit(ALERTA_CH_OK))
        .alias("ALERTA_STATUS_CH")
    )
    return df


def transformar(
    df: pl.DataFrame,
    cbo_lookup: dict[str, str] | None = None,
) -> pl.DataFrame:
    """Aplica strip, RQ-002 (CPF), RQ-003 (CH flag) e fillna de equipe.

    Args:
        df: DataFrame bruto de cnes_client.extrair_profissionais().
        cbo_lookup: Dict CBO → descrição; adiciona DESCRICAO_CBO.

    Returns:
        DataFrame transformado com ALERTA_STATUS_CH.
    """
    resultado = df.clone()

    strip_exprs = [
        pl.col(c).cast(pl.Utf8).str.strip_chars().alias(c)
        for c in _COLUNAS_TEXTO if c in resultado.columns
    ]
    if strip_exprs:
        resultado = resultado.with_columns(strip_exprs)

    if "CPF" in resultado.columns:
        resultado = resultado.with_columns(
            pl.when(pl.col("CPF").is_in(_SENTINELAS_NULO))
            .then(pl.col("CPF"))
            .otherwise(
                pl.when(pl.col("CPF").str.replace_all(r"\D", "") == "")
                .then(pl.lit(""))
                .otherwise(
                    pl.col("CPF")
                    .str.replace_all(r"\D", "")
                    .str.pad_start(11, "0")
                )
            )
            .alias("CPF")
        )

    resultado = _aplicar_rq002_validar_cpf(resultado)

    resultado = _aplicar_rq003_flag_carga_horaria(resultado)

    fill_exprs = [
        pl.col(col).fill_null(pl.lit(val))
        for col, val in _MAPEAMENTO_NULOS_EQUIPE.items()
        if col in resultado.columns
    ]
    if fill_exprs:
        resultado = resultado.with_columns(fill_exprs)

    if cbo_lookup is not None:
        resultado = resultado.with_columns(
            pl.col("CBO")
            .replace_strict(cbo_lookup, default="CBO NAO CATALOGADO")
            .alias("DESCRICAO_CBO")
        )

    return resultado
