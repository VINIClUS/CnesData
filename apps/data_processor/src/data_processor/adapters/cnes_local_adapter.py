"""Adapter: raw Parquet local (Firebird) para schema canonico."""

import logging
import unicodedata

import polars as pl

from cnes_domain.contracts.columns import (
    SCHEMA_EQUIPE,
    SCHEMA_ESTABELECIMENTO,
    SCHEMA_PROFISSIONAL,
)

logger = logging.getLogger(__name__)

_FONTE_LOCAL: str = "LOCAL"

_MAP_PROFISSIONAL_RAW: dict[str, str] = {
    "CPF_PROF": "CPF",
    "COD_CNS": "CNS",
    "NOME_PROF": "NOME_PROFISSIONAL",
    "NO_SOCIAL": "NOME_SOCIAL",
    "DATA_NASC": "DATA_NASCIMENTO",
    "COD_CBO": "CBO",
    "IND_VINC": "TIPO_VINCULO",
    "TP_SUS_NAO_SUS": "SUS",
    "CARGA_HORARIA_TOTAL": "CH_TOTAL",
    "CG_HORAAMB": "CH_AMBULATORIAL",
    "CGHORAOUTR": "CH_OUTRAS",
    "CGHORAHOSP": "CH_HOSPITALAR",
    "NOME_FANTA": "ESTABELECIMENTO",
    "TP_UNID_ID": "TIPO_UNIDADE",
    "CODMUNGEST": "COD_MUNICIPIO",
}

_MAP_ESTABELECIMENTO_RAW: dict[str, str] = {
    "NOME_FANTA": "NOME_FANTASIA",
    "TP_UNID_ID": "TIPO_UNIDADE",
    "CODMUNGEST": "COD_MUNICIPIO",
    "CNPJ_MANT": "CNPJ_MANTENEDORA",
}

_MAP_EQUIPE_RAW: dict[str, str] = {
    "DS_AREA": "NOME_EQUIPE",
    "TP_EQUIPE": "TIPO_EQUIPE",
    "COD_MUN": "COD_MUNICIPIO",
}


def _normalizar_nfkd(serie: pl.Expr) -> pl.Expr:
    return serie.map_elements(
        lambda v: unicodedata.normalize("NFKD", v) if v else v,
        return_dtype=pl.Utf8,
    )


class CnesLocalAdapter:
    """Adapter entre raw Parquet (Firebird) e schema canonico."""

    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def listar_profissionais(self) -> pl.DataFrame:
        """Retorna profissionais com colunas canonicas (FONTE=LOCAL).

        Returns:
            DataFrame conforme SCHEMA_PROFISSIONAL.
        """
        df = self._df.clone()
        df = df.rename(
            {k: v for k, v in _MAP_PROFISSIONAL_RAW.items() if k in df.columns},
        )
        df = df.with_columns(
            pl.col("CPF").cast(pl.Utf8).str.strip_chars(),
            pl.col("CNS").cast(pl.Utf8).str.strip_chars(),
            pl.col("CNES").cast(pl.Utf8).str.strip_chars().str.pad_start(7, "0"),
            _normalizar_nfkd(pl.col("NOME_PROFISSIONAL")).alias(
                "NOME_PROFISSIONAL",
            ),
            _normalizar_nfkd(pl.col("NOME_SOCIAL")).alias("NOME_SOCIAL"),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        df = df.drop("ESTABELECIMENTO", "TIPO_UNIDADE", "COD_MUNICIPIO")
        logger.debug("listar_profissionais fonte=LOCAL rows=%d", len(df))
        return df.select(list(SCHEMA_PROFISSIONAL))

    def listar_estabelecimentos(self) -> pl.DataFrame:
        """Retorna estabelecimentos com colunas canonicas (FONTE=LOCAL).

        Returns:
            DataFrame conforme SCHEMA_ESTABELECIMENTO.
        """
        df = self._df.clone()
        df = df.rename(
            {k: v for k, v in _MAP_ESTABELECIMENTO_RAW.items() if k in df.columns},
        )
        df = df.with_columns(
            pl.col("CNES").cast(pl.Utf8).str.strip_chars().str.pad_start(7, "0"),
            pl.lit(None).cast(pl.Utf8).alias("NATUREZA_JURIDICA"),
            pl.lit(None).cast(pl.Utf8).alias("VINCULO_SUS"),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        logger.debug(
            "listar_estabelecimentos fonte=LOCAL rows=%d", len(df),
        )
        return df.select(list(SCHEMA_ESTABELECIMENTO))

    def listar_equipes(self) -> pl.DataFrame:
        """Retorna equipes com colunas canonicas (FONTE=LOCAL).

        Returns:
            DataFrame conforme SCHEMA_EQUIPE.
        """
        df = self._df.clone()
        df = df.rename(
            {k: v for k, v in _MAP_EQUIPE_RAW.items() if k in df.columns},
        )
        cnes_col = "SEQ_EQUIPE" if "SEQ_EQUIPE" in df.columns else "CNES"
        df = df.with_columns(
            pl.col(cnes_col).cast(pl.Utf8).str.strip_chars().str.pad_start(
                7, "0",
            ).alias("CNES"),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        logger.debug("listar_equipes fonte=LOCAL rows=%d", len(df))
        return df.select(list(SCHEMA_EQUIPE))
