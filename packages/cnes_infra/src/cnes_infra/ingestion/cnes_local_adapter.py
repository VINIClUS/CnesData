"""Adapter: banco Firebird local → schema padronizado da camada de ingestão."""

import logging
import unicodedata

import polars as pl

from cnes_domain.contracts.columns import SCHEMA_EQUIPE, SCHEMA_ESTABELECIMENTO, SCHEMA_PROFISSIONAL
from cnes_infra.ingestion import cnes_client

logger = logging.getLogger(__name__)

_FONTE_LOCAL: str = "LOCAL"

_MAP_PROFISSIONAL: dict[str, str] = {
    "COD_CNES": "CNES",
    "COD_VINCULO": "TIPO_VINCULO",
    "SUS_NAO_SUS": "SUS",
    "CARGA_HORARIA_TOTAL": "CH_TOTAL",
}

_MAP_ESTABELECIMENTO: dict[str, str] = {
    "COD_CNES": "CNES",
    "ESTABELECIMENTO": "NOME_FANTASIA",
    "COD_TIPO_UNIDADE": "TIPO_UNIDADE",
    "COD_MUN_GESTOR": "COD_MUNICIPIO",
}

_MAP_EQUIPE: dict[str, str] = {
    "COD_INE_EQUIPE": "INE",
    "COD_TIPO_EQUIPE": "TIPO_EQUIPE",
    "COD_CNES": "CNES",
    "COD_MUN_GESTOR": "COD_MUNICIPIO",
}


def _normalizar_nfkd(s: str | None) -> str | None:
    if s is None:
        return None
    return unicodedata.normalize("NFKD", str(s))


class CnesLocalAdapter:
    """Adapter entre o cliente Firebird e o schema padronizado da ingestão."""

    def __init__(self, con: object) -> None:
        self._con = con
        self._cache: pl.DataFrame | None = None

    def listar_profissionais(
        self, competencia: tuple[int, int] | None = None,
    ) -> pl.DataFrame:
        """Retorna vínculos locais com colunas padronizadas (FONTE=LOCAL).

        Args:
            competencia: Ignorado — banco local não é particionado por competência.

        Returns:
            DataFrame conforme SCHEMA_PROFISSIONAL.
        """
        df = self._extrair().rename(_MAP_PROFISSIONAL)
        df = df.with_columns(
            pl.col("CNS").str.strip_chars(),
            pl.col("CPF").str.strip_chars(),
            pl.col("CNES").str.strip_chars().str.pad_start(7, "0"),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        for col in ("NOME_PROFISSIONAL", "NOME_SOCIAL"):
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).map_elements(
                        _normalizar_nfkd, return_dtype=pl.Utf8,
                    )
                )
        logger.debug("listar_profissionais fonte=LOCAL rows=%d", len(df))
        return df.select(list(SCHEMA_PROFISSIONAL))

    def listar_estabelecimentos(
        self, competencia: tuple[int, int] | None = None,
    ) -> pl.DataFrame:
        """Retorna estabelecimentos únicos derivados dos vínculos locais.

        Args:
            competencia: Ignorado — banco local não é particionado por competência.

        Returns:
            DataFrame conforme SCHEMA_ESTABELECIMENTO.
        """
        df = self._extrair()
        estab = df.select(
            list(_MAP_ESTABELECIMENTO.keys()),
        ).rename(_MAP_ESTABELECIMENTO)
        estab = estab.with_columns(
            pl.col("CNES").str.strip_chars().str.pad_start(7, "0"),
            pl.col("NOME_FANTASIA").map_elements(
                _normalizar_nfkd, return_dtype=pl.Utf8,
            ),
        ).unique(subset=["CNES"])
        estab = estab.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("CNPJ_MANTENEDORA"),
            pl.lit(None).cast(pl.Utf8).alias("NATUREZA_JURIDICA"),
            pl.lit(None).cast(pl.Utf8).alias("VINCULO_SUS"),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        logger.debug(
            "listar_estabelecimentos fonte=LOCAL rows=%d", len(estab),
        )
        return estab.select(list(SCHEMA_ESTABELECIMENTO))

    def listar_equipes(
        self, competencia: tuple[int, int] | None = None,
    ) -> pl.DataFrame:
        """Retorna equipes únicas derivadas dos vínculos locais.

        Args:
            competencia: Ignorado — banco local não é particionado por competência.

        Returns:
            DataFrame conforme SCHEMA_EQUIPE.
        """
        df = self._extrair()
        eq = df.select(
            "COD_INE_EQUIPE", "NOME_EQUIPE", "COD_TIPO_EQUIPE",
            "COD_CNES", "COD_MUN_GESTOR",
        ).rename(_MAP_EQUIPE)
        eq = eq.with_columns(pl.col("INE").cast(pl.Utf8))
        eq = eq.drop_nulls(subset=["INE"]).unique(subset=["INE"])
        eq = eq.with_columns(
            pl.col("INE").str.strip_chars(),
            pl.col("CNES").cast(pl.Utf8).str.strip_chars().str.pad_start(7, "0"),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        logger.debug("listar_equipes fonte=LOCAL rows=%d", len(eq))
        return eq.select(list(SCHEMA_EQUIPE))

    def _extrair(self) -> pl.DataFrame:
        if self._cache is None:
            self._cache = cnes_client.extrair_profissionais(self._con)
        return self._cache
