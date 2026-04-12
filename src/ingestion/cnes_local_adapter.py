"""Adapter: banco Firebird local → schema padronizado da camada de ingestão."""

import logging

import pandas as pd

from ingestion import cnes_client
from ingestion.schemas import SCHEMA_ESTABELECIMENTO, SCHEMA_PROFISSIONAL, SCHEMA_EQUIPE

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


class CnesLocalAdapter:
    """Adapter entre o cliente Firebird e o schema padronizado da ingestão."""

    def __init__(self, con: object) -> None:
        self._con = con
        self._cache: pd.DataFrame | None = None

    def listar_profissionais(self, competencia: tuple[int, int] | None = None) -> pd.DataFrame:
        """Retorna vínculos locais com colunas padronizadas (FONTE=LOCAL).

        Args:
            competencia: Ignorado — banco local não é particionado por competência.

        Returns:
            DataFrame conforme SCHEMA_PROFISSIONAL.
        """
        df = self._extrair().rename(columns=_MAP_PROFISSIONAL)
        for col in ("CNS", "CPF"):
            df[col] = df[col].str.strip()
        df["CNES"] = df["CNES"].str.strip().str.zfill(7)
        for col in ("NOME_PROFISSIONAL", "NOME_SOCIAL"):
            if col in df.columns:
                df[col] = df[col].str.normalize("NFKD")
        df["FONTE"] = _FONTE_LOCAL
        logger.debug("listar_profissionais fonte=LOCAL rows=%d", len(df))
        return df[list(SCHEMA_PROFISSIONAL)]

    def listar_estabelecimentos(self, competencia: tuple[int, int] | None = None) -> pd.DataFrame:
        """Retorna estabelecimentos únicos derivados dos vínculos locais.

        Args:
            competencia: Ignorado — banco local não é particionado por competência.

        Returns:
            DataFrame conforme SCHEMA_ESTABELECIMENTO.
        """
        df = self._extrair()
        estab = df[list(_MAP_ESTABELECIMENTO.keys())].rename(columns=_MAP_ESTABELECIMENTO)
        estab["CNES"] = estab["CNES"].str.strip().str.zfill(7)
        estab["NOME_FANTASIA"] = estab["NOME_FANTASIA"].str.normalize("NFKD")
        estab = estab.drop_duplicates("CNES")
        estab["CNPJ_MANTENEDORA"] = None
        estab["NATUREZA_JURIDICA"] = None
        estab["VINCULO_SUS"] = None
        estab["FONTE"] = _FONTE_LOCAL
        logger.debug("listar_estabelecimentos fonte=LOCAL rows=%d", len(estab))
        return estab[list(SCHEMA_ESTABELECIMENTO)]

    def listar_equipes(self, competencia: tuple[int, int] | None = None) -> pd.DataFrame:
        """Retorna equipes únicas derivadas dos vínculos locais.

        Args:
            competencia: Ignorado — banco local não é particionado por competência.

        Returns:
            DataFrame conforme SCHEMA_EQUIPE.
        """
        df = self._extrair()
        eq = (
            df[["COD_INE_EQUIPE", "NOME_EQUIPE", "COD_TIPO_EQUIPE", "COD_CNES", "COD_MUN_GESTOR"]]
            .rename(columns=_MAP_EQUIPE)
        )
        eq = eq.dropna(subset=["INE"]).drop_duplicates("INE")
        eq["INE"] = eq["INE"].str.strip()
        eq["CNES"] = eq["CNES"].str.strip().str.zfill(7)
        eq["FONTE"] = _FONTE_LOCAL
        logger.debug("listar_equipes fonte=LOCAL rows=%d", len(eq))
        return eq[list(SCHEMA_EQUIPE)]

    def _extrair(self) -> pd.DataFrame:
        if self._cache is None:
            self._cache = cnes_client.extrair_profissionais(self._con)
        return self._cache
