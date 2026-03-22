"""Adapter: BigQuery (basedosdados) → schema padronizado da camada de ingestão."""

import logging

import pandas as pd

from ingestion.schemas import SCHEMA_ESTABELECIMENTO, SCHEMA_PROFISSIONAL
from ingestion.web_client import CnesWebClient

logger = logging.getLogger(__name__)

_FONTE_NACIONAL: str = "NACIONAL"

_MAP_ESTABELECIMENTO: dict[str, str] = {
    "id_estabelecimento_cnes": "CNES",
    "cnpj_mantenedora": "CNPJ_MANTENEDORA",
    "id_natureza_juridica": "NATUREZA_JURIDICA",
    "tipo_unidade": "TIPO_UNIDADE",
    "indicador_vinculo_sus": "VINCULO_SUS",
    "id_municipio_6": "COD_MUNICIPIO",
}

_MAP_PROFISSIONAL: dict[str, str] = {
    "id_estabelecimento_cnes": "CNES",
    "cartao_nacional_saude": "CNS",
    "nome": "NOME_PROFISSIONAL",
    "cbo_2002": "CBO",
    "tipo_vinculo": "TIPO_VINCULO",
    "indicador_atende_sus": "SUS",
    "carga_horaria_ambulatorial": "CH_AMBULATORIAL",
    "carga_horaria_outros": "CH_OUTRAS",
    "carga_horaria_hospitalar": "CH_HOSPITALAR",
}


class CnesNacionalAdapter:
    """Adapter entre o BigQuery (basedosdados) e o schema padronizado da ingestão."""

    def __init__(self, billing_project_id: str, id_municipio: str) -> None:
        self._client = CnesWebClient(billing_project_id)
        self._id_municipio = id_municipio

    def listar_estabelecimentos(self, competencia: tuple[int, int] | None = None) -> pd.DataFrame:
        """Retorna estabelecimentos nacionais com colunas padronizadas (FONTE=NACIONAL).

        Args:
            competencia: (ano, mes) obrigatório para o adapter nacional.

        Returns:
            DataFrame conforme SCHEMA_ESTABELECIMENTO.

        Raises:
            ValueError: Se competencia for None.
        """
        if competencia is None:
            raise ValueError("nacional_adapter competencia=obrigatoria")
        ano, mes = competencia
        df = self._client.fetch_estabelecimentos(self._id_municipio, ano, mes)
        df = df.rename(columns=_MAP_ESTABELECIMENTO).copy()
        df["NOME_FANTASIA"] = None
        df["VINCULO_SUS"] = df["VINCULO_SUS"].map({1: "S", 0: "N"}).fillna("N")
        df["FONTE"] = _FONTE_NACIONAL
        logger.debug("listar_estabelecimentos fonte=NACIONAL rows=%d", len(df))
        return df[list(SCHEMA_ESTABELECIMENTO)]

    def listar_profissionais(self, competencia: tuple[int, int] | None = None) -> pd.DataFrame:
        """Retorna vínculos nacionais com colunas padronizadas (FONTE=NACIONAL).

        Args:
            competencia: (ano, mes) obrigatório para o adapter nacional.

        Returns:
            DataFrame conforme SCHEMA_PROFISSIONAL.

        Raises:
            ValueError: Se competencia for None.
        """
        if competencia is None:
            raise ValueError("nacional_adapter competencia=obrigatoria")
        ano, mes = competencia
        df = self._client.fetch_profissionais(self._id_municipio, ano, mes)
        df = df.rename(columns=_MAP_PROFISSIONAL).copy()
        df["CPF"] = None
        df["SUS"] = df["SUS"].map({1: "S", 0: "N"}).fillna("N")
        df["CH_TOTAL"] = (
            df["CH_AMBULATORIAL"].fillna(0)
            + df["CH_OUTRAS"].fillna(0)
            + df["CH_HOSPITALAR"].fillna(0)
        ).astype(int)
        df["FONTE"] = _FONTE_NACIONAL
        logger.debug("listar_profissionais fonte=NACIONAL rows=%d", len(df))
        return df[list(SCHEMA_PROFISSIONAL)]
