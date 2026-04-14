"""Adapter: banco SIHD2 Firebird local → schema padronizado de AIH."""

import logging

import polars as pl

from cnes_domain.contracts.sihd_columns import SCHEMA_AIH, SCHEMA_PROCEDIMENTO_AIH
from cnes_infra.ingestion import sihd_client

logger = logging.getLogger(__name__)

_FONTE_LOCAL: str = "LOCAL"

_MAP_AIH: dict[str, str] = {
    "AH_NUM_AIH": "NUM_AIH",
    "AH_CNES": "CNES",
    "AH_CMPT": "COMPETENCIA",
    "AH_PACIENTE_NOME": "PACIENTE_NOME",
    "AH_PACIENTE_NUMERO_CNS": "PACIENTE_CNS",
    "AH_PACIENTE_SEXO": "PACIENTE_SEXO",
    "AH_PACIENTE_DT_NASCIMENTO": "PACIENTE_DT_NASCIMENTO",
    "AH_PACIENTE_MUN_ORIGEM": "PACIENTE_MUN_ORIGEM",
    "AH_DIAG_PRI": "DIAG_PRI",
    "AH_DIAG_SEC": "DIAG_SEC",
    "AH_PROC_SOLICITADO": "PROC_SOLICITADO",
    "AH_PROC_REALIZADO": "PROC_REALIZADO",
    "AH_DT_INTERNACAO": "DT_INTERNACAO",
    "AH_DT_SAIDA": "DT_SAIDA",
    "AH_MOT_SAIDA": "MOT_SAIDA",
    "AH_CAR_INTERNACAO": "CAR_INTERNACAO",
    "AH_ESPECIALIDADE": "ESPECIALIDADE",
    "AH_SITUACAO": "SITUACAO",
    "AH_MED_SOL_DOC": "MED_SOL_DOC",
    "AH_MED_RESP_DOC": "MED_RESP_DOC",
}

_MAP_PROCEDIMENTO: dict[str, str] = {
    "PA_NUM_AIH": "NUM_AIH",
    "PA_CMPT": "COMPETENCIA",
    "PA_PROCEDIMENTO": "PROCEDIMENTO",
    "PA_PROCEDIMENTO_QTD": "QTD",
    "PA_VALOR": "VALOR",
    "PA_PF_CBO": "CBO_EXEC",
    "PA_EXEC_DOC": "DOC_EXEC",
}


class SihdLocalAdapter:
    """Adapter entre o cliente SIHD2 Firebird e os schemas de AIH."""

    def __init__(self, con: object) -> None:
        self._con = con
        self._cache_aihs: pl.DataFrame | None = None
        self._cache_procs: pl.DataFrame | None = None

    def listar_aihs(self, competencia: str) -> pl.DataFrame:
        """Retorna AIHs com colunas padronizadas (FONTE=LOCAL).

        Args:
            competencia: Competência no formato AAAAMM.

        Returns:
            DataFrame conforme SCHEMA_AIH.
        """
        df = self._extrair_aihs(competencia)
        df = df.rename(_MAP_AIH)
        df = df.with_columns(
            pl.col("CNES").str.strip_chars().str.pad_start(7, "0"),
            pl.col("NUM_AIH").str.strip_chars(),
            pl.col("PACIENTE_CNS").str.strip_chars(),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        logger.debug("listar_aihs fonte=LOCAL rows=%d", len(df))
        return df.select(list(SCHEMA_AIH))

    def listar_procedimentos(self, competencia: str) -> pl.DataFrame:
        """Retorna procedimentos de AIH com colunas padronizadas.

        Args:
            competencia: Competência no formato AAAAMM.

        Returns:
            DataFrame conforme SCHEMA_PROCEDIMENTO_AIH.
        """
        df = self._extrair_procedimentos(competencia)
        df = df.rename(_MAP_PROCEDIMENTO)
        df = df.with_columns(
            pl.col("NUM_AIH").str.strip_chars(),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        logger.debug(
            "listar_procedimentos fonte=LOCAL rows=%d", len(df),
        )
        return df.select(list(SCHEMA_PROCEDIMENTO_AIH))

    def _extrair_aihs(self, competencia: str) -> pl.DataFrame:
        if self._cache_aihs is None:
            self._cache_aihs = sihd_client.extrair_aihs(
                self._con, competencia,
            )
        return self._cache_aihs.clone()

    def _extrair_procedimentos(self, competencia: str) -> pl.DataFrame:
        if self._cache_procs is None:
            self._cache_procs = sihd_client.extrair_procedimentos(
                self._con, competencia,
            )
        return self._cache_procs.clone()
