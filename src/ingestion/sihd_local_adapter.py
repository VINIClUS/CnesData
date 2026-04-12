"""Adapter: banco SIHD2 Firebird local → schema padronizado de AIH."""

import logging

import pandas as pd

from ingestion import sihd_client
from ingestion.sihd_schemas import SCHEMA_AIH, SCHEMA_PROCEDIMENTO_AIH

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
        self._cache_aihs: pd.DataFrame | None = None
        self._cache_procs: pd.DataFrame | None = None

    def listar_aihs(self, competencia: str) -> pd.DataFrame:
        """Retorna AIHs com colunas padronizadas (FONTE=LOCAL).

        Args:
            competencia: Competência no formato AAAAMM.

        Returns:
            DataFrame conforme SCHEMA_AIH.
        """
        df = self._extrair_aihs(competencia)
        df = df.rename(columns=_MAP_AIH)
        df["CNES"] = df["CNES"].str.strip().str.zfill(7)
        df["NUM_AIH"] = df["NUM_AIH"].str.strip()
        df["PACIENTE_CNS"] = df["PACIENTE_CNS"].str.strip()
        df["FONTE"] = _FONTE_LOCAL
        logger.debug("listar_aihs fonte=LOCAL rows=%d", len(df))
        return df[list(SCHEMA_AIH)]

    def listar_procedimentos(self, competencia: str) -> pd.DataFrame:
        """Retorna procedimentos de AIH com colunas padronizadas.

        Args:
            competencia: Competência no formato AAAAMM.

        Returns:
            DataFrame conforme SCHEMA_PROCEDIMENTO_AIH.
        """
        df = self._extrair_procedimentos(competencia)
        df = df.rename(columns=_MAP_PROCEDIMENTO)
        df["NUM_AIH"] = df["NUM_AIH"].str.strip()
        df["FONTE"] = _FONTE_LOCAL
        logger.debug(
            "listar_procedimentos fonte=LOCAL rows=%d", len(df),
        )
        return df[list(SCHEMA_PROCEDIMENTO_AIH)]

    def _extrair_aihs(self, competencia: str) -> pd.DataFrame:
        if self._cache_aihs is None:
            self._cache_aihs = sihd_client.extrair_aihs(
                self._con, competencia,
            )
        return self._cache_aihs.copy()

    def _extrair_procedimentos(self, competencia: str) -> pd.DataFrame:
        if self._cache_procs is None:
            self._cache_procs = sihd_client.extrair_procedimentos(
                self._con, competencia,
            )
        return self._cache_procs.copy()
