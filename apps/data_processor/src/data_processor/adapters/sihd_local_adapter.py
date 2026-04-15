"""Adapter: raw Parquet SIHD (Firebird) para schema canonico de AIH."""

import logging

import polars as pl

from cnes_domain.contracts.sihd_columns import SCHEMA_AIH

logger = logging.getLogger(__name__)

_FONTE_LOCAL: str = "LOCAL"

_MAP_AIH_RAW: dict[str, str] = {
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


class SihdLocalAdapter:
    """Adapter entre raw Parquet SIHD e schema canonico de AIH."""

    def __init__(self, df: pl.DataFrame) -> None:
        self._df = df

    def listar_aihs(self) -> pl.DataFrame:
        """Retorna AIHs com colunas canonicas (FONTE=LOCAL).

        Returns:
            DataFrame conforme SCHEMA_AIH.
        """
        df = self._df.clone()
        df = df.rename(
            {k: v for k, v in _MAP_AIH_RAW.items() if k in df.columns},
        )
        df = df.with_columns(
            pl.col("CNES").cast(pl.Utf8).str.strip_chars().str.pad_start(
                7, "0",
            ),
            pl.col("NUM_AIH").cast(pl.Utf8).str.strip_chars(),
            pl.col("PACIENTE_CNS").cast(pl.Utf8).str.strip_chars(),
            pl.lit(_FONTE_LOCAL).alias("FONTE"),
        )
        logger.debug("listar_aihs fonte=LOCAL rows=%d", len(df))
        return df.select(list(SCHEMA_AIH))
