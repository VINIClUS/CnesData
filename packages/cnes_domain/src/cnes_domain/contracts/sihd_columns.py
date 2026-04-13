"""Schemas padronizados do subsistema SIHD (AIH)."""

from typing import Final

SCHEMA_AIH: Final[tuple[str, ...]] = (
    "NUM_AIH",
    "CNES",
    "COMPETENCIA",
    "PACIENTE_NOME",
    "PACIENTE_CNS",
    "PACIENTE_SEXO",
    "PACIENTE_DT_NASCIMENTO",
    "PACIENTE_MUN_ORIGEM",
    "DIAG_PRI",
    "DIAG_SEC",
    "PROC_SOLICITADO",
    "PROC_REALIZADO",
    "DT_INTERNACAO",
    "DT_SAIDA",
    "MOT_SAIDA",
    "CAR_INTERNACAO",
    "ESPECIALIDADE",
    "SITUACAO",
    "MED_SOL_DOC",
    "MED_RESP_DOC",
    "FONTE",
)

SCHEMA_PROCEDIMENTO_AIH: Final[tuple[str, ...]] = (
    "NUM_AIH",
    "COMPETENCIA",
    "PROCEDIMENTO",
    "QTD",
    "VALOR",
    "CBO_EXEC",
    "DOC_EXEC",
    "FONTE",
)
