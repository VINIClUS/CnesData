"""Contrato SIHD: layout, colunas permitidas, dominios congelados e deny-list de PII."""

from __future__ import annotations

from typing import Final

import polars as pl

from cnes_domain.orchestration.source_definitions.sihd import (
    SIHD_DEFINITION,
    SIHD_DEPENDENCIES,
    SIHD_LAYOUT,
)

SUBTYPE_INTERNACAO: Final = "SIHD_INTERNACAO"
SUBTYPE_PROC_AIH: Final = "SIHD_PROC_AIH"
SUBTYPE_FILES: Final[dict[str, tuple[str, str]]] = {
    layout.file_subtype: (layout.normalized_filenames[0], layout.normalized_filenames[1])
    for layout in SIHD_LAYOUT.normalized
}

PROVENANCE_SCHEMA: Final[dict[str, type[pl.DataType]]] = {
    "_source_manifest_id": pl.String,
    "_source_snapshot_id": pl.String,
    "_source_type": pl.String,
    "_normalized_at": pl.String,
}
INTERNACAO_SOURCE_SCHEMA: Final[dict[str, type[pl.DataType]]] = {
    "NUM_AIH": pl.String, "OE_GESTOR": pl.String, "SEQ": pl.Int64, "CNES": pl.String,
    "COMPETENCIA": pl.String,
    "PROC_SOLICITADO": pl.String, "PROC_REALIZADO": pl.String, "DIAG_PRI": pl.String,
    "DIAG_SEC": pl.String, "DT_INTERNACAO": pl.String, "DT_SAIDA": pl.String,
    "CAR_INTERNACAO": pl.String, "SITUACAO": pl.String, "IDENT": pl.String,
    "MODALIDADE": pl.String, "PACIENTE_SEXO": pl.String, "PACIENTE_MUN_ORIGEM": pl.String,
}
PROC_AIH_SOURCE_SCHEMA: Final[dict[str, type[pl.DataType]]] = {
    "NUM_AIH": pl.String, "OE_GESTOR": pl.String, "SEQ_PRINC": pl.Int64, "INDX": pl.Int64,
    "CNES": pl.String,
    "COMPETENCIA": pl.String, "PROCEDIMENTO": pl.String, "QTD": pl.Int64,
    "VALOR": pl.String, "CBO_EXEC": pl.String,
}
INTERNACAO_SCHEMA: Final[dict[str, type[pl.DataType]]] = {
    "SIHD_KEY": pl.String,
    **INTERNACAO_SOURCE_SCHEMA,
    "DT_INTERNACAO": pl.Date,
    "DT_SAIDA": pl.Date,
    **PROVENANCE_SCHEMA,
}
PROC_AIH_SCHEMA: Final[dict[str, type[pl.DataType]]] = {
    "SIHD_KEY": pl.String,
    **{name: dtype for name, dtype in PROC_AIH_SOURCE_SCHEMA.items() if name != "VALOR"},
    "VALOR_CENTAVOS": pl.Int64,
    **PROVENANCE_SCHEMA,
}
QUALITY_SCHEMA: Final[dict[str, type[pl.DataType]]] = {
    "SIHD_KEY": pl.String,
    "field": pl.String,
    "value": pl.String,
    "issue_code": pl.String,
    "_source_manifest_id": pl.String,
}

PROCEDURE_PATTERN: Final = r"^[0-9]{10}$"
DATE_FORMAT: Final = "%Y%m%d"
INTERNACAO_DOMAINS: Final[dict[str, tuple[str, ...]]] = {
    "SITUACAO": ("0", "1"),
    "PACIENTE_SEXO": ("F", "M"),
    "IDENT": ("1", "3", "4", "5"),
    "MODALIDADE": ("02", "03", "04"),
}

PII_DENY_LIST: Final[frozenset[str]] = frozenset({
    "AH_PACIENTE_NOME", "AH_PACIENTE_NOME_MAE", "AH_PACIENTE_NOME_RESP",
    "AH_PACIENTE_DT_NASCIMENTO", "AH_PACIENTE_IDENT_DOC", "AH_PACIENTE_NUMERO_CNS",
    "AH_PACIENTE_NUMERO_DOC", "AH_PACIENTE_LOGR", "AH_PACIENTE_LOGR_BAIRRO",
    "AH_PACIENTE_LOGR_CEP", "AH_PACIENTE_LOGR_COMPL", "AH_PACIENTE_LOGR_MUNICIPIO",
    "AH_PACIENTE_LOGR_NUMERO", "AH_PACIENTE_LOGR_UF", "AH_PACIENTE_TIPO_LOGR",
    "AH_PACIENTE_TEL_DDD", "AH_PACIENTE_TEL_NUM", "AH_PRONTUARIO", "AH_MED_SOL_DOC",
    "AH_MED_RESP_DOC", "AH_AUTORIZADOR_DOC", "AH_DIR_CLINICO_DOC", "AH_GESTOR_DOC",
    "AH_ACDTRAB_CNPJ_EMP", "PA_PF_DOC", "PA_CREDITO_DOC", "PA_PJ_DOC", "PACIENTE_NOME",
    "PACIENTE_CNS", "PACIENTE_DT_NASCIMENTO", "MED_SOL_DOC", "MED_RESP_DOC", "DOC_EXEC",
    "paciente_nome", "paciente_cns", "cpf", "data_nascimento",
})

INTERNACAO_SCHEMA_VERSION: Final = "sihd-internacao-v1"
PROC_AIH_SCHEMA_VERSION: Final = "sihd-proc-aih-v1"
QUALITY_SCHEMA_VERSION: Final = "sihd-quality-v1"
RECONCILIATION_SCHEMA_VERSION: Final = "sihd-reconciliation-v1"
DIVERGENCE_SCHEMA_VERSION: Final = "sihd-divergence-v1"
SERVING_SCHEMA_VERSION: Final = "sihd-serving-v1"

__all__ = [
    "DATE_FORMAT",
    "DIVERGENCE_SCHEMA_VERSION",
    "INTERNACAO_DOMAINS",
    "INTERNACAO_SCHEMA",
    "INTERNACAO_SCHEMA_VERSION",
    "INTERNACAO_SOURCE_SCHEMA",
    "PII_DENY_LIST",
    "PROCEDURE_PATTERN",
    "PROC_AIH_SCHEMA",
    "PROC_AIH_SCHEMA_VERSION",
    "PROC_AIH_SOURCE_SCHEMA",
    "PROVENANCE_SCHEMA",
    "QUALITY_SCHEMA",
    "QUALITY_SCHEMA_VERSION",
    "RECONCILIATION_SCHEMA_VERSION",
    "SERVING_SCHEMA_VERSION",
    "SIHD_DEFINITION",
    "SIHD_DEPENDENCIES",
    "SIHD_LAYOUT",
    "SUBTYPE_FILES",
    "SUBTYPE_INTERNACAO",
    "SUBTYPE_PROC_AIH",
]
