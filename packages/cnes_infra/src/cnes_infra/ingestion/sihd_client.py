"""Camada de Ingestão: Cliente do Banco SIHD2 Firebird (AIH)."""

import logging
from pathlib import Path

import fdb
import polars as pl

from cnes_infra.ingestion import config_sihd

logger = logging.getLogger(__name__)

_SQL_AIHS = """
    SELECT
        AH_NUM_AIH,
        AH_CNES,
        AH_CMPT,
        AH_PACIENTE_NOME,
        AH_PACIENTE_NUMERO_CNS,
        AH_PACIENTE_SEXO,
        AH_PACIENTE_DT_NASCIMENTO,
        AH_PACIENTE_MUN_ORIGEM,
        AH_DIAG_PRI,
        AH_DIAG_SEC,
        AH_PROC_SOLICITADO,
        AH_PROC_REALIZADO,
        AH_DT_INTERNACAO,
        AH_DT_SAIDA,
        AH_MOT_SAIDA,
        AH_CAR_INTERNACAO,
        AH_ESPECIALIDADE,
        AH_SITUACAO,
        AH_MED_SOL_DOC,
        AH_MED_RESP_DOC,
        AH_OE_GESTOR,
        AH_SEQ
    FROM TB_HAIH
    WHERE AH_CMPT = '{competencia}'
    ORDER BY AH_NUM_AIH
"""

_SQL_PROCEDIMENTOS = """
    SELECT
        PA_NUM_AIH,
        PA_CMPT,
        PA_PROCEDIMENTO,
        PA_PROCEDIMENTO_QTD,
        PA_VALOR,
        PA_PF_CBO,
        PA_EXEC_DOC
    FROM TB_HPA
    WHERE PA_CMPT = '{competencia}'
    ORDER BY PA_SEQ_PRINC, PA_INDX
"""


def _carregar_driver(dll_path: Path) -> None:
    if not dll_path.exists():
        raise FileNotFoundError(f"dll_nao_encontrada path={dll_path}")
    fdb.load_api(str(dll_path))


def conectar() -> fdb.Connection:
    """Abre conexão com o banco SIHD2 Firebird.

    Returns:
        fdb.Connection ativa.

    Raises:
        FileNotFoundError: DLL do Firebird não encontrada.
        fdb.fbcore.DatabaseError: Falha na conexão.
    """
    _carregar_driver(Path(config_sihd.SIHD_FIREBIRD_DLL))
    con: fdb.Connection = fdb.connect(
        dsn=config_sihd.SIHD_DB_DSN,
        user=config_sihd.SIHD_DB_USER,
        password=config_sihd.SIHD_DB_PASSWORD,
        charset="WIN1252",
    )
    logger.info("conexao_sihd=ok")
    return con


def extrair_aihs(
    con: fdb.Connection, competencia: str,
) -> pl.DataFrame:
    """Extrai AIHs processadas (TB_HAIH) para uma competência.

    Args:
        con: Conexão ativa com o banco SIHD2.
        competencia: Competência no formato AAAAMM.

    Returns:
        DataFrame com colunas de TB_HAIH.
    """
    sql = _SQL_AIHS.format(competencia=competencia)
    df = _executar_query(con, sql)
    logger.info(
        "extrair_aihs competencia=%s rows=%d", competencia, len(df),
    )
    return df


def extrair_procedimentos(
    con: fdb.Connection, competencia: str,
) -> pl.DataFrame:
    """Extrai procedimentos de AIH (TB_HPA) para uma competência.

    Args:
        con: Conexão ativa com o banco SIHD2.
        competencia: Competência no formato AAAAMM.

    Returns:
        DataFrame com colunas de TB_HPA.
    """
    sql = _SQL_PROCEDIMENTOS.format(competencia=competencia)
    df = _executar_query(con, sql)
    logger.info(
        "extrair_procedimentos competencia=%s rows=%d",
        competencia, len(df),
    )
    return df


def _executar_query(con: fdb.Connection, sql: str) -> pl.DataFrame:
    cur = con.cursor()
    try:
        cur.execute(sql)
        linhas = cur.fetchall()
        colunas: list[str] = [d[0] for d in cur.description]
    finally:
        cur.close()
    if not linhas:
        return pl.DataFrame(schema={c: pl.Utf8 for c in colunas})
    return pl.DataFrame(linhas, schema=colunas, orient="row")
