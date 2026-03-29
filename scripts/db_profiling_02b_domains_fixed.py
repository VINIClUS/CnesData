"""
db_profiling_02b_domains_fixed.py
====================================
Script de Profiling — Iteração 2b: Mapeamento de Domínios (CORRIGIDO)

Correções aplicadas vs. versão anterior:
  - ORDER BY agora usa posição numérica (ex: ORDER BY 2 DESC) para
    compatibilidade com Firebird que não aceita alias no ORDER BY + GROUP BY.
  - TRIM() removido das queries em tabelas de sistema RDB$ (não suportado).
  - guard de DataFrame vazio antes de groupby/print.

COMO EXECUTAR:
  > cd c:\\Users\\CPD\\Projetos\\CnesData
  > python scripts/db_profiling_02b_domains_fixed.py
"""

import logging
import sys
import warnings
from pathlib import Path

import fdb
import pandas as pd

RAIZ = Path(__file__).parent.parent
sys.path.insert(0, str(RAIZ / "src"))

import config  # noqa: E402

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)-7s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("profiling.02b")

OUTPUT_DIR = RAIZ / "data" / "discovery"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

COD_MUN     = config.COD_MUN_IBGE       # "354130"
CNPJ_MANT   = config.CNPJ_MANTENEDORA   # "55293427000117"


# ─────────────────────────────────────────────────────────────────────────────
def conectar() -> fdb.Connection:
    dll_path = Path(config.FIREBIRD_DLL)
    fdb.load_api(str(dll_path))
    logger.info("Conectando a: %s", config.DB_DSN)
    return fdb.connect(dsn=config.DB_DSN, user=config.DB_USER, password=config.DB_PASSWORD)


def q(con: fdb.Connection, sql: str, label: str) -> pd.DataFrame:
    """Executa query e retorna DataFrame com colunas limpas (strip)."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_sql(sql, con)
        df.columns = [c.strip() for c in df.columns]
        logger.info("  [%s] → %d linhas x %d colunas", label, len(df), len(df.columns))
        return df
    except Exception as exc:
        logger.error("  ERRO [%s]: %s", label, exc)
        return pd.DataFrame()


def salvar(df: pd.DataFrame, nome: str) -> None:
    p = OUTPUT_DIR / nome
    df.to_csv(p, index=False, sep=";", encoding="utf-8-sig")
    logger.info("  → Salvo: %s", p)


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 1 — Domínios de LFCES021
# FIX: ORDER BY <posição> em vez de alias
# ─────────────────────────────────────────────────────────────────────────────
SQL_IND_VINC = """
SELECT
    v.IND_VINC,
    COUNT(*)                    AS QTD_REGISTROS,
    COUNT(DISTINCT v.PROF_ID)   AS QTD_PROFISSIONAIS
FROM LFCES021 v
GROUP BY v.IND_VINC
ORDER BY 2 DESC
"""

SQL_STATUS_VINCULO = """
SELECT
    v.STATUS,
    v.STATUSMOV,
    COUNT(*) AS QTD
FROM LFCES021 v
GROUP BY v.STATUS, v.STATUSMOV
ORDER BY 3 DESC
"""

SQL_TP_SUS = """
SELECT
    v.TP_SUS_NAO_SUS,
    COUNT(*) AS QTD
FROM LFCES021 v
GROUP BY v.TP_SUS_NAO_SUS
ORDER BY 2 DESC
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 2 — Status de LFCES018
# ─────────────────────────────────────────────────────────────────────────────
SQL_STATUS_PROF = """
SELECT
    p.STATUS,
    p.STATUSMOV,
    COUNT(*) AS QTD
FROM LFCES018 p
GROUP BY p.STATUS, p.STATUSMOV
ORDER BY 3 DESC
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 3 — Tipos de Unidade em LFCES004
# ─────────────────────────────────────────────────────────────────────────────
SQL_TP_UNID_DISTRIB = """
SELECT
    e.TP_UNID_ID,
    COUNT(*)                        AS QTD_ESTAB,
    COUNT(DISTINCT e.CODMUNGEST)    AS QTD_MUNICIPIOS
FROM LFCES004 e
GROUP BY e.TP_UNID_ID
ORDER BY 2 DESC
"""

# Tipos de unidade no nosso município-alvo (para identificar COVEPE/CCZ/Secretaria)
SQL_TP_UNID_MUNICIPIO = f"""
SELECT
    e.TP_UNID_ID,
    e.CNES,
    e.NOME_FANTA,
    e.CNPJ_MANT,
    e.STATUS,
    e.STATUSMOV
FROM LFCES004 e
WHERE e.CODMUNGEST = '{COD_MUN}'
ORDER BY e.TP_UNID_ID, e.NOME_FANTA
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 4 — Explorar tabelas NFCES candidatas a domínio de TP_UNID
# FIX: sem TRIM() nas queries a RDB$; usa STARTING WITH direto
# ─────────────────────────────────────────────────────────────────────────────
SQL_NFCES_CANDIDATAS = """
SELECT
    f.RDB$RELATION_NAME     AS TABELA,
    f.RDB$FIELD_NAME        AS COLUNA
FROM RDB$RELATION_FIELDS f
JOIN RDB$RELATIONS r ON r.RDB$RELATION_NAME = f.RDB$RELATION_NAME
WHERE r.RDB$SYSTEM_FLAG = 0
  AND r.RDB$VIEW_BLR IS NULL
  AND f.RDB$RELATION_NAME STARTING WITH 'NFCES'
  AND (  f.RDB$FIELD_NAME STARTING WITH 'COD'
      OR f.RDB$FIELD_NAME STARTING WITH 'TP_UNID'
      OR f.RDB$FIELD_NAME STARTING WITH 'DS_'
      OR f.RDB$FIELD_NAME STARTING WITH 'DESCR'
      OR f.RDB$FIELD_NAME STARTING WITH 'NO_' )
ORDER BY f.RDB$RELATION_NAME, f.RDB$FIELD_POSITION
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 5 — Exploração de LFCES020
# ─────────────────────────────────────────────────────────────────────────────
SQL_LFCES020_COLS = """
SELECT
    f.RDB$FIELD_NAME        AS COLUNA,
    f.RDB$FIELD_POSITION    AS POSICAO
FROM RDB$RELATION_FIELDS f
WHERE f.RDB$RELATION_NAME = 'LFCES020'
ORDER BY f.RDB$FIELD_POSITION
"""

SQL_LFCES020_AMOSTRA = "SELECT FIRST 10 * FROM LFCES020"

SQL_LFCES020_STATUS = """
SELECT
    STATUS,
    STATUSMOV,
    COUNT(*) AS QTD
FROM LFCES020
GROUP BY STATUS, STATUSMOV
ORDER BY 3 DESC
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 6 — Validação CODMUNGEST (LFCES004) vs COD_MUN (LFCES048)
# ─────────────────────────────────────────────────────────────────────────────
SQL_CODMUN_LFCES004 = """
SELECT DISTINCT
    e.CODMUNGEST,
    CHAR_LENGTH(e.CODMUNGEST) AS LEN_CODMUNGEST
FROM LFCES004 e
WHERE e.CODMUNGEST STARTING WITH '354130'
"""

SQL_CODMUN_LFCES048 = """
SELECT
    m.COD_MUN,
    CHAR_LENGTH(m.COD_MUN) AS LEN_COD_MUN,
    COUNT(*)               AS QTD_MEMBROS
FROM LFCES048 m
WHERE m.COD_MUN STARTING WITH '354130'
GROUP BY m.COD_MUN, CHAR_LENGTH(m.COD_MUN)
ORDER BY 3 DESC
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 7 — Amostra diagnóstica do município com STATUS exposto
# ─────────────────────────────────────────────────────────────────────────────
SQL_AMOSTRA_MUNICIPIO = f"""
SELECT FIRST 30
    prof.CPF_PROF,
    prof.NOME_PROF,
    vinc.COD_CBO,
    vinc.IND_VINC,
    vinc.TP_SUS_NAO_SUS,
    vinc.STATUS          AS VINC_STATUS,
    vinc.STATUSMOV       AS VINC_STATUSMOV,
    prof.STATUS          AS PROF_STATUS,
    prof.STATUSMOV       AS PROF_STATUSMOV,
    (COALESCE(vinc.CG_HORAAMB,0) + COALESCE(vinc.CGHORAOUTR,0) + COALESCE(vinc.CGHORAHOSP,0)) AS CH_TOTAL,
    est.CNES,
    est.NOME_FANTA,
    est.TP_UNID_ID
FROM LFCES021 vinc
INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
WHERE est.CODMUNGEST = '{COD_MUN}'
  AND est.CNPJ_MANT  = '{CNPJ_MANT}'
ORDER BY prof.NOME_PROF
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 8 — Análise de NFCES088 (descoberta: tabela Profissional×Programa)
# Mapear os valores únicos de IND_VINCULACAO para confirmar equivalência
# com IND_VINC de LFCES021
# ─────────────────────────────────────────────────────────────────────────────
SQL_NFCES088_IND_VINC = """
SELECT
    n.IND_VINCULACAO,
    n.TP_SUS_NAO_SUS,
    COUNT(*)  AS QTD
FROM NFCES088 n
GROUP BY n.IND_VINCULACAO, n.TP_SUS_NAO_SUS
ORDER BY 3 DESC
"""

SQL_NFCES088_TP_UNIDADE = """
SELECT
    n.TP_UNIDADE,
    COUNT(DISTINCT n.CNES) AS QTD_ESTAB,
    COUNT(*)               AS QTD_PROF
FROM NFCES088 n
GROUP BY n.TP_UNIDADE
ORDER BY 3 DESC
"""


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    con = None
    try:
        con = conectar()
        logger.info("Conexão estabelecida.\n")

        # ── ETAPA 1 ──────────────────────────────────────────────────────
        logger.info("=" * 60)
        logger.info("ETAPA 1 — Domínios de LFCES021 (Vínculos)")
        logger.info("=" * 60)

        logger.info("\n>>> IND_VINC — tipos de vínculo empregatício:")
        df = q(con, SQL_IND_VINC, "IND_VINC")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "02b_dominios_ind_vinc.csv")

        logger.info("\n>>> STATUS / STATUSMOV dos Vínculos:")
        df = q(con, SQL_STATUS_VINCULO, "STATUS_VINCULO")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "02b_dominios_status_vinculo.csv")

        logger.info("\n>>> TP_SUS_NAO (SUS vs não-SUS):")
        df = q(con, SQL_TP_SUS, "TP_SUS")
        if not df.empty:
            print(df.to_string(index=False))

        # ── ETAPA 2 ──────────────────────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 2 — Status de LFCES018 (Profissionais)")
        logger.info("=" * 60)
        df = q(con, SQL_STATUS_PROF, "STATUS_PROF")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "02b_dominios_status_profissional.csv")

        # ── ETAPA 3 ──────────────────────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 3 — Tipos de Unidade")
        logger.info("=" * 60)

        logger.info("\n>>> Distribuição global de TP_UNID_ID:")
        df = q(con, SQL_TP_UNID_DISTRIB, "TP_UNID_GLOBAL")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "02b_tp_unid_global.csv")

        logger.info("\n>>> Estabelecimentos do NOSSO MUNICÍPIO (354130) — todos os tipos:")
        df = q(con, SQL_TP_UNID_MUNICIPIO, "TP_UNID_MUNICIPIO")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "02b_tp_unid_municipio_354130.csv")

        # ── ETAPA 4 ──────────────────────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 4 — Tabelas NFCES candidatas a domínio")
        logger.info("=" * 60)
        df = q(con, SQL_NFCES_CANDIDATAS, "NFCES_CANDIDATAS")
        if not df.empty:
            # Agrupa por tabela para exibição compacta
            grupos = df.groupby("TABELA")["COLUNA"].apply(list)
            for tabela, colunas in grupos.items():
                logger.info("  %s → %s", tabela, ", ".join(colunas))
            salvar(df, "02b_nfces_candidatas_dominio.csv")

        # ── ETAPA 5 ──────────────────────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 5 — Exploração de LFCES020")
        logger.info("=" * 60)

        logger.info("\n>>> Colunas de LFCES020:")
        df = q(con, SQL_LFCES020_COLS, "LFCES020_COLS")
        if not df.empty:
            print(df.to_string(index=False))

        logger.info("\n>>> Amostra (10 linhas) de LFCES020:")
        df = q(con, SQL_LFCES020_AMOSTRA, "LFCES020_AMOSTRA")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "02b_lfces020_amostra.csv")

        logger.info("\n>>> Distribuição STATUS/STATUSMOV em LFCES020:")
        df = q(con, SQL_LFCES020_STATUS, "LFCES020_STATUS")
        if not df.empty:
            print(df.to_string(index=False))

        # ── ETAPA 6 ──────────────────────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 6 — Validação CODMUNGEST vs COD_MUN para '354130'")
        logger.info("=" * 60)

        df4 = q(con, SQL_CODMUN_LFCES004, "CODMUN_LFCES004")
        if not df4.empty:
            logger.info("LFCES004.CODMUNGEST (valores distintos):")
            print(df4.to_string(index=False))

        df8 = q(con, SQL_CODMUN_LFCES048, "CODMUN_LFCES048")
        if not df8.empty:
            logger.info("LFCES048.COD_MUN (valores distintos):")
            print(df8.to_string(index=False))

        if not df4.empty or not df8.empty:
            salvar(
                pd.concat([
                    df4.rename(columns={"CODMUNGEST": "VALOR", "LEN_CODMUNGEST": "COMPRIMENTO"}).assign(ORIGEM="LFCES004"),
                    df8.rename(columns={"COD_MUN": "VALOR", "LEN_COD_MUN": "COMPRIMENTO"}).assign(ORIGEM="LFCES048"),
                ], ignore_index=True),
                "02b_validacao_codmun.csv",
            )

        # ── ETAPA 7 ──────────────────────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 7 — Amostra Diagnóstica do Município (com STATUS)")
        logger.info("=" * 60)
        df = q(con, SQL_AMOSTRA_MUNICIPIO, "AMOSTRA_MUNICIPIO")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "02b_amostra_municipio_status.csv")

        # ── ETAPA 8 ──────────────────────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 8 — NFCES088: Análise de IND_VINCULACAO e TP_UNIDADE")
        logger.info("(Descoberta: esta tabela é Profissional×Programa, não domínio)")
        logger.info("=" * 60)

        logger.info("\n>>> IND_VINCULACAO × TP_SUS_NAO_SUS em NFCES088:")
        df = q(con, SQL_NFCES088_IND_VINC, "NFCES088_IND_VINC")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "02b_nfces088_ind_vinc.csv")

        logger.info("\n>>> TP_UNIDADE em NFCES088 (tipos de programa de saúde):")
        df = q(con, SQL_NFCES088_TP_UNIDADE, "NFCES088_TP_UNIDADE")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "02b_nfces088_tp_unidade.csv")

        logger.info("\n" + "=" * 60)
        logger.info("PROFILING 02b CONCLUÍDO. Arquivos em: %s", OUTPUT_DIR)
        logger.info("=" * 60)

    finally:
        if con:
            con.close()
            logger.info("Conexão encerrada.")


if __name__ == "__main__":
    main()
