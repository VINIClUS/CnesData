"""
db_profiling_02_domains_and_status.py
======================================
Script de Profiling — Iteração 2: Mapeamento de Domínios e Validação de Status

OBJETIVO:
  1. Mapear os domínios das colunas-chave descobertas na Iteração 1:
     - LFCES021.IND_VINC  → que valores existem? Qual é o "ativo"?
     - LFCES021.STATUS / LFCES021.STATUSMOV → idem
     - LFCES018.STATUS / LFCES018.STATUSMOV
     - LFCES004.TP_UNID_ID → tipos de unidade (para RQ-005: ACS/ACE)
  2. Explorar NFCES088 (tabela com 13 colunas — suspeita de conter TP_UNID)
  3. Explorar LFCES020 (suspeita de vínculos históricos por competência)
  4. Verificar o mismatch de CODMUNGEST (7) vs COD_MUN (6) no município-alvo

COMO EXECUTAR:
  > cd c:\\Users\\CPD\\Projetos\\CnesData
  > python scripts/db_profiling_02_domains_and_status.py

SAÍDA:
  - data/discovery/02_dominios_*.csv (um por domínio)
  - data/discovery/02_lfces020_amostra.csv
  - data/discovery/02_nfces_tipo_unidade.csv
  - data/discovery/02_validacao_codmun.csv
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
logger = logging.getLogger("profiling.02")

OUTPUT_DIR = RAIZ / "data" / "discovery"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

COD_MUN = config.COD_MUN_IBGE  # "354130"


# ─────────────────────────────────────────────────────────────────────────────
# CONEXÃO
# ─────────────────────────────────────────────────────────────────────────────
def conectar() -> fdb.Connection:
    dll_path = Path(config.FIREBIRD_DLL)
    if not dll_path.exists():
        raise FileNotFoundError(f"DLL não encontrada: {dll_path}")
    fdb.load_api(str(dll_path))
    logger.info("Driver carregado. Conectando a: %s", config.DB_DSN)
    con = fdb.connect(
        dsn=config.DB_DSN,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
    )
    logger.info("Conexão estabelecida.")
    return con


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def rodar_query(con: fdb.Connection, sql: str, label: str) -> pd.DataFrame:
    """Executa SQL e retorna DataFrame, logando resultado."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_sql(sql, con)
        df.columns = [c.strip() for c in df.columns]
        logger.info("  [%s] → %d linhas, %d colunas.", label, len(df), len(df.columns))
        return df
    except Exception as exc:
        logger.error("  ERRO em [%s]: %s", label, exc)
        return pd.DataFrame()


def salvar(df: pd.DataFrame, nome_arquivo: str) -> None:
    caminho = OUTPUT_DIR / nome_arquivo
    df.to_csv(caminho, index=False, sep=";", encoding="utf-8-sig")
    logger.info("  Salvo: %s", caminho)


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 1 — Domínios de LFCES021 (Vínculos)
# ─────────────────────────────────────────────────────────────────────────────
SQL_IND_VINC = """
SELECT
    v.IND_VINC,
    COUNT(*)            AS QTD_REGISTROS,
    COUNT(DISTINCT v.PROF_ID) AS QTD_PROFISSIONAIS
FROM LFCES021 v
GROUP BY v.IND_VINC
ORDER BY QTD_REGISTROS DESC
"""

SQL_STATUS_VINCULO = """
SELECT
    v.STATUS,
    v.STATUSMOV,
    COUNT(*) AS QTD
FROM LFCES021 v
GROUP BY v.STATUS, v.STATUSMOV
ORDER BY QTD DESC
"""

SQL_TP_SUS = """
SELECT
    v.TP_SUS_NAO_SUS,
    COUNT(*) AS QTD
FROM LFCES021 v
GROUP BY v.TP_SUS_NAO_SUS
ORDER BY QTD DESC
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 2 — Status de LFCES018 (Profissionais)
# ─────────────────────────────────────────────────────────────────────────────
SQL_STATUS_PROF = """
SELECT
    p.STATUS,
    p.STATUSMOV,
    COUNT(*) AS QTD
FROM LFCES018 p
GROUP BY p.STATUS, p.STATUSMOV
ORDER BY QTD DESC
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 3 — Tipos de Unidade (TP_UNID_ID em LFCES004)
# ─────────────────────────────────────────────────────────────────────────────
SQL_TP_UNID_DISTRIB = """
SELECT
    e.TP_UNID_ID,
    COUNT(*)        AS QTD_ESTAB,
    COUNT(DISTINCT e.CODMUNGEST) AS QTD_MUNICIPIOS
FROM LFCES004 e
GROUP BY e.TP_UNID_ID
ORDER BY QTD_ESTAB DESC
"""

# Explora NFCES088 — suspeita de ser a tabela de domínio de TP_UNID_ID
SQL_NFCES088_TUDO = "SELECT FIRST 100 * FROM NFCES088"

# Verifica quais NFCES têm colunas que parecem ser chave "COD" + descrição
SQL_NFCES_CANDIDATAS = """
SELECT
    f.RDB$RELATION_NAME     AS TABELA,
    f.RDB$FIELD_NAME        AS COLUNA
FROM RDB$RELATION_FIELDS f
JOIN RDB$RELATIONS r ON r.RDB$RELATION_NAME = f.RDB$RELATION_NAME
WHERE r.RDB$SYSTEM_FLAG = 0
  AND r.RDB$VIEW_BLR IS NULL
  AND f.RDB$RELATION_NAME STARTING WITH 'NFCES'
  AND (   TRIM(f.RDB$FIELD_NAME) STARTING WITH 'COD'
       OR TRIM(f.RDB$FIELD_NAME) STARTING WITH 'TP_UNID'
       OR TRIM(f.RDB$FIELD_NAME) STARTING WITH 'DS_'
       OR TRIM(f.RDB$FIELD_NAME) STARTING WITH 'DESCR' )
ORDER BY f.RDB$RELATION_NAME, f.RDB$FIELD_POSITION
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 4 — Exploração de LFCES020 (Vínculos históricos?)
# ─────────────────────────────────────────────────────────────────────────────
SQL_LFCES020_AMOSTRA = "SELECT FIRST 10 * FROM LFCES020"

SQL_LFCES020_COLUNAS = """
SELECT
    f.RDB$FIELD_NAME        AS COLUNA,
    f.RDB$FIELD_POSITION    AS POSICAO
FROM RDB$RELATION_FIELDS f
WHERE TRIM(f.RDB$RELATION_NAME) = 'LFCES020'
ORDER BY f.RDB$FIELD_POSITION
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 5 — Validação do Mismatch CODMUNGEST vs COD_MUN (Presidente Epitácio)
# ─────────────────────────────────────────────────────────────────────────────
SQL_CODMUNGEST_VALORES = """
SELECT DISTINCT
    TRIM(e.CODMUNGEST)  AS CODMUNGEST_VALOR,
    LENGTH(TRIM(e.CODMUNGEST)) AS COMPRIMENTO
FROM LFCES004 e
WHERE TRIM(e.CODMUNGEST) STARTING WITH '354130'
"""

SQL_CODMUN_LFCES048 = """
SELECT DISTINCT
    TRIM(m.COD_MUN) AS COD_MUN_VALOR,
    LENGTH(TRIM(m.COD_MUN)) AS COMPRIMENTO,
    COUNT(*) AS QTD
FROM LFCES048 m
WHERE TRIM(m.COD_MUN) STARTING WITH '354130'
GROUP BY TRIM(m.COD_MUN), LENGTH(TRIM(m.COD_MUN))
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 6 — Amostra filtrada para nosso município (diagnóstico do pipeline atual)
# ─────────────────────────────────────────────────────────────────────────────
SQL_AMOSTRA_MUNICIPIO = f"""
SELECT FIRST 20
    prof.CPF_PROF,
    prof.NOME_PROF,
    vinc.COD_CBO,
    vinc.IND_VINC,
    vinc.TP_SUS_NAO_SUS,
    vinc.STATUS          AS VINC_STATUS,
    vinc.STATUSMOV       AS VINC_STATUSMOV,
    prof.STATUS          AS PROF_STATUS,
    prof.STATUSMOV       AS PROF_STATUSMOV,
    (COALESCE(vinc.CG_HORAAMB, 0) + COALESCE(vinc.CGHORAOUTR, 0) + COALESCE(vinc.CGHORAHOSP, 0)) AS CARGA_TOTAL,
    est.CNES,
    est.NOME_FANTA,
    est.TP_UNID_ID
FROM LFCES021 vinc
INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
WHERE est.CODMUNGEST = '{COD_MUN}'
  AND est.CNPJ_MANT  = '{config.CNPJ_MANTENEDORA}'
ORDER BY prof.NOME_PROF
"""

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    con = None
    try:
        con = conectar()

        # ── ETAPA 1: Domínios de LFCES021 ────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 1 — Domínios de LFCES021 (Vínculos)")
        logger.info("=" * 60)

        logger.info("\n>>> IND_VINC (tipo de vínculo empregatício):")
        df_ind_vinc = rodar_query(con, SQL_IND_VINC, "IND_VINC")
        print(df_ind_vinc.to_string(index=False))
        salvar(df_ind_vinc, "02_dominios_ind_vinc.csv")

        logger.info("\n>>> STATUS / STATUSMOV dos Vínculos:")
        df_status_vinc = rodar_query(con, SQL_STATUS_VINCULO, "STATUS_VINCULO")
        print(df_status_vinc.to_string(index=False))
        salvar(df_status_vinc, "02_dominios_status_vinculo.csv")

        logger.info("\n>>> TP_SUS_NAO (SUS vs não-SUS):")
        df_sus = rodar_query(con, SQL_TP_SUS, "TP_SUS")
        print(df_sus.to_string(index=False))

        # ── ETAPA 2: Status de LFCES018 ──────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 2 — Status de LFCES018 (Profissionais)")
        logger.info("=" * 60)
        df_status_prof = rodar_query(con, SQL_STATUS_PROF, "STATUS_PROF")
        print(df_status_prof.to_string(index=False))
        salvar(df_status_prof, "02_dominios_status_profissional.csv")

        # ── ETAPA 3: Tipos de Unidade ─────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 3 — Tipos de Unidade (TP_UNID_ID)")
        logger.info("=" * 60)

        logger.info("\n>>> Distribuição de TP_UNID_ID em LFCES004:")
        df_tp_unid = rodar_query(con, SQL_TP_UNID_DISTRIB, "TP_UNID_DISTRIB")
        print(df_tp_unid.to_string(index=False))
        salvar(df_tp_unid, "02_tp_unid_distribuicao.csv")

        logger.info("\n>>> Conteúdo de NFCES088 (suspeita de tabela de domínio TP_UNID):")
        df_nfces088 = rodar_query(con, SQL_NFCES088_TUDO, "NFCES088")
        print(df_nfces088.to_string(index=False))
        salvar(df_nfces088, "02_nfces088_conteudo.csv")

        logger.info("\n>>> Colunas NFCES com padrão COD/DS_ (candidatas a domínio):")
        df_nfces_cand = rodar_query(con, SQL_NFCES_CANDIDATAS, "NFCES_CANDIDATAS")
        print(df_nfces_cand.groupby("TABELA")["COLUNA"].apply(list).to_string())

        # ── ETAPA 4: LFCES020 ────────────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 4 — Exploração de LFCES020")
        logger.info("=" * 60)

        logger.info("\n>>> Colunas de LFCES020:")
        df_lfces020_cols = rodar_query(con, SQL_LFCES020_COLUNAS, "LFCES020_COLUNAS")
        print(df_lfces020_cols.to_string(index=False))

        logger.info("\n>>> Amostra de 10 linhas LFCES020:")
        df_lfces020 = rodar_query(con, SQL_LFCES020_AMOSTRA, "LFCES020_AMOSTRA")
        print(df_lfces020.to_string(index=False))
        salvar(df_lfces020, "02_lfces020_amostra.csv")

        # ── ETAPA 5: Validação CODMUNGEST ────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 5 — Validação Mismatch CODMUNGEST(7) vs COD_MUN(6)")
        logger.info("=" * 60)

        logger.info("\n>>> Valores de CODMUNGEST em LFCES004 para 354130:")
        df_codmun_est = rodar_query(con, SQL_CODMUNGEST_VALORES, "CODMUNGEST_LFCES004")
        print(df_codmun_est.to_string(index=False))

        logger.info("\n>>> Valores de COD_MUN em LFCES048 para 354130:")
        df_codmun_eq = rodar_query(con, SQL_CODMUN_LFCES048, "COD_MUN_LFCES048")
        print(df_codmun_eq.to_string(index=False))

        salvar(
            pd.concat([
                df_codmun_est.assign(ORIGEM="LFCES004.CODMUNGEST"),
                df_codmun_eq.assign(ORIGEM="LFCES048.COD_MUN"),
            ]),
            "02_validacao_codmun.csv",
        )

        # ── ETAPA 6: Diagnóstico do município ────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 6 — Amostra Diagnóstica do Município (com STATUS)")
        logger.info("=" * 60)
        df_amostra_mun = rodar_query(con, SQL_AMOSTRA_MUNICIPIO, "AMOSTRA_MUNICIPIO")
        print(df_amostra_mun.to_string(index=False))
        salvar(df_amostra_mun, "02_amostra_municipio_com_status.csv")

        logger.info("\n" + "=" * 60)
        logger.info("PROFILING 02 CONCLUÍDO. Arquivos em: %s", OUTPUT_DIR)
        logger.info("=" * 60)

    finally:
        if con is not None:
            con.close()
            logger.info("Conexão encerrada.")


if __name__ == "__main__":
    main()
