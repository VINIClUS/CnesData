"""
db_profiling_03_domains_lookup_and_audit.py
============================================
Script de Profiling — Iteração 3: Tabelas de Domínio + Auditoria de Lotação

OBJETIVOS:
  1. Consultar tabelas NFCES de domínio confirmadas:
     - NFCES010 → descrições de TP_UNID_ID (tipo de estabelecimento)
     - NFCES026 → descrições de CBO (cargo/ocupação)
     - NFCES056 → domínio de tipo de vínculo (IND_VINC - nível 1)
     - NFCES057 → domínio de subtipo de vínculo
     - NFCES058 → domínio de sub-subtipo (IND_VINC 6 dígitos)
     - NFCES046 → descrições de tipos de equipe
     - NFCES055 → subtipos de unidade por TP_UNID_ID
  2. Executar a Regra de Auditoria RQ-005:
     - ACS (516220) e ACE (515320) lotados × tipo de unidade
     - Checar se ACS estão fora de ESF (TP_UNID 02)
     - Checar se ACE estão fora de CCZ/COVEPE (TP_UNID 50)
  3. Validar CODMUNGEST vs COD_MUN (fix: sem CHAR_LENGTH — usar OCTET_LENGTH ou cast)

COMO EXECUTAR:
  > cd c:\\Users\\CPD\\Projetos\\CnesData
  > python scripts/db_profiling_03_domains_lookup_and_audit.py
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
logger = logging.getLogger("profiling.03")

OUTPUT_DIR = RAIZ / "data" / "discovery"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

COD_MUN   = config.COD_MUN_IBGE      # "354130"
CNPJ_MANT = config.CNPJ_MANTENEDORA  # "55293427000117"


# ─────────────────────────────────────────────────────────────────────────────
def conectar() -> fdb.Connection:
    dll_path = Path(config.FIREBIRD_DLL)
    fdb.load_api(str(dll_path))
    logger.info("Conectando a: %s", config.DB_DSN)
    return fdb.connect(dsn=config.DB_DSN, user=config.DB_USER, password=config.DB_PASSWORD)


def q(con: fdb.Connection, sql: str, label: str) -> pd.DataFrame:
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
    logger.info("  → %s", p)


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 1 — Tabelas de Domínio NFCES
# ─────────────────────────────────────────────────────────────────────────────

# NFCES010 — Domínio de Tipo de Unidade (TP_UNID_ID)
SQL_NFCES010 = "SELECT * FROM NFCES010 ORDER BY 1"

# NFCES026 — Domínio de CBO (apenas os relevantes para saúde municipal)
SQL_NFCES026_ACS_ACE = """
SELECT * FROM NFCES026
WHERE COD_CBO IN ('516220', '515320', '223565', '223605', '223810',
                   '225135', '225125', '225142', '251605', '322430',
                   '322205', '322245', '515105', '411010', '422105')
ORDER BY COD_CBO
"""

# NFCES056 — DS_VINCULA (nível 1 do IND_VINC — primeiros 2 dígitos?)
SQL_NFCES056 = "SELECT FIRST 30 * FROM NFCES056"

# NFCES057 — DS_VINCULO
SQL_NFCES057 = "SELECT FIRST 30 * FROM NFCES057"

# NFCES058 — DS_SUBVINC (subtipo completo de IND_VINC?)
SQL_NFCES058 = "SELECT FIRST 30 * FROM NFCES058"

# NFCES046 — DS_EQUIPE (tipos de equipe: ESF, EAP, eSB...)
SQL_NFCES046 = "SELECT * FROM NFCES046"

# NFCES055 — Subtipos de unidade (TP_UNID_ID, DS_SUBTIPO)
SQL_NFCES055 = "SELECT * FROM NFCES055 ORDER BY 1"

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 2 — Auditoria RQ-005: ACS/ACE × Tipo de Unidade
# Hipótese: ACS (516220) → TP_UNID 02 (ESF/UBS)
#           ACE (515320) → TP_UNID 50 (CCZ/COVEPE)
# Listamos TODOS os vínculos ativos de ACS e ACE no município
# para confirmar a distribuição e identificar anomalias.
# ─────────────────────────────────────────────────────────────────────────────
SQL_ACS_ACE_LOTACAO = f"""
SELECT
    prof.CPF_PROF,
    prof.NOME_PROF,
    vinc.COD_CBO,
    vinc.IND_VINC,
    est.CNES,
    est.NOME_FANTA,
    est.TP_UNID_ID,
    (COALESCE(vinc.CG_HORAAMB,0) + COALESCE(vinc.CGHORAOUTR,0) + COALESCE(vinc.CGHORAHOSP,0)) AS CH_TOTAL
FROM LFCES021 vinc
INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
WHERE vinc.COD_CBO IN ('516220', '515320')
  AND est.CODMUNGEST = '{COD_MUN}'
ORDER BY vinc.COD_CBO, prof.NOME_PROF
"""

# Resumo por CBO × TP_UNID_ID
SQL_ACS_ACE_RESUMO = f"""
SELECT
    vinc.COD_CBO,
    est.TP_UNID_ID,
    COUNT(DISTINCT prof.CPF_PROF) AS QTD_PROFISSIONAIS,
    COUNT(*)                      AS QTD_VINCULOS
FROM LFCES021 vinc
INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
WHERE vinc.COD_CBO IN ('516220', '515320')
  AND est.CODMUNGEST = '{COD_MUN}'
GROUP BY vinc.COD_CBO, est.TP_UNID_ID
ORDER BY vinc.COD_CBO, est.TP_UNID_ID
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 3 — Validação do mismatch CODMUNGEST vs COD_MUN
# FIX: usar CAST e SUBSTRING em vez de CHAR_LENGTH (não suportado neste Firebird)
# ─────────────────────────────────────────────────────────────────────────────
SQL_CODMUN_CAST = f"""
SELECT DISTINCT
    e.CODMUNGEST,
    CAST(e.CODMUNGEST AS VARCHAR(10)) AS CODMUNGEST_CAST
FROM LFCES004 e
WHERE e.CODMUNGEST STARTING WITH '354130'
"""

SQL_COD_MUN_LFCES048 = f"""
SELECT DISTINCT
    m.COD_MUN,
    COUNT(*) AS QTD
FROM LFCES048 m
WHERE m.COD_MUN STARTING WITH '354130'
GROUP BY m.COD_MUN
ORDER BY 2 DESC
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 4 — Diagnóstico de IND_VINC em LFCES021 × CBO para nosso município
# Objetivo: entender distribuição de tipos de vínculo por categoria de CBO
# ─────────────────────────────────────────────────────────────────────────────
SQL_IND_VINC_POR_CBO = f"""
SELECT
    vinc.COD_CBO,
    vinc.IND_VINC,
    vinc.TP_SUS_NAO_SUS,
    COUNT(DISTINCT prof.CPF_PROF) AS QTD_PROF,
    COUNT(*)                      AS QTD_VINCULOS
FROM LFCES021 vinc
INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
WHERE est.CODMUNGEST = '{COD_MUN}'
  AND est.CNPJ_MANT  = '{CNPJ_MANT}'
GROUP BY vinc.COD_CBO, vinc.IND_VINC, vinc.TP_SUS_NAO_SUS
ORDER BY 4 DESC
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 5 — Profissionais com MÚLTIPLOS VÍNCULOS na mesma unidade (RQ-004)
# ─────────────────────────────────────────────────────────────────────────────
SQL_MULTIPLOS_VINCULOS = f"""
SELECT
    prof.CPF_PROF,
    prof.NOME_PROF,
    est.CNES,
    est.NOME_FANTA,
    COUNT(*)    AS QTD_VINCULOS_MESMA_UNIDADE,
    SUM(COALESCE(vinc.CG_HORAAMB,0) + COALESCE(vinc.CGHORAOUTR,0) + COALESCE(vinc.CGHORAHOSP,0)) AS CH_TOTAL
FROM LFCES021 vinc
INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
WHERE est.CODMUNGEST = '{COD_MUN}'
  AND est.CNPJ_MANT  = '{CNPJ_MANT}'
GROUP BY prof.CPF_PROF, prof.NOME_PROF, est.CNES, est.NOME_FANTA
HAVING COUNT(*) > 1
ORDER BY 5 DESC, prof.NOME_PROF
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 6 — Profissionais com vínculos em múltiplas unidades (RQ-003)
# ─────────────────────────────────────────────────────────────────────────────
SQL_MULTIPLAS_UNIDADES = f"""
SELECT
    prof.CPF_PROF,
    prof.NOME_PROF,
    COUNT(DISTINCT est.UNIDADE_ID) AS QTD_UNIDADES,
    COUNT(DISTINCT vinc.COD_CBO)   AS QTD_CBOS,
    SUM(COALESCE(vinc.CG_HORAAMB,0) + COALESCE(vinc.CGHORAOUTR,0) + COALESCE(vinc.CGHORAHOSP,0)) AS CH_TOTAL_ACUMULADA
FROM LFCES021 vinc
INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
WHERE est.CODMUNGEST = '{COD_MUN}'
  AND est.CNPJ_MANT  = '{CNPJ_MANT}'
GROUP BY prof.CPF_PROF, prof.NOME_PROF
HAVING COUNT(DISTINCT est.UNIDADE_ID) > 1
ORDER BY 3 DESC, prof.NOME_PROF
"""

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    con = None
    try:
        con = conectar()
        logger.info("Conexão estabelecida.\n")

        # ── ETAPA 1: Tabelas de Domínio ───────────────────────────────────
        logger.info("=" * 60)
        logger.info("ETAPA 1 — Tabelas de Domínio NFCES")
        logger.info("=" * 60)

        logger.info("\n>>> NFCES010 — Domínio de TP_UNID_ID (tipo de estabelecimento):")
        df = q(con, SQL_NFCES010, "NFCES010")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "03_nfces010_tp_unid_descricao.csv")

        logger.info("\n>>> NFCES026 — CBOs relevantes (ACS, ACE e demais saúde municipal):")
        df = q(con, SQL_NFCES026_ACS_ACE, "NFCES026")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "03_nfces026_cbo_descricao.csv")

        logger.info("\n>>> NFCES056 — Domínio DS_VINCULA (nível 1 IND_VINC):")
        df56 = q(con, SQL_NFCES056, "NFCES056")
        if not df56.empty:
            print(df56.to_string(index=False))
            salvar(df56, "03_nfces056_ds_vincula.csv")

        logger.info("\n>>> NFCES057 — Domínio DS_VINCULO (nível 2 IND_VINC):")
        df57 = q(con, SQL_NFCES057, "NFCES057")
        if not df57.empty:
            print(df57.to_string(index=False))
            salvar(df57, "03_nfces057_ds_vinculo.csv")

        logger.info("\n>>> NFCES058 — Domínio DS_SUBVINC (subtipo IND_VINC):")
        df58 = q(con, SQL_NFCES058, "NFCES058")
        if not df58.empty:
            print(df58.to_string(index=False))
            salvar(df58, "03_nfces058_ds_subvinc.csv")

        logger.info("\n>>> NFCES046 — Tipos de Equipe:")
        df = q(con, SQL_NFCES046, "NFCES046")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "03_nfces046_tipos_equipe.csv")

        logger.info("\n>>> NFCES055 — Subtipos de Unidade:")
        df = q(con, SQL_NFCES055, "NFCES055")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "03_nfces055_subtipos_unidade.csv")

        # ── ETAPA 2: Auditoria ACS/ACE ────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 2 — Auditoria RQ-005: ACS/ACE × Tipo de Unidade")
        logger.info("=" * 60)

        logger.info("\n>>> Resumo: CBO × TP_UNID_ID (visão agregada):")
        df_resumo = q(con, SQL_ACS_ACE_RESUMO, "ACS_ACE_RESUMO")
        if not df_resumo.empty:
            print(df_resumo.to_string(index=False))
            salvar(df_resumo, "03_audit_acs_ace_resumo.csv")

        logger.info("\n>>> Detalhe: todos os vínculos ACS/ACE no município:")
        df_det = q(con, SQL_ACS_ACE_LOTACAO, "ACS_ACE_DETALHE")
        if not df_det.empty:
            print(df_det.to_string(index=False))
            salvar(df_det, "03_audit_acs_ace_detalhe.csv")

        # ── ETAPA 3: Validação CODMUNGEST ─────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 3 — Validação CODMUNGEST vs COD_MUN")
        logger.info("=" * 60)

        df4 = q(con, SQL_CODMUN_CAST, "CODMUN_LFCES004")
        if not df4.empty:
            logger.info("LFCES004.CODMUNGEST para 354130:")
            print(df4.to_string(index=False))

        df8 = q(con, SQL_COD_MUN_LFCES048, "CODMUN_LFCES048")
        if not df8.empty:
            logger.info("LFCES048.COD_MUN para 354130:")
            print(df8.to_string(index=False))

        # ── ETAPA 4: IND_VINC por CBO ────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 4 — Distribuição IND_VINC por CBO (nosso município)")
        logger.info("=" * 60)
        df = q(con, SQL_IND_VINC_POR_CBO, "IND_VINC_POR_CBO")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "03_ind_vinc_por_cbo_municipio.csv")

        # ── ETAPA 5: Múltiplos vínculos mesma unidade ────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 5 — RQ-004: Profissionais com múltiplos vínculos na mesma unidade")
        logger.info("=" * 60)
        df = q(con, SQL_MULTIPLOS_VINCULOS, "MULTIPLOS_VINCULOS")
        if not df.empty:
            logger.info("  TOTAL: %d casos", len(df))
            print(df.to_string(index=False))
            salvar(df, "03_rq004_multiplos_vinculos_mesma_unidade.csv")
        else:
            logger.info("  Nenhum caso encontrado.")

        # ── ETAPA 6: Múltiplas unidades ──────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 6 — RQ-003: Profissionais com vínculos em múltiplas unidades")
        logger.info("=" * 60)
        df = q(con, SQL_MULTIPLAS_UNIDADES, "MULTIPLAS_UNIDADES")
        if not df.empty:
            logger.info("  TOTAL: %d profissionais com mais de 1 unidade", len(df))
            print(df.to_string(index=False))
            salvar(df, "03_rq003_multiplas_unidades.csv")
        else:
            logger.info("  Nenhum profissional com múltiplas unidades.")

        logger.info("\n" + "=" * 60)
        logger.info("PROFILING 03 CONCLUÍDO. Arquivos em: %s", OUTPUT_DIR)
        logger.info("=" * 60)

    finally:
        if con:
            con.close()
            logger.info("Conexão encerrada.")


if __name__ == "__main__":
    main()
