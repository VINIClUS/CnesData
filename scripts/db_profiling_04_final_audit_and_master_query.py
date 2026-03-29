"""
db_profiling_04_final_audit_and_master_query.py
================================================
Script de Profiling — Iteração 4 (FINAL): Auditoria de Lotação + Query Master

OBJETIVOS:
  1. Executar auditoria definitiva de lotação com CBOs e TP_UNID_IDs CORRETOS:
     - ACS (515105) e TACS (322255) -> devem estar em TP_UNID_ID: 01, 02, 15
     - ACE (515140, 322210) e TACE (322260) -> devem estar em: 02, 69, 22, 15
  2. Verificar quais desses CBOs realmente existem no banco do município
  3. Gerar relatório completo de profissionais com total de carga horária e vínculo
  4. Documentar a Query Master para src/ingestion/cnes_client.py

COMO EXECUTAR:
  > cd c:\\Users\\CPD\\Projetos\\CnesData
  > python scripts/db_profiling_04_final_audit_and_master_query.py
"""

import logging
import sys
import warnings
from pathlib import Path

# Força UTF-8 no terminal Windows (evita UnicodeEncodeError com acentos/emojis)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

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
logger = logging.getLogger("profiling.04")


OUTPUT_DIR = RAIZ / "data" / "discovery"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

COD_MUN   = config.COD_MUN_IBGE      # "354130"
CNPJ_MANT = config.CNPJ_MANTENEDORA  # "55293427000117"

# ── CBOs de Auditoria (CONFIRMADOS PELO USUÁRIO) ───────────────────────────
CBOS_ACS_TACS   = ("515105", "322255")
CBOS_ACE_TACE   = ("515140", "322210", "322260")
TP_VALIDOS_ACS  = ("01", "02", "15")
TP_VALIDOS_ACE  = ("02", "69", "22", "15", "50")


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
        logger.info("  [%s] -> %d linhas x %d colunas", label, len(df), len(df.columns))
        return df
    except Exception as exc:
        logger.error("  ERRO [%s]: %s", label, exc)
        return pd.DataFrame()


def q_cursor(con: fdb.Connection, sql: str, label: str) -> pd.DataFrame:
    """Executa query via cursor direto (necessario para LEFT JOINs com fdb)."""
    try:
        cur = con.cursor()
        cur.execute(sql)
        cols = [desc[0].strip() for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        df = pd.DataFrame(rows, columns=cols)
        logger.info("  [%s] -> %d linhas x %d colunas", label, len(df), len(df.columns))
        return df
    except Exception as exc:
        logger.error("  ERRO [%s]: %s", label, exc)
        return pd.DataFrame()


def salvar(df: pd.DataFrame, nome: str) -> None:
    p = OUTPUT_DIR / nome
    df.to_csv(p, index=False, sep=";", encoding="utf-8-sig")
    logger.info("  -> %s", p)


# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 1 — Verificar presença dos CBOs relevantes no município
# ─────────────────────────────────────────────────────────────────────────────
_cbos_todos = CBOS_ACS_TACS + CBOS_ACE_TACE + ("516220",)
_cbos_in    = ", ".join(f"'{c}'" for c in _cbos_todos)

SQL_CBOS_PRESENTES = f"""
SELECT
    vinc.COD_CBO,
    COUNT(DISTINCT prof.CPF_PROF) AS QTD_PROF,
    COUNT(*)                      AS QTD_VINCULOS
FROM LFCES021 vinc
INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
WHERE vinc.COD_CBO IN ({_cbos_in})
  AND est.CODMUNGEST = '{COD_MUN}'
GROUP BY vinc.COD_CBO
ORDER BY 2 DESC
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 2 — Auditoria ACS/TACS (515105, 322255) × TP_UNID_ID
# Lota çao INCORRETA: qualquer TP_UNID_ID fora de 01, 02, 15
# ─────────────────────────────────────────────────────────────────────────────
_cbos_acs_in   = ", ".join(f"'{c}'" for c in CBOS_ACS_TACS)
_tp_valids_acs = ", ".join(f"'{t}'" for t in TP_VALIDOS_ACS)

SQL_AUDIT_ACS_RESUMO = f"""
SELECT
    vinc.COD_CBO,
    est.TP_UNID_ID,
    COUNT(DISTINCT prof.CPF_PROF) AS QTD_PROF,
    COUNT(*)                      AS QTD_VINCULOS
FROM LFCES021 vinc
INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
WHERE vinc.COD_CBO IN ({_cbos_acs_in})
  AND est.CODMUNGEST = '{COD_MUN}'
GROUP BY vinc.COD_CBO, est.TP_UNID_ID
ORDER BY vinc.COD_CBO, est.TP_UNID_ID
"""

SQL_AUDIT_ACS_ANOMALIAS = f"""
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
WHERE vinc.COD_CBO IN ({_cbos_acs_in})
  AND est.TP_UNID_ID NOT IN ({_tp_valids_acs})
  AND est.CODMUNGEST = '{COD_MUN}'
ORDER BY vinc.COD_CBO, prof.NOME_PROF
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 3 — Auditoria ACE/TACE (515140, 322210, 322260) × TP_UNID_ID
# ─────────────────────────────────────────────────────────────────────────────
_cbos_ace_in   = ", ".join(f"'{c}'" for c in CBOS_ACE_TACE)
_tp_valids_ace = ", ".join(f"'{t}'" for t in TP_VALIDOS_ACE)

SQL_AUDIT_ACE_RESUMO = f"""
SELECT
    vinc.COD_CBO,
    est.TP_UNID_ID,
    COUNT(DISTINCT prof.CPF_PROF) AS QTD_PROF,
    COUNT(*)                      AS QTD_VINCULOS
FROM LFCES021 vinc
INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
WHERE vinc.COD_CBO IN ({_cbos_ace_in})
  AND est.CODMUNGEST = '{COD_MUN}'
GROUP BY vinc.COD_CBO, est.TP_UNID_ID
ORDER BY vinc.COD_CBO, est.TP_UNID_ID
"""

SQL_AUDIT_ACE_ANOMALIAS = f"""
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
WHERE vinc.COD_CBO IN ({_cbos_ace_in})
  AND est.TP_UNID_ID NOT IN ({_tp_valids_ace})
  AND est.CODMUNGEST = '{COD_MUN}'
ORDER BY vinc.COD_CBO, prof.NOME_PROF
"""

# ─────────────────────────────────────────────────────────────────────────────
# ETAPA 4 — QUERY MASTER (enriquecida com IND_VINC decodificado + nomes de
# unidade e equipe) para documentar como base do cnes_client.py
# ─────────────────────────────────────────────────────────────────────────────
_SQL_MASTER = (
    f"SELECT"
    f" prof.CPF_PROF AS CPF,"
    f" prof.NOME_PROF AS NOME_PROFISSIONAL,"
    f" prof.NO_SOCIAL AS NOME_SOCIAL,"
    f" prof.SEXO,"
    f" prof.DATA_NASC AS DATA_NASCIMENTO,"
    f" vinc.COD_CBO AS CBO,"
    f" vinc.IND_VINC AS COD_VINCULO,"
    f" vinc.TP_SUS_NAO_SUS AS SUS_NAO_SUS,"
    f" (COALESCE(vinc.CG_HORAAMB,0)+COALESCE(vinc.CGHORAOUTR,0)+COALESCE(vinc.CGHORAHOSP,0)) AS CARGA_HORARIA_TOTAL,"
    f" COALESCE(vinc.CG_HORAAMB,0) AS CH_AMBULATORIAL,"
    f" COALESCE(vinc.CGHORAOUTR,0) AS CH_OUTRAS,"
    f" COALESCE(vinc.CGHORAHOSP,0) AS CH_HOSPITALAR,"
    f" est.CNES AS COD_CNES,"
    f" est.NOME_FANTA AS ESTABELECIMENTO,"
    f" est.TP_UNID_ID AS COD_TIPO_UNIDADE,"
    f" est.CODMUNGEST AS COD_MUN_GESTOR,"
    f" eq.INE AS COD_INE_EQUIPE,"
    f" eq.DS_AREA AS NOME_EQUIPE,"
    f" eq.TP_EQUIPE AS COD_TIPO_EQUIPE"
    f" FROM LFCES021 vinc"
    f" INNER JOIN LFCES004 est ON est.UNIDADE_ID=vinc.UNIDADE_ID"
    f" INNER JOIN LFCES018 prof ON prof.PROF_ID=vinc.PROF_ID"
    f" LEFT JOIN LFCES048 me ON me.CPF_PROF=prof.CPF_PROF AND me.COD_CBO=vinc.COD_CBO AND me.COD_MUN=est.CODMUNGEST"
    f" LEFT JOIN LFCES060 eq ON eq.SEQ_EQUIPE=me.SEQ_EQUIPE AND eq.COD_AREA=me.COD_AREA AND eq.COD_MUN=me.COD_MUN"
    f" WHERE est.CODMUNGEST='{COD_MUN}' AND est.CNPJ_MANT='{CNPJ_MANT}'"
    f" ORDER BY prof.NOME_PROF, vinc.COD_CBO"
)


def main() -> None:
    con = None
    try:
        con = conectar()
        logger.info("Conexão estabelecida.\n")

        # ── ETAPA 1: CBOs presentes no município ──────────────────────────
        logger.info("=" * 60)
        logger.info("ETAPA 1 — CBOs de auditoria presentes no município")
        logger.info("=" * 60)
        df = q(con, SQL_CBOS_PRESENTES, "CBOS_PRESENTES")
        if not df.empty:
            print(df.to_string(index=False))
            salvar(df, "04_cbos_auditoria_presentes.csv")
        else:
            logger.warning("Nenhum dos CBOs de auditoria encontrado no município!")

        # ── ETAPA 2: Auditoria ACS/TACS ───────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 2 — Auditoria ACS/TACS (515105, 322255)")
        logger.info("Lota çao correta: TP_UNID_ID IN ('01','02','15')")
        logger.info("=" * 60)

        logger.info("\n>>> Distribuição ACS/TACS por tipo de unidade:")
        df = q(con, SQL_AUDIT_ACS_RESUMO, "ACS_RESUMO")
        if not df.empty:
            print(df.to_string(index=False))

        logger.info("\n>>> Anomalias — ACS/TACS FORA de unidades tipo 01/02/15:")
        df = q(con, SQL_AUDIT_ACS_ANOMALIAS, "ACS_ANOMALIAS")
        if not df.empty:
            logger.warning("  ⚠️  %d CASO(S) DE LOTAÇÃO INCORRETA!", len(df))
            print(df.to_string(index=False))
            salvar(df, "04_audit_acs_anomalias.csv")
        else:
            logger.info("  ✅ Nenhuma anomalia — todos ACS/TACS em unidade correta.")

        # ── ETAPA 3: Auditoria ACE/TACE ───────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 3 — Auditoria ACE/TACE (515140, 322210, 322260)")
        logger.info("Lota çao correta: TP_UNID_ID IN ('02','69','22','15')")
        logger.info("=" * 60)

        logger.info("\n>>> Distribuição ACE/TACE por tipo de unidade:")
        df = q(con, SQL_AUDIT_ACE_RESUMO, "ACE_RESUMO")
        if not df.empty:
            print(df.to_string(index=False))
        else:
            logger.info("  Nenhum ACE/TACE encontrado com o filtro CNPJ.")

        logger.info("\n>>> Anomalias — ACE/TACE FORA das unidades previstas:")
        df = q(con, SQL_AUDIT_ACE_ANOMALIAS, "ACE_ANOMALIAS")
        if not df.empty:
            logger.warning("  ⚠️  %d CASO(S) DE LOTAÇÃO INCORRETA!", len(df))
            print(df.to_string(index=False))
            salvar(df, "04_audit_ace_anomalias.csv")
        else:
            logger.info("  ✅ Nenhuma anomalia (ou CBO não presente no banco).")

        # ── ETAPA 4: Query Master ─────────────────────────────────────────
        logger.info("\n" + "=" * 60)
        logger.info("ETAPA 4 — QUERY MASTER (base do cnes_client.py)")
        logger.info("=" * 60)
        logger.info("Executando Query Master completa com enriquecimento de equipe...")
        df_master = q_cursor(con, _SQL_MASTER, "QUERY_MASTER")
        if not df_master.empty:
            # Stats da query master
            logger.info("\n  📊 Estatísticas da Query Master:")
            logger.info("     Total de vínculos : %d", len(df_master))
            logger.info("     Profissionais únicos: %d", df_master["CPF"].nunique())
            logger.info("     Estabelecimentos   : %d", df_master["COD_CNES"].nunique())
            logger.info(
                "     Com equipe (INE)   : %d (%.1f%%)",
                df_master["COD_INE_EQUIPE"].notna().sum(),
                100 * df_master["COD_INE_EQUIPE"].notna().mean(),
            )
            logger.info(
                "     Sem equipe         : %d (%.1f%%)",
                df_master["COD_INE_EQUIPE"].isna().sum(),
                100 * df_master["COD_INE_EQUIPE"].isna().mean(),
            )

            # Distribuição por tipo de unidade
            logger.info("\n>>> Distribuição por TP_UNID_ID:")
            print(df_master.groupby("COD_TIPO_UNIDADE").agg(
                QTD_VINCULOS=("CPF", "count"),
                QTD_PROF=("CPF", "nunique")
            ).to_string())

            # Distribuição por IND_VINC (com descrição decodificada)
            decodificador_vinc = {
                "010101": "Servidor Próprio (Efetivo)",
                "010102": "Servidor Cedido",
                "010202": "Empregado CLT Próprio",
                "010203": "Empregado CLT Cedido",
                "010301": "Contratado Temporário Público",
                "010302": "Contratado Temporário Privado",
                "010403": "Cargo Comissão",
                "010500": "CLT Privado / OSS / OSCIP",
                "020900": "Autônomo",
                "021000": "Autônomo (PF)",
                "060101": "Estagiário",
                "070101": "Bolsista",
            }
            df_master["DS_VINCULO"] = df_master["COD_VINCULO"].map(decodificador_vinc).fillna("Outros")

            logger.info("\n>>> Distribuição por Tipo de Vínculo:")
            print(df_master.groupby(["COD_VINCULO", "DS_VINCULO"]).agg(
                QTD_VINCULOS=("CPF", "count"),
                QTD_PROF=("CPF", "nunique")
            ).to_string())

            salvar(df_master, "04_query_master_resultado.csv")
            logger.info("\n  -> Arquivo salvo para validação final.")

        logger.info("\n" + "=" * 60)
        logger.info("PROFILING 04 (FINAL) CONCLUÍDO. Outputs em: %s", OUTPUT_DIR)
        logger.info("=" * 60)

        # ── Exibe a Query Master para copiar para cnes_client.py ──────────
        logger.info("\n%s", "─" * 60)
        logger.info("QUERY MASTER (para copiar para src/ingestion/cnes_client.py):")
        logger.info("─" * 60)
        print(_SQL_MASTER)

    finally:
        if con:
            con.close()
            logger.info("Conexão encerrada.")


if __name__ == "__main__":
    main()
