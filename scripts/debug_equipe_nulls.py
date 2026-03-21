"""Script de diagnóstico para NULLs nas colunas de equipe — LFCES048/LFCES060."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import fdb
import pandas as pd
import config
from ingestion.cnes_client import carregar_driver

COD_MUN = config.COD_MUN_IBGE
CNPJ    = config.CNPJ_MANTENEDORA


def _cursor_df(con: fdb.Connection, sql: str) -> pd.DataFrame:
    cur = con.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    finally:
        cur.close()
    return pd.DataFrame(rows, columns=cols)


def teste_1(con):
    print("\n=== TESTE 1: COUNT LFCES048 para município ===")
    df = _cursor_df(con, f"SELECT COUNT(*) AS TOTAL FROM LFCES048 WHERE COD_MUN = '{COD_MUN}'")
    print(df.to_string(index=False))


def teste_2(con):
    print("\n=== TESTE 2: COUNT LFCES060 para município ===")
    df = _cursor_df(con, f"SELECT COUNT(*) AS TOTAL FROM LFCES060 WHERE COD_MUN = '{COD_MUN}'")
    print(df.to_string(index=False))


def teste_3(con):
    print("\n=== TESTE 3: JOIN LFCES048 isolado (sem TRIM) ===")
    sql = f"""
        SELECT FIRST 10
            prof.CPF_PROF,
            me.SEQ_EQUIPE,
            me.COD_AREA,
            me.COD_MUN
        FROM LFCES021 vinc
        INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
        INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
        LEFT  JOIN LFCES048 me   ON me.CPF_PROF = prof.CPF_PROF
                                AND me.COD_CBO  = vinc.COD_CBO
                                AND me.COD_MUN  = est.CODMUNGEST
        WHERE est.CODMUNGEST = '{COD_MUN}'
          AND est.CNPJ_MANT  = '{CNPJ}'
    """
    df = _cursor_df(con, sql)
    null_count = df["SEQ_EQUIPE"].isna().sum()
    print(df.to_string(index=False))
    print(f"\n  → SEQ_EQUIPE NULL: {null_count}/{len(df)}")


def teste_4(con):
    # T4 (CHAR_LENGTH) — pulado: funções de string não disponíveis no Firebird 2.5
    # T3 já confirmou que o JOIN funciona; padding será detectado indiretamente pelo T5.
    print("\n=== TESTE 4: pulado (funções de string indisponíveis no Firebird 2.5) ===")


def teste_5(con):
    # TRIM indisponível neste Firebird embedado — H1 descartada via T3 (JOIN já funciona sem TRIM)
    print("\n=== TESTE 5: pulado (TRIM indisponível no Firebird embedado) ===")


def teste_6(con):
    print("\n=== TESTE 6: Query Master ORIGINAL (sem CNS) ===")
    sql = f"""
        SELECT FIRST 10
            prof.CPF_PROF    AS CPF,
            prof.NOME_PROF   AS NOME_PROFISSIONAL,
            eq.INE           AS COD_INE_EQUIPE,
            eq.DS_AREA       AS NOME_EQUIPE,
            eq.TP_EQUIPE     AS COD_TIPO_EQUIPE
        FROM LFCES021 vinc
        INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
        INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
        LEFT  JOIN LFCES048 me   ON me.CPF_PROF = prof.CPF_PROF
                                AND me.COD_CBO  = vinc.COD_CBO
                                AND me.COD_MUN  = est.CODMUNGEST
        LEFT  JOIN LFCES060 eq   ON eq.SEQ_EQUIPE = me.SEQ_EQUIPE
                                AND eq.COD_AREA   = me.COD_AREA
                                AND eq.COD_MUN    = me.COD_MUN
        WHERE est.CODMUNGEST = '{COD_MUN}'
          AND est.CNPJ_MANT  = '{CNPJ}'
        ORDER BY prof.NOME_PROF
    """
    df = _cursor_df(con, sql)
    null_ine = df["COD_INE_EQUIPE"].isna().sum()
    print(df.to_string(index=False))
    print(f"\n  → INE NULL: {null_ine}/{len(df)}")


def teste_7(con):
    print("\n=== TESTE 7: Query split — LFCES048+LFCES060 isolado (sem TRIM) ===")
    sql = f"""
        SELECT FIRST 10
            me.CPF_PROF  AS CPF,
            me.COD_CBO   AS CBO,
            eq.INE        AS COD_INE_EQUIPE,
            eq.DS_AREA    AS NOME_EQUIPE,
            eq.TP_EQUIPE  AS COD_TIPO_EQUIPE
        FROM LFCES048 me
        INNER JOIN LFCES060 eq ON eq.SEQ_EQUIPE = me.SEQ_EQUIPE
                               AND eq.COD_AREA   = me.COD_AREA
                               AND eq.COD_MUN    = me.COD_MUN
        WHERE me.COD_MUN = '{COD_MUN}'
    """
    df = _cursor_df(con, sql)
    print(df.to_string(index=False))
    print(f"\n  → Registros com equipe: {len(df)}")


def teste_8(con):
    print("\n=== TESTE 8: Query de produção atual (com CNS) — amostra 10 linhas ===")
    sql = f"""
        SELECT FIRST 10
            prof.CPF_PROF            AS CPF,
            prof.COD_CNS             AS CNS,
            prof.NOME_PROF           AS NOME_PROFISSIONAL,
            eq.INE                   AS COD_INE_EQUIPE,
            eq.DS_AREA               AS NOME_EQUIPE,
            eq.TP_EQUIPE             AS COD_TIPO_EQUIPE
        FROM LFCES021 vinc
        INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
        INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
        LEFT  JOIN LFCES048 me   ON me.CPF_PROF    = prof.CPF_PROF
                                AND me.COD_CBO     = vinc.COD_CBO
                                AND me.COD_MUN     = est.CODMUNGEST
        LEFT  JOIN LFCES060 eq   ON eq.SEQ_EQUIPE  = me.SEQ_EQUIPE
                                AND eq.COD_AREA    = me.COD_AREA
                                AND eq.COD_MUN     = me.COD_MUN
        WHERE est.CODMUNGEST = '{COD_MUN}'
          AND est.CNPJ_MANT  = '{CNPJ}'
        ORDER BY prof.NOME_PROF, vinc.COD_CBO
    """
    df = _cursor_df(con, sql)
    null_ine = df["COD_INE_EQUIPE"].isna().sum()
    print(df.to_string(index=False))
    print(f"\n  → INE NULL: {null_ine}/{len(df)}")


def teste_9(con):
    print("\n=== TESTE 9: Conteúdo bruto de LFCES060 (primeiros 10) ===")
    sql = f"SELECT FIRST 10 * FROM LFCES060 WHERE COD_MUN = '{COD_MUN}'"
    df = _cursor_df(con, sql)
    print(df.to_string(index=False))

    print("\n=== TESTE 9b: Conteúdo bruto de LFCES048 (primeiros 5 com SEQ_EQUIPE não nulo) ===")
    sql2 = f"""
        SELECT FIRST 5 SEQ_EQUIPE, COD_AREA, COD_MUN, CPF_PROF, COD_CBO
        FROM LFCES048
        WHERE COD_MUN = '{COD_MUN}'
          AND SEQ_EQUIPE IS NOT NULL
    """
    df2 = _cursor_df(con, sql2)
    print(df2.to_string(index=False))

    print("\n=== TESTE 9c: JOIN direto LFCES048 × LFCES060 só por SEQ_EQUIPE ===")
    sql3 = f"""
        SELECT FIRST 10
            me.SEQ_EQUIPE AS SEQ_048,
            eq.SEQ_EQUIPE AS SEQ_060,
            me.COD_MUN    AS MUN_048,
            eq.COD_MUN    AS MUN_060
        FROM LFCES048 me
        INNER JOIN LFCES060 eq ON eq.SEQ_EQUIPE = me.SEQ_EQUIPE
        WHERE me.COD_MUN = '{COD_MUN}'
    """
    df3 = _cursor_df(con, sql3)
    print(df3.to_string(index=False))
    print(f"\n  → Registros: {len(df3)}")


def teste_10(con):
    print("\n=== TESTE 10: Fix — JOIN somente por SEQ_EQUIPE (sem COD_AREA, sem COD_MUN) ===")
    sql = f"""
        SELECT FIRST 15
            prof.CPF_PROF   AS CPF,
            prof.NOME_PROF  AS NOME,
            me.SEQ_EQUIPE   AS SEQ_048,
            eq.SEQ_EQUIPE   AS SEQ_060,
            eq.INE          AS COD_INE_EQUIPE,
            eq.DS_AREA      AS NOME_EQUIPE,
            eq.TP_EQUIPE    AS COD_TIPO_EQUIPE
        FROM LFCES021 vinc
        INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
        INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
        LEFT  JOIN LFCES048 me   ON me.CPF_PROF    = prof.CPF_PROF
                                AND me.COD_CBO     = vinc.COD_CBO
                                AND me.COD_MUN     = est.CODMUNGEST
        LEFT  JOIN LFCES060 eq   ON eq.SEQ_EQUIPE  = me.SEQ_EQUIPE
        WHERE est.CODMUNGEST = '{COD_MUN}'
          AND est.CNPJ_MANT  = '{CNPJ}'
        ORDER BY prof.NOME_PROF
    """
    df = _cursor_df(con, sql)
    null_ine = df["COD_INE_EQUIPE"].isna().sum()
    nao_null_ine = len(df) - null_ine
    print(df.to_string(index=False))
    print(f"\n  → INE preenchido: {nao_null_ine}/{len(df)}")
    print(f"  → INE NULL: {null_ine}/{len(df)}")


def teste_11(con):
    print("\n=== TESTE 11: SEQ_EQUIPE 2421/1580/3462 existem em LFCES060? ===")
    for seq in [2421, 1580, 3462, 2402, 2239]:
        sql = f"SELECT COUNT(*) AS CNT FROM LFCES060 WHERE SEQ_EQUIPE = {seq}"
        df = _cursor_df(con, sql)
        cnt = df["CNT"].iloc[0]
        print(f"  SEQ_EQUIPE={seq}: {cnt} registro(s) em LFCES060")

    print("\n=== TESTE 11b: Total de SEQ_EQUIPE distintos em LFCES048 para o município ===")
    sql2 = f"""
        SELECT SEQ_EQUIPE, COD_AREA, COD_MUN, COUNT(*) AS MEMBROS
        FROM LFCES048
        WHERE COD_MUN = '{COD_MUN}'
          AND SEQ_EQUIPE IS NOT NULL
        GROUP BY SEQ_EQUIPE, COD_AREA, COD_MUN
        ORDER BY SEQ_EQUIPE
    """
    df2 = _cursor_df(con, sql2)
    print(df2.to_string(index=False))

    print("\n=== TESTE 11c: LFCES060 completo para SEQ_EQUIPE presentes em LFCES048 ===")
    sql3 = f"""
        SELECT FIRST 20
            eq.SEQ_EQUIPE, eq.COD_MUN, eq.COD_AREA, eq.INE, eq.DS_AREA, eq.TP_EQUIPE
        FROM LFCES060 eq
        WHERE eq.SEQ_EQUIPE IN (
            SELECT DISTINCT SEQ_EQUIPE FROM LFCES048
            WHERE COD_MUN = '{COD_MUN}'
              AND SEQ_EQUIPE IS NOT NULL
        )
    """
    df3 = _cursor_df(con, sql3)
    print(df3.to_string(index=False))
    print(f"\n  → Registros: {len(df3)}")


def main():
    carregar_driver(Path(config.FIREBIRD_DLL))
    con = fdb.connect(dsn=config.DB_DSN, user=config.DB_USER, password=config.DB_PASSWORD)
    try:
        teste_11(con)
    finally:
        con.close()
    print("\n=== Diagnóstico concluído ===")


if __name__ == "__main__":
    main()
