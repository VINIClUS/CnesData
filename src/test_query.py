import fdb
import pandas as pd

def test_query():
    fdb.load_api(r"C:\Users\CPD\Projetos\CnesData\fb_64\fbembed.dll")
    con = fdb.connect(dsn=r'localhost:C:\Datasus\CNES\CNES.GDB', user='SYSDBA', password='masterkey')
    
    query = """
    SELECT FIRST 5
        prof.CPF_PROF AS CPF,
        prof.NOME_PROF AS NOME_PROFISSIONAL,
        vinc.COD_CBO AS CBO,
        (COALESCE(vinc.CG_HORAAMB, 0) + COALESCE(vinc.CGHORAOUTR, 0) + COALESCE(vinc.CGHORAHOSP, 0)) AS CARGA_HORARIA,
        est.CNES AS COD_CNES,
        est.NOME_FANTA AS ESTABELECIMENTO,
        est.TP_UNID_ID AS TIPO_ESTAB,
        eq.INE AS COD_INE_EQUIPE,
        eq.DS_AREA AS NOME_EQUIPE,
        eq.DS_SEGMENTO AS TIPO_EQUIPE
    FROM LFCES021 vinc
    INNER JOIN LFCES004 est ON est.UNIDADE_ID = vinc.UNIDADE_ID
    INNER JOIN LFCES018 prof ON prof.PROF_ID = vinc.PROF_ID
    LEFT JOIN LFCES048 me 
        ON (me.CPF_PROF = prof.CPF_PROF AND me.COD_CBO = vinc.COD_CBO AND me.COD_MUN = est.CODMUNGEST)
    LEFT JOIN LFCES060 eq 
        ON (eq.SEQ_EQUIPE = me.SEQ_EQUIPE AND eq.COD_AREA = me.COD_AREA AND eq.COD_MUN = me.COD_MUN)
    WHERE est.CODMUNGEST = '354130' 
      AND est.CNPJ_MANT = '55293427000117'
    """
    try:
        df = pd.read_sql(query, con)
        print("Membros x Equipe TESTE:")
        print(df)
    except Exception as e:
        print("Erro SQL:", e)
    
    con.close()

if __name__ == '__main__':
    test_query()
