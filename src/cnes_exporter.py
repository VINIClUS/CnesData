import fdb
import pandas as pd
import os

def export_cnes_data():
    print("="*50)
    print("Iniciando Exportação de Dados do Banco CNES (Local)")
    print("="*50)
    
    try:
        # Carrega a API do Firebird de 64 bits (nossa solução para o erro de Win32)
        dll_path = r"C:\Users\CPD\Projetos\CnesData\fb_64\fbembed.dll"
        if not os.path.exists(dll_path):
            raise FileNotFoundError(f"DLL do Firebird não encontrada em: {dll_path}")
            
        fdb.load_api(dll_path)
        print("✔️ DLL do Firebird (64-bits) carregada com sucesso.")
        
        # Conecta no banco local do DataSUS
        db_path = r"localhost:C:\Datasus\CNES\CNES.GDB"
        con = fdb.connect(dsn=db_path, user='SYSDBA', password='masterkey')
        print("✔️ Conectado ao banco CNES.GDB")
        
        # ----------------------------------------------------
        # CRIANDO A QUERY (A "Mágica" do SQL com Tabelas CNES)
        # ----------------------------------------------------
        # vinc = LFCES021 (Vínculo de Profissionais a Estabelecimentos)
        # est  = LFCES004 (Estabelecimentos e suas Mantenedoras)
        # prof = LFCES018 (Cadastro de Profissionais - Pessoas Físicas)
        # me   = LFCES048 (Vínculos de Profissional com Equipe)
        # eq   = LFCES060 (Dados das Equipes)
        query = """
        SELECT 
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
        WHERE 
            est.CODMUNGEST = '354130'
            AND est.CNPJ_MANT = '55293427000117'
        """
        
        print("\n⏳ Executando consulta cruzando Profissionais, Estabelecimentos e Equipes...")
        
        # Ignora avisos do Alchemy para conexões nativas DBAPI do Pandas
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            df = pd.read_sql(query, con)
        
        print("\n--- Amostra dos Dados Coletados ---")
        print(df.head())
        print(f"\nTotal de Profissionais extraídos: {len(df)}")
        
        if len(df) == 0:
            print("Nenhum dado retornado. Verifique a base de dados.")
            return

        # Preencher vazios
        df['NOME_EQUIPE'] = df['NOME_EQUIPE'].fillna('SEM EQUIPE VINCULADA')
        df['COD_INE_EQUIPE'] = df['COD_INE_EQUIPE'].fillna('-')
        df['TIPO_EQUIPE'] = df['TIPO_EQUIPE'].fillna('-')
        
        # ----------------------------------------------------
        # EXPORTANDO PARA CSV
        # ----------------------------------------------------
        output_dir = r"data\processed"
        os.makedirs(output_dir, exist_ok=True)
        csv_path = os.path.join(output_dir, "Relatorio_Profissionais_CNES.csv")
        
        print("\n⏳ Gerando o arquivo CSV...")
        df.to_csv(csv_path, index=False, sep=';', encoding='utf-8-sig')
        print(f"✅ Relatório CSV salvo com sucesso em: {csv_path}")
        
    except Exception as e:
        print(f"\n❌ Ocorreu um erro durante a integração CNES: {e}")
    finally:
        if 'con' in locals() and con:
            con.close()
            print("✔️ Conexão com banco CNES encerrada.")

if __name__ == '__main__':
    export_cnes_data()
