import fdb

def run():
    fdb.load_api(r'C:\Users\CPD\Projetos\CnesData\fb_64\fbembed.dll')
    con = fdb.connect(dsn=r'localhost:C:\Datasus\CNES\CNES.GDB', user='SYSDBA', password='masterkey')
    cur = con.cursor()
    
    tables = [
        # Tabelas de equipe prováveis
        'LFCES048', 'LFCES060', 'LFCES020', 'LFCES044', 'HT_LFCES04', 'LFCES076', 'LFCES043', 
        # Tabelas Base
        'LFCES004', 'LFCES018', 'NFCES088'
    ]
    
    with open('invest_out.txt', 'w', encoding='utf-8') as f:
        for t in tables:
            try:
                cur.execute(f"SELECT FIRST 1 * FROM {t}")
                col_names = [desc[0] for desc in cur.description]
                row = cur.fetchone()
                
                f.write(f"\n--- Tabela: {t} ---\n")
                f.write(f"Colunas: {col_names}\n")
                f.write(f"Exemplo: {row}\n")
            except Exception:
                f.write(f"Erro ao ler {t}\n")

    con.close()

if __name__ == '__main__':
    run()
