import fdb

def find_columns():
    try:
        fdb.load_api(r'C:\Users\CPD\Projetos\CnesData\fb_64\fbembed.dll')
        con = fdb.connect(dsn=r'localhost:C:\Datasus\CNES\CNES.GDB', user='SYSDBA', password='masterkey')
        cur = con.cursor()
        
        # Buscar tabelas que tem as strings CPF, CNES, CBO, EQUIPE, NOME
        query = """
        SELECT RDB$RELATION_NAME, RDB$FIELD_NAME 
        FROM RDB$RELATION_FIELDS 
        WHERE RDB$SYSTEM_FLAG = 0
        """
        cur.execute(query)
        rows = cur.fetchall()
        
        tables_with_cpf = set()
        tables_with_cnes = set()
        tables_with_nome = set()
        tables_with_equipe = set()
        tables_with_cns = set()
        
        for table, col in rows:
            table = table.strip()
            col = col.strip()
            
            if 'CPF' in col: tables_with_cpf.add(table)
            if 'CNES' in col: tables_with_cnes.add(table)
            if 'NOME' in col: tables_with_nome.add(table)
            if 'EQUIP' in col: tables_with_equipe.add(table)
            if 'CNS' in col: tables_with_cns.add(table)
            
        print("Tabelas com CPF e NOME:", tables_with_cpf.intersection(tables_with_nome))
        print("Tabelas com CNES e NOME:", tables_with_cnes.intersection(tables_with_nome))
        print("Tabelas com EQUIPE:", tables_with_equipe)
        
        print("\nColunas da tabela com CPF e NOME (possíveis Profissionais):")
        for t in list(tables_with_cpf.intersection(tables_with_nome))[:5]:
            cur.execute(f"SELECT RDB$FIELD_NAME FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = '{t}'")
            print(t, [c[0].strip() for c in cur.fetchall()])
            
        con.close()
    except Exception as e:
        print(f"Erro: {e}")

if __name__ == "__main__":
    find_columns()
