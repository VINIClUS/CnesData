import os

import fdb


def run():
    dll = os.environ.get("FIREBIRD_DLL")
    dsn = os.environ.get("FIREBIRD_DSN")
    senha = os.environ.get("FIREBIRD_PASSWORD")
    if not dll or not dsn or not senha:
        raise RuntimeError(
            "defina FIREBIRD_DLL, FIREBIRD_DSN e FIREBIRD_PASSWORD (ex.: via .env)",
        )
    fdb.load_api(dll)
    con = fdb.connect(dsn=dsn, user="SYSDBA", password=senha)
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
