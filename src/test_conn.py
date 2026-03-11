import fdb
import traceback

def run():
    try:
        fdb.load_api(r'C:\Users\CPD\Projetos\CnesData\fb_64\fbembed.dll')
        con = fdb.connect(dsn=r'localhost:C:\Datasus\CNES\CNES.GDB', user='SYSDBA', password='masterkey')
        cur = con.cursor()
        cur.execute('SELECT RDB$RELATION_NAME FROM RDB$RELATIONS WHERE RDB$SYSTEM_FLAG=0')
        open('tables.txt', 'w', encoding='utf-8').write('\n'.join(row[0].strip() for row in cur.fetchall()))
        con.close()
        print("Sucesso!")
    except Exception as e:
        open('error.txt', 'w', encoding='utf-8').write(traceback.format_exc())
        print("Erro gravado em error.txt")

run()
