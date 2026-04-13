import pandas as pd
from sqlalchemy import create_engine


def load_from_sql(query: str, connection_string: str) -> pd.DataFrame:
    """
    Executa uma consulta SQL em um banco de dados e retorna os resultados em um DataFrame do Pandas.
    
    Didática:
    - O 'SQLAlchemy' é a biblioteca que cria uma "ponte" entre o Python e o Banco de Dados.
    - Essa ponte é chamada de 'Engine'.
    - O Pandas tem uma função nativa 'read_sql' que usa a Engine para executar o SQL (SELECT) 
      e já devolver a tabela pronta.
    """
    try:
        # Cria a conexão com o banco de dados (ex: SQLite, Postgres, SQL Server)
        engine = create_engine(connection_string)
        
        # O Pandas faz o trabalho pesado de rodar a query e estruturar a tabela
        df = pd.read_sql(query, engine)
        print(f"✅ Consulta SQL executada com sucesso. Linhas retornadas: {len(df)}")
        return df
    except Exception as e:
        print(f"❌ Erro ao executar SQL: {e}")
        return None
