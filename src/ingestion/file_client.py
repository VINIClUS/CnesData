import pandas as pd

def load_csv(filepath: str) -> pd.DataFrame:
    """
    Lê um arquivo CSV do disco e transforma numa tabela do Pandas.
    
    Didática:
    - O 'Pandas' é como se fosse um "Excel com esteroides" dentro do Python.
    - A estrutura de dados base do Pandas é chamada de 'DataFrame' (abreviado por df).
    - Um DataFrame é exatamente uma tabela com Linhas e Colunas.
    """
    try:
        # read_csv é a função mágica que lê o arquivo e cria a tabela na memória
        df = pd.read_csv(filepath)
        print(f"✅ CSV [{filepath}] carregado. Tamanho: {df.shape[0]} linhas e {df.shape[1]} colunas.")
        return df
    except Exception as e:
        print(f"❌ Erro ao ler o CSV {filepath}: {e}")
        return None

def load_excel(filepath: str, sheet_name=0) -> pd.DataFrame:
    """
    Lê uma planilha Excel legada (.xls) ou moderna (.xlsx).
    
    O openpyxl e o xlrd (que instalamos) rodam nos bastidores para que
    o Pandas consiga ler as abas do Excel.
    """
    try:
        # sheet_name pode ser o nome da aba ("Plan1") ou o índice (0 para a primeira aba)
        df = pd.read_excel(filepath, sheet_name=sheet_name)
        print(f"✅ Excel [{filepath}] carregado. Tamanho: {df.shape[0]} linhas e {df.shape[1]} colunas.")
        return df
    except Exception as e:
        print(f"❌ Erro ao ler o Excel {filepath}: {e}")
        return None
