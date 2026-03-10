import pandas as pd

def clean_employee_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Recebe um DataFrame bruto de funcionários e aplica regras de limpeza e transformação.
    
    Didática:
    - Na vida real, os dados nunca vêm perfeitos. Podem vir valores nulos, textos com espaços extras
      ou tipos de dados errados (um número que o Python acha que é texto, por exemplo).
    - O Pandas tem funções maravilhosas para resolver isso numa tacada só.
    """
    print("\n--- Iniciando Limpeza de Dados ---")
    
    # 1. Copiamos o DataFrame para não alterar os dados originais sem querer 
    # (boa prática de engenharia de dados).
    df_clean = df.copy()
    
    # 2. Tratamento de Nulos (Missing Values)
    # Lembra que no nosso CSV o Pedro não tinha Idade? O Pandas lê isso como 'NaN' (Not a Number).
    # Vamos preencher as idades vazias com a média das idades do resto da equipe!
    if df_clean['idade'].isnull().any():
        media_idade = df_clean['idade'].mean()
        # fillna preenche todos os vazios de uma vez
        df_clean['idade'] = df_clean['idade'].fillna(media_idade)
        print(f"✔️ Idades nulas preenchidas com a média: {media_idade:.1f} anos.")
    
    # 3. Transformação de Texto (Padronização)
    # Vamos garantir que todas as profissões fiquem com a primeira letra maiúscula e sem espaços inúteis.
    df_clean['profissao'] = df_clean['profissao'].str.strip().str.title()
    print("✔️ Profissões padronizadas (Textos limpos).")
    
    # 4. Criação de Colunas Calculadas
    # Vamos criar uma coluna nova chamada 'salario_anual'.
    # O Pandas consegue multiplicar uma coluna inteira de uma vez sem precisarmos fazer um 'for'.
    if 'salario' in df_clean.columns:
        df_clean['salario_anual'] = df_clean['salario'] * 12
        print("✔️ Coluna 'salario_anual' calculada com sucesso.")
        
    return df_clean
