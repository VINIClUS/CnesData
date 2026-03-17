import pandas as pd
import os

def analyze_and_export(df: pd.DataFrame, output_path: str):
    """
    Realiza análises básicas nos dados e os exporta para um arquivo final pronto para uso
    (ou para enviar para uma ferramenta de Dashboard como PowerBI, Metabase, etc).
    """
    print("\n--- Fase 3: Análises Gerenciais ---")
    
    # Exemplo Didático de Agrupamento (Group By)
    # Qual é o salário médio por profissão?
    print("\n[Insights] Salário Médio Anual por Cargo:")
    salarios = df.groupby('profissao')['salario_anual'].mean().reset_index()
    # Formatando para duas casas decimais
    pd.options.display.float_format = '{:,.2f}'.format
    print(salarios.to_string(index=False))
    
    # Exportação de Dados Prontos
    try:
        # Pega só as pastas (ex: data/processed) e garante que existem
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Salva o DataFrame em um arquivo CSV limpo, sem imprimir o índice do pandas
        df.to_csv(output_path, index=False)
        print(f"\n✅ Relatório final exportado com sucesso para: {output_path}")
    except Exception as e:
        print(f"❌ Erro ao exportar resultado final: {e}")
