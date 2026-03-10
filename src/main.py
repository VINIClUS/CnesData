"""
Arquivo principal (Orquestrador) do Projeto CnesData.
É por aqui que o fluxo de dados principal será iniciado e gerenciado.
"""
from ingestion.api_client import fetch_data_from_api
from ingestion.file_client import load_csv
from processing.transformer import clean_employee_data
from analysis.analyzer import analyze_and_export
# importaremos a leitura de bancos de dados futuramente!

def main():
    print("="*40)
    print("Iniciando o pipeline do Projeto CnesData")
    print("="*40)
    
    print("\n--- Teste de Ingestão 1: API (ViaCEP) ---")
    # API pública brasileira livre e sem necessidade de tokens, perfeita para testes.
    # Vamos pesquisar as informações da Avenida Paulista pelo CEP
    url_teste = "https://viacep.com.br/ws/01310100/json/"
    
    dados = fetch_data_from_api(url_teste)
    
    if dados:
        print("Sucesso! O Dicionário Python gerado a partir do JSON recebido é:")
        print(f"> CEP: {dados.get('cep')}")
        print(f"> Logradouro: {dados.get('logradouro')}")
        print(f"> Bairro: {dados.get('bairro')}")
        print(f"> Cidade/UF: {dados.get('localidade')} / {dados.get('uf')}")
        
    print("\n--- Teste de Ingestão 2: Planilhas (CSV) ---")
    caminho_csv = r"data\raw\funcionarios.csv"
    df = load_csv(caminho_csv)
    
    
    if df is not None:
        print("\nOs primeiros registros do nosso DataFrame (tabela BRUTA) são:")
        print(df.head())
        
        # --- Fase 2: Processamento e Limpeza ---
        df_limpo = clean_employee_data(df)
        print("\nOs registros do nosso DataFrame (tabela LIMPA) ficaram assim:")
        print(df_limpo.head())
        
        # --- Fase 3: Análise e Exportação ---
        caminho_saida = r"data\processed\funcionarios_limpos.csv"
        analyze_and_export(df_limpo, caminho_saida)


if __name__ == "__main__":
    # Garante que o main só será chamado se executarmos este arquivo diretamente.
    main()
