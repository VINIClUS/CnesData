import requests

def fetch_data_from_api(url: str, params: dict = None) -> dict:
    """
    Busca dados de uma API na internet.
    
    Didática:
    - O pacote 'requests' é a forma moderna e Elegante do Python fazer o que o comando 'curl' faz no terminal.
    - O método 'requests.get' envia um "pedido" (request) para a URL informada.
    - Se o servidor responder com o código 200 (OK), sabemos que a requisição deu certo.
    - Finalmente, na web, quase todos os dados trafegam no formato JSON. O requests.json() converte
      isso automaticamente para um 'Dicionário' no Python.
    """
    try:
        print(f"Iniciando requisição GET para a URL: {url}")
        response = requests.get(url, params=params)
        
        # Verifica se ocorreu algum erro HTML puro (ex: 404 Not Found, 500 Internal Error)
        response.raise_for_status() 
        
        data = response.json()
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao buscar dados na API: {e}")
        return None
