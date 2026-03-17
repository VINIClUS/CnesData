# CnesData

Pipeline de Engenharia e Análise de Dados para o banco **CNES** (Cadastro Nacional de Estabelecimentos de Saúde) da **Prefeitura Municipal de Presidente Epitácio/SP** (IBGE 354130).

## Sobre o Projeto

Extrai, transforma e exporta dados de profissionais de saúde vinculados aos estabelecimentos mantidos pelo CNPJ `55.293.427/0001-17`, a partir do banco Firebird local do DATASUS.

## Pré-requisitos

- Python 3.11+
- Banco `CNES.GDB` em execução (padrão: `localhost`)
- DLL do Firebird 64-bits (`fbembed.dll`)

## Instalação

```powershell
# 1. Criar e ativar o ambiente virtual
python -m venv env
.\env\Scripts\Activate.ps1

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Configurar o ambiente
# Copie o .env.example e preencha com seus valores locais
copy .env.example .env
```

## Configuração (`.env`)

```ini
DB_HOST=localhost
DB_PATH=C:\Datasus\CNES\CNES.GDB
DB_USER=SYSDBA
DB_PASSWORD=masterkey
FIREBIRD_DLL=C:\caminho\para\fb_64\fbembed.dll
COD_MUN_IBGE=354130
CNPJ_MANTENEDORA=55293427000117
OUTPUT_DIR=data/processed
OUTPUT_FILENAME=Relatorio_Profissionais_CNES.csv
```

## Como Executar

```powershell
# Ativar o ambiente
.\env\Scripts\Activate.ps1

# Rodar o pipeline completo
python src\main.py
```

O relatório é gerado em `data/processed/Relatorio_Profissionais_CNES.csv`  
O log da execução é salvo em `logs/cnes_exporter.log`

## Testes

```powershell
# Testes unitários (rápidos, sem banco)
pytest tests/ -m "not integration" -v

# Testes de integração (requer banco em execução)
pytest tests/ -m integration -v

# Todos os testes
pytest tests/ -v
```

## Estrutura do Projeto

```
CnesData/
├── .env                    # Credenciais locais (não vai ao Git)
├── .gitignore
├── requirements.txt
├── data/
│   ├── raw/                # Dados brutos de entrada
│   └── processed/          # Relatórios gerados (não vai ao Git)
├── logs/                   # Logs de execução (não vai ao Git)
├── scripts/                # Scripts de exploração e referência
├── src/
│   ├── config.py           # Configuração centralizada (lê o .env)
│   ├── main.py             # Ponto de entrada do pipeline
│   ├── cnes_exporter.py    # ETL: conectar → extrair → transformar → exportar
│   └── exemplos/           # Módulos didáticos (API, CSV, análise)
└── tests/
    ├── conftest.py
    ├── test_config.py          # Testes unitários de configuração
    ├── test_transformer.py     # Testes unitários de transformação
    └── test_exporter_integration.py  # Testes de integração E2E
```