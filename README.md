# CnesData

Pipeline de Engenharia e Análise de Dados para o banco **CNES** (Cadastro Nacional de Estabelecimentos de Saúde) da **Prefeitura Municipal de Presidente Epitácio/SP** (IBGE 354130).

## Sobre o Projeto

Extrai, transforma e exporta dados de profissionais de saúde vinculados aos estabelecimentos mantidos pelo CNPJ `55.293.427/0001-17`, a partir do banco Firebird local do DATASUS. Cruza esses dados com a base nacional CNES via BigQuery para detectar inconsistências.

## Pré-requisitos

- Python 3.11+
- Banco `CNES.GDB` em execução (padrão: `localhost`)
- DLL do Firebird 64-bits (`fbembed.dll`)
- Conta Google Cloud com acesso ao projeto BigQuery (para cross-check nacional)

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
# Banco Firebird
DB_HOST=localhost
DB_PATH=C:\Datasus\CNES\CNES.GDB
DB_USER=SYSDBA
DB_PASSWORD=masterkey
FIREBIRD_DLL=C:\caminho\para\fb_64\fbembed.dll

# Filtros do município
COD_MUN_IBGE=354130
CNPJ_MANTENEDORA=55293427000117

# Saída
OUTPUT_DIR=data/processed
OUTPUT_FILENAME=Relatorio_Profissionais_CNES.csv

# Google Cloud / BigQuery (cross-check nacional)
GCP_PROJECT_ID=seu-projeto-gcp

# Competência da base nacional (padrão: dez/2024)
COMPETENCIA_ANO=2024
COMPETENCIA_MES=12

# Folha de pagamento RH (opcional — omita para pular cross-check CNES × RH)
FOLHA_HR_PATH=C:\caminho\para\folha.xlsx
```

## Como Executar

```powershell
# Ativar o ambiente
.\env\Scripts\Activate.ps1

# Rodar o pipeline completo
python src\main.py
```

## Saídas Geradas

Todos os arquivos são criados em `data/processed/`:

| Arquivo | Regra | Descrição |
|---|---|---|
| `Relatorio_Profissionais_CNES.csv` | — | Relatório principal com todos os vínculos ativos |
| `Relatorio_Profissionais_CNES.xlsx` | — | Relatório Excel multi-aba com recomendações |
| `auditoria_rq003b_multiplas_unidades.csv` | RQ-003-B | Profissionais com vínculos em 2+ unidades |
| `auditoria_rq005_acs_tacs_incorretos.csv` | RQ-005 | ACS/TACS lotados em unidade incorreta |
| `auditoria_rq005_ace_tace_incorretos.csv` | RQ-005 | ACE/TACE lotados em unidade incorreta |
| `auditoria_ghost_payroll.csv` | WP-003 | Ativos no CNES, ausentes/inativos no RH |
| `auditoria_missing_registration.csv` | WP-004 | Ativos no RH, ausentes no CNES local |
| `auditoria_rq006_estab_fantasma.csv` | RQ-006 | Estabelecimentos locais sem correspondência nacional |
| `auditoria_rq007_estab_ausente_local.csv` | RQ-007 | Estabelecimentos nacionais ausentes no local |
| `auditoria_rq008_prof_fantasma_cns.csv` | RQ-008 | Profissionais locais (por CNS) sem correspondência nacional |
| `auditoria_rq009_prof_ausente_local_cns.csv` | RQ-009 | Profissionais nacionais (por CNS) ausentes no local |
| `auditoria_rq010_divergencia_cbo.csv` | RQ-010 | Divergência de CBO entre local e nacional |
| `auditoria_rq011_divergencia_ch.csv` | RQ-011 | Divergência de carga horária (tolerância: 2h) |

Arquivos de auditoria são criados apenas quando há anomalias detectadas. O log da execução é salvo em `logs/cnes_exporter.log`.

## Testes

```powershell
# Testes unitários (rápidos, sem banco)
pytest tests/ -m "not integration" -v

# Testes de integração (requer banco em execução)
pytest tests/ -m integration -v

# Todos os testes
pytest tests/ -v
```

271 testes unitários passando.

## Estrutura do Projeto

```
CnesData/
├── .env                        # Credenciais locais (não vai ao Git)
├── .gitignore
├── requirements.txt
├── data_dictionary.md          # Schema Firebird e regras de auditoria documentadas
├── ROADMAP.md                  # Work Packages e critérios de aceite
├── data/
│   ├── raw/                    # Dados brutos de entrada
│   ├── processed/              # Relatórios gerados (não vai ao Git)
│   └── snapshots/              # Snapshots históricos JSON (não vai ao Git)
├── logs/                       # Logs de execução (não vai ao Git)
├── scripts/                    # Scripts de exploração e referência (não entram no pipeline)
├── src/
│   ├── config.py               # Configuração centralizada (lê o .env)
│   ├── main.py                 # Ponto de entrada e orquestrador do pipeline
│   ├── ingestion/
│   │   ├── base.py             # Protocols PEP 544 (contratos de interface)
│   │   ├── schemas.py          # Schema canônico de colunas
│   │   ├── cnes_client.py      # Queries Firebird (cursor-based)
│   │   ├── cnes_local_adapter.py   # Adapter Firebird → schema canônico
│   │   ├── cnes_nacional_adapter.py # Adapter BigQuery → schema canônico
│   │   ├── hr_client.py        # Parser de planilhas RH (.xlsx/.csv)
│   │   └── web_client.py       # Cliente BigQuery via basedosdados
│   ├── processing/
│   │   └── transformer.py      # Limpeza e validação (RQ-002, RQ-003)
│   ├── analysis/
│   │   ├── rules_engine.py     # Motor de regras de auditoria (RQ-003-B, RQ-005–RQ-011)
│   │   └── evolution_tracker.py # Snapshots históricos JSON
│   └── export/
│       ├── csv_exporter.py     # Exportação CSV (padrão BR)
│       └── report_generator.py # Relatório Excel multi-aba com recomendações
└── tests/
    ├── ingestion/
    │   ├── test_base.py
    │   ├── test_cnes_local_adapter.py
    │   ├── test_cnes_nacional_adapter.py
    │   ├── test_hr_client.py
    │   └── test_web_client.py
    ├── analysis/
    │   ├── test_cross_check.py
    │   ├── test_evolution_tracker.py
    │   └── test_rules_engine.py
    ├── export/
    │   └── test_report_generator.py
    └── test_main.py
```
