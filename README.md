# CnesData

Pipeline de auditoria do CNES (Cadastro Nacional de Estabelecimentos de Saúde)
para a Prefeitura Municipal de Presidente Epitácio/SP (IBGE 354130).

Cruza dados do banco Firebird local com a base nacional CNES via BigQuery,
detectando inconsistências cadastrais, profissionais fantasma e divergências
de atributos entre as duas fontes.

## Início Rápido

### Pré-requisitos

- Python 3.11+
- Banco `CNES.GDB` em execução local (DATASUS)
- DLL do Firebird 64-bits (`fbembed.dll`)
- (Opcional) Conta Google Cloud para cross-check nacional via BigQuery

### Instalação

```powershell
# 1. Criar e ativar o ambiente virtual
python -m venv venv
.\venv\Scripts\Activate.ps1

# 2. Instalar dependências
pip install -r requirements.txt

# 3. Configurar o ambiente
copy .env.example .env
# Edite .env com seus valores locais (ver seção Configuração abaixo)
```

### Primeira Execução

```powershell
# Modo offline — sem BigQuery, ideal para primeira vez
python src\main.py --skip-nacional -v

# Pipeline completo — requer internet e credenciais Google
python src\main.py -c 2024-12
```

## Uso via Linha de Comando

```
python src\main.py [opções]

Opções:
  -c, --competencia YYYY MM    Competência da base nacional (padrão: definido no .env)
  -o, --output-dir CAMINHO     Diretório de saída (padrão: data/processed/)
      --skip-nacional          Pular cross-check BigQuery (modo offline)
      --skip-hr                Pular cross-check com folha de RH
  -v, --verbose                Log detalhado no console (nível DEBUG)
      --version                Exibe a versão do pipeline
  -h, --help                   Exibe esta ajuda
```

### Exemplos de Uso

```powershell
# Competência dezembro/2024, modo offline, saída em pasta específica
python src\main.py -c 2024-12 --skip-nacional -o data\relatorios\dez2024

# Pipeline completo com log detalhado
python src\main.py -c 2024-12 -v

# Apenas auditoria local (sem BigQuery e sem RH)
python src\main.py --skip-nacional --skip-hr
```

## O Que o Pipeline Faz

### Fontes de Dados

| Fonte | Origem | Chave de JOIN |
|---|---|---|
| Local | Banco Firebird `CNES.GDB` | CPF (profissionais), CNES (estabelecimentos) |
| Nacional | BigQuery via `basedosdados` | CNS (profissionais), CNES (estabelecimentos) |
| RH | Planilha Excel/CSV da folha de pagamento | CPF |

### Regras de Auditoria

| Regra | O Que Detecta | Severidade |
|---|---|---|
| RQ-002 | CPF nulo ou inválido (excluído da análise) | — |
| RQ-003 | Vínculo com carga horária zero ("zumbi") | Flag |
| RQ-003-B | Profissional com vínculos em 2+ unidades | MÉDIA |
| RQ-005 | ACS/TACS ou ACE/TACE lotados em unidade incorreta | ALTA |
| RQ-006 | Estabelecimento local sem correspondência nacional | ALTA |
| RQ-007 | Estabelecimento nacional ausente no local | ALTA |
| RQ-008 | Profissional (CNS) local sem correspondência nacional | CRÍTICA |
| RQ-009 | Profissional (CNS) nacional ausente no local | ALTA |
| RQ-010 | CBO divergente entre local e nacional | MÉDIA |
| RQ-011 | Carga horária divergente (tolerância: 2h) | BAIXA |
| Ghost Payroll | Ativo no CNES, ausente ou inativo no RH | CRÍTICA |
| Missing Reg. | Ativo no RH, ausente no CNES local | ALTA |

## Saídas Geradas

Todos os arquivos em `data/processed/` (ou diretório customizado via `-o`):

| Arquivo | Regra | Descrição |
|---|---|---|
| `Relatorio_Profissionais_CNES.csv` | — | Relatório principal com todos os vínculos ativos |
| `Relatorio_Profissionais_CNES.xlsx` | — | Relatório Excel multi-aba com recomendações |
| `auditoria_rq003b_multiplas_unidades.csv` | RQ-003-B | Profissionais com vínculos em 2+ unidades |
| `auditoria_rq005_acs_tacs_incorretos.csv` | RQ-005 | ACS/TACS lotados em unidade incorreta |
| `auditoria_rq005_ace_tace_incorretos.csv` | RQ-005 | ACE/TACE lotados em unidade incorreta |
| `auditoria_ghost_payroll.csv` | Ghost Payroll | Ativos no CNES, ausentes/inativos no RH |
| `auditoria_missing_registration.csv` | Missing Reg. | Ativos no RH, ausentes no CNES local |
| `auditoria_rq006_estab_fantasma.csv` | RQ-006 | Estabelecimentos locais sem correspondência nacional |
| `auditoria_rq007_estab_ausente_local.csv` | RQ-007 | Estabelecimentos nacionais ausentes no local |
| `auditoria_rq008_prof_fantasma_cns.csv` | RQ-008 | Profissionais locais (por CNS) sem correspondência nacional |
| `auditoria_rq009_prof_ausente_local_cns.csv` | RQ-009 | Profissionais nacionais (por CNS) ausentes no local |
| `auditoria_rq010_divergencia_cbo.csv` | RQ-010 | Divergência de CBO entre local e nacional |
| `auditoria_rq011_divergencia_ch.csv` | RQ-011 | Divergência de carga horária (tolerância: 2h) |

Arquivos de auditoria são criados apenas quando há anomalias detectadas. O log da execução é salvo em `logs/cnes_exporter.log`.

### Relatório Excel (.xlsx)

O arquivo `.xlsx` consolida todas as auditorias em um único workbook:

- **Aba RESUMO**: métricas-chave e tabela de anomalias com severidade colorida.
- **Aba Principal**: todos os vínculos processados.
- **1 aba por regra violada**: dados e coluna RECOMENDAÇÃO com ação corretiva.

Abas de auditoria são criadas apenas quando há anomalias detectadas.

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

## Testes

```powershell
# Testes unitários (rápidos, sem banco — padrão CI)
pytest tests/ -m "not integration" -v

# Testes de integração (requer banco Firebird ativo)
pytest tests/ -m "integration and not bigquery" -v

# Testes de integração com BigQuery (requer internet e credenciais Google)
pytest tests/ -m "integration and bigquery" -v

# Todos os testes
pytest tests/ -v
```

313+ testes unitários passando.

## Dashboard Analítico

Visualização interativa de tendências e drill-down de anomalias (requer ao menos uma execução do pipeline):

```bash
./venv/Scripts/streamlit.exe run scripts/dashboard.py
```

Abre automaticamente em http://localhost:8501. Três páginas:
- **Visão Geral** — KPIs do mês selecionado + tabela por severidade
- **Tendências** — gráfico de linhas multi-regra (Plotly, responsivo)
- **Por Regra** — drill-down de registros individuais com download CSV

## Estrutura do Projeto

```
CnesData/
├── src/
│   ├── cli.py                  # Interface de linha de comando (argparse)
│   ├── config.py               # Configuração centralizada (.env)
│   ├── main.py                 # Ponto de entrada do pipeline
│   ├── ingestion/
│   │   ├── base.py             # Protocols PEP 544 (contratos de interface)
│   │   ├── schemas.py          # Schema canônico de colunas
│   │   ├── cnes_client.py      # Extração Firebird (cursor-based)
│   │   ├── cnes_local_adapter.py
│   │   ├── cnes_nacional_adapter.py
│   │   ├── hr_client.py        # Parser de planilhas RH (.xlsx/.csv)
│   │   └── web_client.py       # Cliente BigQuery via basedosdados
│   ├── processing/
│   │   └── transformer.py      # Limpeza e validação (RQ-002, RQ-003)
│   ├── analysis/
│   │   ├── rules_engine.py     # Motor de regras de auditoria (11 regras)
│   │   └── evolution_tracker.py # Snapshots históricos JSON
│   └── export/
│       ├── csv_exporter.py     # Exportação CSV (padrão BR)
│       └── report_generator.py # Relatório Excel multi-aba com recomendações
├── tests/
│   ├── ingestion/
│   │   ├── test_base.py
│   │   ├── test_cnes_client.py
│   │   ├── test_cnes_local_adapter.py
│   │   ├── test_cnes_nacional_adapter.py
│   │   ├── test_hr_client.py
│   │   └── test_web_client.py
│   ├── analysis/
│   │   ├── test_cross_check.py
│   │   ├── test_evolution_tracker.py
│   │   └── test_rules_engine.py
│   ├── export/
│   │   └── test_report_generator.py
│   ├── test_cli.py
│   ├── test_config.py
│   ├── test_main.py
│   └── test_pipeline_integration.py  # Integração real (requer banco)
├── data/
│   ├── processed/              # Relatórios gerados (não vai ao Git)
│   └── snapshots/              # Histórico JSON (não vai ao Git)
├── logs/                       # Logs de execução (não vai ao Git)
├── scripts/                    # Scripts de exploração e referência
├── CLAUDE.md
├── ROADMAP.md
└── data_dictionary.md
```
