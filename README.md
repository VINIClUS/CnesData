# CnesData

Motor de reconciliação de dados CNES para secretarias municipais de saúde.

Recebe dados de profissionais e estabelecimentos via **agentes de dump locais** (HTTP POST) ou diretamente da base nacional via BigQuery, aplica 11 regras de auditoria e persiste os resultados em PostgreSQL.

**Piloto:** Presidente Epitácio/SP (IBGE 354130, CNPJ 55.293.427/0001-17).  
**Direção:** arquitetura multi-município — o mesmo engine serve qualquer prefeitura cujo agente de dump esteja configurado.

---

## Arquitetura

```
Município A               Município B
┌───────────────────┐     ┌───────────────────┐
│  CNES.GDB         │     │  CNES.GDB         │
│  Firebird local   │     │  Firebird local   │
│                   │     │                   │
│  [ Dump Agent ]   │     │  [ Dump Agent ]   │
│  extrai → parquet │     │  extrai → parquet │
└────────┬──────────┘     └────────┬──────────┘
         │ HTTP POST                │ HTTP POST
         └──────────┬───────────────┘
                    ▼
         ┌──────────────────────────┐
         │  CnesData API            │
         │  Reconciliation Engine   │
         │                          │
         │  1. IngestaoLocalStage   │◄─ parquet do dump agent
         │  2. ProcessamentoStage   │
         │  3. IngestaoNacionalStage│◄─ BigQuery (DATASUS)
         │  4. ExportacaoStage      │
         └──────────┬───────────────┘
                    ▼
              PostgreSQL
              (auditoria por município × competência)
```

### Dump Agents

Agentes leves que rodam no ambiente do município:

1. Conectam ao `CNES.GDB` Firebird local
2. Extraem vínculos profissional-estabelecimento via queries parametrizadas
3. Serializam para parquet e fazem POST para o endpoint de ingestão da API
4. São stateless — agendados via cron ou Windows Task Scheduler

O engine aceita dados locais via parquet no `HISTORICO_DIR` (implementado) ou via API endpoint (roadmap).

### Seleção de Fonte (`--source`)

| Valor | Comportamento |
|---|---|
| `LOCAL` (padrão) | Usa parquet do dump agent ou Firebird direto. `StageSkipError` se ausente — sem fallback silencioso. |
| `NACIONAL` | Apenas BigQuery. Firebird nunca é consultado. |
| `AMBOS` | Ingere as duas fontes; exporta com proveniência explícita (`FONTE=LOCAL` / `FONTE=NACIONAL`). |

Proveniência é imutável: dados locais e nacionais nunca se mesclam implicitamente.

---

## Início Rápido

### Pré-requisitos

- Python 3.11+
- uv (`pip install uv`)
- (Opcional) `CNES.GDB` Firebird para execução local sem dump agent
- (Opcional) Conta Google Cloud para cross-check nacional via BigQuery

### Instalação

```powershell
uv sync
copy .env.example .env
# Edite .env com seus valores (ver seção Configuração)
```

### Primeira Execução

```powershell
# Apenas dados nacionais (sem Firebird local)
python src\main.py --source NACIONAL -c 2024-12 -v

# Dados locais via Firebird direto (desenvolvimento)
python src\main.py --source LOCAL -c 2024-12 -v

# Ambas as fontes (reconciliação completa)
python src\main.py --source AMBOS -c 2024-12
```

---

## Uso via CLI

```
python src\main.py [opções]

Opções:
  -c, --competencia YYYY-MM    Competência (padrão: definido no .env)
  -o, --output-dir CAMINHO     Diretório de saída (padrão: data/processed/)
      --source {LOCAL,NACIONAL,AMBOS}
                               Fonte de dados (padrão: LOCAL)
      --force-reingestao       Reingere do Firebird mesmo com snapshot existente
  -v, --verbose                Log DEBUG no console
  -h, --help                   Ajuda
```

---

## Regras de Auditoria

### Locais (dados do município)

| Regra | O Que Detecta | Severidade |
|---|---|---|
| RQ-002 | CPF nulo ou inválido | — (excluído) |
| RQ-003 | Vínculo com carga horária zero | Flag |
| RQ-003-B | Profissional com vínculos em 2+ unidades | MÉDIA |
| RQ-005 ACS/TACS | Agente comunitário em tipo de unidade incorreto | ALTA |
| RQ-005 ACE/TACE | Agente de endemias em tipo de unidade incorreto | ALTA |

### Cruzamento Local × Nacional

| Regra | O Que Detecta | Chave | Severidade |
|---|---|---|---|
| RQ-006 | Estabelecimento local ausente no nacional | CNES | ALTA |
| RQ-007 | Estabelecimento nacional ausente no local | CNES | ALTA |
| RQ-008 | Profissional local (CNS) ausente no nacional | CNS | CRÍTICA |
| RQ-009 | Profissional nacional (CNS) ausente no local | CNS | ALTA |
| RQ-010 | CBO divergente entre local e nacional | CNS+CNES | MÉDIA |
| RQ-011 | Carga horária divergente (tolerância: 2h) | CNS+CNES | BAIXA |

### Cruzamento Local × Folha de RH (requer `hr_padronizado.csv`)

| Regra | O Que Detecta | Severidade |
|---|---|---|
| Ghost Payroll | Ativo no CNES, ausente/inativo no RH | CRÍTICA |
| Missing Registration | Ativo no RH, ausente no CNES local | ALTA |

---

## Saídas

Todos os arquivos em `data/processed/` (ou `-o`):

| Arquivo | Descrição |
|---|---|
| `Relatorio_Profissionais_CNES.xlsx` | Workbook Excel com aba RESUMO + 1 aba por regra violada |
| `Relatorio_Profissionais_CNES.csv` | Vínculos processados (formato BR, separador `;`) |
| `auditoria_rq*.csv` | Um arquivo por regra com anomalias detectadas |
| `auditoria_ghost_payroll.csv` | Folha fantasma (quando RH disponível) |
| `auditoria_missing_registration.csv` | Registro ausente (quando RH disponível) |

Arquivos de auditoria são criados apenas quando há anomalias. Logs em `logs/cnes_exporter.log`.

---

## Configuração (`.env`)

```ini
# Banco Firebird (apenas para execução direta ou desenvolvimento)
DB_HOST=localhost
DB_PATH=C:\Datasus\CNES\CNES.GDB
DB_USER=SYSDBA
DB_PASSWORD=masterkey
FIREBIRD_DLL=C:\caminho\para\fb_64\fbembed.dll

# Filtros do município
COD_MUN_IBGE=354130
ID_MUNICIPIO_IBGE7=3541307
CNPJ_MANTENEDORA=55293427000117

# Saída
OUTPUT_DIR=data/processed

# Google Cloud / BigQuery (cross-check nacional)
GCP_PROJECT_ID=seu-projeto-gcp

# Competência padrão
COMPETENCIA_ANO=2024
COMPETENCIA_MES=12

# PostgreSQL (persistência)
DB_URL=postgresql+psycopg2://user:pass@localhost:5432/cnesdata

# Folha de pagamento RH (opcional)
FOLHA_HR_PATH=C:\caminho\para\hr_padronizado.csv
```

Em modo API, `DB_PATH` e `FIREBIRD_DLL` são configurados no lado do dump agent, não do servidor.

---

## Testes

```powershell
# Unitários (sem banco — padrão CI)
.venv\Scripts\python.exe -m pytest tests/ -m "not integration" -q

# Integração Firebird (requer banco ativo)
.venv\Scripts\python.exe -m pytest tests/ -m "integration and not bigquery" -v

# Todos
.venv\Scripts\python.exe -m pytest tests/ -v
```

319+ testes unitários passando.

---

## Estrutura do Projeto

```
CnesData/
├── src/
│   ├── cli.py                    # Argparse CLI
│   ├── config.py                 # Configuração centralizada (.env)
│   ├── main.py                   # Ponto de entrada
│   ├── ingestion/
│   │   ├── schemas.py            # Schema canônico (fonte de verdade de colunas)
│   │   ├── cnes_client.py        # Extração Firebird (charset WIN1252, 3 queries)
│   │   ├── cnes_local_adapter.py # Firebird → schema canônico
│   │   ├── cnes_nacional_adapter.py # BigQuery → schema canônico
│   │   ├── hr_client.py          # Parser planilhas RH (.xlsx/.csv)
│   │   └── web_client.py         # Cliente BigQuery via basedosdados
│   ├── pipeline/
│   │   ├── orchestrator.py       # PipelineOrchestrator + StageSkipError/StageFatalError
│   │   ├── state.py              # PipelineState (target_source, DataFrames)
│   │   └── stages/
│   │       ├── ingestao_local.py    # Carrega parquet do dump agent ou Firebird direto
│   │       ├── ingestao_nacional.py # Busca BigQuery com circuit breaker
│   │       ├── processamento.py     # Limpeza CPF, datas ISO, dedup
│   │       └── exportacao.py        # Persiste no PostgreSQL, deriva status
│   ├── processing/
│   │   └── transformer.py        # RQ-002, RQ-003, enriquecimento CBO
│   ├── analysis/
│   │   ├── rules_engine.py       # 11 regras de auditoria
│   │   └── evolution_tracker.py  # Snapshots históricos JSON
│   └── storage/
│       ├── ports.py              # StoragePort Protocol
│       └── postgres_adapter.py   # PostgreSQL (upsert por competência)
├── tests/
├── data/
│   ├── historico/              # Parquets dos dump agents (não vai ao Git)
│   └── processed/              # Relatórios gerados (não vai ao Git)
├── docs/
├── scripts/                    # Run-CnesAudit.ps1, hr_pre_processor.py
├── CLAUDE.md
├── PROJECT_CONTEXT.md
├── ROADMAP.md
└── data_dictionary.md
```

---

## Dashboard Analítico

```bash
.venv\Scripts\streamlit.exe run scripts/dashboard.py
```

Abre em http://localhost:8501 com três páginas: Visão Geral, Tendências e drill-down por Regra.
