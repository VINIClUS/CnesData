# CnesData V3 — Strict ELT Architecture

## Decisões Arquiteturais

| Decisão | Valor |
|---|---|
| Paradigma | Strict ELT — Zero-T no dump_agent |
| Intent Whitelist | StrEnum + Pydantic `extra="forbid"` + Extractor Registry |
| Circuit Breakers | 3 camadas (pre-flight, spool limit, cleanup guarantee) |
| Client ownership | cnes_client/sihd_client migram para dump_agent |
| Adapter ownership | adapters migram para data_processor |
| dump_agent deps | cnes_domain + fdb (remove cnes_infra[etl]) |

## 1. Reestruturação de Pacotes

### Migrações

| Artefato | De | Para |
|---|---|---|
| `cnes_client.py` | `cnes_infra/ingestion/` | `dump_agent/extractors/cnes_extractor.py` |
| `sihd_client.py` | `cnes_infra/ingestion/` | `dump_agent/extractors/sihd_extractor.py` |
| `cnes_local_adapter.py` | `cnes_infra/ingestion/` | `data_processor/adapters/` |
| `sihd_local_adapter.py` | `cnes_infra/ingestion/` | `data_processor/adapters/` |
| `cnes_nacional_adapter.py` | `cnes_infra/ingestion/` | `data_processor/adapters/` |

### Permanecem em cnes_infra

`web_client.py`, `hr_client.py`, `db_client.py`, `base.py`, `schemas.py`.

### Nova estrutura dump_agent

```
apps/dump_agent/src/dump_agent/
├── cli.py
├── main.py
├── extractors/
│   ├── __init__.py
│   ├── intents.py           # ExtractionIntent StrEnum
│   ├── registry.py          # dict[Intent → Extractor]
│   ├── cnes_extractor.py    # Firebird cursor → raw Parquet
│   └── sihd_extractor.py    # SIHD cursor → raw Parquet
├── io_guard.py              # Circuit breakers
└── worker/
    ├── consumer.py
    └── executor.py
```

### Nova estrutura data_processor

```
apps/data_processor/src/data_processor/
├── main.py
├── processor.py
├── consumer.py
├── adapters/
│   ├── __init__.py
│   ├── cnes_local_adapter.py
│   ├── cnes_nacional_adapter.py
│   └── sihd_local_adapter.py
└── config.py
```

## 2. Intent Whitelist Architecture

### Contratos no domínio (cnes_domain/models/api.py)

```python
class ExtractionIntent(StrEnum):
    PROFISSIONAIS = "profissionais"
    ESTABELECIMENTOS = "estabelecimentos"
    EQUIPES = "equipes"
    SIHD_PRODUCAO = "sihd_producao"

class ExtractionParams(BaseModel):
    intent: ExtractionIntent
    competencia: str            # YYYY-MM, regex validated
    cod_municipio: str          # 6 dígitos IBGE
    model_config = {"extra": "forbid"}
```

### Dupla validação

**Gate 1 — central_api:** `ExtractionParams.model_validate(payload)` antes de INSERT
no job queue. Payload inválido → 422 Unprocessable Entity.

**Gate 2 — dump_agent:** re-valida ao consumir o job. Intent desconhecido ou campo
extra → job marcado como FAILED + enviado para DLQ.

### Segurança

- `extra = "forbid"`: qualquer campo fora do schema gera ValidationError
- Zero SQL na rede: payload carrega intent + parâmetros primitivos
- Queries SQL hardcoded nos Extractors dentro do dump_agent
- Superfície de ataque = 1 arquivo Python com 4 valores enum

### Extractor Registry (dump_agent)

```python
REGISTRY: dict[ExtractionIntent, Extractor] = {
    ExtractionIntent.PROFISSIONAIS: CnesExtractorProfissionais(),
    ExtractionIntent.ESTABELECIMENTOS: CnesExtractorEstabelecimentos(),
    ExtractionIntent.EQUIPES: CnesExtractorEquipes(),
    ExtractionIntent.SIHD_PRODUCAO: SihdExtractorProducao(),
}
```

Cada `Extractor` implementa um Protocol com método `extract(params, tmp_dir) → Path`.

## 3. Circuit Breakers — I/O Guard

Deploy target: PCs de secretarias de saúde municipais (disco limitado, sem monitoramento).

### Camada 1: Pre-flight Check

Antes de iniciar extração, verifica espaço livre em disco.
- Default: `DUMP_MIN_FREE_DISK_MB=500`
- Insuficiente → abort + log + fail job

### Camada 2: Streaming Spool Limit

Durante escrita do Parquet temporário, monitora bytes escritos.
- Default: `DUMP_MAX_SPOOL_MB=200`
- Município grande (~50k vínculos) ≈ 15 MB Parquet. 200 MB = 13x margem.
- Excedido → abort + cleanup temp + fail job com `SpoolLimitExceeded`

### Camada 3: Cleanup Guarantee

- `tempfile.TemporaryDirectory()` como context manager
- Signal handlers para SIGTERM/SIGINT
- `atexit` handler como fallback
- Nenhum arquivo temporário sobrevive ao processo

### Extração batched

`cursor.fetchmany(BATCH_SIZE)` ao invés de `fetchall()`. Default: `DUMP_BATCH_SIZE=5000`.
Controla pico de RAM. Cada batch escrito como chunk Parquet, spool_guard contabiliza
bytes acumulados.

### Configuração via environment

| Variável | Default | Descrição |
|---|---|---|
| `DUMP_MIN_FREE_DISK_MB` | 500 | Mínimo livre antes de iniciar |
| `DUMP_MAX_SPOOL_MB` | 200 | Máximo que um job pode escrever |
| `DUMP_TEMP_DIR` | (sistema) | Override do tempdir |
| `DUMP_BATCH_SIZE` | 5000 | Rows por fetchmany |

## 4. Data Flow V3

### Pipeline completo

```
dump_agent (PC municipal)
  │
  ├─ Gate: ExtractionParams.validate()
  │    intent ∈ StrEnum? extra fields? → DLQ
  │
  ├─ pre_flight_check()
  │    disco livre ≥ 500MB? → abort
  │
  ├─ registry[intent].extract(params)
  │    SQL hardcoded → cursor.fetchmany(5000)
  │    ZERO transform — rows brutas (CPF_PROF, COD_CNS, CODMUNGEST)
  │
  ├─ serialize Parquet.gz (spool_guard: max 200MB)
  │    TemporaryDirectory + signal handlers
  │
  └─ upload → MinIO
       │
       ▼
data_processor (servidor)
  │
  ├─ download Parquet.gz
  ├─ adapter: rename cols, NFKD, nulls, FONTE
  ├─ transformar() — RQ-002..RQ-011
  └─ upsert → PostgreSQL gold
```

### Dependency graph

```
                    cnes_domain
              (contratos, zero I/O)
               /                \
         dump_agent          data_processor
     (fdb + cnes_domain)   (cnes_infra + cnes_domain)
              \                /
               \              /
              cnes_infra
        (storage, persistence)
```

### Mudança de dependência do dump_agent

**V2:** `dump_agent → cnes_infra[etl] → sqlalchemy, psycopg, basedosdados, google-cloud...`
PC municipal instala ~50 deps transitivas que nunca usa.

**V3:** `dump_agent → cnes_domain (pydantic, polars) + fdb`
PC municipal instala ~5 deps.

## 5. Módulos Afetados — Resumo de Impacto

### Criações

| Arquivo | Pacote | Responsabilidade |
|---|---|---|
| `extractors/intents.py` | dump_agent | ExtractionIntent StrEnum |
| `extractors/registry.py` | dump_agent | Intent → Extractor mapping |
| `extractors/cnes_extractor.py` | dump_agent | Firebird cursor → raw Parquet |
| `extractors/sihd_extractor.py` | dump_agent | SIHD cursor → raw Parquet |
| `io_guard.py` | dump_agent | Pre-flight, spool limit, cleanup |
| `adapters/` | data_processor | cnes_local, cnes_nacional, sihd_local |

### Refatorações

| Arquivo | Mudança |
|---|---|
| `dump_agent/worker/consumer.py` | Usar registry + ExtractionParams validation |
| `dump_agent/worker/executor.py` | Delegar para registry, integrar io_guard |
| `dump_agent/pyproject.toml` | Remover dep cnes_infra[etl], adicionar fdb |
| `data_processor/processor.py` | Usar adapters locais ao invés de cnes_infra |
| `central_api/routes/jobs.py` | Validar ExtractionParams no POST |
| `cnes_domain/models/api.py` | Adicionar ExtractionIntent, ExtractionParams |

### Remoções de cnes_infra

| Arquivo | Ação |
|---|---|
| `cnes_infra/ingestion/cnes_client.py` | Remover (migrado para dump_agent) |
| `cnes_infra/ingestion/sihd_client.py` | Remover (migrado para dump_agent) |
| `cnes_infra/ingestion/cnes_local_adapter.py` | Remover (migrado para data_processor) |
| `cnes_infra/ingestion/sihd_local_adapter.py` | Remover (migrado para data_processor) |
| `cnes_infra/ingestion/cnes_nacional_adapter.py` | Remover (migrado para data_processor) |

## 6. Riscos e Mitigações

| Risco | Mitigação |
|---|---|
| Parquet raw com encoding WIN1252 | Firebird driver entrega bytes; Parquet preserva binary. Adapter no data_processor faz decode UTF-8 + NFKD |
| Adapter no data_processor não reconhece colunas raw | Mapeamento explícito em dicionário constante. Coluna desconhecida → raise ValueError |
| dump_agent deployado sem novo intent | Gate 2 envia para DLQ com erro. Central_api não deveria enfileirar intents que o agente não suporta — versioning via enum |
| Testes existentes quebram com migração | Testes migram junto com os módulos. Imports atualizados. CI valida |
| fetchmany retorna menos rows que BATCH_SIZE | Comportamento normal no final do cursor. Loop termina quando batch vazio |
