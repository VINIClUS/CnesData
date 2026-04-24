# data_processor — Transform & persist worker

## Executive Summary

Worker assíncrono que consome jobs da fila do `central_api`, baixa Parquet
gzip do MinIO, aplica transformações canônicas (CPF zero-pad + strip
pontuação, dedup, flags de carga horária) via
`cnes_domain.processing.transformer`, e persiste no schema Gold Postgres
(`dim_estabelecimento`, `dim_profissional`, `fato_vinculo`) com upsert
idempotente e merge JSONB de `fontes` para suportar múltiplas origens
sobre a mesma chave (LOCAL, NACIONAL, WEB).

## Role

**Central worker**. Stateless entre jobs; estado inteiramente em Postgres.
Horizontalmente escalável — múltiplas réplicas puxam da mesma fila sem
colisão (lease-based).

## Functionalities

- Poll de jobs via HTTP ao `central_api` (mesmo endpoint do `dump_agent`,
  filtro por status `ready_to_process`)
- Download streaming de Parquet gzip do MinIO via presigned GET
- Roteamento por source (CNES / SIHD) via `adapters/*_adapter.py`
- Aplicação de `transformar()` (clean CPF, pad, RQ-002 filter, `ALERTA_CH` flag)
- Mapeamento raw → canonical via `row_mapper.mapear_*`
- Upsert idempotente (`ON CONFLICT DO UPDATE` com merge JSONB `fontes || EXCLUDED.fontes`)
- `CircuitBreaker` em chamadas MinIO e `central_api` (backoff exp capado)

## Objectives

- Throughput ≥ 10k rows/s no upsert (gate do stress test)
- Idempotência total: N execuções do mesmo job = mesmo resultado no Gold
- Zero perda de dados via DLQ (`job_queue` tabelas `public.job_retries`)

## Limitations

- **Não faz extract** — só consome Parquet pronto
- **Não aplica regras de auditoria** — persiste dados canônicos; regras
  rodam em serviço externo que consome Gold via SQL JOINs
- **Não tem UI** — é daemon puro, monitorado via logs + OTel
- **Não conecta ao Firebird** — totalmente desacoplado das fontes
- **Não orquestra jobs** — só executa; `central_api` decide prioridade/fila

## Requirements

**Runtime deps (apps/data_processor/pyproject.toml):** `sqlalchemy`, `psycopg`,
`polars`, `httpx`, `minio`, `cnes_domain`, `cnes_infra`.

**Env vars:**

| Var | Obrigatória | Descrição |
|---|---|---|
| `DB_URL` | sim | Postgres Gold (mesmo cluster do `central_api`) |
| `CENTRAL_API_URL` | sim | Para polling da fila |
| `MINIO_ENDPOINT` | sim | Host:port MinIO |
| `MINIO_ACCESS_KEY` | sim | Credencial MinIO |
| `MINIO_SECRET_KEY` | sim | Credencial MinIO |
| `MINIO_BUCKET` | opcional | Default `cnesdata-landing` |
| `TENANT_ID` | sim | Tenant do worker (1 worker = 1 tenant por enquanto) |
| `WORKER_POLL_INTERVAL` | opcional | Default `5s` |

## Module Map

| Arquivo | Responsabilidade |
|---|---|
| `src/data_processor/main.py` | Entrypoint async + `_setup_logging` + `_create_storage` + run_processor |
| `src/data_processor/consumer.py` | Loop de pull de jobs do `central_api` |
| `src/data_processor/processor.py` | Pipeline download → transform → upsert (`_persist_profissionais`, etc) |
| `src/data_processor/config.py` | Config do worker (bucket, intervalos) |
| `src/data_processor/adapters/cnes_local_adapter.py` | Parquet CNES raw → DataFrame canônico |
| `src/data_processor/adapters/cnes_nacional_adapter.py` | Parquet BigQuery nacional → canônico |
| `src/data_processor/adapters/sihd_local_adapter.py` | Parquet SIHD/AIH → canônico |

## Gotchas

- **`fontes` JSONB merge:** upsert usa `||` (idempotente para object).
  Contrato fixo: `dict[str, bool]` ex.: `{"LOCAL": true, "WEB": true}`.
  Se virar array, regressão `test_fontes_idempotency_integration` falha.
- **`vinculo_repo` usa upsert, não plain INSERT** — fix aplicado em Fase 2.
  Múltiplas fontes (LOCAL, NACIONAL) podem upsertar a mesma
  `(tenant, cnes, cpf, competencia)` sem violar FK/PK.
- **CircuitBreaker é sync + async, APIs separadas:** use `.call()` para
  função síncrona, `.call_async()` para coroutine. Misturar dispara
  `TypeError` explícito (evita falha silenciosa que motivou o fix original).
- **Column names do BigQuery nacional** (confirmados empiricamente):
  `cbo_2002` (não `id_cbo`), `indicador_atende_sus` inteiro 1/0 (não
  `indicador_sus` string "S"/"N"). Ver `docs/data-dictionary-firebird-bigquery.md`.
- **`set_tenant_id` obrigatório antes de qualquer query Postgres:** worker
  chama `set_tenant_id(config.TENANT_ID)` no start de cada job. Sem isso,
  RLS bloqueia e job falha com 0 rows.
- **Streaming download gzip:** parquet baixado chunk a chunk via httpx
  stream para evitar OOM em arquivos grandes. Marcado `# pragma: no cover`
  nos fallbacks de tempfile.

## BPA + SIA adapters (T12/T13, 2026-04-23)

- `adapters/bpa_adapter.py` — `map_bpa_c_to_fato`, `map_bpa_i_to_fato`. BPA_C uses sentinel `_SK_PROFISSIONAL_AGREGADO=1` (seed dim_profissional row 1 required).
- `adapters/sia_adapter.py` — `map_apa_to_fato`, `map_bpi_to_fato` (historico flag toggles SIA_BPI vs SIA_BPIHST).
- `adapters/sia_dim_sync.py` — `sync_dim_procedimento` (S_CDN), `sync_dim_municipio` (CADMUN with ibge7 check-digit).
- `producao_ambulatorial_repo.gravar` upserts idempotent; `fontes_reportadas` JSONB merged via `||`.
- Migration 012 added natural-key unique index on `fato_producao_ambulatorial` to support ON CONFLICT upsert.
- Migration 013 extended `chk_fonte_amb` CHECK to allow SIA_BPIHST.
