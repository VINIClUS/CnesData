# data_processor — Transform & persist worker

## Executive Summary

Worker assíncrono Gold v2 que consome `landing.extractions` diretamente no
Postgres, define o tenant do job reclamado e marca conclusão/falha. Mantém
rotas auxiliares para validar SHA-256, ler Parquet delta (`_op`) e delegar
I/U para callbacks/upserts específicos quando uma ingestão usa esse caminho.

## Role

**Central worker**. Stateless entre jobs; estado inteiramente em Postgres.
Horizontalmente escalável — múltiplas réplicas puxam da mesma fila sem
colisão (lease-based).

## Functionalities

- Claim global de `landing.extractions` via `extractions_repo.claim_next`
- `set_tenant_id(claimed.tenant_id)` antes de mutar estado do job
- `mark_completed` / `mark_failed` no mesmo storage repository
- `integrity_check.verify_parquet` para SHA-256 quando esperado
- `cdc_merger.merge_delta` para linhas `_op ∈ {I,U,D}`
- Adapters CNES/SIHD/BPA/SIA preservados para rotas de ingestão específicas

## Objectives

- Claim idempotente e seguro entre réplicas horizontais
- Zero cross-tenant leak em marcação de jobs
- Integridade verificável quando `sha256` vem do edge agent

## Limitations

- **Não faz extract** — só consome metadata/artefatos já produzidos
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
| `WORKER_POLL_INTERVAL` | opcional | Default `5s` |

## Module Map

| Arquivo | Responsabilidade |
|---|---|
| `src/data_processor/main.py` | Entrypoint async + `_setup_logging` + `_create_storage` + run_processor |
| `src/data_processor/consumer.py` | Compat wrapper para `poll.loop` |
| `src/data_processor/poll.py` | Claim `landing.extractions` + mark completed/failed |
| `src/data_processor/processor.py` | SHA-256 + delta route helpers |
| `src/data_processor/config.py` | Config do worker (bucket, intervalos) |
| `src/data_processor/adapters/cnes_local_adapter.py` | Parquet CNES raw → DataFrame canônico |
| `src/data_processor/adapters/cnes_nacional_adapter.py` | Parquet BigQuery nacional → canônico |
| `src/data_processor/adapters/sihd_local_adapter.py` | Parquet SIHD/AIH → canônico |
| `src/data_processor/adapters/bpa_adapter.py` | `map_bpa_c_to_fato` / `map_bpa_i_to_fato` |
| `src/data_processor/adapters/sia_adapter.py` | `map_apa_to_fato` / `map_bpi_to_fato` |
| `src/data_processor/adapters/sia_dim_sync.py` | `sync_dim_procedimento`, `sync_dim_municipio` |
| `src/data_processor/cdc_merger.py` | `merge_delta` — roteia `_op ∈ {I,U,D}` |
| `src/data_processor/integrity_check.py` | `verify_parquet` — SHA-256 sobre Parquet baixado |

## Gotchas

- **`fontes` JSONB merge + `vinculo_repo` upsert:** ver
  `packages/cnes_infra/CLAUDE.md` (Gotchas) — regras únicas, não duplicar aqui.
- **CircuitBreaker é sync + async, APIs separadas:** use `.call()` para
  função síncrona, `.call_async()` para coroutine. Misturar dispara
  `TypeError` explícito (evita falha silenciosa que motivou o fix original).
- **Column names do BigQuery nacional** (confirmados empiricamente):
  `cbo_2002` (não `id_cbo`), `indicador_atende_sus` inteiro 1/0 (não
  `indicador_sus` string "S"/"N"). Ver `docs/data-dictionary-firebird-bigquery.md`.
- **Worker é global (multi-tenant):** poll varre `landing.extractions`
  de todos os tenants via `SET LOCAL row_security = off` scoped à
  transação do `claim_next`. A cada job reclamado, `process_one` chama
  `set_tenant_id(claimed.tenant_id)` antes de qualquer
  `mark_completed`/`mark_failed`/escrita Gold subsequente. Sem env
  `TENANT_ID`; o tenant vem do row reclamado.
- **Streaming download gzip:** parquet baixado chunk a chunk via httpx
  stream para evitar OOM em arquivos grandes. Marcado `# pragma: no cover`
  nos fallbacks de tempfile.
- **Delta é o único formato de entrada** (sem flag, sem snapshot legado):
  `cdc_merger.merge_delta` roteia por `_op ∈ {I,U,D}`; D vira `DELETE` inline
  por template PK `(source, intent)` alinhado com `delta/profiles.go` do
  edge agent; I/U passam por `apply_iu_fn` (upsert existente). `_op` ausente
  → `ValueError("missing_op_column")`.
- **`verify_and_route_delta`** encadeia `verify_parquet` (SHA-256, pula se
  `expected_sha256 None`) → `pl.read_parquet` → `route_delta`; mismatch
  propaga `IntegrityError` e falha o job. `landing.extractions.sha256` é
  nullable (Alembic 018).
- **`bpa_adapter`:** BPA_C usa sentinel `_SK_PROFISSIONAL_AGREGADO=1` — exige
  seed da row 1 em `dim_profissional`. `producao_ambulatorial_repo.gravar`
  faz upsert idempotente; `fontes_reportadas` JSONB merge via `||`.

Histórico de fases (T12/T13, P2, P3): `CHANGELOG.md`.
