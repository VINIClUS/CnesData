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
- **Worker é global (multi-tenant):** poll varre `landing.extractions`
  de todos os tenants via `SET LOCAL row_security = off` scoped à
  transação do `claim_next`. A cada job reclamado, `process_one` chama
  `set_tenant_id(claimed.tenant_id)` antes de qualquer
  `mark_completed`/`mark_failed`/escrita Gold subsequente. Sem env
  `TENANT_ID`; o tenant vem do row reclamado.
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

## CDC delta mode (P3, 2026-05-03)

- Delta is the only inbound shape (no flag, no legacy snapshot path).
- `cdc_merger.merge_delta(df, conn, source, intent, apply_iu_fn=None)` branches Parquet rows on `_op ∈ {I,U,D}`. D applied inline via `text("DELETE FROM gold.X WHERE pk = :pk")` per (source, intent) PK template aligned with edge agent's `delta/profiles.go`. I/U applied via `apply_iu_fn(df_iu) -> int` callback (existing upsert path).
- `processor.route_delta(df, conn, source, intent, apply_iu_fn=None)` raises `ValueError("missing_op_column")` if `_op` absent.
- DELETE idempotency: `delete_no_op` INFO log when rowcount=0 (already-deleted row).

## P2 integrity (2026-05-04)

`integrity_check.verify_parquet(path, expected_sha256)` recomputes SHA-256 over downloaded Parquet (1MB chunks); raises `IntegrityError` on mismatch; skips when `expected_sha256 None`. `processor.verify_and_route_delta(parquet_path, expected_sha256, conn, source, intent, apply_iu_fn=None)` calls verify_parquet → pl.read_parquet → route_delta. Mismatch propagates `IntegrityError` to caller (caller fails the job). landing.extractions gains nullable `sha256 char(64)` column (Alembic 018).
