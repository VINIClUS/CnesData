# cnes_infra — Infra adapters (Postgres + MinIO + external APIs)

## Executive Summary

Implementações concretas dos Ports declarados em `cnes_domain`. Conecta a
infra real (Postgres, MinIO, Firebird, HR spreadsheets, BigQuery, DATASUS
API) e expõe repositories, storage clients e migrações Alembic. Depende de
`cnes_domain`; apps deployáveis usam este pacote via DI pelos Ports — nunca
importam classes concretas diretamente (exceto factories no bootstrap).

## Scope

- **Storage:** SQLAlchemy Core + repositórios (upsert idempotente),
  job queue com leases, landing schema (raw), RLS policies, schema Gold
- **Ingestion clients:** Firebird (`db_client`), HR (.xlsx/.csv com
  encoding fallback), DATASUS API (`cnes_oficial_web_adapter`), BigQuery
  (`web_client`)
- **Alembic migrations:** schemas `gold`, `landing`, `public.jobs`.
  Versions numeradas `001_*` a `006_*`
- **Telemetry:** init OTel (opcional — pragmas no cover se SDK não instalado)
- **Config:** `_exigir`, `_exigir_inteiro`, `_sanitizar_db_url`,
  lazy lookups com `@lru_cache` (ex.: `DB_PASSWORD`, `FIREBIRD_DLL`)

## Conventions

- Todo repositório aceita `Connection` no construtor (UoW injection)
- Upserts usam `ON CONFLICT DO UPDATE` + merge JSONB em `fontes`
- Migrations são numeradas e **imutáveis após merge** — novo schema muda
  requer nova migration
- OTel é **opcional** — imports em try/except com `# pragma: no cover -
  otel optional` quando aplicável
- RLS policy install é idempotente (safe para rodar em cada boot)

## Module Map

| Path | Responsabilidade |
|---|---|
| `storage/schema.py` | `dim_estabelecimento` / `dim_profissional` / `fato_vinculo` (SQLAlchemy Core) |
| `storage/landing.py` | `landing.raw_extractions` (histórico de Parquets recebidos) |
| `storage/job_queue.py` | Fila + leases + DLQ + retry. Função `reap_expired_leases` |
| `storage/rls.py` | Policies RLS + `install_rls_listener(engine)` (event hook SQLAlchemy) |
| `storage/object_storage.py` | `MinioObjectStorage` implementa `ObjectStoragePort` |
| `storage/repositories/unit_of_work.py` | `PostgresUnitOfWork` wrap SQLAlchemy Session |
| `storage/repositories/estabelecimento_repo.py` | Upsert `dim_estabelecimento` com merge `fontes` |
| `storage/repositories/profissional_repo.py` | Upsert `dim_profissional` com merge `fontes` |
| `storage/repositories/vinculo_repo.py` | Upsert `fato_vinculo` — **usa INSERT ON CONFLICT, não plain INSERT** |
| `ingestion/db_client.py` | `fdb.connect(charset="WIN1252")` wrapper |
| `ingestion/hr_client.py` | Parser .xlsx/.csv com cp1252 fallback |
| `ingestion/web_client.py` | BigQuery via `basedosdados` (OAuth browser flow) |
| `ingestion/cnes_oficial_web_adapter.py` | DATASUS API protegido por `CircuitBreaker` |
| `alembic.ini` | Config Alembic raiz do pacote |
| `alembic/env.py` | `_resolver_db_url` — config override → `DB_URL` fallback |
| `alembic/versions/*.py` | Migrations numeradas (omitidas em coverage) |
| `telemetry.py` | `init_telemetry(service)` + `instrument_engine(engine)` |
| `config.py` | `DB_URL`, `MINIO_*`, `API_*`, `COMPETENCIA_*`, `_LAZY_ATTRS` |

## Gotchas

- **`fontes` é JSONB-object, NÃO array:** `{"LOCAL": true, "WEB": true}`.
  Upsert via `||` (merge raso) é idempotente. **Não migrar para array** sem
  revisar semântica — regressão `test_fontes_idempotency` trava isso.
- **Alembic script_location:** `src/cnes_infra/alembic` (relativo ao
  `alembic.ini` em `packages/cnes_infra/`). CLI precisa rodar a partir de
  `packages/cnes_infra/`.
- **`_LAZY_ATTRS` com `@lru_cache`:** `_firebird_db_path`,
  `_firebird_db_password`, `_firebird_dll`, `_gcp_project_id`. Testes que
  mudam env precisam chamar `.cache_clear()` antes do access.
- **`load_dotenv(override=False)`** no topo de `config.py` repopula env a
  partir de `.env` em IMPORT time. Se `monkeypatch.delenv` rodar antes do
  import, a env é restaurada silenciosamente. **Ordem correta:** importar
  `cnes_infra.config` primeiro, depois `monkeypatch.delenv(...)`.
- **`_resolver_db_url`** em `alembic/env.py` prefere `sqlalchemy.url` do
  Config (setado por fixtures via `cfg.set_main_option`) e só cai em
  `DB_URL` env var. Isso destrava testes sem precisar setar env global.
- **Migration 006 (GIN index em `fontes`)** é necessária para queries de
  volume por fonte. Não remover.
- **`vinculo_repo.gravar` usa `INSERT ON CONFLICT DO UPDATE`:** múltiplas
  fontes (LOCAL + NACIONAL) podem upsertar a mesma `(tenant, cnes, cpf,
  competencia)` sem violar constraint. Troca para plain INSERT quebra o
  fluxo multi-fonte.
