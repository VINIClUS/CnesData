# cnes_infra — Infra adapters (Postgres + MinIO + external APIs)

## Executive Summary

Implementações concretas dos Ports declarados em `cnes_domain`. Conecta a
infra real (Postgres, MinIO, Firebird, HR spreadsheets, BigQuery, DATASUS
API) e expõe repositories, storage clients e migrações Alembic. Depende de
`cnes_domain`; apps deployáveis usam este pacote via DI pelos Ports — nunca
importam classes concretas diretamente (exceto factories no bootstrap).

## Scope

- **Storage:** SQLAlchemy Core + `extractions_repo` (claim/lease N-file
  manifest em `landing.extractions`), repositórios Gold v2 (upsert
  idempotente para dims, plain INSERT para fatos), RLS policies
- **Ingestion clients:** Firebird (`db_client`), HR (.xlsx/.csv com
  encoding fallback), DATASUS API (`cnes_oficial_web_adapter`), BigQuery
  (`web_client`)
- **Alembic migrations:** schemas `gold`, `landing`, `queue`, `marketing`,
  `dashboard`. Versions numeradas `001_*` a `019_*` (latest
  `019_marketing_leads.py`)
- **Telemetry:** init OTel (opcional — pragmas no cover se SDK não instalado)
- **Config:** `_exigir`, `_exigir_inteiro`, `_sanitizar_db_url`,
  lazy lookups com `@lru_cache` (ex.: `DB_PASSWORD`, `FIREBIRD_DLL`)

## Conventions

- Toda função de repositório recebe `Connection` por parâmetro (sem classe
  UoW concreta — `cnes_domain.ports.storage.UnitOfWork` é só o Protocol)
- Dims usam `ON CONFLICT DO UPDATE` + merge JSONB em `fontes`; fatos Gold v2
  usam **plain INSERT** (N rows por tupla, sem merge — agregação em query time)
- Migrations são numeradas e **imutáveis após merge** — novo schema muda
  requer nova migration
- OTel é **opcional** — imports em try/except com `# pragma: no cover -
  otel optional` quando aplicável
- RLS policy install é idempotente (safe para rodar em cada boot)

## Module Map

| Path | Responsabilidade |
|---|---|
| `storage/schema_v2.py` | Tabelas Gold v2 (`dim_*`, `fato_*`, `extractions_table`) SQLAlchemy Core |
| `storage/extractions_repo.py` | Claim/lease N-file manifest em `landing.extractions` |
| `storage/dim_lookup.py` | `PostgresDimLookup` (surrogate key cache) + `upsert_dim_*` (merge `fontes`) |
| `storage/rls.py` | Policies RLS + `install_rls_listener(engine)` (event hook SQLAlchemy) |
| `storage/object_storage.py` | `MinioObjectStorage` implementa `ObjectStoragePort` |
| `storage/repositories/vinculo_repo_v2.py` | `gravar(fato_vinculo_cnes)` — **plain INSERT**, sem ON CONFLICT |
| `storage/repositories/producao_ambulatorial_repo.py` | Upsert `fato_producao_ambulatorial` (BPA/SIA), merge `fontes_reportadas` |
| `storage/repositories/internacao_repo.py` | Grava `fato_internacao` (SIHD) |
| `storage/repositories/procedimento_aih_repo.py` | Grava `fato_procedimento_aih` (SIHD) |
| `ingestion/db_client.py` | `fdb.connect(charset="WIN1252")` wrapper |
| `ingestion/hr_client.py` | Parser .xlsx/.csv com cp1252 fallback |
| `ingestion/web_client.py` | BigQuery via `basedosdados` (OAuth browser flow) |
| `ingestion/cnes_oficial_web_adapter.py` | DATASUS API protegido por `CircuitBreaker` |
| `alembic.ini` | Config Alembic raiz do pacote |
| `alembic/env.py` | `_resolver_db_url` — config override → `DB_URL` fallback |
| `alembic/versions/*.py` | Migrations numeradas (omitidas em coverage) |
| `telemetry.py` | `init_telemetry(service)` + `instrument_engine(engine)` |
| `config.py` | `DB_URL`, `MINIO_*`, `API_*`, `COMPETENCIA_*`, `_LAZY_ATTRS` |
| `auth/jwt.py` | JWKSValidator for OIDC JWT verification |
| `storage/dashboard_models.py` | SQLAlchemy ORM for `dashboard.*` tables |

## Gotchas

- **`fontes` é JSONB-object, NÃO array:** `{"LOCAL": true, "WEB": true}`.
  `dim_lookup.upsert_dim_*` merge via `||` (merge raso) é idempotente.
  **Não migrar para array** sem revisar semântica — regressão
  `test_fontes_idempotency` trava isso.
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
- **`vinculo_repo_v2.gravar` usa plain INSERT, NÃO upsert:** semântica Gold v2
  é N rows por `(sk_prof, sk_estab, sk_cbo, sk_competencia)` quando múltiplas
  fontes (LOCAL + NACIONAL) chegam para a mesma tupla — sem merge JSONB.
  Agregação (SUM/MAX por `fonte_sistema`) fica para query time. Adicionar
  `ON CONFLICT DO UPDATE` aqui reintroduziria a semântica v1 e perderia linhas.
- **`web_client` mocks retornam pandas, não polars:** `basedosdados.read_sql`
  retorna `pd.DataFrame`; o adapter converte via `pl.from_pandas()`. Mock de
  teste precisa devolver pandas, senão a conversão mascara o erro real.
