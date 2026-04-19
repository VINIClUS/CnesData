# central_api — FastAPI ingestion + job orchestration

## Executive Summary

Servidor FastAPI que recebe requisições de `dump_agent` (poll de jobs,
upload de artefatos, heartbeat) e de serviços administrativos (health,
reap-leases). Emite URLs pré-assinadas MinIO, gerencia fila de jobs em
Postgres com leases (reclama jobs expirados via background task), expõe
OpenAPI em `/openapi.json`. Stateless em nível de request — toda persistência
em Postgres; MinIO é conteúdo, não estado.

## Role

**Central orchestrator**. Único ponto de entrada HTTP do sistema.
Horizontalmente escalável, com cuidado: reaper background task só deve rodar
em 1 réplica (gate via env `ENABLE_REAPER`).

## Functionalities

- `GET /api/v1/system/health` — healthcheck + ping Postgres
- `GET /api/v1/jobs/next` — próximo job para `(tenant_id, machine_id)` com lease
- `POST /api/v1/jobs/{id}/heartbeat` — estende lease
- `POST /api/v1/jobs/{id}/artifact` — emite presigned PUT MinIO
- `POST /api/v1/jobs/{id}/complete` — marca job uploaded (processor pega depois)
- `POST /api/v1/jobs/{id}/fail` — marca falha (retryable ou DLQ)
- `POST /api/v1/admin/reap-leases` — libera jobs com lease expirado (admin)
- `TenantMiddleware` — extrai `X-Tenant-Id` header e chama `set_tenant_id()`
- Background task: `_lease_reaper_loop` (a cada `_REAPER_INTERVAL=60s`) no lifespan

## Objectives

- p99 de `/jobs/next` < 200ms (Postgres-only, sem I/O externo)
- Zero cross-tenant leak (RLS + middleware + teste contratual)
- Uptime 99.9% (single-replica inicialmente; rolling restart no k8s suporta)

## Limitations

- **Não executa jobs** — só orquestra (execução fica no `data_processor`)
- **Não lê Firebird/SIHD direto** — todo dado chega via `dump_agent`
- **Não processa Parquet** — só emite presigned URLs; MinIO armazena
- **Sem auth próprio** — assume rede confiável (VPN municipal) ou gateway
  externo (ingress com auth) na frente

## Requirements

**Runtime deps (apps/central_api/pyproject.toml):** `fastapi`, `uvicorn`,
`sqlalchemy`, `psycopg`, `cnes_domain`, `cnes_infra` (`storage/*`, `telemetry`).

**Env vars:**

| Var | Obrigatória | Descrição |
|---|---|---|
| `DB_URL` | sim | Postgres URL (`postgresql+psycopg://...`) |
| `MINIO_ENDPOINT` | sim | `host:port` MinIO (ex.: `minio:9000`) |
| `MINIO_ACCESS_KEY` | sim | Credencial MinIO |
| `MINIO_SECRET_KEY` | sim | Credencial MinIO |
| `MINIO_BUCKET` | opcional | Default `cnesdata-landing` |
| `MINIO_SECURE` | opcional | `true` para HTTPS (default `false`) |
| `API_HOST` | opcional | Default `0.0.0.0` |
| `API_PORT` | opcional | Default `8000` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | opcional | Tracing (se OTel SDK instalado) |
| `ENABLE_REAPER` | opcional | `true` em 1 réplica para reaper rodar (futuro) |

## Module Map

| Arquivo | Responsabilidade |
|---|---|
| `src/central_api/app.py` | `create_app()` factory — FastAPI + lifespan + middleware + routers |
| `src/central_api/deps.py` | `get_engine()`, `lifespan`, `_lease_reaper_loop`, RLS listener install |
| `src/central_api/middleware.py` | `TenantMiddleware` — extrai `X-Tenant-Id` header |
| `src/central_api/routes/health.py` | `/api/v1/system/health` — ping DB |
| `src/central_api/routes/jobs.py` | `/api/v1/jobs/*` — fila, artifact, heartbeat, complete, fail |
| `src/central_api/routes/admin.py` | `/api/v1/admin/*` — reap-leases, ops |

## Gotchas

- **TenantMiddleware obrigatório:** toda request precisa de header
  `X-Tenant-Id`. Sem ele, `set_tenant_id()` não é chamado e queries quebram
  silenciosamente (RLS bloqueia). Exception: rotas de health podem ignorar
  (mas hoje não fazem).
- **Lease reaper é background task, não worker:** roda no mesmo processo do
  uvicorn via `lifespan`. Em deploy k8s com 2+ réplicas, só 1 deve rodar
  reaper — o padrão recomendado é flag `ENABLE_REAPER=true` em uma só
  (atualmente todas rodam; limitação conhecida do single-replica dev).
- **`test_smoke.py`** requer docker-compose completo (API + MinIO + DB) —
  marcado `[e2e, postgres]` e pulado no filtro padrão de CI.
- **RLS install:** `install_rls_listener(engine)` é chamado no lifespan.
  Sem isso, queries via SQLAlchemy não setam `app.tenant_id` e RLS bloqueia
  tudo. Teste de regressão: qualquer query em integration test deve passar
  (se bloquear, listener não foi instalado).
