# central_api — FastAPI ingestion + job orchestration

## Executive Summary

Servidor FastAPI que recebe registros de manifests do `dump_agent_go`, expõe
dashboard API, device flow, provisionamento mTLS e rotas administrativas.
Gerencia `landing.extractions` em Postgres, expõe OpenAPI em `/openapi.json`
e mantém MinIO como conteúdo referenciado, não estado transacional.

## Role

**Central orchestrator**. Único ponto de entrada HTTP do sistema.
Horizontalmente escalável, com cuidado: reaper background task só deve rodar
em 1 réplica (gate via env `ENABLE_REAPER`).

## Functionalities

- `GET /api/v1/system/health` — healthcheck + ping Postgres
- `POST /api/v1/jobs/upload-url` — cria row PENDING + URL presigned PUT
- `POST /api/v1/jobs/register` — registra manifest N-file em `landing.extractions`
- `POST /api/v1/jobs/{job_id}/fail` — marca FAILED + persiste `error_detail`
  (status-guarded, `PENDING`/`CLAIMED` apenas — idempotente em retry)
- `POST /api/v1/extractions/enqueue` — cria extractions por fonte/competência
- `POST /api/v1/admin/reap-leases` — libera jobs com lease expirado (admin)
- `GET /api/v1/agents/status` — status agregado do agent (Bearer + `require_tenant_header`)
- Background task: `_lease_reaper_loop` (a cada `_REAPER_INTERVAL=60s`) no lifespan
- AuthMiddleware (JWKS) — gates Bearer JWT for /api/v1/dashboard/* + /activate/confirm
- /api/v1/dashboard/auth/me, /tenants, /agents/status, /agents/runs
- /api/v1/dashboard/overview, /faturamento/by-establishment
- /api/v1/dashboard/access-requests/*
- /activate/confirm — RFC 8628 redemption (Bearer JWT + tenant gate + rate limit 10/min)
- /oauth/device_authorization, /oauth/token — device flow
- /provision/cert, /provision/cert/rotate — cert enrollment + rotation

- `POST /api/v1/public/leads` — captação pública do formulário de contato (sem auth).
  Persiste em `marketing.leads` (migração 019), responde `202 {"status":"received"}`,
  `422` payload inválido, `429` + `Retry-After` acima de `LEADS_RATE_LIMIT` (slowapi, chave =
  `client_ip()` em `ratelimit.py`: só confia em `X-Forwarded-For` quando o peer do socket
  está em `TRUSTED_PROXY_CIDRS`; caso contrário usa sempre o IP do socket — ver Gotchas),
  `503 leads_unavailable` se o banco falhar.
- CORS explícito: `CORS_ALLOWED_ORIGINS` (lista separada por vírgula; `*` é ignorado).
  `CORSMiddleware` é o middleware mais externo para responder preflight antes do Auth.

## Objectives

- p99 de rotas Postgres-only < 200ms
- Zero cross-tenant leak (RLS + middleware + teste contratual)
- Uptime 99.9% (single-replica inicialmente; rolling restart no k8s suporta)

## Limitations

- **Não executa jobs** — só orquestra (execução fica no `data_processor`)
- **Não lê Firebird/SIHD direto** — todo dado chega via `dump_agent_go`
- **Não processa Parquet** — manifests apontam para artefatos; processamento fica no worker
- **Auth dividido por superfície** — dashboard usa Bearer JWT; agentes usam
  device flow + mTLS; rotas admin dev ainda usam token simples onde indicado
- **mTLS termina no Caddy** — `client_auth verify_if_given` nos vhosts `api.*`
  repassa o cert em `X-SSL-Client-Cert` (DER base64); `agent_auth.py` só aceita
  o header de `TRUSTED_PROXY_CIDRS`, revalida cadeia/serial/refresh e vincula
  o tenant (e grava o CN como machine_id) em `/api/v1/jobs/*`; rotação idem.
  `AGENT_MTLS_REQUIRED=false` só no stack local sem Caddy. Fora do profile
  local, o lifespan sobrescreve `raw_jobs.get_edge_identity` com
  `edge_identity_from_cert` (fingerprint = SHA-256 do DER).

## Requirements

**Runtime deps (apps/central_api/pyproject.toml):** `fastapi`, `uvicorn`,
`sqlalchemy`, `psycopg`, `cnes_domain`, `cnes_infra` (`storage/*`, `telemetry`).

**Env vars:**

| Var | Obrigatória | Descrição |
|---|---|---|
| `DB_URL` | sim | Postgres URL (`postgresql+psycopg://...`) |
| `AWS_ACCESS_KEY_ID` | sim (endpoint não-AWS) | Cadeia padrão do boto3; obrigatório se `S3_ENDPOINT_URL` setado |
| `AWS_SECRET_ACCESS_KEY` | sim (endpoint não-AWS) | Idem |
| `S3_ENDPOINT_URL` | opcional | Vazio = S3 real; `http://minio:9000` em dev, LocalStack em CI |
| `S3_PUBLIC_ENDPOINT_URL` | opcional | Host usado nas URLs presigned entregues ao edge agent; default = `S3_ENDPOINT_URL`. Só relevante em dev (self-hosted); S3 real já é público, prod não seta |
| `S3_REGION` | opcional | Default `sa-east-1` |
| `S3_BUCKET` | opcional | Default `cnesdata-landing` |
| `S3_ADDRESSING_STYLE` | opcional | `auto`\|`path`\|`virtual`, default `auto`. Sem `S3_ENDPOINT_URL`, só o default `auto` é normalizado para `virtual` (`auto` quebra presign com 307 fora de `us-east-1` — ver `s3_presigned.py`); um `path`/`virtual` explícito passa direto — necessário setar `path` se `S3_BUCKET` tiver ponto no nome, senão virtual-hosted falha TLS |
| `API_HOST` | opcional | Default `0.0.0.0` |
| `API_PORT` | opcional | Default `8000` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | opcional | Tracing (se OTel SDK instalado) |
| `ENABLE_REAPER` | opcional | `true` em 1 réplica para reaper rodar (futuro) |
| `AUTH_CA_CERT_PATH` | sim (no boot) | Path to PEM root CA cert |
| `AUTH_CA_KEY_PATH` | sim (no boot) | Path to PEM root CA private key |
| `AUTH_DEVICE_VERIFICATION_URI` | sim (no boot) | Public URL of dashboard /activate page |
| `AUTH_DEVICE_CODE_TTL` | não | seconds; device_code TTL (default 600) |
| `AUTH_ACCESS_TOKEN_TTL` | não | seconds; access_token TTL (default 300) |
| `AUTH_CERT_TTL_DAYS` | não | leaf cert validity (default 90) |
| `TRUST_X_FORWARDED_PROTO` | não | `true` atrás de proxy TLS-terminating confiável; default `false` |
| `TRUSTED_PROXY_CIDRS` | não | CIDRs separados por vírgula confiáveis para `X-Forwarded-For` no rate limiter; default vazio = nunca confia em XFF |

**Local run:**
```bash
docker compose up -d postgres minio
uv run uvicorn central_api.app:create_app --factory --reload
```

## Module Map

| Arquivo | Responsabilidade |
|---|---|
| `src/central_api/app.py` | `create_app()` factory — FastAPI + lifespan + middleware + routers |
| `src/central_api/deps.py` | `get_engine()`, `lifespan`, `_lease_reaper_loop`, RLS listener install |
| `src/central_api/middleware.py` | `AuthMiddleware` (Bearer JWT) + `QueryCounterMiddleware` |
| `src/central_api/routes/health.py` | `/api/v1/system/health` — ping DB |
| `src/central_api/routes/jobs.py` | `/api/v1/jobs/upload-url` + `/api/v1/jobs/register` + `/api/v1/jobs/{id}/fail` |
| `src/central_api/routes/extractions.py` | `/api/v1/extractions/enqueue` — enqueue admin |
| `src/central_api/routes/admin.py` | `/api/v1/admin/*` — reap-leases, ops |
| `src/central_api/routes/dashboard.py` | `/api/v1/dashboard/auth/me`, tenants, agents |
| `src/central_api/routes/overview.py` | `/api/v1/dashboard/overview`, faturamento |
| `src/central_api/routes/access_requests.py` | signup JIT access request |
| `src/central_api/routes/oauth.py` | device flow + `/activate/confirm` |
| `src/central_api/routes/provision.py` | cert enrollment |
| `src/central_api/routes/provision_rotate.py` | cert rotation |
| `repositories/dashboard_repo.py` | DashboardRepo (user/tenant/audit + agents/status + recent_runs) |
| `src/central_api/bootstrap.py` | `python -m central_api.bootstrap` — primeiro usuário do profile local |
| `src/central_api/local_backup.py` | `python -m central_api.local_backup {create,restore}` — backup/restore do profile local |

## Gotchas

- **Rota pública `/api/v1/public/*`** é isenta em `AuthMiddleware`; nunca use `Depends(require_auth)`
  nela nem exponha dados de tenant. O limiter compartilhado vive em `central_api/ratelimit.py`.
- **`TRUSTED_PROXY_CIDRS` vazio (default) faz o rate limiter ignorar `X-Forwarded-For` por
  completo** e usar sempre o IP do socket. Só popule com os CIDRs reais do proxy (Caddy/nginx)
  — nunca com `0.0.0.0/0` ou similar, senão qualquer chamador pode forjar XFF e contornar o
  limite (issue #235).
- **CORS nunca com `*`**: `cors_origins()` em `app.py` descarta wildcard; cada ambiente define
  só a origem do dashboard (`deploy/{dev,prod}` compose). Preflight de origem desconhecida → 400.

- **Tenant vem só de credencial (#260):** `X-Tenant-Id` sozinho não define
  tenant. `require_tenant_header` (async, Bearer + membership) ou o cert mTLS
  (`require_agent_cert`) chamam `set_tenant_id()`. Dependência que define
  tenant precisa ser `async` — dep sync roda no threadpool e o ContextVar não
  chega ao endpoint (por isso `jobs.py` redefine no corpo da rota).
- **Lease reaper é background task, não worker:** roda no mesmo processo do
  uvicorn via `lifespan`. Em deploy k8s com 2+ réplicas, só 1 deve rodar
  reaper — o padrão recomendado é flag `ENABLE_REAPER=true` em uma só
  (atualmente todas rodam; limitação conhecida do single-replica dev).
- **`test_smoke.py`** requer docker-compose completo (API + MinIO + DB) —
  marcado `[e2e, postgres]` e pulado no filtro padrão de CI. `conftest.py`
  sobe o profile `dev` (`docker compose --profile dev`); sem licença AIStor
  em `./minio.license`, `minio` nunca fica `healthy` e o compose aborta —
  ver comentário do serviço `minio` em `docker-compose.yml`.
- **RLS install:** `install_rls_listener(engine)` é chamado no lifespan.
  Sem isso, queries via SQLAlchemy não setam `app.tenant_id` e RLS bloqueia
  tudo. Teste de regressão: qualquer query em integration test deve passar
  (se bloquear, listener não foi instalado).
- **`/jobs/{id}/complete` does not exist** — edge no longer calls it (FU1
  dropped). `/jobs/{id}/fail` now exists (A1,
  `docs/edge-agent-audit-2026-09-20.md`) — before it did, every agent-side
  extraction failure 404'd, the outbox terminal-dropped the envelope, and
  the row orphaned at `PENDING` with the real error lost.
