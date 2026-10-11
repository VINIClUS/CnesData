# central_api — FastAPI ingestion + job orchestration

## Executive Summary

Servidor FastAPI que recebe manifests raw do `dump_agent_go` (`/api/v1/edge/*`),
expõe dashboard API, device flow, provisionamento mTLS e rotas administrativas.
A ingestão legada em `landing.extractions` está aposentada (MIG-012): sem escrita
de landing nem URL presigned. Expõe OpenAPI em `/openapi.json`.

## Role

**Central orchestrator**. Único ponto de entrada HTTP do sistema.
Horizontalmente escalável; o lifespan não agenda nenhuma background task de
lease (reaper removido em MIG-012).

## Functionalities

- `GET /api/v1/system/health` — healthcheck + ping Postgres
- `POST /api/v1/jobs/upload-url`, `/jobs/register`, `/jobs/{job_id}/fail`,
  `POST /api/v1/extractions/enqueue` e `POST /api/v1/admin/reap-leases` — aposentadas
  (MIG-012): 410 `legacy_ingestion_retired` incondicional (sem checar `PROFILE`, sem flag
  de runtime), antes de validar corpo, abrir engine ou tocar S3. A auth roda antes: sem cert
  mTLS (jobs) ou token (admin) segue 401; `ADMIN_TOKEN` vazio segue 503 `admin_disabled`
- `POST /api/v1/admin/raw-jobs/enqueue` — cria até dez jobs raw idempotentes por competência
- `GET /api/v1/agents/status` — status agregado do agent (Bearer + `require_tenant_header`)
- `GET /api/v1/agents/whoami` — identidade do cert mTLS (`require_agent_cert`); smoke do `register`
- AuthMiddleware (JWKS) — gates Bearer JWT for /api/v1/dashboard/* + /activate/confirm
- /api/v1/dashboard/auth/me, /tenants, /agents/status, /agents/runs
- /api/v1/dashboard/overview, /faturamento/by-establishment — aposentadas (MIG-011): 410
  `legacy_route_retired`, sem tocar o `DashboardRepo`
- `GET /api/v1/dashboard/serving/{dataset_name}/{document_name}` — única leitura de produto: só o
  pointer `current`; dataset fora de `build_source_catalog()` → 404 `dataset_unknown`; conteúdo
  ativo ausente → 503 `active_serving_unavailable`, sem fallback a versão anterior nem a Postgres
- /api/v1/dashboard/access-requests/*
- /activate/confirm — RFC 8628 redemption (Bearer JWT + tenant gate + rate limit 10/min)
- /oauth/device_authorization, /oauth/token — device flow
- /provision/cert, /provision/cert/rotate — cert enrollment + rotation

- `/api/v1/billing/{accounts,accounts/{id}/transfer,checkout,portal,status}` + `POST
  /api/v1/billing/webhooks/stripe` — montados sempre. `PROFILE=local|aws` com `disabled`:
  404 `billing_disabled` (status responde `disabled`; webhook 404 sem ler o body). `stripe`
  (só aws): `billing_deps.install_billing` sobrescreve as dependências dos routers (principal
  OIDC + `MembershipAuthorizer`); segredos só no `StripeClient`/verificador. Legado: 503.
- `POST /accounts` com chave nova para tenant já vinculado devolve a conta existente (201) via
  `get_tenant_account` forte + `require_billing_owner`; Customer em `billing_customers.py`.
- Gates 17B (`ApiBillingGates` de `composition.api_billing_gates`, uma composição por modo):
  `POST /api/v1/billing/accounts/{id}/tenants` (tenant + links + capacidade + membership
  `gestor` do criador, com o `oidc_issuer` do token, numa transação; membership órfã → 409),
  `POST /api/v1/admin/billing/{id}/revoke` (fora do router de token legado), gate de agente
  novo em `require_edge_agent` e gate de serving antes de emitir URL/stream (leitura forte).
- Shadow (`stripe`+`shadow`): `ApiBillingGates.observer` audita `entitlement.shadow_denied`
  sem mudar a resposta (agente novo, serving, tenant); fora disso é nulo. Ver runbook.
- Onboarding (conta nova): `POST /billing/accounts` sem `X-Tenant-Id` cria conta do próprio
  usuário sem link (capacidade semeada zerada) → checkout → `POST /accounts/{id}/tenants`
  como dono. Com `X-Tenant-Id`, exige `gestor` e vincula o tenant.
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
  o tenant; `/api/v1/jobs/*` (aposentadas) só exigem o cert antes do 410; rotação idem.
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
| `S3_*` (`S3_ENDPOINT_URL`, `S3_PUBLIC_ENDPOINT_URL`, `S3_REGION`, `S3_BUCKET`, `S3_ADDRESSING_STYLE`) | não | Sem leitor em `central_api` desde MIG-012 (presign legado removido); seguem definidas em `cnes_infra.config` |
| `API_HOST` | opcional | Default `0.0.0.0` |
| `API_PORT` | opcional | Default `8000` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | opcional | Tracing (se OTel SDK instalado) |
| `BILLING_MODE` | não | `disabled` (default) \| `stripe` (só `PROFILE=aws`); demais `BILLING_*`/`STRIPE_*`: `.env.example` |
| `ADMIN_TOKEN` | opcional | `X-Admin-Token` das rotas admin (`deps.require_admin_token`); vazio = 503 `admin_disabled` |
| `RAW_LOCAL_TOKEN` | sim (profile local raw) | Token próprio do agente para `/api/v1/edge/*` |
| `RAW_BACKEND` | sim (VPS raw) | `aws` ativa DynamoDB/S3 raw sem alterar o legado |
| `RAW_DYNAMODB_TABLE` | sim (`RAW_BACKEND=aws`) | Tabela raw do ambiente |
| `RAW_S3_BUCKET` | sim (`RAW_BACKEND=aws`) | Bucket raw do ambiente |
| `RAW_AWS_REGION` | sim (`RAW_BACKEND=aws`) | Região dos recursos raw |
| `RAW_AWS_ACCESS_KEY_ID` | sim (`RAW_BACKEND=aws`) | IAM raw do ambiente |
| `RAW_AWS_SECRET_ACCESS_KEY` | sim (`RAW_BACKEND=aws`) | Segredo IAM raw |
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
| `src/central_api/deps.py` | `get_engine()`, `lifespan` (perfis legado/local/aws), RLS listener install |
| `src/central_api/middleware.py` | `AuthMiddleware` (Bearer JWT) + `QueryCounterMiddleware` |
| `src/central_api/routes/health.py` | `/api/v1/system/health` — ping DB |
| `src/central_api/routes/jobs.py` | `/api/v1/jobs/upload-url` + `/register` + `/{id}/fail` — aposentadas (410) |
| `src/central_api/routes/extractions.py` | `/api/v1/extractions/enqueue` — aposentada (410) |
| `src/central_api/routes/admin.py` | `/api/v1/admin/reap-leases` — aposentada (410) |
| `src/central_api/routes/dashboard.py` | `/api/v1/dashboard/auth/me`, tenants, agents |
| `src/central_api/routes/overview.py` | rotas legadas `/overview` e `/faturamento/by-establishment` aposentadas (410) |
| `src/central_api/routes/serving.py` | `/api/v1/dashboard/serving/{dataset}/{doc}` — leitura pointer-only (stream local ou 307 aws) |
| `src/central_api/routes/access_requests.py` | signup JIT access request |
| `src/central_api/routes/oauth.py` | device flow + `/activate/confirm` |
| `src/central_api/routes/provision.py` | cert enrollment |
| `src/central_api/routes/provision_rotate.py` | cert rotation |
| `src/central_api/routes/billing*.py`, `stripe_webhook.py` | Billing Owner API + webhook Stripe (BIL-020/021); composição em `billing_deps.install_billing` |
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
  chega ao endpoint.
- **410, nunca 503, nas rotas aposentadas (MIG-012):** o outbox do agent descarta o envelope
  em 4xx (exceto 429) e reenvia 5xx para sempre. O corpo é `{"detail":"legacy_ingestion_retired"}`
  (string, fora do schema `HTTPValidationError`); o snapshot OpenAPI não declara o 410 — não
  adicione `responses=` nem docstring nessas rotas, senão o gate de diff do `openapi.json` quebra.
- **`test_smoke.py`** requer docker-compose completo (API + MinIO + DB) —
  marcado `[e2e, postgres]` e pulado no filtro padrão de CI. `conftest.py`
  sobe o profile `dev` (`docker compose --profile dev`); sem licença AIStor
  em `./minio.license`, `minio` nunca fica `healthy` e o compose aborta —
  ver comentário do serviço `minio` em `docker-compose.yml`.
- **RLS install:** `install_rls_listener(engine)` é chamado no lifespan.
  Sem isso, queries via SQLAlchemy não setam `app.tenant_id` e RLS bloqueia
  tudo. Teste de regressão: qualquer query em integration test deve passar
  (se bloquear, listener não foi instalado).
