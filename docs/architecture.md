# CnesData — Arquitetura

> Visão sistêmica do monorepo. Para contexto histórico/narrativa ver `docs/project-context.md`.
> Para roadmap ver `docs/roadmap.md`.

## Visão macro

Plataforma distribuída edge/central para reconciliação de dados de saúde
pública. Edge Agents (`apps/dump_agent_go`) rodam próximo às fontes
municipais (Firebird CNES, SIHD hospitalar, BPA-Mag, SIA DBF), extraem
Parquet raw e registram manifests N-arquivos. O `central_api` (FastAPI)
orquestra cadastro/enfileiramento de extrações, dashboard, ativação de
agentes e provisionamento de certificados. O Postgres guarda schemas
`landing`, `gold`, `dashboard` e `auth` com isolamento por tenant (RLS).

Estado atual: contratos, migrations, edge agent, API e dashboard estão
ativos. O `data_processor` ainda é um esqueleto de polling/download; a
ligação completa Parquet -> Gold está pendente. Regras de auditoria são
aplicadas por serviço externo via SQL JOINs contra Gold/landing.

## Data flow

```
┌───────────────────────────────────────────────────────────────┐
│ EDGE (município)                                              │
│                                                               │
│  [Firebird CNES.GDB]        [SIHD DB]                         │
│         │                      │                              │
│         └──────┬───────────────┘                              │
│                ▼                                              │
│          dump_agent_go (daemon)                               │
│          - 3-query extraction + merge (CNES)                  │
│          - streaming Parquet gzip                             │
│          - single-instance lock                               │
│                │                                              │
│                │ HTTPS + manifest register                    │
└────────────────┼──────────────────────────────────────────────┘
                 │
┌────────────────┼──────────────────────────────────────────────┐
│ CENTRAL        ▼                                              │
│         central_api (FastAPI)                                 │
│          - /api/v1/extractions/enqueue                        │
│          - /api/v1/jobs/register                              │
│          - /oauth/device_authorization                        │
│          - /provision/cert                                    │
│          - TenantMiddleware (X-Tenant-Id)                     │
│          - lease reaper (background task)                     │
│                │                                              │
│       ┌────────┴────────┐                                     │
│       ▼                 ▼                                     │
│  [Postgres landing] [MinIO]                                   │
│       │                 ▲                                     │
│       │                 │                                     │
│       ▼                 │                                     │
│   data_processor ───────┘                                     │
│   - polling/download skeleton                                 │
│   - adapters + repos existem                                  │
│   - Parquet -> Gold wiring pendente                           │
│                │                                              │
│                ▼                                              │
│       [Postgres Gold]                                         │
│        gold.dim_estabelecimento                               │
│        gold.dim_profissional                                  │
│        gold.fato_vinculo                                      │
│        (RLS per tenant)                                       │
│                │                                              │
│                │ SQL JOINs                                    │
│                ▼                                              │
│       [Rules service — OUT OF SCOPE, repo externo]            │
└───────────────────────────────────────────────────────────────┘
```

## BPA + SIA edge flow (2026-04)

`dump_agent_go` also handles two additional sources alongside CNES + SIHD:

- **BPA-Mag:** reads FB 1.5 `BPAMAG.GDB` via `nakagami/firebirdsql` Go driver.
  Requires FB 1.5 server running on edge Windows x86 host.
  Fixtures + server zip at `docs/fixtures/firebird/`; CI setup via
  `scripts/fb156_setup.py --server` + `apps/dump_agent_go/scripts/ci/start_fb15.ps1`.
- **SIA:** reads `.DBF` files (S_APA, S_BPI, S_BPIHST, S_CDN, CADMUN) via
  `LindsayBradford/go-dbf` with cp1252 sanitize.

Both emit **N-file manifests**: one `ClaimedJob` per
`(source_type, competencia)` → N Parquets uploaded to MinIO via N presigned
PUTs → single `POST /api/v1/jobs/register` with the manifest list.
`data_processor` has BPA + SIA mapping helpers downstream, but the current
worker loop does not yet execute the full ingestion path.

**Spike status:** FB 1.5 runtime parity is validated in CI through synthetic
schema coverage. Production nullability introspection against a real
`BPAMAG.GDB` remains an operational follow-up.

## Contratos entre apps

### API surface atual

| Verb + Path | Consumidor | Descrição |
|---|---|---|
| `GET /api/v1/system/health` | ops / smoke | Health da API + Postgres |
| `POST /api/v1/extractions/enqueue` | admin/dev | Cria jobs `landing.extractions` por `source_type` |
| `POST /api/v1/jobs/register` | edge agent | Registra manifest N-arquivos extraído pelo agente |
| `GET /api/v1/dashboard/auth/me` | dashboard | Identidade/tenant do usuário |
| `GET /api/v1/dashboard/tenants` | dashboard | Tenants disponíveis |
| `GET /api/v1/dashboard/agents/status` | dashboard | Status agregado de agentes |
| `GET /api/v1/dashboard/agents/runs` | dashboard | Execuções recentes |
| `GET /api/v1/dashboard/overview` | dashboard | KPIs do tenant |
| `GET /api/v1/dashboard/overview/faturamento` | dashboard | Série de faturamento |
| `GET /api/v1/dashboard/access-requests/mine` | dashboard | Solicitações do usuário |
| `POST /api/v1/dashboard/access-requests` | dashboard | Cria solicitação de acesso |
| `GET /api/v1/dashboard/access-requests/available-tenants` | dashboard | Tenants solicitáveis |
| `POST /oauth/device_authorization` | edge agent | Inicia OAuth device flow |
| `POST /oauth/token` | edge agent | Troca device code/refresh por token |
| `POST /activate/confirm` | dashboard | Aprova device flow |
| `POST /provision/cert` | edge agent | Emite certificado cliente |
| `POST /provision/cert/rotate` | edge agent | Rotaciona certificado cliente |

O fluxo antigo `/jobs/next`, `/jobs/{id}/artifact`, heartbeat e complete
por rota HTTP não é a superfície atual do código. A forma ativa é
`landing.extractions` + manifest via `/api/v1/jobs/register`.

### Central → MinIO

- Fluxo alvo: URLs pré-assinadas para PUT/GET de Parquet
- Código atual: manifests registram `minio_key`, `fato_subtype`, `size_bytes`
  e `sha256`; emissão de presigned URLs não está exposta na API atual
- Bucket único: `cnesdata-landing` (configurável via `MINIO_BUCKET`)
- Convenção recomendada de key:
  `<tenant_id>/<source_type>/<competencia>/<job_id>.parquet.gz`

### Processor

O processor atual não chama o `central_api` para polling. Ele acessa
`landing.extractions` via `cnes_infra.storage.extractions_repo` e ainda
possui funções finais (`complete`, `fail`, `heartbeat`, `mark_uploaded`,
`reap_expired`) pendentes.

## Modelo de dados Gold

```
┌──────────────────────────────┐
│ dim_estabelecimento          │
├──────────────────────────────┤
│ tenant_id     VARCHAR  PK    │
│ cnes          VARCHAR  PK    │
│ cnpj          VARCHAR        │
│ nome_fantasia VARCHAR        │
│ municipio     VARCHAR        │
│ fontes        JSONB          │  ← {"LOCAL": true, "WEB": true}
│ criado_em     TIMESTAMPTZ    │
│ atualizado_em TIMESTAMPTZ    │
└──────────────────────────────┘
          │
          │ 1:N
          ▼
┌──────────────────────────────┐         ┌──────────────────────────────┐
│ fato_vinculo                 │         │ dim_profissional             │
├──────────────────────────────┤         ├──────────────────────────────┤
│ tenant_id   VARCHAR  PK      │ N:1 ────│ tenant_id VARCHAR PK         │
│ cnes        VARCHAR  PK,FK   │         │ cpf       VARCHAR PK         │
│ cpf         VARCHAR  PK,FK   │         │ cns       VARCHAR            │
│ competencia VARCHAR  PK      │         │ nome      VARCHAR            │
│ cbo         VARCHAR          │         │ fontes    JSONB              │
│ ch_total    INTEGER          │         │ atualizado_em TIMESTAMPTZ    │
│ alerta_ch   VARCHAR          │         └──────────────────────────────┘
│ fontes      JSONB            │
│ atualizado_em TIMESTAMPTZ    │
└──────────────────────────────┘
```

Todas as tabelas têm Row-Level Security ativa. Queries sem `tenant_id` no
contexto (via `set_tenant_id()` do `cnes_domain.tenant`) retornam vazio.

## Fluxo de jobs (landing.extractions)

Tabela principal: `landing.extractions`.

Campos centrais: `job_id`, `tenant_id`, `source_type`, `competencia`,
`files` JSONB, `depends_on` UUID[], `status`, `lease_until`,
`agent_version`, `machine_id`.

Status aceitos pela migration atual:

```text
PENDING, UPLOADED, PROCESSING, INGESTED, FAILED, DLQ,
REGISTERED, CLAIMED, COMPLETED
```

Fluxo implementado no código atual:

```text
POST /api/v1/extractions/enqueue
  -> cria landing.extractions status=PENDING

POST /api/v1/jobs/register
  -> atualiza PENDING/CLAIMED para REGISTERED
  -> grava files, agent_version, machine_id

data_processor.poll
  -> claim_next tenta mover PENDING para CLAIMED
  -> complete/fail ainda pendentes em extractions_repo
```

Fluxo alvo: `REGISTERED`/`UPLOADED` -> `PROCESSING` -> `INGESTED` ou
`FAILED`/`DLQ`, preservando `depends_on` para dimensões SIA antes dos fatos.

## Multi-tenancy

Fluxo do `tenant_id` em cada request:

```
[Edge agent / client]
    │  header: X-Tenant-Id: 354130
    ▼
[central_api.TenantMiddleware]
    │  call: set_tenant_id("354130")   ← ContextVar
    ▼
[route handler]
    │  usa cnes_domain.tenant.get_tenant_id() se precisa
    ▼
[repository / UoW]
    │  SQLAlchemy executa com RLS ativo (via event listener)
    │  Postgres injeta WHERE tenant_id = current_setting('app.tenant_id')
    ▼
[Postgres Gold]
```

Instalação do listener RLS: `cnes_infra.storage.rls.install_rls_listener(engine)`
no bootstrap do engine (em `central_api.deps` e `data_processor.main`).

## Observabilidade

- **Logs:** structured `key=value` via `logging` stdlib (sem prose). Root
  handler escreve em stdout (k8s) e `logs/` (local).
- **Tracing:** OTel opcional — se `OTEL_EXPORTER_OTLP_ENDPOINT` setado,
  `cnes_infra.telemetry.init_telemetry("<service>")` exporta spans.
  Sem a env var, `tracer` é no-op (pragma no cover).
- **Métricas:** via OTel quando ativo; senão, nenhuma coleta própria (k8s/prom
  pode scrapar métricas do FastAPI via middleware se configurado).
- **Health:** `GET /api/v1/system/health` retorna `{status: ok, db_connected: bool}`.

## Deploy target

Kubernetes. Layout planejado:

```
Namespace: cnesdata
├── Deployment: central-api    (2+ réplicas, only 1 com ENABLE_REAPER=true)
├── Deployment: data-processor (N réplicas, escala horizontal)
├── StatefulSet: (ou Deployment) minio  (ou managed S3)
├── StatefulSet: postgres     (managed preferencial)
└── Jobs transitórios:
    └── cnes-db-migrator (initContainer em pre-sync)

Edge (on-prem):
└── dump_agent como Windows Service (municípios) ou systemd (servidores Linux)
```

Ainda não está em produção. Dockerfiles existem em cada `apps/*/Dockerfile`.

## Fixtures (git-lfs)

LFS-tracked test data:

| Path | Purpose | Size |
|---|---|---|
| `docs/fixtures/shadow-seed/` | FB 2.5 seed SQL + CNES reference Parquet for shadow-e2e | ~100KB |
| `docs/fixtures/firebird/` | Firebird 1.5.6 embedded client for BPA-Mag local tests (x86-only) | 1.6MB |

Fresh-clone setup:

```bash
git lfs pull
python scripts/fb156_setup.py   # extract FB 1.5.6 client to .cache/
```

## Docker Compose (local)

Single `docker-compose.yml` com 3 profiles:

- **`dev`** — postgres, minio, migrator, central-api, data-processor, pg-seed, minio-init, web_dashboard, keycloak. Portas 5433/9000/9001/8000/5173/8080.
- **`perf`** — postgres_perf (tuned), firebird_perf. Portas 5434/3051.
- **`shadow`** — firebird-shadow (FB 2.5-ss), minio-shadow. Portas 3052/9100. Usado por `.github/workflows/shadow-e2e.yml`.

Uso:
```bash
docker compose --profile dev up -d
docker compose --profile perf up -d
docker compose --profile shadow up -d
```

Nota: na branch atual, `data-processor` pode logar erros das funções
pendentes em `extractions_repo` se houver jobs a processar.

## web_dashboard (2026-04 — v1.0 + v1.1)

`apps/web_dashboard/` — SPA Bun+React+TypeScript que oferece:

**v1.0 (entregue):**

- Login OIDC para gestor saúde municipal
- Página `/activate` (RFC 8628 device flow) para aprovação de edge agents
- Status dos edge agents do tenant (lag por fonte, últimas execuções) via
  agregação de `landing.extractions` por `source_type`

**v1.1 (entregue 2026-04):**

- `/overview` — KPIs do tenant (total estabelecimentos, com produção mês,
  procedimentos competência atual, % cobertura) + faturamento area chart
  12m por estabelecimento via `@tremor/react` lazy-loaded
- `/access-pending` — fluxo JIT de signup self-service: usuário sem tenant
  preenche solicitação (`POST /api/v1/dashboard/access-requests`), grava em
  `dashboard.access_requests` (status `pending`); aprovação manual via
  SQL admin v1.1 (UI em v1.2 — ver `docs/runbooks/access-request-approval.md`)
- Dark mode 3-state (light/dark/system) via `ThemeProvider` + matchMedia +
  localStorage; FOUC mitigado por script inline no `<head>`
- Per-chunk bundle budget gated em CI: main ≤ 200KB, tremor ≤ 100KB,
  recharts ≤ 100KB, qualquer rota ≤ 100KB

Servida por Nginx em pod separado, reverse-proxy para `central-api`.
Single-origin TLS terminado em ingress-nginx + cert-manager. JWT validado
em `central_api.middleware.AuthMiddleware` via
`cnes_infra.auth.jwt.JWKSValidator`. Mapping user→tenant via
`dashboard.user_tenants`. Audit em `dashboard.audit_log` (RLS por
`app.tenant_id`, FORCE) — actions estendidas em v1.1: `request_access`,
`approve_access`, `reject_access`, `view_overview`, `view_faturamento`.

Roadmap: Faturamento+regressão e Drill estabelecimento (v1.2);
admin UI para approve/reject (v1.2).

## Governance — Quality Gates

Python PRs run 6 quality jobs via `.github/workflows/python-quality.yml`:

- `n-plus-1` — middleware + SQLAlchemy listener + `assert_query_limit` fixture. Threshold: 15 queries / request.
- `race` — hypothesis property-based tests on job queue, tenant context, MinIO presign, lease reaper.
- `memleak` — pytest-memray per-test memory limits (Linux only; skipped on Windows).
- `chaos` — fault-injection fixtures (DB, MinIO, HTTP).
- `chaos-infra` — testcontainers-python container restart chaos (PR label `run-chaos` or nightly).
- `negative` — hypothesis-driven invalid input tests (CPF/CNS/competencia/tenant/SQL-injection).

Violations auto-apply PR labels via `scripts/flag_quality_violation.py`:

- `needs-quality-review` — N+1, race, memleak
- `needs-chaos-review` — chaos test failure (design bug)
- `needs-security-review` — negative-test failure (input handling bug)

Branch protection rule (`main`):
- CI status green
- No labels: `needs-quality-review`, `needs-chaos-review`, `needs-security-review`
- CODEOWNERS approval required for paths listed in `.github/CODEOWNERS`

Configure via GitHub ruleset UI.
