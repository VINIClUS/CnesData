# CnesData — Arquitetura

> Visão sistêmica do monorepo. Para contexto histórico/narrativa ver `docs/project-context.md`.
> Para roadmap ver `docs/roadmap.md`.

## Visão macro

Plataforma distribuída edge/central para reconciliação de dados de saúde
pública. Edge Agents (`dump_agent_go`) rodam próximo às fontes municipais
(CNES Firebird, SIHD, BPA-Mag Firebird 1.5 e SIA DBF), extraem Parquet e
registram manifests no `central_api`. O `central_api` (FastAPI) orquestra
`landing.extractions`, dashboard, device flow e provisionamento mTLS. O
`data_processor` (worker) consome a fila Gold v2, aplica rotas de delta e
integridade quando recebe artefatos e persiste no Postgres com isolamento por
tenant (RLS). Regras de auditoria são aplicadas por serviço externo via SQL
JOINs contra o Gold.

## Data flow

```
┌───────────────────────────────────────────────────────────────┐
│ EDGE (município)                                              │
│                                                               │
│  [CNES.GDB] [SIHD DB] [BPAMAG.GDB] [SIA DBF]                 │
│         │        │          │           │                    │
│         └────────┴──────────┴───────────┘                    │
│                ▼                                              │
│          dump_agent_go (daemon)                               │
│          - discovery + per-source secrets                     │
│          - row-fingerprint delta (CNES/SIHD/BPA)              │
│          - full extract SIA DBF                               │
│          - outbox + circuit breaker                           │
│                │                                              │
│                │ HTTPS API                                    │
└────────────────┼──────────────────────────────────────────────┘
                 │
┌────────────────┼──────────────────────────────────────────────┐
│ CENTRAL        ▼                                              │
│         central_api (FastAPI)                                 │
│          - /extractions/enqueue                               │
│          - /jobs/register                                     │
│          - /oauth/device_authorization + /oauth/token         │
│          - /provision/cert + /provision/cert/rotate           │
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
│   - claim landing.extractions                                 │
│   - verify SHA-256 when provided                              │
│   - route _op delta rows                                      │
│   - mark completed/failed                                     │
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

All sources emit **N-file manifests**: one extraction per
`(source_type, competencia)` → N Parquets uploaded to MinIO → single
`POST /api/v1/jobs/register` with the manifest list and optional SHA-256.
`data_processor` has BPA + SIA adapters downstream (see
`apps/data_processor/CLAUDE.md`).

**Spike status:** FB 1.5 compatibility is covered by schema-parity CI using a
synthetic FB 2.5 ODS-11 GDB. Production nullability still needs manual
`RDB$RELATION_FIELDS` introspection against a real `BPAMAG.GDB`; capture the
result in `docs/data-dictionary-bpa.md`.

## Edge agent reliability

Current `dump_agent_go` production path includes:

- mTLS registration via `dumpagent register` and cert rotation via
  `/provision/cert/rotate`.
- Persistent bbolt outbox for `CompleteJob`/`FailJob` envelopes.
- Circuit breaker and jittered backoff around `central_api` drain.
- `dumpagent diagnose` for cert, auth dir, outbox, log dir and optional live
  probes.
- Source discovery into `%PROGRAMDATA%\dumpagent\config.yaml` with DPAPI-wrapped
  per-source Firebird secrets.
- Delta store at `%PROGRAMDATA%\dumpagent\state\delta.db`; SIA remains
  full-extract.
- HMAC-signed audit JSONL and SHA-256 integrity metadata per upload.

## Contratos entre apps

### API surface atual

| Verb + Path | Consumidor | Descrição |
|---|---|---|
| `POST /oauth/device_authorization` | device flow init | `{device_code, user_code, verification_uri}` |
| `POST /oauth/token` | device code grant | access token ou pending/denied |
| `POST /activate/confirm` | Bearer JWT + user code | activation confirmation |
| `POST /provision/cert` | CSR + registration token | client certificate |
| `POST /provision/cert/rotate` | mTLS + CSR | renewed client certificate |
| `POST /api/v1/jobs/register` | manifest with files + optional `sha256` | `{job_id, status}` |

### Central → S3 (MinIO AIStor em dev, LocalStack em CI, S3 real em prod)

- Bucket único: `cnesdata-landing` (configurável via `S3_BUCKET`), prefixo por
  tenant. Endpoint via `S3_ENDPOINT_URL` (vazio = S3 real).
- Artefatos Parquet ficam referenciados em `landing.extractions.files`
- Hash SHA-256 opcional fica em `landing.extractions.sha256`

### Dashboard → Central

| Verb + Path | Purpose |
|---|---|
| `GET /api/v1/dashboard/auth/me` | usuário autenticado + tenants |
| `GET /api/v1/dashboard/tenants` | tenants disponíveis |
| `GET /api/v1/dashboard/agents/status` | status dos edge agents |
| `GET /api/v1/dashboard/agents/runs` | execuções recentes |
| `GET /api/v1/dashboard/overview` | KPIs do tenant |
| `GET /api/v1/dashboard/faturamento/by-establishment` | série 12m por estabelecimento |
| `POST /api/v1/dashboard/access-requests` | solicitação JIT de acesso |
| `GET /api/v1/dashboard/access-requests/mine` | solicitações do usuário |
| `GET /api/v1/dashboard/access-requests/available-tenants` | tenants solicitáveis |

### Processor → Postgres

O loop atual consome `landing.extractions` diretamente por
`cnes_infra.storage.extractions_repo.claim_next`, define `tenant_id` com
`set_tenant_id()` e marca `completed` ou `failed`. Rotas auxiliares de
`data_processor.processor` validam SHA-256 e roteiam deltas `_op` para
callbacks/upserts quando usadas por ingestões específicas.

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
  -> mark_completed/mark_failed implementados
  -> download, heartbeat e transição UPLOADED pendentes
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
  handler escreve em stdout (container) e `logs/` (local).
- **Tracing:** OTel opcional — se `OTEL_EXPORTER_OTLP_ENDPOINT` setado,
  `cnes_infra.telemetry.init_telemetry("<service>")` exporta spans.
  Sem a env var, `tracer` é no-op (pragma no cover).
- **Métricas:** via OTel quando ativo; senão, nenhuma coleta própria
  (CloudWatch no perfil AWS pode scrapar métricas do FastAPI via middleware
  se configurado).
- **Health:** `GET /api/v1/system/health` retorna `{status: ok, db_connected: bool}`.

## Deploy target

Não é Kubernetes. Dois perfis, sem sobreposição:

- **VPS (perfil ativo):** Hostinger VPS via Docker Compose
  (`deploy/prod/docker-compose.prod.yml` — postgres, minio, migrator,
  central-api, data-processor, web-dashboard, keycloak, caddy). Deploy via
  `deploy-main.yml`/`deploy-develop.yml` em self-hosted runners (homelab
  Proxmox) que fazem SSH forced-command para o VPS; ver `### Self-hosted
  runners` abaixo. Pipeline aponta para `cnesdata.com.br` /
  `api.cnesdata.com.br`; status do piloto (produção real vs. infra pronta)
  segue `CLAUDE.md` ("Not yet in production").
- **AWS (alvo, EPIC #94):** S3+CloudFront (frontend), FastAPI seguindo no
  mesmo VPS, Step Functions Standard + ECS Fargate (processamento
  on-demand), DynamoDB (control plane), Cognito (OIDC). Ver
  `docs/superpowers/specs/2026-08-29-cnesdata-production-deployment-design.md`
  e `docs/superpowers/plans/2026-08-31-cnesdata-production-*.md`. Gate de
  entrada: `AWS-010…014` (EPIC #94), atualmente sem código, atrás de
  `CND-064`.

Edge (on-prem, ambos os perfis): `dump_agent_go` como Windows Service
(municípios) ou systemd (servidores Linux) — não muda com o perfil de
produção central.

Distribuição do binário do edge agent é independente dos dois perfis acima: GitHub
Releases é o registro canônico de versões (tag `dumpagent-go-v*`, notas, checksums) e
Cloudflare R2 (`releases.cnesdata.com.br`, leitura pública) é a camada primária
de download, com o GitHub Release como fallback. Workflow
`.github/workflows/dump-agent-go-release.yml`; contrato do manifesto de update em
`docs/contracts/dumpagent-update-manifest.schema.json`; corte de release, canais
(`stable`/`rc`) e rollback em `docs/runbooks/dumpagent-release.md`. O cliente de update
check / self-update no agente ainda não existe — hoje a instalação/atualização é manual
(`docs/runbooks/dumpagent-install-windows.md`).

Dockerfiles existem em cada `apps/*/Dockerfile`. `charts/web-dashboard/` é um
Helm chart legado do frontend, não usado pelo caminho de deploy ativo.

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

Single `docker-compose.yml` com 5 profiles:

- **`dev`** — postgres, minio, migrator, central-api, data-processor,
  web_dashboard, keycloak, pg-seed, minio-init. Portas
  5433/9000/9001/8000/5173/8080.
- **`local`** — central-api-local, data-processor-local, web-dashboard-local.
  SQLite + filesystem, sem Postgres/MinIO/Keycloak/AWS. Volume nomeado
  `local_data`. Bring-up e operação: `docs/runbooks/local-profile.md`.
- **`perf`** — postgres_perf (tuned), firebird_perf. Portas 5434/3051.
- **`shadow`** — firebird-shadow (FB 2.5-ss), minio-shadow. Portas 3052/9100. Usado por `.github/workflows/shadow-e2e.yml`.
- **`aws-test`** — dynamodb-local, localstack. Usado pela matriz de adapters AWS
  (`tests/integration/test_aws_adapter_matrix.py`).

Uso:
```bash
docker compose --profile dev up -d
docker compose --profile local up -d
docker compose --profile perf up -d
docker compose --profile shadow up -d
docker compose --profile aws-test up -d
```

Nota: o worker atual marca jobs reclamados como `COMPLETED` sem baixar artefatos;
download, roteamento, heartbeat e transição `UPLOADED` permanecem pendentes.

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

Servida por container Nginx próprio (build estático, `apps/web_dashboard/Dockerfile`)
atrás de Caddy — único entrypoint público na VPS (`deploy/prod/caddy/Caddyfile`),
que termina TLS e roteia `cnesdata.com.br` para `web-dashboard` e
`api.cnesdata.com.br` para `central-api`. JWT validado
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

Branch protection rules (`main` + `develop`, verified via
`gh api repos/.../rules/branches/<branch>`):
- Required status checks (ruleset `required-checks-long-lived`, both
  branches): `image (central_api)`, `image (data_processor)`,
  `image (cnes_db_migrator)`, `image (web_dashboard)` (the four Trivy
  image-scan matrix legs from `trivy.yml`), plus `sonar`, `dependencies`
  (Trivy), `config` (Trivy) and `lint-test-coverage`. Not strict (branches
  don't need to be up to date before merge). A flaky leg blocks all PRs into
  either branch; the repo owner's `RepositoryRole` bypass actor can override.
- `required_linear_history`, `non_fast_forward` (no direct force-pushes)
- PR required before merge, but with `required_approving_review_count: 0`
  and `require_code_owner_review: false` — no reviewer/CODEOWNERS approval
  is actually enforced by the ruleset today, despite `.github/CODEOWNERS`
  existing.
- `sonar`, `dependencies` and `config` used to be excluded from required
  checks because `main` carried an unaddressed Reliability/Security baseline
  (69 open issues, all counted as new code under a 30-day New Code Definition
  on a new repo). That baseline is now fixed or suppressed in-repo (see
  "Baseline de segurança" below); PR #194 confirmed `new_reliability_rating`
  and `new_security_rating` come back at `1` on PR-level analysis, and a
  throwaway negative-gate proof (PRs #197/#198, closed unmerged) confirmed a
  real regression on either rating fails the gate. `ci.yml`'s `pull_request`
  trigger had its `paths:` filter removed so `lint-test-coverage` runs
  unconditionally (a required check gated by a filter that doesn't always
  fire would leave matching PRs permanently pending) — `push` keeps the
  filter. `sonar.yml` and `trivy.yml` have no `paths:` filter on either
  trigger, so they needed no change.
- The ruleset's `conditions.ref_name.include` covers `main` and `develop`
  under one rule with no per-branch check scoping. `main`'s `ci.yml` now
  also lacks the `pull_request` `paths:` filter (carried over by the
  `develop` → `main` promotion, PR #200), so `lint-test-coverage` runs
  unconditionally on PRs against either branch.
- `sonar.yml`'s `push` trigger only runs on `main`, not `develop`: `develop`
  is a `short`-type branch in SonarCloud (not the project's main/long-lived
  branch), which has no quality gate to compute. `sonar.qualitygate.wait=true`
  polling a `short` branch's status fails with a misleading "Not authorized
  or project not found" error — a branch-type mismatch, not a token problem
  (confirmed by comparing `main`'s `push` run, type `long`, which
  authenticates and reports a real gate verdict, against `develop`'s, type
  `short`, run `35372745045`). PR-triggered analysis on `develop` (the
  actual enforcement path, before and after this becomes a required check)
  is unaffected — it authenticates and evaluates normally on both branches.
- Quality-gate labels (`needs-quality-review`, `needs-chaos-review`,
  `needs-security-review`, applied by `scripts/flag_quality_violation.py`)
  are informational — not enforced as a merge block by any ruleset rule.

Configured via GitHub rulesets (`gh api repos/.../rulesets`), not classic
branch protection.

### Baseline de segurança

Achados residuais de SonarCloud e Trivy que são falso positivo ou restrição
externa documentada (não bug real) ficam registrados como baseline
versionado, nunca silenciados por exclusão de diretório inteiro:

- **SonarCloud**: `sonar.issue.ignore.multicriteria` em
  `sonar-project.properties`, escopado por `ruleKey` + `resourceKey` (nunca
  um diretório inteiro sem regra específica). Cada entrada carrega
  justificativa + data de revisão em comentário.
- **Trivy**: `.trivyignore`, uma linha por CVE/AVD-ID com `# reason + review
  date (YYYY-MM-DD)`.

Ambos os arquivos são revisados na cadência atual (2026-12-17). Alargar
qualquer um dos dois — adicionar uma entrada nova ou ampliar o escopo de uma
existente — é mudança revisável em PR, visível em code review; não há
mecanismo para suprimir achados fora desses dois arquivos versionados.

### Trivy image scans — OS patch cache busting

`trivy.yml`, `deploy-develop.yml` and `deploy-main.yml` all build the four app
images with `cache-from/cache-to: type=gha,scope=<app>`. Because Docker keys a
`RUN` layer on its command string, the `apt-get upgrade`/`apk upgrade` layer in
each runtime `Dockerfile` would cache-hit forever and freeze at whatever OS
packages were available on first build. All three workflows pass a
`OS_PATCH_LEVEL` build-arg (`date -u +%Y-%m-%d`) into that layer's `RUN`, which
busts just that layer once per day — the expensive builder stages (`uv build`,
`bun run build`) stay cached. Same-day builds across workflows produce
byte-identical runtime layers, so the image Trivy scans matches what gets
pushed to GHCR.

### Self-hosted runners

`deploy-develop.yml` and `deploy-main.yml` run their `deploy` job on
self-hosted runners (`runner-cnes-dev` / `runner-cnes-prod`, homelab Proxmox,
labels `cnesdata` + `deploy-dev`/`deploy-prod`) holding the SSH key to the
Hostinger VPS. Both jobs trigger only on `push`/`workflow_dispatch` for
`develop`/`main` respectively — never `pull_request` — so untrusted PR code
never reaches a self-hosted runner or the deploy credential. All other jobs
(CI, quality gates, Sonar, Trivy) stay on GitHub-hosted `ubuntu-latest`/
`windows-latest`, which is free and unlimited for this public repo.
