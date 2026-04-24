# CnesData — Arquitetura

> Visão sistêmica do monorepo. Para contexto histórico/narrativa ver `docs/project-context.md`.
> Para roadmap ver `docs/roadmap.md`.

## Visão macro

Plataforma distribuída edge/central para reconciliação de dados de saúde
pública. Edge Agents (`dumpagent_go`) rodam próximo às fontes municipais
(Firebird CNES, SIHD hospitalar), extraem Parquet raw e fazem upload via
URL pré-assinada para MinIO. O `central_api` (FastAPI) orquestra jobs e
emite presigned URLs. O `data_processor` (worker) consome jobs, baixa
Parquet do MinIO, aplica transformações canônicas e persiste no schema
Gold Postgres com isolamento por tenant (RLS). Regras de auditoria são
aplicadas por serviço externo via SQL JOINs contra o Gold.

## Data flow

```
┌───────────────────────────────────────────────────────────────┐
│ EDGE (município)                                              │
│                                                               │
│  [Firebird CNES.GDB]        [SIHD DB]                         │
│         │                      │                              │
│         └──────┬───────────────┘                              │
│                ▼                                              │
│          dump_agent (daemon)                                  │
│          - 3-query extraction + merge (CNES)                  │
│          - streaming Parquet gzip                             │
│          - single-instance lock                               │
│                │                                              │
│                │ HTTPS (long poll)                            │
└────────────────┼──────────────────────────────────────────────┘
                 │
┌────────────────┼──────────────────────────────────────────────┐
│ CENTRAL        ▼                                              │
│         central_api (FastAPI)                                 │
│          - /jobs/next (lease)                                 │
│          - /jobs/{id}/artifact (presigned PUT)                │
│          - /jobs/{id}/heartbeat                               │
│          - /jobs/{id}/complete                                │
│          - TenantMiddleware (X-Tenant-Id)                     │
│          - lease reaper (background task)                     │
│                │                                              │
│       ┌────────┴────────┐                                     │
│       ▼                 ▼                                     │
│  [Postgres queue]   [MinIO]  ← presigned GET/PUT              │
│       │                 ▲                                     │
│       │                 │                                     │
│       ▼                 │                                     │
│   data_processor ───────┘                                     │
│   - download Parquet                                          │
│   - transform (CPF pipeline, dedup, CH flag)                  │
│   - row_mapper (dim/fato)                                     │
│   - upsert Postgres Gold (merge JSONB fontes)                 │
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
`data_processor` has BPA + SIA adapters downstream (see
`apps/data_processor/CLAUDE.md`).

**Spike status:** T1 FB 1.5 wire-protocol validation via `nakagami/firebirdsql`
is **DEFERRED**. Runtime validation blocked at fixture generation
(fdb/FB1.5 symbol mismatch); pivot tracked in issue #51.

## Contratos entre apps

### Edge → Central (HTTPS)

| Verb + Path | Payload | Response |
|---|---|---|
| `GET /api/v1/jobs/next?machine_id=<id>` | header `X-Tenant-Id` | `{job_id, intent, lease_until}` ou 204 |
| `POST /api/v1/jobs/{id}/heartbeat` | `{}` | `{lease_until}` |
| `POST /api/v1/jobs/{id}/artifact` | `{filename, size}` | `{presigned_url, object_key}` |
| `POST /api/v1/jobs/{id}/complete` | `{object_key, rows}` | 204 |
| `POST /api/v1/jobs/{id}/fail` | `{reason, retryable}` | 204 |

### Central → MinIO

- Presigned PUT URLs (validade: 1h) para upload de Parquet
- Presigned GET URLs (validade: 1h) para download pelo `data_processor`
- Bucket único: `cnesdata-landing` (configurável via `MINIO_BUCKET`)
- Key convention: `<tenant_id>/<intent>/<competencia>/<job_id>.parquet.gz`

### Processor → Central

Mesmo pattern de polling dos edges — o processor consome da mesma fila,
filtrando por `status=ready_to_process`.

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

## Fluxo de jobs (estados)

```
   ┌─────────┐       lease       ┌────────┐
   │ pending ├──────────────────▶│ leased │
   └────┬────┘                   └───┬────┘
        │                            │
        │ lease expired              │ heartbeat
        │ (reaper)                   │
        ◀────────────────────────────┤
        │                            │
        │                            │ complete
        │                            ▼
        │                      ┌──────────┐
        │                      │ uploaded │
        │                      └────┬─────┘
        │                           │ processor picks
        │                           ▼
        │                   ┌─────────────┐
        │                   │ processing  │
        │                   └──────┬──────┘
        │                          │
        │                          │ success
        │                          ▼
        │                   ┌─────────────┐
        │                   │ completed   │
        │                   └─────────────┘
        │
        │ fail (retryable)
        ▼
   ┌─────────┐  exceeded retries  ┌─────────────┐
   │ pending │ ──────────────────▶│ dead_letter │
   └─────────┘                    └─────────────┘
```

Tabelas: `public.jobs` (fila), `public.job_retries`, `public.job_leases`.
Reaper do `central_api` roda a cada `_REAPER_INTERVAL=60s` e volta leases
expirados para `pending`.

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

- **`dev`** — postgres, minio, migrator, central-api, data-processor, pg-seed, minio-init. Portas 5433/9000/8000.
- **`perf`** — postgres_perf (tuned), firebird_perf. Portas 5434/3051.
- **`shadow`** — firebird-shadow (FB 2.5-ss), minio-shadow. Portas 3052/9100. Usado por `.github/workflows/shadow-e2e.yml`.

Uso:
```bash
docker compose --profile dev up -d
docker compose --profile perf up -d
docker compose --profile shadow up -d
```

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
