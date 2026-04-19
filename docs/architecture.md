# CnesData — Arquitetura

> Visão sistêmica do monorepo. Para contexto histórico/narrativa ver `docs/project-context.md`.
> Para roadmap ver `docs/roadmap.md`.

## Visão macro

Plataforma distribuída edge/central para reconciliação de dados de saúde
pública. Edge Agents (`dump_agent`) rodam próximo às fontes municipais
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
