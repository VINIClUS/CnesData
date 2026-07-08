# CnesData

Distributed data platform for municipal SUS data reconciliation.

CnesData moves raw municipal health data from edge environments into a
tenant-isolated central store. Edge agents extract source data as Parquet,
the central API coordinates landing jobs and provisioning, and shared domain
packages define the contracts that downstream processors and audit services
consume.

Pilot tenant: Presidente Epitacio/SP (`tenant_id=354130`).

## Current Status

| Area | Status |
|---|---|
| Canonical contracts | Implemented in `packages/cnes_contracts` and exported to `docs/contracts/` |
| Domain + infra packages | Implemented for contracts, tenant context, storage, auth, migrations, MinIO and ingestion clients |
| Go edge agent | Active implementation in `apps/dump_agent_go` for CNES, SIHD, BPA-Mag and SIA sources |
| Central API | Active FastAPI app for health, dashboard, OAuth/device activation, cert provisioning, extraction enqueue and job registration |
| Web dashboard | Active Bun + React SPA with OIDC, activation, agent status, overview KPIs and access-request flow |
| Data processor | Polling/downloading skeleton plus adapters/repositories exist; full Parquet-to-Gold ingestion wiring is still in progress |
| Audit rules | Out of scope for this repo; external rules service consumes the Gold/landing schemas |
| Production deploy | Target is Kubernetes plus on-prem edge agents; local development uses Docker Compose |

## Architecture

```text
Municipal edge
  Firebird CNES.GDB / SIHD / BPAMAG.GDB / SIA DBF
        |
        v
  dumpagent (Go)
  - extracts raw rows
  - writes parquet.gz manifests
  - registers N-file manifests with object-storage keys
        |
        v
Central platform
  central_api (FastAPI)
  - tenant middleware and auth
  - extraction enqueue/register flow
  - device activation and certificate provisioning
        |
        +--> MinIO / S3 landing bucket
        |
        +--> Postgres
             - landing.extractions
             - gold.* tables
             - dashboard.* tables
             - RLS per tenant
        |
        v
  data_processor
  - claims landing work
  - ingestion path under active development
```

Source types accepted by the landing contract:

| Source type | Meaning | File subtypes |
|---|---|---|
| `CNES_LOCAL` | Municipal CNES Firebird extract | `CNES_VINCULO` |
| `CNES_NACIONAL` | National CNES source | `CNES_VINCULO` |
| `SIHD` | Hospital AIH production | `SIHD_INTERNACAO`, `SIHD_PROC_AIH` |
| `BPA_MAG` | BPA-Mag Firebird 1.5 extract | `BPA_C`, `BPA_I` |
| `SIA_LOCAL` | SIA DBF extract | `DIM_SIGTAP`, `DIM_MUNICIPIO`, `SIA_APA`, `SIA_BPI`, `SIA_BPIHST` |

See [docs/architecture.md](docs/architecture.md) for the full system model.

## Repository Map

| Path | Purpose |
|---|---|
| `packages/cnes_contracts/` | Pydantic contracts, Protocols and JSON Schema export |
| `packages/cnes_domain/` | Pure domain layer: ports, models, validation, tenant context, processing primitives |
| `packages/cnes_infra/` | SQLAlchemy storage, Alembic migrations, MinIO, auth, ingestion clients and telemetry |
| `apps/central_api/` | FastAPI API for orchestration, dashboard data, OAuth/device flow and cert provisioning |
| `apps/data_processor/` | Async worker skeleton for landing extraction processing |
| `apps/cnes_db_migrator/` | Alembic migration runner for init-container/local migration use |
| `apps/dump_agent_go/` | Go edge agent for municipal source extraction and upload |
| `apps/web_dashboard/` | Bun + React dashboard |
| `charts/web-dashboard/` | Helm chart for dashboard deployment |
| `docs/` | Architecture, dictionaries, runbooks, contracts and performance notes |
| `tests/` | Cross-cutting integration, property, memory, chaos, negative and perf suites |

## Quick Start

Prerequisites:

- Python 3.13
- `uv`
- Docker + Docker Compose
- Go 1.26 for `apps/dump_agent_go`
- Bun 1.3 for `apps/web_dashboard`
- Git LFS for Firebird fixture archives

Install Python workspace dependencies:

```bash
uv sync
cp .env.example .env
```

Start the local dev stack:

```bash
docker compose --profile dev up -d
```

Local endpoints:

| Service | URL |
|---|---|
| Central API Swagger | http://localhost:8000/docs |
| Central API health | http://localhost:8000/api/v1/system/health |
| Web dashboard | http://localhost:5173 |
| Keycloak dev realm | http://localhost:8080 |
| MinIO API | http://localhost:9000 |
| MinIO console | http://localhost:9001 |
| Postgres | `localhost:5433` |

Run the API directly during development:

```bash
uv run uvicorn central_api.app:create_app --factory --reload
```

Regenerate API and schema contracts after contract changes:

```bash
uv run python scripts/gen_openapi.py
uv run python scripts/gen_contracts.py
```

The exported OpenAPI file used by clients is
[docs/contracts/openapi.json](docs/contracts/openapi.json).

## Common Commands

Python lint and tests:

```bash
uv run ruff check .
uv run pytest packages/cnes_domain packages/cnes_infra -m "not bigquery and not e2e and not stress and not soak and not spike" --cov --cov-config=pyproject.toml
uv run pytest apps/ -m "not integration and not bigquery and not e2e and not stress and not soak and not spike and not windows_only" --cov --cov-config=.coveragerc
```

Go edge agent:

```bash
cd apps/dump_agent_go
make install-tools
make lint
make test
make build-windows
```

Web dashboard:

```bash
cd apps/web_dashboard
bun install
bun run codegen
bun run lint
bun run typecheck
bun run test
bun run build
```

More detail is in [docs/development.md](docs/development.md).

## Documentation

- [docs/architecture.md](docs/architecture.md) - system architecture, contracts and deploy shape
- [docs/project-context.md](docs/project-context.md) - product/domain context and historical decisions
- [docs/roadmap.md](docs/roadmap.md) - current priorities and removed scope
- [docs/development.md](docs/development.md) - local setup, verification commands and CI mirrors
- [docs/perf-testing.md](docs/perf-testing.md) - performance test tiers
- [docs/data-dictionary-cnes.md](docs/data-dictionary-cnes.md) - canonical CNES/Gold dictionary
- [docs/data-dictionary-firebird-bigquery.md](docs/data-dictionary-firebird-bigquery.md) - local/national CNES dictionary
- [docs/data-dictionary-bpa.md](docs/data-dictionary-bpa.md) - BPA dictionary
- [docs/data-dictionary-sia.md](docs/data-dictionary-sia.md) - SIA dictionary
- [docs/data-dictionary-sihd-hospital.md](docs/data-dictionary-sihd-hospital.md) - SIHD dictionary
- [docs/runbooks/](docs/runbooks/) - operational runbooks for agent setup, cutover and access requests

## Notes for Contributors

- Read the nearest `CLAUDE.md` before editing an app or package.
- Keep tenant isolation explicit; Postgres access must run under the tenant context/RLS path.
- Do not reintroduce the removed monolithic `src/main.py` CLI or Excel/CSV report flow.
- Generated contract files in `docs/contracts/` must stay in sync with source models.
