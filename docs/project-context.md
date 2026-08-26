# CnesData — Project Context

> Living document. Last updated: 2026-07-08.
> Audience: developers, operators, and future AI sessions that need the
> project shape without rediscovering the whole repository.

## What This Project Is

CnesData is a distributed data platform for Brazilian public-health data
reconciliation. It captures raw municipal SUS data at the edge, lands it in a
central multi-tenant platform, and exposes canonical contracts for downstream
processing and audit services.

Pilot municipality: Presidente Epitacio/SP (`tenant_id=354130`, CNPJ
`55.293.427/0001-17`). The architecture is multi-municipality by design:
tenant isolation is enforced with `X-Tenant-Id`, `cnes_domain.tenant`, and
Postgres RLS.

Current repository scope:

- Edge extraction agent in Go (`apps/dump_agent_go`)
- Central API and dashboard (`apps/central_api`, `apps/web_dashboard`)
- Canonical contracts and storage/adapters (`packages/cnes_contracts`,
  `packages/cnes_domain`, `packages/cnes_infra`)
- Migration runner (`apps/cnes_db_migrator`)
- Data processor worker loop plus source adapters/repositories

Out of scope for this repo: the rules/audit service that consumes the central
Gold or landing schemas through SQL.

## Why It Exists

Municipal CNES data and national DATASUS/CNES data often diverge. Those
differences affect SUS funding, workforce planning, legal compliance, and audit
readiness. Before this project, reconciliation was manual: operators exported
local Firebird data, checked national data by hand, and assembled findings in
spreadsheets.

CnesData exists to make that process deterministic and repeatable:

- Capture source data near the municipality instead of requiring central
  Firebird access.
- Preserve provenance by source (`CNES_LOCAL`, `CNES_NACIONAL`, `SIHD`,
  `BPA_MAG`, `SIA_LOCAL`).
- Store raw landing manifests and canonical facts under tenant isolation.
- Let audit rules run downstream against stable contracts instead of raw source
  schemas.

## Current Architecture

The current implementation is a monorepo with shared packages and deployable
apps.

```text
Municipal edge sources
  CNES Firebird / SIHD / BPA-Mag Firebird / SIA DBF
        |
        v
  dump_agent_go
  - extracts raw rows
  - writes parquet.gz files
  - registers N-file manifests with object-storage keys
  - registers manifests with central_api
        |
        v
  central_api
  - tenant middleware
  - extraction enqueue/register endpoints
  - dashboard read APIs
  - OAuth device activation
  - certificate provisioning and rotation
        |
        +--> MinIO/S3 landing bucket
        +--> Postgres schemas: landing, gold, dashboard, auth
        |
        v
  data_processor
  - claims landing.extractions
  - marks work completed or failed without downloading artifacts
  - full Parquet-to-Gold routing is not wired into the worker loop
```

See `docs/architecture.md` for the more detailed system model, route map, job
state, and deploy target.

## Source Contracts

Landing source types are defined in `packages/cnes_contracts/src/cnes_contracts/landing.py`.

| Source type | Meaning | File subtypes |
|---|---|---|
| `CNES_LOCAL` | Municipal CNES Firebird extract | `CNES_VINCULO` |
| `CNES_NACIONAL` | National CNES extract | `CNES_VINCULO` |
| `SIHD` | Hospital AIH production | `SIHD_INTERNACAO`, `SIHD_PROC_AIH` |
| `BPA_MAG` | BPA-Mag Firebird extract | `BPA_C`, `BPA_I` |
| `SIA_LOCAL` | Local SIA DBF extract | `DIM_SIGTAP`, `DIM_MUNICIPIO`, `SIA_APA`, `SIA_BPI`, `SIA_BPIHST` |

Contract artifacts:

- OpenAPI: `docs/contracts/openapi.json`
- JSON Schemas: `docs/contracts/schemas/`
- Regeneration scripts: `scripts/gen_openapi.py`, `scripts/gen_contracts.py`

## Historical Audit Context

Earlier versions of this project produced Excel/CSV reconciliation reports from
a monolithic CLI (`src/main.py`). That flow was removed during the monorepo
migration. The audit concepts remain useful domain context, but the rules now
belong to an external service.

Historical rule families:

| Family | Examples | Current home |
|---|---|---|
| Local CNES quality | invalid CPF, zero workload, duplicate professional allocation | External rules service |
| Local vs national CNES | establishment/professional missing on either side, CBO mismatch, workload delta | External rules service |
| HR/payroll cross-check | ghost payroll, missing CNES registration | Planned external workflow |
| Team-level audit | ESF/EAP/ESB composition and INE mismatches | Conceptual, blocked by source mapping |

Important historical discoveries still matter:

- Firebird CNES uses legacy WIN1252 data; sanitize before writing UTF-8 Parquet.
- CNES extraction cannot be simplified into a single join because of the known
  Firebird 2.5 join issue around team tables; keep the three-query merge.
- National CNES does not expose CPF; CNS is the cross-source professional key.
- Provenance must stay explicit. Do not silently merge local and national data.

## Technical Environment

| Component | Current value |
|---|---|
| Python | 3.13 |
| Python workspace | `uv` monorepo |
| Go edge agent | Go 1.26 |
| Frontend | Bun 1.3, React, Vite, TanStack Router/Query |
| Central API | FastAPI + SQLAlchemy + MinIO + Keycloak/OIDC |
| Database | PostgreSQL 16 locally; Kubernetes target for central services |
| Object storage | MinIO locally; S3-compatible interface |
| Edge runtime | Windows Service target, Linux supported for dev/CI |
| Observability | stdlib structured logs; optional OTel |
| Generated contracts | OpenAPI + JSON Schema under `docs/contracts/` |

## Current Operational State

Implemented and useful now:

- Go edge extractors and Parquet writers for the active source families.
- Central API route surface for health, dashboard, activation, provisioning,
  extraction enqueue, and job registration.
- Postgres migrations through `018_add_sha256_landing`.
- Dashboard v1.1 for tenant overview, agent status, activation, dark mode, and
  access-request flow.
- Quality suites for Python, Go and frontend paths.

Known boundaries:

- `data_processor` claims landing work and marks it completed without downloading
  artifacts or invoking the available SHA-256, delta, and source-specific routes.
- `extractions_repo` implements claim, completion, failure, and expired-lease
  reaping; heartbeat and uploaded transitions are still deferred.
- Rules/audit output is intentionally external to this repository.
- Production Kubernetes manifests are incomplete outside the dashboard chart.

## New Session Guide

When starting work:

1. Read `README.md` for the current repo map and quick start.
2. Read `docs/architecture.md` for system contracts and data flow.
3. Read the nearest `apps/<app>/CLAUDE.md` or `packages/<pkg>/CLAUDE.md`
   before editing inside an app/package.
4. Use `docs/development.md` for commands that mirror CI.
5. Consult data dictionaries before changing source mappings or test fixtures:
   `docs/data-dictionary-cnes.md`,
   `docs/data-dictionary-firebird-bigquery.md`,
   `docs/data-dictionary-bpa.md`,
   `docs/data-dictionary-sia.md`,
   `docs/data-dictionary-sihd-hospital.md`.

Do not reintroduce the removed monolithic CLI, implicit source fallback, or
Excel/CSV report generation as current architecture.
