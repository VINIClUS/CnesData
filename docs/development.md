# CnesData — Development

This page keeps local setup and verification commands in one place. It mirrors
the CI workflows where practical.

## Toolchain

| Tool | Version / source |
|---|---|
| Python | 3.13 |
| Python package manager | `uv` |
| Go | 1.26 |
| Frontend runtime | Bun 1.3 |
| Database | PostgreSQL 16 via Docker Compose |
| Object storage | MinIO via Docker Compose |

For Firebird fixture archives, run:

```bash
git lfs pull
uv run python scripts/fb156_setup.py
```

## Local Stack

Start all local development services:

```bash
docker compose --profile dev up -d
```

Useful endpoints:

| Service | URL |
|---|---|
| Central API | http://localhost:8000 |
| Swagger UI | http://localhost:8000/docs |
| Health | http://localhost:8000/api/v1/system/health |
| Web dashboard | http://localhost:5173 |
| Keycloak | http://localhost:8080 |
| MinIO console | http://localhost:9001 |
| Postgres | `localhost:5433` |

Run only the API from the workspace:

```bash
uv run uvicorn central_api.app:create_app --factory --reload
```

Run Alembic migrations manually:

```bash
cd packages/cnes_infra
uv run alembic -c alembic.ini upgrade head
```

## Contract Generation

The central API OpenAPI document and Pydantic JSON Schemas are generated
artifacts. Regenerate them after changing routes or contracts:

```bash
uv run python scripts/gen_openapi.py
uv run python scripts/gen_contracts.py
```

CI checks that:

- `docs/contracts/openapi.json` matches `scripts/gen_openapi.py`
- `docs/contracts/schemas/` matches `scripts/gen_contracts.py`
- `apps/web_dashboard/src/api/generated.ts` matches `docs/contracts/openapi.json`

For the dashboard client:

```bash
cd apps/web_dashboard
bun run codegen
```

## Python Verification

Lint everything:

```bash
uv run ruff check .
```

Core packages with coverage:

```bash
uv run pytest \
  packages/cnes_domain packages/cnes_infra \
  -m "not bigquery and not e2e and not stress and not soak and not spike" \
  --cov --cov-config=pyproject.toml \
  --cov-report=term-missing
```

Apps with coverage:

```bash
uv run pytest \
  apps/ \
  -m "not integration and not bigquery and not e2e and not stress and not soak and not spike and not windows_only" \
  --cov --cov-config=.coveragerc \
  --cov-report=term-missing
```

Quality suites:

```bash
uv run pytest tests/property/ -m race --hypothesis-show-statistics -v
uv run pytest tests/memory/ -m memleak --memray --memray-bin-path=/tmp/memray/
uv run pytest tests/chaos/ -m "chaos and not chaos_infra" -v
uv run pytest tests/negative/ -m negative -v
```

## Go Edge Agent

```bash
cd apps/dump_agent_go
make install-tools
make lint
make test
make build-linux
make build-windows
```

Filtered coverage gate used by CI:

```bash
go test -race -count=1 -coverprofile=coverage.out ./...
grep -v -E "internal/apiclient/generated\.go|cmd/|internal/service/|_windows\.go:" \
  coverage.out > coverage.filtered.out
go tool cover -func=coverage.filtered.out | tail -1
```

Integration test labels in GitHub:

| Label | Effect |
|---|---|
| `run-windows-integration` | Runs Windows Firebird integration |
| `run-integration` | Runs Linux SIA integration |

## Web Dashboard

```bash
cd apps/web_dashboard
bun install
bun run codegen
bun run lint
bun run format:check
bun run typecheck
bun run test --coverage
bun run e2e
bun run build
bun run bundle:check
```

Playwright may need browser dependencies locally:

```bash
bunx playwright install --with-deps chromium
```

## Performance Suites

See [perf-testing.md](perf-testing.md) for thresholds and interpretation.

```bash
uv run pytest tests/perf/micro -m perf_micro --benchmark-only
uv run pytest tests/perf/macro -m perf_macro --benchmark-only
uv run pytest tests/perf/stress -m stress -v
uv run pytest tests/perf/soak -m soak -v
uv run pytest tests/perf/spike -m spike -v
```
