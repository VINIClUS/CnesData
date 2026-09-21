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
| Object storage | MinIO AIStor via Docker Compose (S3 API) |

For Firebird fixture archives, run:

```bash
git lfs pull
uv run python scripts/fb156_setup.py
```

## Local Stack

MinIO AIStor (the `minio` service, profile `dev`) needs a Free-tier license
before `mc ready local` passes — without one, `central-api` and
`data-processor` (both `depends_on: minio: condition: service_healthy`)
never start at all, not just their S3 calls. See [Object storage
license](#object-storage-license) below.

Start all local development services:

```bash
docker compose --profile dev up -d
```

For direct/local HTTP access, keep `TRUST_X_FORWARDED_PROTO=false`. Only enable
it when the API is behind the trusted TLS-terminating Caddy proxy used by the
dev and production deployment stacks.

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

Sem Postgres/MinIO/Keycloak (SQLite + filesystem, `--profile local`): ver
`docs/runbooks/local-profile.md`.

### Object storage license

`minio/minio` was archived and removed from Docker Hub; the `dev`/`shadow`
profiles run `quay.io/minio/aistor/minio`. Free tier permits commercial use
in standalone mode but requires a license file — get one at
https://min.io/pricing and place it at `./minio.license` (repo root,
gitignored, never commit it). Without one the container starts and its
`/minio/health/live` endpoint reports healthy, but every S3 operation is
denied — the compose healthcheck runs `mc ready local` instead, which
correctly fails in that state. If `./minio.license` doesn't exist on the
host, Docker's bind mount silently creates a directory at that path instead
of failing — `docker compose --profile dev up` then hangs on the same
health check with no obvious cause; check `ls -la minio.license` if that
happens.

`--profile aws-test` (LocalStack, used by `scripts/ci_phase2_adapters.sh`)
and `--profile shadow` (`minio-shadow`, now also LocalStack — see
`docker-compose.yml`) need no license.

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

Release workflow (`.github/workflows/dump-agent-go-release.yml`): cut a release, dry-run,
channels, rollback documented in `docs/runbooks/dumpagent-release.md`. Lint the workflow
locally before pushing (no dedicated CI job for this — 14 other workflows already lint
each other's blast radius):

```bash
go install github.com/rhysd/actionlint/cmd/actionlint@latest
actionlint .github/workflows/dump-agent-go-release.yml .github/workflows/dump-agent-go.yml
```

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
