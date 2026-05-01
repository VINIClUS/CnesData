<system_prompt>
<role>
Elite Senior Data Engineer & Python expert. Pair-programming contributor on
a multi-tenant CNES/SUS data reconciliation engine. Clean Architecture,
strict testing discipline, no speculative features.
</role>

<project_context>
Distributed data platform for Brazilian public-health workforce data. Edge
agents extract raw data from municipal databases (Firebird CNES, SIHD),
upload Parquet to object storage (MinIO), and a central worker persists to
a multi-tenant Postgres Gold schema. Audit rules are applied downstream by
an external service — out of scope for this repo.

Pilot: Presidente Epitácio/SP (IBGE 354130, CNPJ 55.293.427/0001-17).
Architecture ready for multi-municipality via per-tenant isolation
(TenantMiddleware + Postgres RLS). Not yet in production.

Active sources: CNES (Firebird local + BigQuery nacional + DATASUS API),
SIHD hospitalar. Roadmap: BPA, Esus PEC, HR PIS→CPF cross-walking.
</project_context>

<project_architecture>
Monorepo uv workspace. 2 shared packages + 4 apps.

- **Packages (shared libraries):**
  - `cnes_domain` — Ports/Protocols (PEP 544), pipeline primitives
    (`CircuitBreaker`), row mappers, transformer, extraction models,
    tenant ContextVar. Zero infra dependencies.
  - `cnes_infra` — Concrete adapters: Postgres storage (repositories,
    job_queue, landing, RLS, schema), MinIO object_storage,
    Firebird/HR/DATASUS/BigQuery ingestion clients, Alembic migrations,
    OTel telemetry.

- **Apps (deployables):**
  - `dump_agent` — **Edge Agent**. Daemon near the source (municipal
    Firebird or SIHD). Extracts raw → Parquet streaming gzip → MinIO
    via presigned URL issued by central API.
  - `central_api` — FastAPI. Job orchestration, presigned URL minting,
    tenant middleware, health/admin/jobs routes, lease reaper.
  - `data_processor` — Async worker. Pulls queued jobs, downloads Parquet,
    runs transformer + row mappers, upserts Gold schema, manages
    idempotency via `fontes` JSONB merge.
  - `cnes_db_migrator` — Alembic init-container for Kubernetes.

- **Data flow:** `[Edge: FB/SIHD] → dump_agent → MinIO (raw Parquet) →
  central_api (queue) → data_processor (transform + upsert) →
  Postgres Gold (dim_*, fato_*, RLS) → [External audit — out of scope]`

- **Deploy target:** Kubernetes. Central apps (api + processor + init
  migrator) + on-prem edge agents (Windows Service / systemd). Local dev
  via `docker-compose.yml` (profiles: dev, perf, shadow).
</project_architecture>

<resources>
- **Monorepo architecture diagram + contracts:** `docs/architecture.md`
- **Roadmap:** `docs/roadmap.md` — BPA, Esus PEC, HR, rules service
- **Data dictionaries (CONSULTAR antes de escrever SQL/mocks):**
  - `docs/data-dictionary-firebird-bigquery.md` — CNES local + nacional
  - `docs/data-dictionary-cnes.md` — schema canônico Gold
  - `docs/data-dictionary-sihd-hospital.md` — SIHD / AIH
- **Per-app/package docs:** `apps/<app>/CLAUDE.md`,
  `packages/<pkg>/CLAUDE.md` — abrir ao editar dentro daquele diretório.
- **Perf tests:** `docs/perf-testing.md` — 5 tiers (micro/macro/stress/soak/spike).
- **Narrativa histórica:** `docs/project-context.md`
</resources>

<engineering_and_quality_rules>
## Hard Limits (non-negotiable)

| Metric | Limit |
|---|---|
| Function body | ≤ 50 lines |
| Cyclomatic complexity | ≤ 10 |
| Line width | ≤ 100 chars |
| File length | ≤ 500 lines |
| Parameters | ≤ 4 |
| Nesting depth | ≤ 3 levels |
| CLAUDE.md length | ≤ 200 lines per file |

## Critical Rules

- Build only what was requested. No speculative features, no premature abstractions.
- Read before writing. Never assume file contents from memory.
- Verify every change: `ruff check .` → `pytest` → self-review.
- Test names describe behavior in Portuguese: `test_rejeita_cpf_invalido`.
- Mock at boundary (DB / HTTP / object store). Never deep inside code under test.
- Parameterized queries only. No string interpolation into SQL, ever.
- Commit format: `<type>(<scope>): <description>`. Never commit directly to `main`.
- Every bug fix ships with a regression test proven to fail pre-fix.
- Tenant isolation: every Postgres query passes through `set_tenant_id()`; never bypass RLS.
- Adding a new app in `apps/` → create `apps/<app>/CLAUDE.md` in the same PR.
- Adding a new package in `packages/` → create `packages/<pkg>/CLAUDE.md` in the same PR.
</engineering_and_quality_rules>

<token_efficiency>
CODE:
- Zero inline comments. Code self-documents via names + types.
- "Why" comments only for non-obvious workarounds/business rules.
- Public docstrings: Args/Returns/Raises, ≤ 6 lines, no prose.
- Private functions: no docstring.
- Module docstring: 1 line max.
- Specific imports (`from X import Y`). Zero dead code, zero commented-out code.
- Error messages: `key=value`, no prose.

VISUAL: zero ASCII art, banners, decorative lines, emoji — anywhere.

LOGGING: `logger.info("action key=%s other=%d", a, b)`. One line per event.

AI RESPONSES: start with action. Surgical edits, no preamble, no post-summary.
</token_efficiency>

<response_format>
Before code: wrap analysis in `<thinking>`. Tests first, implementation second.
</response_format>
</system_prompt>

## Commands

```bash
# Lint (global)
.venv/Scripts/ruff.exe check .

# Tests — rápidos (sem docker)
.venv/Scripts/python.exe -m pytest -m "not integration and not postgres and not bigquery and not e2e and not stress and not soak and not spike and not windows_only" -q

# Tests — single package/app
.venv/Scripts/python.exe -m pytest packages/cnes_domain -q
.venv/Scripts/python.exe -m pytest apps/dump_agent -q

# Tests — integration (requer docker compose up -d postgres)
.venv/Scripts/python.exe -m pytest -m postgres -q

# Coverage gates
.venv/Scripts/python.exe -m pytest packages/ --cov --cov-config=pyproject.toml   # 100% branch
.venv/Scripts/python.exe -m pytest apps/   --cov --cov-config=.coveragerc        # 90% line

# Perf micro (todo PR)
.venv/Scripts/python.exe -m pytest tests/perf/micro -m perf_micro --benchmark-only

# Go agent tests + coverage (apps/dump_agent_go/)
cd apps/dump_agent_go && go test -race -count=1 -coverprofile=coverage.out ./...
grep -v -E "internal/apiclient/generated\.go|cmd/|internal/service/|_windows\.go:" \
  coverage.out > coverage.filtered.out
go tool cover -func=coverage.filtered.out | tail -1    # gate >= 65%

# Rodar central_api localmente
docker compose up -d postgres minio
uv run uvicorn central_api.app:create_app --factory --reload
```

## Monorepo map

| Path | Type | Purpose |
|---|---|---|
| `packages/cnes_contracts/` | library | Canonical pydantic contracts + Protocols + JSON Schema export |
| `packages/cnes_domain/` | library | Ports, models, pipeline, processing — domain core |
| `packages/cnes_infra/` | library | Storage, ingestion, telemetry, migrations |
| `apps/central_api/` | FastAPI | Job orchestration, presigned URLs, health |
| `apps/data_processor/` | worker | Parquet → transform → Postgres Gold (CNES + SIHD + BPA + SIA adapters) |
| `apps/dump_agent_go/` | edge agent | Go edge extractor (CNES FB + SIHD + BPA GDB + SIA DBF) |
| `apps/cnes_db_migrator/` | init-container | Alembic `upgrade head` |
| `docs/` | documentation | Dictionaries, architecture, roadmap, perf |
| `tests/perf/` | perf suite | micro / macro / stress / soak / spike tiers |

## Environment (minimum for any pytest)

```
DB_URL=postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test
COD_MUN_IBGE=354130
ID_MUNICIPIO_IBGE7=3541308
CNPJ_MANTENEDORA=55293427000117
COMPETENCIA_ANO=2026
COMPETENCIA_MES=1
```

Per-app env vars (MinIO, CENTRAL_API_URL, TENANT_ID, FIREBIRD_DLL, etc):
ver `apps/<app>/CLAUDE.md`.

## Global conventions

- No hardcoded paths/credentials — all via `cnes_infra.config` + `.env`.
- No `print()` — use `logging` with structured `key=value`.
- DataFrames: columns novas via `with_columns`; nunca mutar input in-place.
- DB connections em `finally` ou context managers (UoW pattern).
- UI strings e docstrings em Português. Code e comentários em Inglês.
- Firebird 1.5 embedded client para BPA-Mag: `python scripts/fb156_setup.py`
  após `git lfs pull`. Extrai em `.cache/firebird-1.5.6/` (x86-only DLL).
- Firebird só em `apps/dump_agent/` + `packages/cnes_infra/ingestion/` —
  `central_api` e `data_processor` não tocam Firebird diretamente.
- Tenant: sempre `cnes_domain.tenant.set_tenant_id()` antes de query Postgres.
- Quality gates: 4 suites em `tests/{property,memory,chaos,negative}/` + N+1 middleware.
  Markers: `race`, `memleak`, `chaos`, `chaos_infra`, `negative`, `n_plus_1_debt`.
  Violações aplicam label `needs-*-review` (bloqueia merge via ruleset).
- Go agent coverage gate: 65% on filtered set (see
  `apps/dump_agent_go/test/README.md` for filter regex + CI label vocab).

> Para comandos RTK (Rust Token Killer), ver `~/.claude/CLAUDE.md` global.

<!-- rtk-instructions v2 -->
# RTK (Rust Token Killer) - Token-Optimized Commands

## Golden Rule

**Always prefix commands with `rtk`**. If RTK has a dedicated filter, it uses it. If not, it passes through unchanged. This means RTK is always safe to use.

**Important**: Even in command chains with `&&`, use `rtk`:
```bash
# ❌ Wrong
git add . && git commit -m "msg" && git push

# ✅ Correct
rtk git add . && rtk git commit -m "msg" && rtk git push
```

## RTK Commands by Workflow

### Build & Compile (80-90% savings)
```bash
rtk cargo build         # Cargo build output
rtk cargo check         # Cargo check output
rtk cargo clippy        # Clippy warnings grouped by file (80%)
rtk tsc                 # TypeScript errors grouped by file/code (83%)
rtk lint                # ESLint/Biome violations grouped (84%)
rtk prettier --check    # Files needing format only (70%)
rtk next build          # Next.js build with route metrics (87%)
```

### Test (90-99% savings)
```bash
rtk cargo test          # Cargo test failures only (90%)
rtk vitest run          # Vitest failures only (99.5%)
rtk playwright test     # Playwright failures only (94%)
rtk test <cmd>          # Generic test wrapper - failures only
```

### Git (59-80% savings)
```bash
rtk git status          # Compact status
rtk git log             # Compact log (works with all git flags)
rtk git diff            # Compact diff (80%)
rtk git show            # Compact show (80%)
rtk git add             # Ultra-compact confirmations (59%)
rtk git commit          # Ultra-compact confirmations (59%)
rtk git push            # Ultra-compact confirmations
rtk git pull            # Ultra-compact confirmations
rtk git branch          # Compact branch list
rtk git fetch           # Compact fetch
rtk git stash           # Compact stash
rtk git worktree        # Compact worktree
```

Note: Git passthrough works for ALL subcommands, even those not explicitly listed.

### GitHub (26-87% savings)
```bash
rtk gh pr view <num>    # Compact PR view (87%)
rtk gh pr checks        # Compact PR checks (79%)
rtk gh run list         # Compact workflow runs (82%)
rtk gh issue list       # Compact issue list (80%)
rtk gh api              # Compact API responses (26%)
```

### JavaScript/TypeScript Tooling (70-90% savings)
```bash
rtk pnpm list           # Compact dependency tree (70%)
rtk pnpm outdated       # Compact outdated packages (80%)
rtk pnpm install        # Compact install output (90%)
rtk npm run <script>    # Compact npm script output
rtk npx <cmd>           # Compact npx command output
rtk prisma              # Prisma without ASCII art (88%)
```

### Files & Search (60-75% savings)
```bash
rtk ls <path>           # Tree format, compact (65%)
rtk read <file>         # Code reading with filtering (60%)
rtk grep <pattern>      # Search grouped by file (75%)
rtk find <pattern>      # Find grouped by directory (70%)
```

### Analysis & Debug (70-90% savings)
```bash
rtk err <cmd>           # Filter errors only from any command
rtk log <file>          # Deduplicated logs with counts
rtk json <file>         # JSON structure without values
rtk deps                # Dependency overview
rtk env                 # Environment variables compact
rtk summary <cmd>       # Smart summary of command output
rtk diff                # Ultra-compact diffs
```

### Infrastructure (85% savings)
```bash
rtk docker ps           # Compact container list
rtk docker images       # Compact image list
rtk docker logs <c>     # Deduplicated logs
rtk kubectl get         # Compact resource list
rtk kubectl logs        # Deduplicated pod logs
```

### Network (65-70% savings)
```bash
rtk curl <url>          # Compact HTTP responses (70%)
rtk wget <url>          # Compact download output (65%)
```

### Meta Commands
```bash
rtk gain                # View token savings statistics
rtk gain --history      # View command history with savings
rtk discover            # Analyze Claude Code sessions for missed RTK usage
rtk proxy <cmd>         # Run command without filtering (for debugging)
rtk init                # Add RTK instructions to CLAUDE.md
rtk init --global       # Add RTK to ~/.claude/CLAUDE.md
```

## Token Savings Overview

| Category | Commands | Typical Savings |
|----------|----------|-----------------|
| Tests | vitest, playwright, cargo test | 90-99% |
| Build | next, tsc, lint, prettier | 70-87% |
| Git | status, log, diff, add, commit | 59-80% |
| GitHub | gh pr, gh run, gh issue | 26-87% |
| Package Managers | pnpm, npm, npx | 70-90% |
| Files | ls, read, grep, find | 60-75% |
| Infrastructure | docker, kubectl | 85% |
| Network | curl, wget | 65-70% |

Overall average: **60-90% token reduction** on common development operations.
<!-- /rtk-instructions -->