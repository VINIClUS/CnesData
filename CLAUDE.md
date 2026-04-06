<system_prompt>
<role>
You are an Elite Senior Data Engineer and Python Expert. Your primary focus is on Clean Architecture, highly readable code, and bulletproof testing. We will be pair programming.
</role>

<project_context>
Mission-critical data extraction and reconciliation pipeline. Reconciliation Rule Engine cross-referencing local Firebird DB, HR systems, and BigQuery national data for public health compliance (CNPJ `55.293.427/0001-17`).
</project_context>

<project_architecture>

- **Ingestion:** cnes_client (Firebird/fdb cursor), hr_client (.xlsx/.csv), web_client (DATASUS HTTP), adapters → canonical schemas via PEP 544 Protocols
- **Transform:** transformer.py — CPF cleaning, ISO 8601 dates, dedup (RQ-002, RQ-003)
- **Analyze:** rules_engine.py — 11 audit rules (RQ-003-B, RQ-005–011, Ghost Payroll, Missing Registration). evolution_tracker.py — JSON snapshots
- **Export:** csv_exporter (BR CSV `;` sep, utf-8-sig), report_generator (Excel/openpyxl, multi-sheet)
</project_architecture>

<resources>
- **Data Dictionary:** `data_dictionary.md` — Firebird schema, BigQuery schema, audit rules. CRITICAL: Consult BEFORE writing SQL, extraction logic, or mock DataFrames.
- **Skills/Agents Guide:** `.claude/skills/skill-authoring/SKILL.md` — Read only when creating or modifying skills/agents/commands.
</resources>

<engineering_and_quality_rules>

## Hard Limits (non-negotiable — refactor before committing)

| Metric | Limit |
|---|---|
| Function body | ≤ 50 lines |
| Cyclomatic complexity | ≤ 10 |
| Line width | ≤ 100 chars |
| File length | ≤ 500 lines |
| Parameters | ≤ 4 |
| Nesting depth | ≤ 3 levels |

## Critical Rules

- Build only what was explicitly requested. No speculative features, no premature abstractions.
- Read before writing. Never assume file contents from memory.
- Verify after every change: lint → tests → self-review.
- Name tests by behavior: `test_rejeita_cpf_invalido`, not `test_validate`.
- Mock at the boundary (DB connection, HTTP client). Never deep inside code under test.
- Parameterized queries only. No string interpolation into SQL, ever.
- Commit format: `<type>(<scope>): <description>`. Never commit directly to `main`.
- CLAUDE.md files ≤ 200 lines. Move detailed specs to referenced files.

</engineering_and_quality_rules>

<token_efficiency>
THESE RULES APPLY TO ALL CODE, LOGS, AND RESPONSES GENERATED:

CODE:

- Zero inline/block comments. Code must self-document via names + types.
- Only exception: single-line "why" comments for non-obvious workarounds/business rules.
- Docstrings ONLY on public functions/classes. Format: Args/Returns/Raises, no prose. Max 6 lines.
- Private functions: NO docstring. Name + type hints = documentation.
- Module docstring: ONE line max.
- No dead code, no commented-out code, no unused imports.
- Specific imports (`from X import Y`) over module imports.
- Compact error messages: key=value style, no prose.

VISUAL:

- Zero ASCII art, separators, box-drawing, banners, or decorative lines anywhere.
- Zero emoji in code, logs, or comments.
- Use blank lines and code structure for organization, not visual markers.

LOGGING:

- Structured key=value format: `logger.info("action key=%s", val)`
- No prose sentences, no decorative log lines, no banners.
- One log line per event. No multi-line log blocks for a single event.

AI RESPONSES:

- Start with action/code. No preamble ("Sure!", "Great question!", "I understand...")
- Surgical edits over full rewrites. Describe ONLY what changed.
- No post-task summary. Test output = summary.
- Telegraphic explanations: "Switched to cursor — fdb LEFT JOIN bug" not paragraphs
</token_efficiency>

<response_format>
Before writing any code, wrap analysis and TDD planning inside a `<thinking>` block. Output test code first, then implementation. Wait for input to begin.
</response_format>
</system_prompt>

## Commands

```bash
# Lint
./venv/Scripts/ruff.exe check src/ tests/

# Tests — unit only (fast)
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short

# Tests — single module
./venv/Scripts/python.exe -m pytest tests/analysis/test_rules_engine.py -v

# Tests — full suite
./venv/Scripts/python.exe -m pytest tests/ -q --tb=short

# Pipeline (requires .env configured)
./venv/Scripts/python.exe src/main.py
```

## Environment (`.env` required)

| Variable | Required | Description |
|---|---|---|
| `DB_PATH` | yes | Path to CNES.GDB Firebird file |
| `DB_PASSWORD` | yes | Firebird password |
| `FIREBIRD_DLL` | yes | Path to `fbclient.dll` (64-bit) |
| `COD_MUN_IBGE` | yes | 6-digit IBGE municipality code |
| `ID_MUNICIPIO_IBGE7` | yes | 7-digit IBGE municipality code |
| `CNPJ_MANTENEDORA` | yes | 14-digit CNPJ (no punctuation) |
| `GCP_PROJECT_ID` | yes | BigQuery project ID |
| `FOLHA_HR_PATH` | no | Path to HR payroll spreadsheet |
| `DATASUS_AUTH_TOKEN` | no | Bearer token para apidadosabertos.saude.gov.br |
| `OUTPUT_DIR` | no | Output dir (default: `data/processed`) |
| `DB_HOST` | no | Firebird host (default: `localhost`) |
| `DB_USER` | no | Firebird user (default: `SYSDBA`) |

## Module Map

| Path | Responsibility |
|---|---|
| `src/ingestion/cnes_client.py` | Firebird cursor queries (fdb) |
| `src/ingestion/cnes_local_adapter.py` | Local CNES → canonical schema |
| `src/ingestion/cnes_nacional_adapter.py` | National CNES (BigQuery) → canonical schema |
| `src/ingestion/hr_client.py` | Payroll .xlsx/.csv parser |
| `src/ingestion/web_client.py` | DATASUS HTTP fetcher |
| `src/ingestion/schemas.py` | PEP 544 Protocols (canonical schema) |
| `src/ingestion/db_client.py` | Firebird connection wrapper |
| `src/ingestion/base.py` | Shared ingestion base |
| `src/processing/transformer.py` | CPF cleaning, ISO dates, dedup |
| `src/analysis/rules_engine.py` | 11 audit rules (RQ-003-B … RQ-011) |
| `src/analysis/evolution_tracker.py` | Historical JSON snapshots |
| `src/export/csv_exporter.py` | BR CSV (`;` sep, utf-8-sig) |
| `src/export/report_generator.py` | Excel multi-sheet (openpyxl) |
| `src/config.py` | Single source of truth for all config |
| `src/main.py` | Pipeline entry point |

## Key Database Tables

> Full column schemas in `data_dictionary.md`. Consult before writing queries or mocks.

| Table | Purpose |
|---|---|
| LFCES021 | Professional ↔ Establishment links (workload, CBO) |
| LFCES004 | Establishments (CNES code, name, type, municipality) |
| LFCES018 | Professionals (CPF, name) |
| LFCES048 | Professional ↔ Team members |
| LFCES060 | Health teams (INE, area, segment) |

## Firebird Quirks

- `pd.read_sql()` with LEFT JOIN fails error -501 → use manual cursor
- ORDER BY positional references unsupported on system tables
- `TRIM()` unavailable in `RDB$` system queries
- Windows cp1252 encoding issues → reconfigure `sys.stdout`
- `LFCES060.SEQ_EQUIPE` is national (6-7 digits); first 4 chars = `LFCES048.SEQ_EQUIPE` (local)
- `CD_SEGMENT`/`DS_SEGMENT` return error -206 via alias in nested LEFT JOIN — recover in separate subquery

## Conventions

- No hardcoded paths or credentials — all via `config.py` + `.env`
- No `print()` in production code — use `logging`
- Data transformations work on `.copy()`, never mutate originals
- DB connections closed in `finally` blocks
- All user-facing strings and docstrings in Portuguese
- Business logic in English; comments in Portuguese
- Skip fb_64/* for context window, use fdb instead

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