# Napkin Runbook

## Curation Rules
- Re-prioritize on every read.
- Keep recurring, high-value notes only.
- Max 10 items per category.
- Each item includes date + "Do instead".

## Execution & Validation
1. **[2026-04-13] Polars replace_strict with default=None fails on non-null columns**
   Do instead: Use `pl.when().then().otherwise()` pattern for SUS mapping.

2. **[2026-04-13] Polars str operations fail on Null-typed columns**
   Do instead: Cast to `pl.Utf8` before calling `.str.strip_chars()` or `.str.pad_start()`.

3. **[2026-04-13] web_client mocks must return pandas (not polars) because bd.read_sql returns pandas**
   Do instead: Mock `bd.read_sql` with `pd.DataFrame`, the adapter converts via `pl.from_pandas()`.

4. **[2026-04-13] venv is `.venv` not `venv` in this project**
   Do instead: `.venv/Scripts/python.exe` and `.venv/Scripts/ruff.exe`.

 (Highest Priority)
1. **[2026-03-27] Always use Windows venv paths for commands**
   Do instead: `./venv/Scripts/python.exe` and `./venv/Scripts/ruff.exe`, never `python` or `ruff` directly.

2. **[2026-03-27] Run lint before tests, always**
   Do instead: `./venv/Scripts/ruff.exe check src/ tests/` then `./venv/Scripts/python.exe -m pytest ...`.

## Shell & Command Reliability
1. **[2026-03-27] pd.read_sql() with LEFT JOIN fails error -501 in Firebird**
   Do instead: use manual cursor iteration instead of `pd.read_sql()` for any query with LEFT JOINs.

2. **[2026-03-27] LFCES060.SEQ_EQUIPE is national (6-7 digits); first 4 chars = LFCES048.SEQ_EQUIPE (local)**
   Do instead: join on `SUBSTRING(lfces060.seq_equipe FROM 1 FOR 4) = lfces048.seq_equipe`.

## Domain Behavior Guardrails
1. **[2026-03-27] CD_SEGMENT/DS_SEGMENT return error -206 via alias in nested LEFT JOIN**
   Do instead: recover those columns in a separate subquery, not inline.

2. **[2026-03-27] Consult data_dictionary.md before writing any SQL, extraction logic, or mock DataFrames**
   Do instead: read the relevant table section in data_dictionary.md first, then write the query/mock.

## User Directives
1. **[2026-03-27] TDD is mandatory for all new features and audit rules**
   Do instead: write failing test first, then implementation. Use tdd-workflow skill.

2. **[2026-03-27] No speculative features — build only what was explicitly requested**
   Do instead: re-read the request, implement only what was asked, no extras.

3. **[2026-03-27] All user-facing strings and docstrings in Portuguese; business logic in English**
   Do instead: check language context before writing any string or docstring.
