---
name: Recurring violations
description: Code patterns that violate CLAUDE.md rules and have been found in reviews; flag on every review
type: feedback
---

## main() body length always exceeds 50 lines

The orchestration function in `src/main.py` is a known A1 violator. It currently spans ~182 body lines.
The function handles 5 pipeline layers, CLI resolution, conditional HR/nacional cross-checks, 11 CSV
exports, Excel report, and snapshot — all in one function body.

**Why:** Author treats main() as a pipeline script, not a function. The rule still applies.
**How to apply:** Flag A1 on every review touching main.py until it is decomposed into sub-functions
(e.g., `_executar_ingestao`, `_executar_cruzamento_nacional`, `_executar_exportacao`).

## Banner/separator comments in source files

`src/main.py` uses `# ── Camada N: ... ─────` decorative comment banners inside function bodies.
This violates D1 (zero decorative lines) and D2 (only "why" comments).

**Why:** These comments describe *what* each block does, not *why*. The code structure and function
names should communicate this instead.
**How to apply:** Flag D1 on every review that touches main.py. The fix is to extract each "camada"
into a dedicated helper function whose name replaces the comment.

## Module docstrings exceed one-line limit

`src/main.py` and test files use multi-line module docstrings (10+ lines) that describe architecture,
usage, and coverage. The limit is one line per D4.

**Why:** The detail belongs in external documentation (docs/data-dictionary-firebird-bigquery.md, CLAUDE.md), not in module
docstrings that bloat every file open.
**How to apply:** Flag D4 on every review where the module docstring exceeds one line.

## Private functions with docstrings

`cli.py:_validar_competencia` and test helper functions (`_aplicar_patches`, `_mocks_simples`) have
docstrings. Rule D3: private functions have no docstring.

**Why:** Name + type hints provide enough documentation for private helpers.
**How to apply:** Flag D3 whenever a `_prefixed` function has a docstring.

## _aplicar_patches / _mocks_simples exceed parameter limit

`_aplicar_patches` takes 13 parameters; `_mocks_simples` takes 11. Rule A5: ≤ 4 parameters.

**Why:** These test helpers were built to avoid test duplication but grew unconstrained. The fix is a
dataclass/TypedDict for patch configuration.
**How to apply:** Flag A5 on every review touching test_main.py until resolved.

## _cruzar_nacional exceeds parameter limit

`src/main.py:_cruzar_nacional` now takes 5 parameters after the `cbo_lookup` addition (commit d1cdaf9).
Rule A5: ≤ 4 parameters. The fix is a dataclass or TypedDict to bundle the cross-check inputs.

**Why:** Each time a new enrichment is threaded through this helper, it gains a parameter. The pattern
will repeat unless the signature is consolidated.
**How to apply:** Flag A5 as BLOCKING on every review touching `_cruzar_nacional` until resolved.

## except Exception catch-all in main.py

`src/main.py:301` uses `except Exception as e:` then calls `.exception()`. Rule B6: no bare `except
Exception:`. A top-level last-resort catch-all is a common exception to this rule but it must be
explicitly acknowledged.

**Why:** In this specific case it is intentional — the top-level guard catches truly unexpected errors
and returns exit code 1. The author calls `.exception()` (full traceback) not `.error()`.
**How to apply:** Flag B6 as a WARNING (not BLOCKING) with a note that a more specific exception
type or a `BaseException` comment would make intent explicit.

## module-scoped fixture using function-scoped monkeypatch

Integration test fixtures with `scope="module"` must not accept `monkeypatch` (which is
function-scoped). pytest raises `ScopeMismatch` at runtime. The fix is to use `monkeypatch`
only in function-scoped tests, or use `sys.argv` direct mutation + teardown for module-scoped
fixtures that need argv patching.

**Why:** Found in tests/test_pipeline_integration.py `pipeline_offline` fixture. The error surfaces
only when the fixture is actually invoked (integration run), not in collection.
**How to apply:** Flag as BLOCKING whenever a module/session-scoped fixture declares `monkeypatch`
as a parameter.

## pytest.mark.bigquery not registered in pytest.ini

Custom marks must be declared in `pytest.ini` under `[pytest] markers =` or pytest emits
`PytestUnknownMarkWarning` and `-m bigquery` filtering silently includes all tests.

**Why:** Found in tests/test_pipeline_integration.py. The `bigquery` mark is used for selective
execution but is not registered, so `-m "not bigquery"` may not filter correctly.
**How to apply:** Flag as WARNING whenever a `@pytest.mark.<name>` is used without a corresponding
entry in pytest.ini markers section.

## job_queue.py at/above 500-line file limit

`packages/cnes_infra/src/cnes_infra/storage/job_queue.py` has been at 533 lines since commit
d23fcc7 (Task 4). Rule A4: ≤ 500 lines. Each commit that adds to this file makes the violation
worse.

**Why:** The file hosts table definitions, 8+ queue functions, DLQ logic, and lease reaping all
in one module. Growth pressure will continue as more queue features are added.
**How to apply:** Flag A4 as BLOCKING on any future commit that touches job_queue.py and does not
split the file. Suggested split: `job_queue_schema.py` (table/metadata defs) + `job_queue.py`
(functions only).

## run_processor body length (consumer.py) — pre-existing, worsened by Tasks 7+8

`apps/data_processor/src/data_processor/consumer.py:run_processor` body is 58 lines (limit 50),
pre-existing but worsened by the Tasks 7+8 additions (6 new body lines). The function now
combines signal handling, state-flag polling, job acquisition, drain closing, and the processing
span all in one body.

**Why:** Each new feature adds 4-6 lines to what is already a monolithic async loop. The next
feature addition will push it further over the limit.
**How to apply:** Flag A1 as BLOCKING on any future commit touching run_processor. Suggested
decomposition: extract `_setup_signal_handlers(loop)` and `_process_one_job(loop, engine, storage, job)`.
The flag-check and drain-check branches are small enough to stay inline.
