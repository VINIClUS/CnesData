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

**Why:** The detail belongs in external documentation (data_dictionary.md, CLAUDE.md), not in module
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

## except Exception catch-all in main.py

`src/main.py:263` uses `except Exception as e:` then calls `.exception()`. Rule B6: no bare `except
Exception:`. A top-level last-resort catch-all is a common exception to this rule but it must be
explicitly acknowledged.

**Why:** In this specific case it is intentional — the top-level guard catches truly unexpected errors
and returns exit code 1. The author calls `.exception()` (full traceback) not `.error()`.
**How to apply:** Flag B6 as a WARNING (not BLOCKING) with a note that a more specific exception
type or a `BaseException` comment would make intent explicit.
