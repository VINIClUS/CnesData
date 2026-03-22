---
name: CnesData Known False Positives
description: Patterns that look dangerous but are acceptable in this project's context
type: project
---

**SQL string interpolation with .format():**
- In cnes_client.py (Firebird) and web_client.py (BigQuery), SQL strings use str.format().
- Variables injected: `cod_mun`, `cnpj`, `id_municipio`, `ano`, `mes`.
- ALL values come from .env environment variables or CLI integer-validated input — never from end-user free-text input.
- Firebird fdb library does not support parameterized queries for embedded mode; cursor.execute(sql, params) is not consistently reliable on Firebird 2.5 embedded (documented driver limitation).
- Risk: LOW (no external user input reaches SQL). Not a practical injection vector given the deployment model.

**DB_PASSWORD in config.py:**
- `src/config.py:49` and `src/ingestion/cnes_client.py:150` reference DB_PASSWORD.
- Not hardcoded — read from environment variable via `_exigir("DB_PASSWORD")`.
- Not logged.
- Not a finding.

**broad except in web_client.py:**
- `except Exception as exc: raise CnesWebError(...)` — this is intentional re-wrapping for a stable public API boundary, not error suppression.
- The original exception is preserved via `from exc`.
