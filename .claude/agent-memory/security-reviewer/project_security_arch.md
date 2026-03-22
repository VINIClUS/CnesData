---
name: CnesData Security Architecture
description: Security-relevant architecture facts for the CnesData pipeline — auth patterns, data flow, injection surface, PII handling
type: project
---

**Auth/Credentials:** All secrets via .env (python-dotenv). Config loaded at import time in src/config.py. .env is in .gitignore. DB_PASSWORD passed directly to fdb.connect() — never logged at INFO, but DB_DSN (host:path, no password) is logged at DEBUG.

**SQL Injection Surface:**
- Firebird queries (cnes_client.py): use Python str.format() with `cod_mun` and `cnpj` from environment variables, NOT user input. Values originate from .env only. Not parameterized but not user-controlled.
- BigQuery queries (web_client.py): use str.format() with `id_municipio`, `ano`, `mes`. The municipio comes from .env (ID_MUNICIPIO_IBGE7); ano/mes come from CLI (validated integer range) or .env. Not parameterized.
- Firebird does not support parameterized queries via fdb in the same way as other DBs — the manual cursor pattern is intentional (documented workaround for -501 error).

**Why:** SELECT * used in `_SQL_EQUIPES` in web_client.py (BigQuery) — fetches all columns from `basedosdados.br_ms_cnes.equipe`. Low risk since it is a public read-only table, but wasteful.

**PII in Logs — Known Issues:**
- `src/processing/transformer.py:77`: CPF values (invalid ones) logged directly in a WARNING: `"RQ-002: %d registro(s) excluído(s) por CPF inválido: %s", total_invalidos, cpfs_invalidos`. CPF is PII under LGPD.
- `src/ingestion/hr_client.py:101`: NOME (full name) of employees with invalid CPF logged at WARNING: `"cpf_invalido nome=%s fonte=%s idx=%d"`. Name is PII.

**Path Traversal:** CLI `--output-dir` arg accepted as raw string, wrapped in `Path()` without `.resolve()` or allowlist check. User can write to any writable filesystem location. This is a local-only CLI tool, so risk is low in practice, but noted.

**Config int() conversion:** `int(os.getenv("COMPETENCIA_ANO", "2026"))` at config load time — raises ValueError (not EnvironmentError) if non-numeric value is set. Unhandled in config.py.

**Dependency audit:** pip-audit failed to install into venv due to protobuf==6.33.6 dependency conflict. Manual audit needed. No known CVE packages identified from version scan.

**No web server, no HTTP endpoints, no user authentication layer** — this is a local batch pipeline run by a trusted operator. OWASP A01 (access control) and A07 (auth failures) are not applicable.

**CBO Lookup (added commit d1cdaf9):**
- `_SQL_CBO_LOOKUP` in cnes_client.py:98 — fully static SELECT, no user input, no injection surface.
- `extrair_lookup_cbo()` builds a `dict[str, str]` (CBO code → job description). Max ~3,000 rows (standard CBO table). Memory footprint negligible.
- `cbo_lookup` passed as optional param through `transformar()` and `detectar_divergencia_cbo()`. No validation at boundary — dict values are job title strings from a reference table, not PII. Low risk.
- DESCRICAO_CBO values (job titles, e.g. "AGENTE COMUNITARIO DE SAUDE") are NOT PII under LGPD. Safe to export in CSV.
- `extrair_lookup_cbo()` returns `{}` on empty table — pipeline continues silently (graceful degradation). If query fails with DatabaseError, exception propagates to main()'s broad handler (returns exit code 1). Acceptable.
- Docstring of `transformar()` not updated to document the `cbo_lookup` parameter in Args section. Minor documentation gap, not a security issue.
- Test coverage: 4 tests in TestExtrairLookupCbo, 4 tests in TestCboEnrichment. Cursor close verified.
