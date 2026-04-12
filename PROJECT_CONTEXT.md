# CnesData — Project Context

> Living document. Last updated: 2026-04-12.
> Audience: the developer (Vinícius), colleagues who will run the pipeline,
> and any future AI session that needs to understand this project deeply.

---

## 1. What This Project Is

CnesData is a **deterministic data reconciliation engine** for public health workforce data. It receives data from municipal CNES databases, cross-references it against the national CNES via BigQuery, and produces structured audit reports identifying inconsistencies, ghost registrations, and allocation errors.

**Pilot municipality:** Presidente Epitácio/SP (IBGE 354130, CNPJ 55.293.427/0001-17, population ~42,000).  
**Architecture direction:** multi-municipality API engine fed by lightweight **dump agents** that run locally at each municipality and send data via HTTP.

The output is a single Excel workbook with a summary dashboard and one tab per violated audit rule, each containing actionable recommendations in Portuguese. The file is designed to be opened by a health department coordinator with no technical background.

**In one sentence:** CnesData tells the municipality "here are the professionals who shouldn't be where they are, aren't where they should be, or don't exist in the system they're supposed to be in."

### Current Operational Mode vs. Target Architecture

| Aspect | Current (CLI) | Target (API Engine) |
|---|---|---|
| Local data source | Direct Firebird connection | Parquet sent via HTTP by dump agent |
| Trigger | Manual / Windows Task Scheduler | API call from dump agent or scheduler |
| Multi-municipality | Config change per run | Multiple dump agents, one engine |
| `--source` flag | `LOCAL` (default), `NACIONAL`, `AMBOS` | Same — source is declared, never inferred |

---

## 2. Why This Project Exists

### The Problem

Brazil's public health system (SUS) requires every municipality to maintain accurate records of health professionals in the CNES system. These records determine:

- **Federal funding allocation** — funding is tied to registered professionals and teams.
- **Workforce planning** — which health units are understaffed, which have ghost staff.
- **Legal compliance** — professionals must be registered before providing SUS-funded care.

In practice, the local CNES database (Firebird, maintained by the municipality) and the national CNES database (DATASUS, published via BigQuery) frequently **diverge**. Professionals appear in one system but not the other. CBO codes (job classifications) don't match. Workload hours differ. Establishments exist locally but were never sent to the national database.

These inconsistencies have real consequences:

- **Ghost Payroll** — professionals registered in CNES but absent from the payroll system may indicate diversion of public funds ("profissional fantasma").
- **Missing Registration** — professionals working and receiving salary but missing from CNES means the municipality can't claim federal funding for their work.
- **Allocation Errors** — community health agents (ACS) must be linked to specific unit types. Wrong assignments violate federal guidelines and risk audit findings from oversight bodies (TCE-SP, Controladoria).

Before CnesData, this reconciliation was done **manually** — someone would open the Firebird database, export to Excel, and visually compare against printouts from the national system. This process took days, was error-prone, and happened at best once per quarter.

### The Solution

CnesData automates this entire process into a single command:

```powershell
python src\main.py -c 2024-12 -v
```

Or, with the scheduled task, it runs unattended on the 15th of every month.

---

## 3. Architecture — How It Works

### Data Flow

```
[ Dump Agent ]                      BigQuery (national)
  (runs at municipality)                    │
  Firebird CNES.GDB                         │
        │                                   │
        │ parquet → HTTP POST               │
        ▼                                   ▼
  CnesLocalAdapter                 CnesNacionalAdapter
        │                                   │
        └──────────────┬────────────────────┘
                       ▼
           Schema Padronizado (schemas.py)
                       │
                       ├──► transformer.py (CPF validation, zero-hours flag)
                       │
                       ▼
               PostgreSQL (upsert por competência)
```

**Current state:** `IngestaoLocalStage` connects to Firebird directly. In production, dump agents will POST parquet to the API endpoint.

**Audit rules:** removed from the pipeline — applied by a separate service via SQL JOINs on the PostgreSQL tables.

### Key Design Decisions

**Deterministic source selection (`target_source`)** — The data source is declared at invocation time (`--source LOCAL|NACIONAL|AMBOS`), never inferred from availability. If `LOCAL` is requested but no parquet exists, `StageSkipError` is raised — no silent fallback to national data. This preserves audit provenance: `FONTE=LOCAL` and `FONTE=NACIONAL` data must never be silently mixed.

**Repository/Protocol Pattern (PEP 544)** — The analysis layer never sees Firebird column names or BigQuery column names. Both adapters translate to a canonical schema (`schemas.py`). Adding a third data source (e.g., dump agent JSON, e-SUS) requires only a new adapter — zero changes to business rules.

**CNS as cross-check key, not CPF** — The national BigQuery database does not expose CPF (privacy). The Cartão Nacional de Saúde (CNS, 15 digits) is the only reliable identifier available in both sources for professional-level reconciliation. This was discovered empirically, not assumed upfront.

**WIN1252 charset + NFKD normalization** — Firebird CNES.GDB is encoded in WIN1252. The `fdb.connect()` call explicitly passes `charset="WIN1252"`. String columns with accented names (e.g., "Atenção Básica") are NFKD-normalized in `cnes_local_adapter.py` before any merge operations to prevent false non-matches against national data.

**Three Firebird queries instead of one** — The original single query with LEFT JOINs to team tables (LFCES048 → LFCES060) produced all NULLs due to a Firebird 2.5 embedded engine bug with mismatched join keys. The solution splits extraction into three queries (professionals, team members, teams) and merges in Python via a 4-character prefix match on `SEQ_EQUIPE`. Documented in `cnes_client.py` and `data_dictionary.md`.

**Competência lag** — DATASUS publishes national data ~2 months delayed. The pipeline accounts for this: CLI auto-detects competência as `current month - 2`, configurable via PowerShell automation.

### Module Inventory

| Module | Purpose | Tests |
|---|---|---|
| `cli.py` | argparse CLI (`--source`, `-c`, `-o`, `-v`) | ~15 |
| `config.py` | .env reader, centralized configuration | 10 |
| `main.py` | Pipeline entry point | 16+ |
| `pipeline/orchestrator.py` | PipelineOrchestrator, StageSkipError, StageFatalError | — |
| `pipeline/state.py` | PipelineState (target_source, DataFrames) | 8 |
| `pipeline/stages/ingestao_local.py` | Parquet/Firebird → state | — |
| `pipeline/stages/ingestao_nacional.py` | BigQuery → state (circuit breaker) | — |
| `pipeline/stages/processamento.py` | Cleaning, dedup | — |
| `pipeline/stages/exportacao.py` | PostgreSQL persistence, status derivation | 15 |
| `ingestion/base.py` | PEP 544 Protocol definitions | 5 |
| `ingestion/schemas.py` | Canonical column names (source of truth) | — |
| `ingestion/cnes_client.py` | Firebird extraction (WIN1252, 3-query) | 15 |
| `ingestion/cnes_local_adapter.py` | Firebird → canonical schema (NFKD) | 29 |
| `ingestion/cnes_nacional_adapter.py` | BigQuery → canonical schema | 14 |
| `ingestion/web_client.py` | BigQuery client via basedosdados | 17 |
| `ingestion/hr_client.py` | HR spreadsheet parser (.xlsx/.csv) | 21 |
| `processing/transformer.py` | Cleaning, RQ-002, RQ-003 | ~20 |
| `storage/postgres_adapter.py` | PostgreSQL upsert by competência | — |

**Total: 319+ unit tests passing.** Integration tests require live Firebird.

---

## 4. The 11 Audit Rules

### Local Rules (Firebird only)

| Rule | What It Detects | Key | Action |
|---|---|---|---|
| RQ-002 | Invalid CPF (null, wrong length) | CPF | Exclude from analysis + log |
| RQ-003 | Zero workload hours ("zombie link") | CH_TOTAL | Flag as ATIVO_SEM_CH |
| RQ-003-B | Professional linked to 2+ health units | CPF × CNES | Review with HR if structural |
| RQ-005 ACS/TACS | Community health agent in wrong unit type | CBO × TIPO_UNIDADE | Transfer to correct unit |
| RQ-005 ACE/TACE | Endemic control agent in wrong unit type | CBO × TIPO_UNIDADE | Transfer to correct unit |

### Cross-Check Rules (Local × National)

| Rule | What It Detects | Key | Severity |
|---|---|---|---|
| RQ-006 | Establishment in local but not in national | CNES | ALTA |
| RQ-007 | Establishment in national but not in local | CNES | ALTA | Excludes tipo 22 (private clinics, other maintainers) |
| RQ-008 | Professional in local but not in national | CNS | CRÍTICA |
| RQ-009 | Professional in national but not in local | CNS | ALTA | Cascade-filters professionals from RQ-007 missing establishments |
| RQ-010 | Same professional+establishment, different CBO | CNS+CNES | MÉDIA | Includes DESCRICAO_CBO_LOCAL/NACIONAL columns |
| RQ-011 | Same professional+establishment, workload delta > 0h | CNS+CNES | BAIXA |

### HR Cross-Check Rules (Local × Payroll — suspended, requires `hr_padronizado.csv`)

| Rule | What It Detects | Key |
|---|---|---|
| Ghost Payroll | Active in CNES, absent/inactive in payroll | CPF | CRÍTICA |
| Missing Registration | Active in payroll, absent in CNES | CPF | ALTA |

---

## 5. The Data Sources — What We Know

### Firebird Local (CNES.GDB)

- **Engine:** Firebird 2.5 embedded (accessed via `fdb` + `fbembed.dll`)
- **Key tables:** LFCES018 (professionals), LFCES004 (establishments), LFCES021 (links), LFCES048 (team members), LFCES060 (teams), NFCES026 (CBO domain — job title lookup)
- **Municipality filter:** `CODMUNGEST = '354130'` AND `CNPJ_MANT = '55293427000117'`
- **Current data:** ~357 professional-establishment links, ~330 unique professionals, ~20 establishments
- **Quirks:** No declared foreign keys between LFCES048 and LFCES060. LEFT JOIN via fdb's `pd.read_sql()` fails with error -501. TRIM() unavailable in embedded engine. `CD_SEGMENT`/`DS_SEGMENT` columns return error -206 via alias in nested LEFT JOIN.

### BigQuery National (basedosdados)

- **Access:** `basedosdados.read_sql()` with OAuth browser flow on first run
- **Project:** `basedosdados` (public dataset), billing via `bd-prof-cnes`
- **Tables:** `br_ms_cnes.profissional`, `br_ms_cnes.estabelecimento`, `br_ms_cnes.equipe`
- **Municipality filter:** `id_municipio = '3541307'` (7-digit IBGE code with check digit)
- **Partition columns:** `ano`, `mes` — MUST be in every WHERE clause (quota: 1TB/month free)
- **Current data:** ~808 professional links, ~85 establishments (competência 2024-12)
- **Key limitation:** No CPF field. CNS (`cartao_nacional_saude`) is the only cross-check key.
- **Schema gotchas:** Column is `cbo_2002` (not `id_cbo`), `indicador_atende_sus` (not `indicador_sus`), integer 1/0 (not string S/N). All confirmed empirically against real schema.

### HR Spreadsheet (not yet available)

- **Parser ready:** `hr_client.py` accepts `.xlsx` or `.csv` with columns `CPF`, `NOME`, `STATUS`
- **STATUS values:** `ATIVO`, `INATIVO`, `AFASTADO`
- **Integration:** When `FOLHA_HR_PATH` is set in `.env`, Ghost Payroll and Missing Registration rules activate automatically
- **Current state:** Direct HR integration (WP-003/WP-004) suspended. The system now requires a pre-processed `hr_padronizado.csv` generated by `scripts/hr_pre_processor.py` (Epic 3), which uses PIS-based cross-walking against LFCES018 to discover CPFs from raw HR spreadsheets.

---

## 6. Where It's Headed (Near-Term Roadmap)

These are concrete next steps, ordered by value:

| Priority | Task | Status | Why |
|---|---|---|---|
| 1 | Data validation + 5 defect fixes | ✅ Done | CPF/CNES zero-padding, RQ-007/009 cascade false positives, COVEPE type 50 |
| 2 | CBO enrichment (human-readable job titles) | ✅ Done | DESCRICAO_CBO column in all reports via NFCES026 |
| 3 | "Double-Check" Nacional (cascade_resolver) | Removed | Audit layer removed 2026-04 — rules applied by separate service via PostgreSQL JOINs |
| 4 | DuckDB Medallion POC (Gold layer) | ✅ Done (POC) | Analytic persistence: evolucao_metricas_mensais + auditoria_resultados |
| 5 | HR Pre-processor (PIS→CPF crosswalk) | ✅ Done | scripts/hr_pre_processor.py via LFCES018 — 61% coverage (240/395) |
| 6 | Evolution dashboard in Excel | Not started | Trend tab comparing snapshots month-over-month (needs 2+ runs) |

---

## 7. Where It Is Going (Architecture Direction)

The engine is transitioning from a local CLI pipeline to a multi-municipality API reconciliation service. The architectural decisions made in 2026-04 (deterministic `target_source`, `StageSkipError` instead of silent fallback, proveniência imutável) are pre-requisites for this transition.

### Active Direction: Dump Agent + API Model

Each municipality runs a lightweight **dump agent** that:
1. Connects to the local Firebird (`CNES.GDB`) with `charset=WIN1252`
2. Extracts and serializes to parquet (`firebird_dump_YYYY-MM.parquet`)
3. POSTs to the CnesData API endpoint
4. Is stateless — no local state beyond the parquet file

The API receives the parquet, validates via contracts, and persists in PostgreSQL with `(municipio, competencia)` as composite key. Audit rules are applied by a separate service via SQL JOINs.

This means **no Firebird access from the server** — the engine never reaches into a remote database. Data arrives as structured artifacts.

### Remaining Possibilities

### 7.1 — Team-Level Audit

The pipeline currently audits professionals and establishments. The next logical entity is **equipes de saúde** (health teams — ESF, EAP, ESB). Rules would include:

- Team exists in local but not in national (analogous to RQ-006/007)
- Team composition doesn't match minimum requirements (e.g., ESF must have at least 1 doctor, 1 nurse, 1 ACS per area)
- INE (team identifier) format mismatch between Firebird (10 chars) and BigQuery (18 chars) — requires prefix matching analysis

**Blocked by:** INE format incompatibility. The `data_dictionary.md` documents this gap. Needs investigation of how `id_equipe` in BigQuery maps to `INE` in Firebird.

### 7.2 — Multi-Municipality

The architecture already supports this — the `config.py` takes `COD_MUN_IBGE` and `ID_MUNICIPIO_IBGE7` as parameters. Running for a different municipality means:

1. Access to their Firebird CNES.GDB file
2. Changing two environment variables
3. Running the same pipeline

A regional health department (DRS) could run this for all municipalities in their jurisdiction. The limiting factor is **access to Firebird files** — each municipality maintains their own.

### 7.3 — Web Dashboard

Replace the Excel output with a web interface (Streamlit, Metabase, or custom). Would enable:

- Interactive filtering by establishment, CBO, team
- Drill-down from summary to individual records
- Historical trend visualization (via PostgreSQL queries)
- Multi-user access without distributing Excel files

**Complexity:** High. The Excel report works well for the current audience (1-3 people). A dashboard makes sense when the audience grows or when managers want self-service analysis.

### 7.4 — Automated DATASUS Submission Check

After running the pipeline, automatically check whether the municipality's latest data has been successfully sent to DATASUS by comparing the local competência against the latest available in BigQuery. If the national data is stale (e.g., local is 2026-03 but national is 2025-11), alert that submissions may be failing.

**Complexity:** Low (just compare competência dates). High operational value.

> **Nota:** Epic 1 (CnesOficialWebAdapter) implementa acesso direto à API DATASUS para validação de estabelecimentos. Este adapter pode ser estendido para comparação de competência (detecção de envios faltantes).

### 7.5 — Integration with e-SUS / SISAB

e-SUS (Sistema Único de Saúde) and SISAB (Sistema de Informação em Saúde para a Atenção Básica) contain production data — actual patient visits, procedures performed. Cross-referencing CNES registration data against e-SUS production data would answer: "this professional is registered for 40h/week at this unit — do they actually produce 40h of patient care?" This is the deepest level of audit and the hardest to game.

**Blocked by:** e-SUS data access (usually via API or local database, varies by municipality). Significant scope expansion.

---

## 8. Technical Environment

| Component | Value |
|---|---|
| Language | Python 3.11+ |
| OS | Windows (development and production) |
| Firebird | 2.5 embedded via `fdb` + `fbembed.dll` (64-bit) |
| BigQuery | Via `basedosdados` package (OAuth browser flow) |
| GCP Project | `bd-prof-cnes` |
| IDE | VS Code |
| AI Tooling | Claude Code (CLI) for development, Claude (web) for prompt design |
| Test Framework | pytest (345 unit tests, integration tests with live Firebird) |
| Automation | PowerShell (scripts/Run-CnesAudit.ps1 + Windows Task Scheduler via Schedule-CnesAudit.ps1) |
| Excel Engine | openpyxl |
| Task Scheduler | Windows Task Scheduler via PowerShell script |
| Version Control | Git (local, not yet on remote) |

### Key Configuration (.env)

```ini
DB_HOST=localhost
DB_PATH=C:\Datasus\CNES\CNES.GDB
DB_USER=SYSDBA
DB_PASSWORD=masterkey
FIREBIRD_DLL=C:\...\fb_64\fbembed.dll
COD_MUN_IBGE=354130              # Firebird (6 digits)
ID_MUNICIPIO_IBGE7=3541307       # BigQuery (7 digits)
CNPJ_MANTENEDORA=55293427000117
GCP_PROJECT_ID=bd-prof-cnes
COMPETENCIA_ANO=2024
COMPETENCIA_MES=12
```

---

## 9. What a New Session Needs to Know

If starting a new Claude Code or Claude session for this project:

1. **Entry point:** `src/main.py` — everything flows from here.
2. **Source of truth for columns:** `src/ingestion/schemas.py` — never reference raw Firebird or BigQuery column names in analysis code.
3. **Source of truth for rules:** `data_dictionary.md` — every CBO, TIPO_UNIDADE, and rule definition is documented here.
4. **Do not recreate:** `cnes_exporter.py` is deleted. The architecture is layered (ingestion → processing → analysis → export). No monolithic pipeline.
5. **Test command:** `pytest tests/ -m "not integration" -v` — all 271+ tests should pass without Firebird or BigQuery.
6. **CLI:** `python src/main.py --help` shows all options.
7. **The Firebird LEFT JOIN bug is real** — do not try to simplify `cnes_client.py` back to a single query. It will silently return NULLs for all team data. The 3-query + Python merge approach is intentional and documented.
8. **BigQuery column names are confirmed empirically** — the data_dictionary.md notes which columns were wrong in earlier iterations (e.g., `id_cbo` doesn't exist, `indicador_sus` doesn't exist). Trust the confirmed schema, not guesses.
9. **CLI:** `python src/main.py --help` — pipeline accepts `-c YYYY-MM`, `--source {LOCAL,NACIONAL,AMBOS}` (default `LOCAL`), `-o OUTPUT_DIR`, `-v`/`--verbose`. `--skip-nacional` was removed in 2026-04 — use `--source LOCAL`.
10. **CBO lookup:** `extrair_lookup_cbo(con)` returns `dict[str, str]` CBO→description from NFCES026. Passed as optional parameter to `transformar()` and `detectar_divergencia_cbo()`.
11. **Zero-padding is intentional:** CPF gets `zfill(11)` and CNES gets `zfill(7)`. Firebird omits leading zeros. Do not remove these zfills.
12. **RQ-009 cascade filter:** professionals from establishments already flagged by RQ-007 are excluded. Without this, 87% of RQ-009 results are false positives. See `cnes_excluir` parameter in `detectar_profissionais_ausentes_local()`.
