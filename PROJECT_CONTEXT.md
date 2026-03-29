# CnesData — Project Context

> Living document. Last updated: 2026-03-27.
> Audience: the developer (Vinícius), colleagues who will run the pipeline,
> and any future AI session that needs to understand this project deeply.

---

## 1. What This Project Is

CnesData is a **data reconciliation and audit pipeline** for public health workforce data in the municipality of Presidente Epitácio, São Paulo, Brazil (IBGE code 354130, population ~42,000).

It extracts professional-establishment links from a local Firebird database (CNES — Cadastro Nacional de Estabelecimentos de Saúde), cross-references them against the national CNES database via BigQuery, and produces structured audit reports identifying inconsistencies, ghost registrations, and allocation errors.

The output is a single Excel workbook with a summary dashboard and one tab per violated audit rule, each containing actionable recommendations in Portuguese. The file is designed to be opened by a health department coordinator with no technical background.

**In one sentence:** CnesData tells the municipality "here are the professionals who shouldn't be where they are, aren't where they should be, or don't exist in the system they're supposed to be in."

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
Firebird CNES.GDB (local)          BigQuery (national)
        │                                  │
        ▼                                  ▼
  CnesLocalAdapter                 CnesNacionalAdapter
        │                                  │
        ▼                                  ▼
    Schema Padronizado (schemas.py — canonical column names)
        │
        ├──► transformer.py (cleaning: RQ-002 CPF validation, RQ-003 zero-hours flag)
        │
        ▼
    rules_engine.py (11 audit rules)
        │
        ├──► csv_exporter.py (individual CSVs per rule)
        ├──► report_generator.py (consolidated Excel with RESUMO tab)
        └──► evolution_tracker.py (JSON snapshots for trend analysis)
```

### Key Design Decisions

**Repository/Protocol Pattern (PEP 544)** — The analysis layer never sees Firebird column names or BigQuery column names. Both adapters translate to a canonical schema (`schemas.py`). This means adding a third data source (e.g., e-SUS, SCNES web) requires only a new adapter — zero changes to business rules.

**CNS as cross-check key, not CPF** — The national BigQuery database does not expose CPF (privacy). The Cartão Nacional de Saúde (CNS, 15 digits) is the only reliable identifier available in both sources for professional-level reconciliation. This was discovered empirically during development, not assumed upfront.

**Three Firebird queries instead of one** — The original single query with LEFT JOINs to team tables (LFCES048 → LFCES060) produced all NULLs due to a Firebird 2.5 embedded engine bug with mismatched join keys between tables. The solution splits extraction into three queries (professionals, team members, teams) and merges in Python via a 4-character prefix match on `SEQ_EQUIPE`. This is documented in `cnes_client.py` and `data_dictionary.md`.

**Competência lag** — DATASUS publishes national data with approximately 2 months delay. The pipeline accounts for this: the CLI auto-detects the correct competência as `current month - 2`, and the PowerShell automation script makes this configurable.

### Module Inventory

| Module | Purpose | Tests |
|---|---|---|
| `cli.py` | argparse CLI interface | ~15 |
| `config.py` | .env reader, centralized configuration | 10 |
| `main.py` | Pipeline orchestrator | 16+ |
| `ingestion/base.py` | PEP 544 Protocol definitions | 5 |
| `ingestion/schemas.py` | Canonical column names (source of truth) | — |
| `ingestion/cnes_client.py` | Firebird extraction (3-query strategy) | 11 |
| `ingestion/cnes_local_adapter.py` | Firebird → canonical schema | 25 |
| `ingestion/cnes_nacional_adapter.py` | BigQuery → canonical schema | 14 |
| `ingestion/web_client.py` | BigQuery client via basedosdados | 17 |
| `ingestion/hr_client.py` | HR spreadsheet parser (.xlsx/.csv) | 21 |
| `processing/transformer.py` | Cleaning, RQ-002, RQ-003 | ~20 |
| `analysis/rules_engine.py` | 11 audit rules | 24 (cross-check) + 30+ (local) |
| `analysis/evolution_tracker.py` | JSON snapshots + trend deltas | 33 |
| `export/csv_exporter.py` | CSV output (Brazilian format) | — |
| `export/report_generator.py` | Excel workbook with RESUMO + tabs | 25+ |

**Total: 345 unit tests**, all passing. Integration tests require live Firebird.

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
| 3 | "Double-Check" Nacional (cascade_resolver) | ✅ Done | API DATASUS cascade validation — eliminates RQ-006 false positives from publication lag |
| 4 | DuckDB Medallion POC (Gold layer) | ✅ Done (POC) | Analytic persistence: evolucao_metricas_mensais + auditoria_resultados |
| 5 | HR Pre-processor (PIS→CPF crosswalk) | ✅ Done | scripts/hr_pre_processor.py via LFCES018 — 61% coverage (240/395) |
| 6 | Evolution dashboard in Excel | Not started | Trend tab comparing snapshots month-over-month (needs 2+ runs) |

---

## 7. Where It Might Evolve (Long-Term Vision)

These are possibilities, not commitments. Each would be a significant expansion.

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
- Historical trend visualization (leverage existing JSON snapshots)
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
9. **CLI:** `python src/main.py --help` — pipeline accepts `-c YYYY-MM`, `--skip-nacional`, `--skip-hr`, `-o OUTPUT_DIR`, `-v`/`--verbose`.
10. **CBO lookup:** `extrair_lookup_cbo(con)` returns `dict[str, str]` CBO→description from NFCES026. Passed as optional parameter to `transformar()` and `detectar_divergencia_cbo()`.
11. **Zero-padding is intentional:** CPF gets `zfill(11)` and CNES gets `zfill(7)`. Firebird omits leading zeros. Do not remove these zfills.
12. **RQ-009 cascade filter:** professionals from establishments already flagged by RQ-007 are excluded. Without this, 87% of RQ-009 results are false positives. See `cnes_excluir` parameter in `detectar_profissionais_ausentes_local()`.
