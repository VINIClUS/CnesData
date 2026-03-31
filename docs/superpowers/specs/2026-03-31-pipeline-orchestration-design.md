# Pipeline Orchestration — Design Spec

**Date:** 2026-03-31
**Status:** Approved
**Scope:** Streamlit dashboard trigger for `src/main.py` with live log streaming

---

## Problem

The DuckDB historical store is empty until someone manually runs `python src/main.py`.
There is no mechanism to trigger, monitor, or stop a pipeline run from the dashboard.

---

## Solution Overview

A dedicated Streamlit page (`scripts/pages/pipeline.py`) with:
- A button **"Executar pipeline agora"**
- An **"Opções avançadas"** expander (competência override + Nacional/RH toggles)
- Live log streaming via `@st.fragment(run_every=1)` reading from a background queue
- A **"Parar execução"** stop button inside the fragment

A pure-Python module `scripts/pipeline_runner.py` handles all subprocess mechanics
with no Streamlit dependency — keeping it independently testable.

---

## Architecture

```
scripts/
  dashboard.py              (existing — Visão Geral, unchanged)
  dashboard_status.py       (existing, unchanged)
  pipeline_runner.py        (NEW — subprocess + reader thread, no st imports)
  pages/
    pipeline.py             (NEW — Streamlit page)
    tendencias.py           (existing, unchanged)

tests/
  scripts/
    test_pipeline_runner.py (NEW)
```

---

## Component: `pipeline_runner.py`

No Streamlit imports. Two public functions:

```python
def iniciar_pipeline(
    competencia: str,       # "YYYY-MM"
    skip_nacional: bool,
    skip_hr: bool,
) -> subprocess.Popen:
    """Spawns `python src/main.py` with the given flags. stdout+stderr merged."""

def iniciar_leitor(proc: subprocess.Popen) -> queue.Queue:
    """Starts a daemon thread that reads proc.stdout line-by-line into a Queue."""
```

Command built: `python src/main.py --competencia <ano> <mes> [--skip-nacional] [--skip-hr]`

The module also exposes:

```python
def competencia_atual() -> tuple[int, int]:
    """Returns (ano, mes) for today's date."""
```

---

## Component: `scripts/pages/pipeline.py`

### Session State

| Key | Type | Initial | Purpose |
|---|---|---|---|
| `pipeline_proc` | `Popen \| None` | `None` | Running process handle |
| `pipeline_log_queue` | `Queue \| None` | `None` | Line buffer from reader thread |
| `pipeline_logs` | `list[str]` | `[]` | Accumulated display log |
| `pipeline_status` | `str` | `"idle"` | Controls UI state |

Valid statuses: `"idle"`, `"running"`, `"done"`, `"error"`

### Layout

```
st.title("Executar Pipeline")

expander("Opções avançadas"):
  col1: number_input "Ano"   default=ano_atual
  col2: number_input "Mês"   default=mes_atual  (1–12)
  toggle "Nacional"          default=True
  toggle "RH"                default=True if FOLHA_HR_PATH else False (disabled)

button "Executar pipeline agora"   disabled=(status=="running")
  → on click: iniciar_pipeline() + iniciar_leitor()
              set status="running", st.rerun()

@st.fragment(run_every=1):   active only while status=="running"
  drain ≤50 lines from queue → append to pipeline_logs
  expander("Logs"):
    st.code("\n".join(pipeline_logs), language=None)
  button "Parar execução":
    → proc.terminate()
    → status="error"
  if proc.poll() is not None:
    returncode==0  → status="done"
    returncode!=0  → status="error"
    → st.rerun()  (refresh main page / sidebar competência list)

status banner:
  "done"    → st.success("Pipeline concluído.")
  "error"   → st.error("Pipeline encerrado com erro ou interrompido. Verifique os logs.")
  "running" → st.info("Pipeline em execução…")
```

---

## Data Flow

```
[Button click]
  iniciar_pipeline("2026-03", skip_nacional=False, skip_hr=True)
    → subprocess.Popen(["python", "src/main.py", "--competencia", "2026", "3", "--skip-hr"],
                       stdout=PIPE, stderr=STDOUT, text=True)
  iniciar_leitor(proc)
    → Thread(target=_ler_stdout, args=(proc, queue), daemon=True).start()
  session_state: proc=Popen, queue=Queue, logs=[], status="running"
  st.rerun()

[Every 1 second — @st.fragment]
  for _ in range(50):
    try: line = queue.get_nowait(); pipeline_logs.append(line)
    except Empty: break
  render st.code(joined logs)
  if proc.poll() is not None:
    status = "done" if returncode==0 else "error"
    st.rerun()

[Stop button]
  proc.terminate()     # SIGTERM / TerminateProcess on Windows
  status = "error"     # fragment detects poll() on next tick
```

---

## Error Handling

| Failure | Behaviour |
|---|---|
| `.env` missing / Firebird unreachable | returncode ≠ 0 → status="error"; full stderr visible in log area |
| User closes browser tab mid-run | Session ends; daemon thread exits; OS process continues to completion silently |
| Stop button pressed | `proc.terminate()`, status="error" |
| Pipeline already running | "Executar" button disabled (`disabled=True`), double-spawn impossible |
| `FOLHA_HR_PATH` not configured | RH toggle rendered as disabled+off; `--skip-hr` always passed |

---

## Testing: `tests/scripts/test_pipeline_runner.py`

All tests mock `subprocess.Popen` — no real pipeline invoked.

| Test | Assertion |
|---|---|
| `test_iniciar_pipeline_comando_correto` | With `skip_nacional=True, skip_hr=True`: args include `--skip-nacional --skip-hr` |
| `test_iniciar_pipeline_sem_flags` | With both False: no `--skip-*` in args |
| `test_iniciar_leitor_captura_stdout` | Lines written to proc stdout appear in the returned Queue |
| `test_iniciar_leitor_encerra_com_processo` | Thread exits after proc stdout closes (EOF) |
| `test_competencia_atual_retorna_mes_corrente` | Returns `(date.today().year, date.today().month)` |

---

## Out of Scope

- Windows Task Scheduler / cron automation (not requested)
- Multi-pipeline concurrency (one run at a time, enforced by disabled button)
- Persisting logs to disk (pipeline already writes to `logs/cnes_exporter.log`)
- Progress bar / stage-level progress (pipeline logs already emit stage markers)
