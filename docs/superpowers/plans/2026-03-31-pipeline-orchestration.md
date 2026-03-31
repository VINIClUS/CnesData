# Pipeline Orchestration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Streamlit page that spawns `src/main.py` as a subprocess, streams its logs live, and lets the user stop the run.

**Architecture:** A pure-Python `scripts/pipeline_runner.py` owns subprocess mechanics (no Streamlit dependency). A Streamlit page `scripts/pages/3_Pipeline.py` imports it, manages `session_state`, and uses `@st.fragment(run_every=1)` to poll a `queue.Queue` filled by a background daemon thread.

**Tech Stack:** Python `subprocess`, `threading`, `queue` (stdlib); Streamlit ≥ 1.33 (`@st.fragment`); `pytest` + `unittest.mock`.

---

## File Map

| Path | Action | Responsibility |
|---|---|---|
| `scripts/pipeline_runner.py` | **Create** | `competencia_atual`, `iniciar_pipeline`, `iniciar_leitor` — no Streamlit |
| `scripts/pages/3_Pipeline.py` | **Create** | Streamlit page — UI, session_state, fragment |
| `tests/scripts/test_pipeline_runner.py` | **Create** | Unit tests for `pipeline_runner` (mock Popen) |

---

## Task 1: `competencia_atual()` — returns current (ano, mes)

**Files:**
- Create: `scripts/pipeline_runner.py`
- Create: `tests/scripts/test_pipeline_runner.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/scripts/test_pipeline_runner.py
from datetime import date
from unittest.mock import patch

from pipeline_runner import competencia_atual


class TestCompetenciaAtual:
    def test_retorna_ano_e_mes_de_hoje(self):
        hoje = date(2026, 3, 31)
        with patch("pipeline_runner.date") as mock_date:
            mock_date.today.return_value = hoje
            ano, mes = competencia_atual()
        assert ano == 2026
        assert mes == 3
```

- [ ] **Step 2: Run test — confirm FAIL**

```bash
./venv/Scripts/python.exe -m pytest tests/scripts/test_pipeline_runner.py::TestCompetenciaAtual -v
```

Expected: `ModuleNotFoundError` or `ImportError` (file does not exist yet).

- [ ] **Step 3: Create `scripts/pipeline_runner.py` with minimal implementation**

```python
"""pipeline_runner — spawns main.py subprocess and streams its output."""
import queue
import subprocess
import sys
import threading
from datetime import date
from pathlib import Path

_SRC = Path(__file__).parent.parent / "src"
_MAIN = _SRC / "main.py"


def competencia_atual() -> tuple[int, int]:
    hoje = date.today()
    return hoje.year, hoje.month
```

- [ ] **Step 4: Run test — confirm PASS**

```bash
./venv/Scripts/python.exe -m pytest tests/scripts/test_pipeline_runner.py::TestCompetenciaAtual -v
```

Expected: `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add scripts/pipeline_runner.py tests/scripts/test_pipeline_runner.py
git commit -m "feat(pipeline_runner): competencia_atual"
```

---

## Task 2: `iniciar_pipeline()` — spawns subprocess with correct CLI flags

**Files:**
- Modify: `scripts/pipeline_runner.py`
- Modify: `tests/scripts/test_pipeline_runner.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/scripts/test_pipeline_runner.py  — add inside file (keep existing imports/class)
import subprocess
import sys
from unittest.mock import MagicMock, patch

from pipeline_runner import iniciar_pipeline


class TestIniciarPipeline:
    def _mock_popen(self, mock_cls, returncode=None):
        proc = MagicMock(spec=subprocess.Popen)
        proc.returncode = returncode
        mock_cls.return_value = proc
        return proc

    @patch("pipeline_runner.subprocess.Popen")
    def test_sem_flags(self, mock_popen):
        self._mock_popen(mock_popen)
        iniciar_pipeline("2026-03", skip_nacional=False, skip_hr=False)
        args = mock_popen.call_args[0][0]
        assert "-c" in args
        assert "2026-03" in args
        assert "--skip-nacional" not in args
        assert "--skip-hr" not in args

    @patch("pipeline_runner.subprocess.Popen")
    def test_skip_nacional(self, mock_popen):
        self._mock_popen(mock_popen)
        iniciar_pipeline("2026-03", skip_nacional=True, skip_hr=False)
        args = mock_popen.call_args[0][0]
        assert "--skip-nacional" in args
        assert "--skip-hr" not in args

    @patch("pipeline_runner.subprocess.Popen")
    def test_skip_hr(self, mock_popen):
        self._mock_popen(mock_popen)
        iniciar_pipeline("2026-03", skip_nacional=False, skip_hr=True)
        args = mock_popen.call_args[0][0]
        assert "--skip-hr" in args
        assert "--skip-nacional" not in args

    @patch("pipeline_runner.subprocess.Popen")
    def test_ambos_skip(self, mock_popen):
        self._mock_popen(mock_popen)
        iniciar_pipeline("2026-03", skip_nacional=True, skip_hr=True)
        args = mock_popen.call_args[0][0]
        assert "--skip-nacional" in args
        assert "--skip-hr" in args

    @patch("pipeline_runner.subprocess.Popen")
    def test_usa_sys_executable(self, mock_popen):
        self._mock_popen(mock_popen)
        iniciar_pipeline("2026-03", skip_nacional=False, skip_hr=False)
        args = mock_popen.call_args[0][0]
        assert args[0] == sys.executable

    @patch("pipeline_runner.subprocess.Popen")
    def test_stdout_pipe_stderr_stdout(self, mock_popen):
        self._mock_popen(mock_popen)
        iniciar_pipeline("2026-03", skip_nacional=False, skip_hr=False)
        kwargs = mock_popen.call_args[1]
        assert kwargs["stdout"] == subprocess.PIPE
        assert kwargs["stderr"] == subprocess.STDOUT
        assert kwargs["text"] is True
```

- [ ] **Step 2: Run tests — confirm FAIL**

```bash
./venv/Scripts/python.exe -m pytest tests/scripts/test_pipeline_runner.py::TestIniciarPipeline -v
```

Expected: `ImportError: cannot import name 'iniciar_pipeline'`.

- [ ] **Step 3: Implement `iniciar_pipeline` in `scripts/pipeline_runner.py`**

```python
def iniciar_pipeline(
    competencia: str,
    skip_nacional: bool,
    skip_hr: bool,
) -> subprocess.Popen:
    cmd = [sys.executable, str(_MAIN), "-c", competencia]
    if skip_nacional:
        cmd.append("--skip-nacional")
    if skip_hr:
        cmd.append("--skip-hr")
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
```

- [ ] **Step 4: Run tests — confirm PASS**

```bash
./venv/Scripts/python.exe -m pytest tests/scripts/test_pipeline_runner.py::TestIniciarPipeline -v
```

Expected: all 6 `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add scripts/pipeline_runner.py tests/scripts/test_pipeline_runner.py
git commit -m "feat(pipeline_runner): iniciar_pipeline com flags CLI"
```

---

## Task 3: `iniciar_leitor()` — daemon thread enqueues stdout lines

**Files:**
- Modify: `scripts/pipeline_runner.py`
- Modify: `tests/scripts/test_pipeline_runner.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/scripts/test_pipeline_runner.py — add imports at top and new class
import io
import queue
import threading
import time

from pipeline_runner import iniciar_leitor


class TestIniciarLeitor:
    def _proc_com_saida(self, linhas: list[str]) -> MagicMock:
        texto = "\n".join(linhas) + "\n"
        proc = MagicMock(spec=subprocess.Popen)
        proc.stdout = io.StringIO(texto)
        return proc

    def test_enfileira_linhas_do_stdout(self):
        proc = self._proc_com_saida(["linha um", "linha dois", "linha três"])
        q = iniciar_leitor(proc)
        time.sleep(0.2)
        linhas = []
        while not q.empty():
            linhas.append(q.get_nowait())
        assert "linha um\n" in linhas
        assert "linha dois\n" in linhas
        assert "linha três\n" in linhas

    def test_thread_e_daemon(self):
        proc = self._proc_com_saida([])
        iniciar_leitor(proc)
        threads_daemon = [t for t in threading.enumerate() if t.daemon and "leitor" in t.name]
        assert len(threads_daemon) >= 1

    def test_retorna_queue(self):
        proc = self._proc_com_saida([])
        resultado = iniciar_leitor(proc)
        assert isinstance(resultado, queue.Queue)
```

- [ ] **Step 2: Run tests — confirm FAIL**

```bash
./venv/Scripts/python.exe -m pytest tests/scripts/test_pipeline_runner.py::TestIniciarLeitor -v
```

Expected: `ImportError: cannot import name 'iniciar_leitor'`.

- [ ] **Step 3: Implement `iniciar_leitor` in `scripts/pipeline_runner.py`**

```python
def iniciar_leitor(proc: subprocess.Popen) -> queue.Queue:
    q: queue.Queue = queue.Queue()

    def _ler() -> None:
        for linha in proc.stdout:
            q.put(linha)

    t = threading.Thread(target=_ler, name="pipeline-leitor", daemon=True)
    t.start()
    return q
```

- [ ] **Step 4: Run full test module — confirm all PASS**

```bash
./venv/Scripts/python.exe -m pytest tests/scripts/test_pipeline_runner.py -v
```

Expected: all tests `PASSED`.

- [ ] **Step 5: Lint**

```bash
./venv/Scripts/ruff.exe check scripts/pipeline_runner.py tests/scripts/test_pipeline_runner.py
```

Expected: no errors.

- [ ] **Step 6: Commit**

```bash
git add scripts/pipeline_runner.py tests/scripts/test_pipeline_runner.py
git commit -m "feat(pipeline_runner): iniciar_leitor — daemon thread para fila de logs"
```

---

## Task 4: Streamlit page `scripts/pages/3_Pipeline.py`

**Files:**
- Create: `scripts/pages/3_Pipeline.py`

No unit tests for the Streamlit page — visual verification in browser.

- [ ] **Step 1: Create `scripts/pages/3_Pipeline.py`**

```python
"""Página 3 — Executar Pipeline: trigger, log streaming e stop."""
import queue
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

import config
from pipeline_runner import competencia_atual, iniciar_leitor, iniciar_pipeline

_ANO_MIN = 2000
_ANO_MAX = 2099

st.title("Executar Pipeline")

# ── Inicializar session_state ──────────────────────────────────────────────
for key, default in [
    ("pipeline_proc", None),
    ("pipeline_log_queue", None),
    ("pipeline_logs", []),
    ("pipeline_status", "idle"),
]:
    if key not in st.session_state:
        st.session_state[key] = default

# ── Opções avançadas ──────────────────────────────────────────────────────
ano_default, mes_default = competencia_atual()
hr_disponivel = config.FOLHA_HR_PATH is not None

with st.expander("Opções avançadas"):
    col1, col2 = st.columns(2)
    with col1:
        ano = st.number_input("Ano", min_value=_ANO_MIN, max_value=_ANO_MAX, value=ano_default)
    with col2:
        mes = st.number_input("Mês", min_value=1, max_value=12, value=mes_default)
    skip_nacional = not st.toggle("Nacional (BigQuery)", value=True)
    skip_hr = not st.toggle(
        "RH (Folha de Pagamento)",
        value=hr_disponivel,
        disabled=not hr_disponivel,
        help=None if hr_disponivel else "FOLHA_HR_PATH não configurado no .env",
    )

competencia = f"{int(ano)}-{int(mes):02d}"

# ── Botão de execução ─────────────────────────────────────────────────────
em_execucao = st.session_state["pipeline_status"] == "running"

if st.button("Executar pipeline agora", disabled=em_execucao, type="primary"):
    proc = iniciar_pipeline(competencia, skip_nacional=skip_nacional, skip_hr=skip_hr)
    q = iniciar_leitor(proc)
    st.session_state["pipeline_proc"] = proc
    st.session_state["pipeline_log_queue"] = q
    st.session_state["pipeline_logs"] = []
    st.session_state["pipeline_status"] = "running"
    st.rerun()

# ── Banner de status ──────────────────────────────────────────────────────
status = st.session_state["pipeline_status"]
if status == "running":
    st.info(f"Pipeline em execução — competência {competencia}…")
elif status == "done":
    st.success("Pipeline concluído com sucesso.")
elif status == "error":
    st.error("Pipeline encerrado com erro ou interrompido. Verifique os logs abaixo.")

# ── Fragment de streaming ─────────────────────────────────────────────────
if st.session_state["pipeline_status"] == "running":

    @st.fragment(run_every=1)
    def _streaming():
        q: queue.Queue = st.session_state["pipeline_log_queue"]
        proc = st.session_state["pipeline_proc"]

        for _ in range(50):
            try:
                linha = q.get_nowait()
                st.session_state["pipeline_logs"].append(linha)
            except queue.Empty:
                break

        with st.expander("Logs", expanded=True):
            st.code("".join(st.session_state["pipeline_logs"]), language=None)

        if st.button("Parar execução", type="secondary"):
            proc.terminate()
            st.session_state["pipeline_status"] = "error"
            st.rerun()
            return

        returncode = proc.poll()
        if returncode is not None:
            st.session_state["pipeline_status"] = "done" if returncode == 0 else "error"
            st.rerun()

    _streaming()

elif st.session_state["pipeline_logs"]:
    with st.expander("Logs da última execução", expanded=False):
        st.code("".join(st.session_state["pipeline_logs"]), language=None)
```

- [ ] **Step 2: Verify the page loads in Streamlit**

```bash
./venv/Scripts/python.exe -m streamlit run scripts/dashboard.py
```

Navigate to "3 Pipeline" in the sidebar. Confirm:
- Page loads without error
- "Opções avançadas" expander shows Ano/Mês inputs defaulting to current month
- "Nacional" toggle is ON; "RH" toggle reflects `FOLHA_HR_PATH` in `.env`
- "Executar pipeline agora" button is visible and enabled

- [ ] **Step 3: Smoke-test with `--skip-nacional --skip-hr`**

In the browser:
1. Open "Opções avançadas" → disable Nacional and RH
2. Click "Executar pipeline agora"
3. Confirm the info banner appears: "Pipeline em execução…"
4. Confirm the "Logs" expander appears and populates with log lines every second
5. Confirm the "Parar execução" button appears inside the expander
6. Let the pipeline finish (or click stop) — confirm banner changes to success/error

- [ ] **Step 4: Lint**

```bash
./venv/Scripts/ruff.exe check scripts/pages/3_Pipeline.py
```

Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add scripts/pages/3_Pipeline.py
git commit -m "feat(dashboard): página Executar Pipeline — streaming de logs, stop button"
```

---

## Task 5: Full regression

- [ ] **Step 1: Run full test suite**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Expected: all tests pass, no regressions in existing test files.

- [ ] **Step 2: Lint entire scripts directory**

```bash
./venv/Scripts/ruff.exe check scripts/ tests/scripts/
```

Expected: no errors.

- [ ] **Step 3: Commit if any lint fixes were made**

```bash
git add -p
git commit -m "fix(lint): ajustes pós-implementação pipeline orchestration"
```

Only needed if lint produced fixable warnings. Skip if clean.
