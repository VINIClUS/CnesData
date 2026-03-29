# Dashboard Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unir home vazia com Visão Geral, adicionar container retrátil de status de dependências, distinguir "0 anomalias" de "regra não executada", carregar CSVs de auditoria em lazy load com st.session_state, aplicar streamlit-aggrid nas tabelas, e corrigir o eixo X do gráfico de tendências para meses (`YYYY-MM`) sem dias/horas.

**Architecture:** `dashboard_status.py` centraliza diagnóstico de deps (lê `.env` + `last_run.json`). Pipeline grava `last_run.json` no final de cada execução via `_gravar_last_run`. `dashboard.py` absorve `1_Visao_Geral.py` (que é removida). Páginas renomeadas: `2_Tendencias→1`, `3_Por_Regra→2`.

**Tech Stack:** Streamlit ≥ 1.32.0, `streamlit-aggrid 0.3.4.post3`, Plotly, DuckDB, `python-dotenv` (já instalado).

---

## File Map

| Ação | Arquivo | Responsabilidade |
|---|---|---|
| Modificar | `src/config.py` | Adicionar `LAST_RUN_PATH` |
| Modificar | `requirements.txt` | Adicionar `streamlit-aggrid==0.3.4.post3` |
| Modificar | `src/pipeline/stages/exportacao.py` | `_gravar_last_run(state, path)` gravada no final do pipeline |
| Criar | `scripts/dashboard_status.py` | `DepStatus`, `carregar_status()`, `renderizar_container_status()` |
| Modificar | `scripts/dashboard.py` | Absorve Visão Geral + usa `dashboard_status` |
| Remover | `scripts/pages/1_Visao_Geral.py` | Conteúdo migrado para `dashboard.py` |
| Criar (renomear) | `scripts/pages/1_Tendencias.py` | Antigo `2_Tendencias.py` + fix eixo X |
| Criar (renomear) | `scripts/pages/2_Por_Regra.py` | Antigo `3_Por_Regra.py` + lazy load + AgGrid |
| Modificar | `tests/pipeline/stages/test_exportacao.py` | 3 testes para `_gravar_last_run` |
| Criar | `tests/scripts/test_dashboard_status.py` | 6 testes para `carregar_status` |

---

## Task 1 — config.py: adicionar LAST_RUN_PATH

**Files:**
- Modify: `src/config.py`

- [ ] **Step 1: Adicionar `LAST_RUN_PATH` após `CACHE_DIR`**

Em `src/config.py`, após a linha:
```python
CACHE_DIR: Path = RAIZ_PROJETO / os.getenv("CACHE_DIR", "data/cache")
```

Adicionar:
```python
LAST_RUN_PATH: Path = CACHE_DIR / "last_run.json"
```

- [ ] **Step 2: Verificar que o teste de config ainda passa**

```bash
./venv/Scripts/python.exe -m pytest tests/test_config.py -q --tb=short
```

Saída esperada: todos passando.

- [ ] **Step 3: Commit**

```bash
git add src/config.py
git commit -m "feat(config): adicionar LAST_RUN_PATH"
```

---

## Task 2 — requirements.txt: adicionar streamlit-aggrid

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Adicionar dependência ao `requirements.txt`**

No final de `requirements.txt`, após `plotly>=5.20.0`, adicionar:
```
streamlit-aggrid==0.3.4.post3
```

- [ ] **Step 2: Instalar**

```bash
./venv/Scripts/pip.exe install streamlit-aggrid==0.3.4.post3
```

Saída esperada: `Successfully installed streamlit-aggrid-0.3.4.post3` (ou "already satisfied").

- [ ] **Step 3: Verificar import**

```bash
./venv/Scripts/python.exe -c "from st_aggrid import AgGrid, GridOptionsBuilder; print('ok')"
```

Saída esperada: `ok`

- [ ] **Step 4: Commit**

```bash
git add requirements.txt
git commit -m "feat(deps): adicionar streamlit-aggrid 0.3.4.post3"
```

---

## Task 3 — Pipeline: _gravar_last_run (TDD)

**Files:**
- Modify: `src/pipeline/stages/exportacao.py`
- Modify: `tests/pipeline/stages/test_exportacao.py`

- [ ] **Step 1: Escrever testes RED**

Adicionar ao final de `tests/pipeline/stages/test_exportacao.py`:

```python
import json
from datetime import datetime


def _state_nacional() -> PipelineState:
    """State com dados nacionais carregados."""
    s = _state()
    s.executar_nacional = True
    s.df_prof_nacional = pd.DataFrame({"CNS": ["001"]})
    s.df_estab_nacional = pd.DataFrame({"CNES": ["001"]})
    return s


def _state_sem_nacional() -> PipelineState:
    s = _state()
    s.executar_nacional = False
    return s


class TestGravarLastRun:

    def test_grava_arquivo_json(self, tmp_path):
        from pipeline.stages.exportacao import _gravar_last_run
        path = tmp_path / "last_run.json"

        _gravar_last_run(_state_nacional(), path)

        assert path.exists()
        dados = json.loads(path.read_text(encoding="utf-8"))
        assert set(dados.keys()) == {"firebird", "bigquery", "hr", "duckdb"}

    def test_firebird_sempre_ok_quando_pipeline_concluiu(self, tmp_path):
        from pipeline.stages.exportacao import _gravar_last_run
        path = tmp_path / "last_run.json"

        _gravar_last_run(_state_nacional(), path)

        dados = json.loads(path.read_text(encoding="utf-8"))
        assert dados["firebird"]["ok"] is True
        assert dados["firebird"]["ts"] is not None

    def test_bigquery_ok_false_quando_nacional_nao_executado(self, tmp_path):
        from pipeline.stages.exportacao import _gravar_last_run
        path = tmp_path / "last_run.json"

        _gravar_last_run(_state_sem_nacional(), path)

        dados = json.loads(path.read_text(encoding="utf-8"))
        assert dados["bigquery"]["ok"] is False
        assert dados["bigquery"]["ts"] is None

    def test_cria_diretorio_pai_se_nao_existir(self, tmp_path):
        from pipeline.stages.exportacao import _gravar_last_run
        path = tmp_path / "subdir" / "last_run.json"

        _gravar_last_run(_state_nacional(), path)

        assert path.exists()
```

- [ ] **Step 2: Confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_exportacao.py::TestGravarLastRun -x --tb=short -q
```

Saída esperada: `ImportError` ou `cannot import name '_gravar_last_run'`

- [ ] **Step 3: Implementar `_gravar_last_run` em `exportacao.py`**

Adicionar imports no topo de `src/pipeline/stages/exportacao.py` (junto aos existentes):
```python
import json
from datetime import datetime
```

Adicionar a função antes da classe `ExportacaoStage`:

```python
def _gravar_last_run(state: PipelineState, last_run_path: Path) -> None:
    agora = datetime.now().isoformat(timespec="seconds")
    nacional_ok = state.executar_nacional and not state.df_prof_nacional.empty
    hr_ok = state.executar_hr

    dados = {
        "firebird": {"ts": agora, "ok": True},
        "bigquery": {
            "ts": agora if nacional_ok else None,
            "ok": nacional_ok,
        },
        "hr": {
            "ts": agora if hr_ok else None,
            "ok": hr_ok if state.executar_hr else None,
        },
        "duckdb": {"ts": agora, "ok": True},
    }
    last_run_path.parent.mkdir(parents=True, exist_ok=True)
    last_run_path.write_text(json.dumps(dados, ensure_ascii=False, indent=2), encoding="utf-8")
```

Chamar ao final de `_persistir_historico`, antes do `logger.info`:

```python
    def _persistir_historico(self, state: PipelineState) -> None:
        # ... (código existente sem alteração até a linha do _arquivar_csvs)
        self._arquivar_csvs(state, competencia)
        _gravar_last_run(state, config.LAST_RUN_PATH)
        logger.info("exportacao concluida output=%s", state.output_path)
```

- [ ] **Step 4: Confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_exportacao.py -v --tb=short
```

Saída esperada: todos os testes passando (incluindo os 4 novos).

- [ ] **Step 5: Lint**

```bash
./venv/Scripts/ruff.exe check src/pipeline/stages/exportacao.py --fix
```

- [ ] **Step 6: Commit**

```bash
git add src/pipeline/stages/exportacao.py tests/pipeline/stages/test_exportacao.py
git commit -m "feat(pipeline): _gravar_last_run — registrar status de execução em last_run.json"
```

---

## Task 4 — dashboard_status.py: DepStatus + carregar_status (TDD)

**Files:**
- Create: `scripts/dashboard_status.py`
- Create: `tests/scripts/test_dashboard_status.py`

- [ ] **Step 1: Escrever testes RED**

Criar `tests/scripts/test_dashboard_status.py`:

```python
"""Testes de dashboard_status.carregar_status — diagnóstico de dependências."""
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from dashboard_status import DepStatus, carregar_status


class TestCarregarStatus:

    def test_retorna_nao_configurada_quando_sem_env_e_sem_last_run(self, tmp_path):
        path = tmp_path / "last_run.json"
        with patch.dict("os.environ", {}, clear=True):
            status = carregar_status(path, tmp_path / "cnesdata.duckdb")

        assert status["firebird"].ok is None
        assert status["bigquery"].ok is None
        assert status["hr"].ok is None

    def test_duckdb_erro_quando_arquivo_nao_existe(self, tmp_path):
        path = tmp_path / "last_run.json"
        with patch.dict("os.environ", {}, clear=True):
            status = carregar_status(path, tmp_path / "inexistente.duckdb")

        assert status["duckdb"].ok is False

    def test_duckdb_ok_none_quando_arquivo_existe_mas_sem_last_run(self, tmp_path):
        duckdb_path = tmp_path / "cnesdata.duckdb"
        duckdb_path.write_bytes(b"")
        path = tmp_path / "last_run.json"

        with patch.dict("os.environ", {}, clear=True):
            status = carregar_status(path, duckdb_path)

        assert status["duckdb"].ok is None

    def test_le_status_firebird_do_last_run(self, tmp_path):
        duckdb_path = tmp_path / "cnesdata.duckdb"
        duckdb_path.write_bytes(b"")
        path = tmp_path / "last_run.json"
        path.write_text(
            json.dumps({"firebird": {"ts": "2026-03-28T14:32:00", "ok": True}}),
            encoding="utf-8",
        )
        env = {"DB_PATH": "/db", "DB_PASSWORD": "x", "FIREBIRD_DLL": "/fbclient.dll"}
        with patch.dict("os.environ", env, clear=False):
            status = carregar_status(path, duckdb_path)

        assert status["firebird"].ok is True
        assert status["firebird"].ts == "2026-03-28T14:32:00"

    def test_le_status_bigquery_false_do_last_run(self, tmp_path):
        duckdb_path = tmp_path / "cnesdata.duckdb"
        duckdb_path.write_bytes(b"")
        path = tmp_path / "last_run.json"
        path.write_text(
            json.dumps({"bigquery": {"ts": None, "ok": False}}),
            encoding="utf-8",
        )
        env = {"GCP_PROJECT_ID": "proj-123"}
        with patch.dict("os.environ", env, clear=False):
            status = carregar_status(path, duckdb_path)

        assert status["bigquery"].ok is False
        assert status["bigquery"].ts is None

    def test_arquivo_corrompido_retorna_status_desconhecido(self, tmp_path):
        duckdb_path = tmp_path / "cnesdata.duckdb"
        duckdb_path.write_bytes(b"")
        path = tmp_path / "last_run.json"
        path.write_text("{ JSON INVÁLIDO", encoding="utf-8")

        with patch.dict("os.environ", {}, clear=True):
            status = carregar_status(path, duckdb_path)

        assert isinstance(status["firebird"], DepStatus)
```

- [ ] **Step 2: Confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/scripts/test_dashboard_status.py -x --tb=short -q
```

Saída esperada: `ImportError: cannot import name 'DepStatus' from 'dashboard_status'`

- [ ] **Step 3: Criar `scripts/dashboard_status.py`**

```python
"""dashboard_status — diagnóstico de dependências para o dashboard CnesData."""
import json
import os
from dataclasses import dataclass
from pathlib import Path

import streamlit as st


@dataclass
class DepStatus:
    """Status de uma dependência do pipeline."""

    ok: bool | None = None
    ts: str | None = None
    erro: str | None = None


def carregar_status(last_run_path: Path, duckdb_path: Path) -> dict[str, DepStatus]:
    """Lê last_run.json e vars de ambiente para diagnosticar cada dependência.

    Args:
        last_run_path: Caminho para data/cache/last_run.json.
        duckdb_path: Caminho para o arquivo DuckDB (verifica existência).

    Returns:
        Dict com chaves 'firebird', 'bigquery', 'hr', 'duckdb'.
    """
    raw = _ler_last_run(last_run_path)
    return {
        "firebird": _status_firebird(raw.get("firebird", {})),
        "bigquery": _status_bigquery(raw.get("bigquery", {})),
        "hr":       _status_hr(raw.get("hr", {})),
        "duckdb":   _status_duckdb(raw.get("duckdb", {}), duckdb_path),
    }


def renderizar_container_status(
    status: dict[str, DepStatus],
    competencias: list[str],
) -> None:
    """Renderiza st.expander com cards de status das 4 dependências.

    Args:
        status: Resultado de carregar_status().
        competencias: Lista de competências disponíveis no DuckDB (YYYY-MM).
    """
    algum_problema = any(s.ok is False for s in status.values())
    range_str = (
        f"{competencias[0]} → {competencias[-1]}" if len(competencias) >= 2
        else (competencias[0] if competencias else "—")
    )
    with st.expander("⚙ Status das dependências", expanded=algum_problema):
        cols = st.columns(4)
        _render_card(cols[0], "CNES Local", "Firebird",    status["firebird"], range_str)
        _render_card(cols[1], "CNES Nacional", "BigQuery", status["bigquery"],
                     range_str if status["bigquery"].ok is True else "—")
        _render_card(cols[2], "Histórico",   "DuckDB",     status["duckdb"],
                     f"{len(competencias)} competência(s)")
        _render_card(cols[3], "RH / Folha",  "HR/XLSX",    status["hr"])


def _render_card(
    col,
    titulo: str,
    fonte: str,
    s: DepStatus,
    range_str: str | None = None,
) -> None:
    with col:
        if s.ok is True:
            icon, label = "🟢", "con."
        elif s.ok is False:
            icon, label = "🔴", f"erro: {s.erro or 'ver logs'}"
        else:
            icon, label = "🟡", "não configurada"
        ts_str = f" · {s.ts[:16].replace('T', ' ')}" if s.ts else ""
        range_html = (
            f'<div style="font-size:10px;margin-top:4px;color:#aaa">'
            f'Range: <strong>{range_str}</strong></div>'
            if range_str else ""
        )
        st.markdown(
            f'<div style="border:1px solid #2d4a7a;border-radius:6px;'
            f'padding:8px;background:#0d1b2a">'
            f'<div style="font-size:10px;color:#888">{titulo}</div>'
            f'<div>{icon} <strong>{fonte}</strong></div>'
            f'<div style="color:#888;font-size:11px">{label}{ts_str}</div>'
            f'{range_html}</div>',
            unsafe_allow_html=True,
        )


def _ler_last_run(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _status_firebird(entry: dict) -> DepStatus:
    if not all(os.getenv(v) for v in ("DB_PATH", "DB_PASSWORD", "FIREBIRD_DLL")):
        return DepStatus(ok=None)
    if not entry:
        return DepStatus(ok=None)
    return DepStatus(ok=entry.get("ok"), ts=entry.get("ts"), erro=entry.get("erro"))


def _status_bigquery(entry: dict) -> DepStatus:
    if not os.getenv("GCP_PROJECT_ID"):
        return DepStatus(ok=None)
    if not entry:
        return DepStatus(ok=None)
    return DepStatus(ok=entry.get("ok"), ts=entry.get("ts"), erro=entry.get("erro"))


def _status_hr(entry: dict) -> DepStatus:
    folha = os.getenv("FOLHA_HR_PATH")
    if not folha or not Path(folha).exists():
        return DepStatus(ok=None)
    if not entry:
        return DepStatus(ok=None)
    return DepStatus(ok=entry.get("ok"), ts=entry.get("ts"), erro=entry.get("erro"))


def _status_duckdb(entry: dict, duckdb_path: Path) -> DepStatus:
    if not duckdb_path.exists():
        return DepStatus(ok=False, erro="arquivo não encontrado")
    if not entry:
        return DepStatus(ok=None)
    return DepStatus(ok=entry.get("ok"), ts=entry.get("ts"), erro=entry.get("erro"))
```

- [ ] **Step 4: Confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/scripts/test_dashboard_status.py -v --tb=short
```

Saída esperada: `6 passed`

- [ ] **Step 5: Lint**

```bash
./venv/Scripts/ruff.exe check scripts/dashboard_status.py --fix
./venv/Scripts/ruff.exe format scripts/dashboard_status.py
```

- [ ] **Step 6: Suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Saída esperada: todos passando.

- [ ] **Step 7: Commit**

```bash
git add scripts/dashboard_status.py tests/scripts/test_dashboard_status.py
git commit -m "feat(dashboard): dashboard_status — DepStatus, carregar_status, renderizar_container_status"
```

---

## Task 5 — dashboard.py: merge Visão Geral + status container + 0 vs N/D

**Files:**
- Modify: `scripts/dashboard.py`
- Remove: `scripts/pages/1_Visao_Geral.py`

Mapeamento de regras para fonte de dados — usado para mostrar `—` quando a fonte não está disponível:

```python
_REGRAS_FONTE: dict[str, str] = {
    "RQ008": "firebird", "GHOST": "hr",       "RQ006": "bigquery",
    "RQ007": "bigquery", "RQ009": "bigquery",  "MISSING": "hr",
    "RQ005_ACS": "firebird", "RQ005_ACE": "firebird",
    "RQ003B": "firebird", "RQ010": "bigquery", "RQ011": "bigquery",
}
```

- [ ] **Step 1: Remover `1_Visao_Geral.py`**

```bash
git rm scripts/pages/1_Visao_Geral.py
```

- [ ] **Step 2: Reescrever `scripts/dashboard.py`**

```python
"""CnesData Analytics — home page (Visão Geral) com container de status."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pandas as pd
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

import config
from dashboard_status import carregar_status, renderizar_container_status
from storage.historico_reader import HistoricoReader

st.set_page_config(
    page_title="CnesData Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

_CSS = """
<style>
@media (prefers-color-scheme: light) {
    .stApp { background-color: #ffffff !important; color: #1a1a2e !important; }
    section[data-testid="stSidebar"] { background-color: #f0f2f6 !important; }
}
.stDataFrame { overflow-x: auto; }
</style>
"""
st.markdown(_CSS, unsafe_allow_html=True)

_REGRAS_META: dict[str, tuple[str, str]] = {
    "RQ008":     ("Prof Fantasma (CNS)",   "CRÍTICA"),
    "GHOST":     ("Ghost Payroll",          "CRÍTICA"),
    "RQ006":     ("Estab Fantasma",         "ALTA"),
    "RQ007":     ("Estab Ausente Local",    "ALTA"),
    "RQ009":     ("Prof Ausente Local",     "ALTA"),
    "MISSING":   ("Missing Registration",  "ALTA"),
    "RQ005_ACS": ("ACS/TACS Incorretos",   "ALTA"),
    "RQ005_ACE": ("ACE/TACE Incorretos",   "ALTA"),
    "RQ003B":    ("Múltiplas Unidades",    "MÉDIA"),
    "RQ010":     ("Divergência CBO",       "MÉDIA"),
    "RQ011":     ("Divergência CH",        "BAIXA"),
}
_SEV_ORDER = {"CRÍTICA": 0, "ALTA": 1, "MÉDIA": 2, "BAIXA": 3}
_SEV_ICON  = {"CRÍTICA": "🔴", "ALTA": "🟠", "MÉDIA": "🟡", "BAIXA": "🟢"}
_KPI_DESTAQUE = ["RQ008", "RQ006", "RQ009", "GHOST", "MISSING"]

_REGRAS_FONTE: dict[str, str] = {
    "RQ008": "firebird", "GHOST": "hr",       "RQ006": "bigquery",
    "RQ007": "bigquery", "RQ009": "bigquery",  "MISSING": "hr",
    "RQ005_ACS": "firebird", "RQ005_ACE": "firebird",
    "RQ003B": "firebird", "RQ010": "bigquery", "RQ011": "bigquery",
}


@st.cache_resource
def _get_reader() -> HistoricoReader:
    return HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)


@st.cache_data(ttl=300)
def _get_status() -> dict:
    return carregar_status(config.LAST_RUN_PATH, config.DUCKDB_PATH)


if "reader" not in st.session_state:
    st.session_state["reader"] = _get_reader()

st.sidebar.title("CnesData Analytics")
st.sidebar.caption("Presidente Epitácio/SP")

st.title("📊 Visão Geral")

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    status = _get_status()
    renderizar_container_status(status, [])
    st.stop()

status = _get_status()
renderizar_container_status(status, competencias)

competencia = st.sidebar.selectbox("Competência", options=competencias[::-1], index=0)

kpis           = reader.carregar_kpis(competencia)
deltas         = reader.carregar_delta(competencia)
total_vinculos = reader.carregar_total_vinculos(competencia)

col_vinculos, *_ = st.columns([1, 1, 1, 1, 1, 1])
with col_vinculos:
    st.metric(
        label="Vínculos processados",
        value=total_vinculos,
        help="Total de vínculos profissionais na competência selecionada",
    )

st.divider()

cols = st.columns(len(_KPI_DESTAQUE))
for i, regra in enumerate(_KPI_DESTAQUE):
    desc, sev = _REGRAS_META[regra]
    fonte_ok = status[_REGRAS_FONTE[regra]].ok is True
    delta = deltas.get(regra, 0)
    with cols[i]:
        if fonte_ok:
            st.metric(
                label=f"{_SEV_ICON[sev]} {desc}",
                value=kpis.get(regra, 0),
                delta=f"+{delta}" if delta > 0 else str(delta),
                delta_color="inverse",
                help=f"Regra {regra} — Severidade: {sev}",
            )
        else:
            fonte_nome = _REGRAS_FONTE[regra].capitalize()
            st.metric(
                label=f"{_SEV_ICON[sev]} {desc}",
                value="—",
                help=f"Regra {regra} — {fonte_nome} não configurado",
            )

st.divider()

if total_vinculos == 0:
    st.warning("Pipeline rodou mas não processou vínculos. Verifique os logs.")
elif not kpis:
    st.warning("Dados de auditoria não encontrados para esta competência.")
elif all(
    kpis.get(r, 0) == 0
    for r, _ in _REGRAS_META.items()
    if status[_REGRAS_FONTE[r]].ok is True
):
    st.info(
        "Nenhuma anomalia detectada nas fontes configuradas. "
        "Se esperava resultados, verifique os logs do pipeline."
    )

rows = []
for regra, (desc, sev) in sorted(_REGRAS_META.items(), key=lambda x: _SEV_ORDER[x[1][1]]):
    fonte_ok = status[_REGRAS_FONTE[regra]].ok is True
    rows.append({
        "Regra":      regra,
        "Descrição":  desc,
        "Anomalias":  kpis.get(regra, 0) if fonte_ok else "—",
        "Δ mês":      (f"+{deltas.get(regra,0)}" if deltas.get(regra,0) > 0
                       else str(deltas.get(regra,0))) if fonte_ok else "—",
        "Severidade": f"{_SEV_ICON[sev]} {sev}",
    })

df_resumo = pd.DataFrame(rows)
gb = GridOptionsBuilder.from_dataframe(df_resumo)
gb.configure_default_column(resizable=True, sortable=True, filter=True)
gb.configure_grid_options(domLayout="autoHeight")
AgGrid(df_resumo, gridOptions=gb.build(), use_container_width=True,
       fit_columns_on_grid_load=True, theme="streamlit")
```

- [ ] **Step 3: Verificar manualmente**

```bash
./venv/Scripts/python.exe -m streamlit run scripts/dashboard.py
```

Verificar:
- [ ] Container de status aparece no topo da home
- [ ] KPIs com fonte não configurada mostram `—`
- [ ] Tabela final usa AgGrid (colunas redimensionáveis, filtros visíveis)
- [ ] Sidebar mostra "CnesData Analytics" + "Presidente Epitácio/SP"

- [ ] **Step 4: Commit**

```bash
git add scripts/dashboard.py scripts/pages/1_Visao_Geral.py
git commit -m "feat(dashboard): merge home+visao-geral, status container, regras N/D para fontes off"
```

---

## Task 6 — 1_Tendencias.py: renomear + fix eixo X

**Files:**
- Create: `scripts/pages/1_Tendencias.py` (renomeado de `2_Tendencias.py`)
- Remove: `scripts/pages/2_Tendencias.py`

- [ ] **Step 1: Renomear via git**

```bash
git mv "scripts/pages/2_Tendencias.py" "scripts/pages/1_Tendencias.py"
```

- [ ] **Step 2: Corrigir eixo X para category**

Em `scripts/pages/1_Tendencias.py`, localizar:
```python
fig.update_layout(hovermode="x unified", legend_title_text="Regra")
```

Substituir por:
```python
fig.update_layout(hovermode="x unified", legend_title_text="Regra")
fig.update_xaxes(type="category", tickangle=-45)
```

- [ ] **Step 3: Verificar manualmente**

```bash
./venv/Scripts/python.exe -m streamlit run scripts/dashboard.py
```

Navegar para Tendências e verificar:
- [ ] Eixo X mostra apenas `YYYY-MM` (ex: `2025-01`, `2025-02`), sem dias ou horas
- [ ] Labels rotacionadas -45° para legibilidade

- [ ] **Step 4: Commit**

```bash
git add scripts/pages/1_Tendencias.py
git commit -m "fix(tendencias): renomear pagina e corrigir eixo X para type=category (YYYY-MM)"
```

---

## Task 7 — 2_Por_Regra.py: renomear + lazy load + AgGrid

**Files:**
- Create: `scripts/pages/2_Por_Regra.py` (renomeado de `3_Por_Regra.py`)
- Remove: `scripts/pages/3_Por_Regra.py`

- [ ] **Step 1: Renomear via git**

```bash
git mv "scripts/pages/3_Por_Regra.py" "scripts/pages/2_Por_Regra.py"
```

- [ ] **Step 2: Reescrever `scripts/pages/2_Por_Regra.py`**

```python
"""Página 2 — Por Regra: drill-down com lazy load, AgGrid e PII masking."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

import config
from dashboard_status import DepStatus, carregar_status
from storage.historico_reader import HistoricoReader

_TABS: list[tuple[str, str]] = [
    ("🔴 RQ-008",  "RQ008"),
    ("🔴 Ghost",   "GHOST"),
    ("🟠 RQ-006",  "RQ006"),
    ("🟠 RQ-007",  "RQ007"),
    ("🟠 RQ-009",  "RQ009"),
    ("🟠 Missing", "MISSING"),
    ("🟠 RQ-005a", "RQ005_ACS"),
    ("🟠 RQ-005b", "RQ005_ACE"),
    ("🟡 RQ-003B", "RQ003B"),
    ("🟡 RQ-010",  "RQ010"),
    ("🟢 RQ-011",  "RQ011"),
]

_REGRA_DESC: dict[str, str] = {
    "RQ003B":    "RQ-003-B — Múltiplas Unidades",
    "RQ005_ACS": "RQ-005 ACS/TACS — Lotação Incorreta",
    "RQ005_ACE": "RQ-005 ACE/TACE — Lotação Incorreta",
    "GHOST":     "Ghost Payroll",
    "MISSING":   "Missing Registration",
    "RQ006":     "RQ-006 — Estabelecimentos Fantasma",
    "RQ007":     "RQ-007 — Estabelecimentos Ausentes Local",
    "RQ008":     "RQ-008 — Profissionais Fantasma (CNS)",
    "RQ009":     "RQ-009 — Profissionais Ausentes Local",
    "RQ010":     "RQ-010 — Divergência CBO",
    "RQ011":     "RQ-011 — Divergência CH",
}

_REGRAS_FONTE: dict[str, str] = {
    "RQ008": "firebird", "GHOST": "hr",       "RQ006": "bigquery",
    "RQ007": "bigquery", "RQ009": "bigquery",  "MISSING": "hr",
    "RQ005_ACS": "firebird", "RQ005_ACE": "firebird",
    "RQ003B": "firebird", "RQ010": "bigquery", "RQ011": "bigquery",
}


@st.cache_data(ttl=300)
def _get_status() -> dict:
    return carregar_status(config.LAST_RUN_PATH, config.DUCKDB_PATH)


def _invalidar_cache_se_competencia_mudou(competencia: str) -> None:
    if st.session_state.get("_por_regra_competencia") != competencia:
        for key in [k for k in st.session_state if k.startswith("df_")]:
            del st.session_state[key]
        st.session_state["_por_regra_competencia"] = competencia


def _carregar_tab(reader: HistoricoReader, regra: str, competencia: str):
    key = f"df_{regra}_{competencia}"
    if key not in st.session_state:
        with st.spinner(f"Carregando {_REGRA_DESC[regra]}..."):
            st.session_state[key] = reader.carregar_registros(regra, competencia)
    return st.session_state[key]


def _mascarar_pii(df, mostrar_completo: bool):
    if mostrar_completo:
        return df
    df_display = df.copy()
    for col in df_display.select_dtypes(include="object").columns:
        upper = col.upper()
        if "CPF" in upper or "CNS" in upper:
            df_display[col] = df_display[col].apply(
                lambda v: f"***{str(v)[-4:]}" if isinstance(v, str) and len(v) >= 4 else v
            )
    return df_display


st.title("🔍 Por Regra")

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

competencia = st.sidebar.selectbox("Competência", options=competencias[::-1], index=0)
_invalidar_cache_se_competencia_mudou(competencia)

status = _get_status()
kpis   = reader.carregar_kpis(competencia)
deltas = reader.carregar_delta(competencia)

tabs = st.tabs([label for label, _ in _TABS])

for tab, (_, regra) in zip(tabs, _TABS):
    with tab:
        fonte = _REGRAS_FONTE[regra]
        fonte_ok: bool = status[fonte].ok is True

        col_metric, _ = st.columns([1, 4])
        with col_metric:
            if fonte_ok:
                valor = kpis.get(regra, 0)
                delta = deltas.get(regra, 0)
                st.metric(
                    label=_REGRA_DESC[regra],
                    value=valor,
                    delta=f"+{delta}" if delta > 0 else str(delta),
                    delta_color="inverse",
                )
            else:
                st.metric(label=_REGRA_DESC[regra], value="—",
                          help=f"Fonte '{fonte}' não configurada")

        if not fonte_ok:
            st.warning(
                f"⚠ Fonte **{fonte}** não disponível para **{_REGRA_DESC[regra]}**. "
                "Configure a dependência no `.env` e reexecute o pipeline."
            )
            continue

        df = _carregar_tab(reader, regra, competencia)

        if df.empty:
            st.info(
                f"Sem registros para **{_REGRA_DESC[regra]}** em {competencia}. "
                "Verifique se o pipeline rodou para essa competência."
            )
            continue

        mostrar_completo = st.checkbox(
            "Mostrar dados completos (CPF/CNS sem máscara)",
            key=f"mask_{regra}",
        )
        df_display = _mascarar_pii(df, mostrar_completo)

        gb = GridOptionsBuilder.from_dataframe(df_display)
        gb.configure_default_column(resizable=True, sortable=True, filter=True)
        gb.configure_grid_options(domLayout="autoHeight")
        AgGrid(df_display, gridOptions=gb.build(), use_container_width=True,
               fit_columns_on_grid_load=False, theme="streamlit", key=f"grid_{regra}")

        st.download_button(
            f"⬇ Baixar CSV — {regra} / {competencia}",
            data=df.to_csv(index=False).encode("utf-8-sig"),
            file_name=f"auditoria_{regra.lower()}_{competencia}.csv",
            mime="text/csv",
            key=f"dl_{regra}",
        )
```

- [ ] **Step 3: Verificar manualmente**

```bash
./venv/Scripts/python.exe -m streamlit run scripts/dashboard.py
```

Navegar para "Por Regra" e verificar:
- [ ] Tabs com fontes não configuradas mostram callout amarelo `⚠`
- [ ] Tabs com dados mostram AgGrid com filtros e colunas redimensionáveis
- [ ] Spinner aparece ao clicar em tab pela primeira vez
- [ ] Trocar competência invalida o cache (nova carga ao voltar para tab)
- [ ] Botão de download disponível após carga do CSV

- [ ] **Step 4: Suite de testes**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Saída esperada: todos passando (426+).

- [ ] **Step 5: Lint**

```bash
./venv/Scripts/ruff.exe check scripts/ --fix
```

- [ ] **Step 6: Commit**

```bash
git add scripts/pages/2_Por_Regra.py scripts/pages/3_Por_Regra.py
git commit -m "feat(por-regra): lazy load session_state, AgGrid com filtros, callout fontes off"
```

---

## Self-Review — Spec Coverage

| Requisito do spec | Task |
|---|---|
| Unir home vazia + Visão Geral | Task 5 |
| Container retrátil de status das deps | Task 4 + Task 5 |
| Timestamp última execução por dep | Task 3 (last_run.json) + Task 4 (renderizar) |
| Status "con." / "não configurada" / "erro" | Task 4 (_status_* functions) |
| Range competências CNES Local | Task 4 (renderizar — de listar_competencias) |
| Range competências CNES Nacional | Task 4 (renderizar — mostra "—" se bigquery.ok não True) |
| Quantitativos 0 vs não executado | Task 5 (REGRAS_FONTE + fonte_ok) |
| Eixo X gráfico = YYYY-MM | Task 6 (type="category") |
| Lazy load + spinner por tab | Task 7 (_carregar_tab + session_state) |
| Colunas retráteis, móveis, filtros | Task 2 + Task 5 + Task 7 (AgGrid) |
| LAST_RUN_PATH em config.py | Task 1 |
| Pipeline grava last_run.json | Task 3 |
| streamlit-aggrid instalado | Task 2 |
