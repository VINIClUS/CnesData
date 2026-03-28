# CnesData Analytics Dashboard — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Streamlit multi-page dashboard (dark mode, responsive) que lê DuckDB Gold + CSVs históricos para análise de tendências e drill-down de anomalias CNES.

**Architecture:** ExportacaoStage é corrigido para usar `state.competencia_str` (fix de bug) e estendido para gravar todas as 11 regras no DuckDB e arquivar CSVs em `data/processed/historico/{competencia}/`. `HistoricoReader` abstrai acesso ao DuckDB Gold e ao arquivo histórico (via `read_csv_auto`). Três páginas Streamlit consomem `HistoricoReader` via `st.session_state`.

**Tech Stack:** Python 3.11+, Streamlit ≥1.32, Plotly ≥5.20, DuckDB (já instalado), openpyxl (já instalado), shutil (stdlib)

---

## File Map

| Ação | Arquivo | O que muda |
|---|---|---|
| Modify | `src/pipeline/stages/exportacao.py` | Fix `competencia_str`, +8 `gravar_auditoria`, `_arquivar_csvs` |
| Modify | `src/config.py` | Adiciona `HISTORICO_DIR` |
| Create | `src/storage/historico_reader.py` | Nova classe `HistoricoReader` + `CSV_MAP` |
| Create | `.streamlit/config.toml` | Tema dark + server config |
| Create | `scripts/dashboard.py` | Entry point: page config + CSS + `get_reader()` |
| Create | `scripts/pages/1_Visao_Geral.py` | KPI cards + tabela resumo |
| Create | `scripts/pages/2_Tendencias.py` | Plotly line chart multi-regra |
| Create | `scripts/pages/3_Por_Regra.py` | Drill-down de registros com máscaras CPF/CNS |
| Modify | `tests/pipeline/stages/test_exportacao.py` | +4 testes |
| Create | `tests/storage/test_historico_reader.py` | 10 testes unitários |
| Modify | `requirements.txt` | streamlit, plotly |
| Modify | `README.md` | Seção "Dashboard" |

---

## Task 1: Fix ExportacaoStage — competencia_str + 11 regras

**Files:**
- Modify: `src/pipeline/stages/exportacao.py:65-87`
- Modify: `tests/pipeline/stages/test_exportacao.py`

- [ ] **Step 1: Escrever os 2 testes que falharão**

Adicionar ao final de `tests/pipeline/stages/test_exportacao.py`:

```python
@patch("pipeline.stages.exportacao.exportar_csv")
@patch("pipeline.stages.exportacao.gerar_relatorio")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.config")
def test_persistir_usa_competencia_str_nao_nome_arquivo(
    mock_config, mock_loader_cls, mock_salvar, mock_criar, mock_gerar, mock_exportar, tmp_path
):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.DUCKDB_PATH = tmp_path / "test.duckdb"
    mock_config.HISTORICO_DIR = tmp_path / "historico"
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=0, total_missing=0, total_rq005=0
    )
    mock_loader_cls.return_value = MagicMock()
    state = _state()
    # Filename real de produção — split("_")[-1] seria "CNES", não "2024-12"
    state.output_path = tmp_path / "Relatorio_Profissionais_CNES.csv"

    ExportacaoStage().execute(state)

    args = mock_criar.call_args[0]
    assert args[0] == "2024-12"  # state.competencia_str, não "CNES"


@patch("pipeline.stages.exportacao.exportar_csv")
@patch("pipeline.stages.exportacao.gerar_relatorio")
@patch("pipeline.stages.exportacao.criar_snapshot")
@patch("pipeline.stages.exportacao.salvar_snapshot")
@patch("pipeline.stages.exportacao.DatabaseLoader")
@patch("pipeline.stages.exportacao.config")
def test_persistir_grava_todas_11_regras(
    mock_config, mock_loader_cls, mock_salvar, mock_criar, mock_gerar, mock_exportar, tmp_path
):
    mock_config.SNAPSHOTS_DIR = tmp_path / "snapshots"
    mock_config.DUCKDB_PATH = tmp_path / "test.duckdb"
    mock_config.HISTORICO_DIR = tmp_path / "historico"
    mock_criar.return_value = MagicMock(
        data_competencia="2024-12", total_ghost=1, total_missing=2, total_rq005=3
    )
    mock_loader = MagicMock()
    mock_loader_cls.return_value = mock_loader

    ExportacaoStage().execute(_state())

    regras = {call.args[1] for call in mock_loader.gravar_auditoria.call_args_list}
    assert regras == {
        "GHOST", "MISSING", "RQ005",
        "RQ003B", "RQ005_ACS", "RQ005_ACE",
        "RQ006", "RQ007", "RQ008", "RQ009", "RQ010", "RQ011",
    }
```

- [ ] **Step 2: Confirmar falha**

```bash
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_exportacao.py::test_persistir_usa_competencia_str_nao_nome_arquivo tests/pipeline/stages/test_exportacao.py::test_persistir_grava_todas_11_regras -v
```
Expected: FAIL (primeiro deve retornar `"CNES"`, segundo só tem 3 regras)

- [ ] **Step 3: Corrigir `_persistir_historico` em `exportacao.py`**

Substituir o método completo (linhas 65–87):

```python
def _persistir_historico(self, state: PipelineState) -> None:
    competencia = state.competencia_str
    snapshot = criar_snapshot(
        competencia,
        state.df_processado,
        state.df_ghost,
        state.df_missing,
        state.df_multi_unidades,
        state.df_acs_incorretos,
        state.df_ace_incorretos,
    )
    salvar_snapshot(snapshot, config.SNAPSHOTS_DIR)
    loader = DatabaseLoader(config.DUCKDB_PATH)
    loader.inicializar_schema()
    loader.gravar_metricas(snapshot)
    loader.gravar_auditoria(competencia, "GHOST", snapshot.total_ghost)
    loader.gravar_auditoria(competencia, "MISSING", snapshot.total_missing)
    loader.gravar_auditoria(competencia, "RQ005", snapshot.total_rq005)
    loader.gravar_auditoria(competencia, "RQ003B", len(state.df_multi_unidades))
    loader.gravar_auditoria(competencia, "RQ005_ACS", len(state.df_acs_incorretos))
    loader.gravar_auditoria(competencia, "RQ005_ACE", len(state.df_ace_incorretos))
    loader.gravar_auditoria(competencia, "RQ006", len(state.df_estab_fantasma))
    loader.gravar_auditoria(competencia, "RQ007", len(state.df_estab_ausente))
    loader.gravar_auditoria(competencia, "RQ008", len(state.df_prof_fantasma))
    loader.gravar_auditoria(competencia, "RQ009", len(state.df_prof_ausente))
    loader.gravar_auditoria(competencia, "RQ010", len(state.df_cbo_diverg))
    loader.gravar_auditoria(competencia, "RQ011", len(state.df_ch_diverg))
    logger.info("exportacao concluida output=%s", state.output_path)
```

- [ ] **Step 4: Confirmar verde**

```bash
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_exportacao.py -v
```
Expected: todos passando (incluindo os 2 novos)

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/stages/exportacao.py tests/pipeline/stages/test_exportacao.py
git commit -m "fix(exportacao): usa competencia_str e grava todas 11 regras no DuckDB"
```

---

## Task 2: HistoricoReader

**Files:**
- Create: `src/storage/historico_reader.py`
- Create: `tests/storage/test_historico_reader.py`

- [ ] **Step 1: Escrever os 10 testes que falharão**

Criar `tests/storage/test_historico_reader.py`:

```python
"""Testes do HistoricoReader — DuckDB Gold + CSVs arquivados."""
import duckdb
import pytest

from storage.historico_reader import HistoricoReader


def _popular_duckdb(path):
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute("""
            CREATE TABLE gold.evolucao_metricas_mensais (
                data_competencia VARCHAR PRIMARY KEY,
                total_vinculos INTEGER,
                total_ghost INTEGER,
                total_missing INTEGER,
                total_rq005 INTEGER
            )
        """)
        con.execute("""
            CREATE TABLE gold.auditoria_resultados (
                data_competencia VARCHAR,
                regra VARCHAR,
                total_anomalias INTEGER,
                PRIMARY KEY (data_competencia, regra)
            )
        """)
        con.execute("INSERT INTO gold.evolucao_metricas_mensais VALUES ('2024-11',350,2,3,5)")
        con.execute("INSERT INTO gold.evolucao_metricas_mensais VALUES ('2024-12',357,3,2,7)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-11','RQ008',9)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-11','RQ006',4)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-12','RQ008',12)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-12','RQ006',3)")
        con.execute("INSERT INTO gold.auditoria_resultados VALUES ('2024-12','RQ009',8)")


@pytest.fixture
def reader(tmp_path):
    db = tmp_path / "test.duckdb"
    _popular_duckdb(db)
    historico = tmp_path / "historico"
    comp_dir = historico / "2024-12"
    comp_dir.mkdir(parents=True)
    (comp_dir / "auditoria_rq008_prof_fantasma_cns.csv").write_text(
        "CNS,NOME_PROFISSIONAL,CNES\n7001234567890123,Ana Silva,2795001\n",
        encoding="utf-8",
    )
    return HistoricoReader(db, historico)


def test_listar_competencias_ordem_cronologica(reader):
    # Act
    resultado = reader.listar_competencias()

    # Assert
    assert resultado == ["2024-11", "2024-12"]


def test_carregar_tendencias_sem_filtro_retorna_tudo(reader):
    # Act
    df = reader.carregar_tendencias()

    # Assert
    assert set(df.columns) >= {"data_competencia", "regra", "total_anomalias"}
    assert len(df) == 5  # 2 linhas em nov + 3 em dez


def test_carregar_tendencias_filtra_por_regra(reader):
    # Act
    df = reader.carregar_tendencias(regras=["RQ008"])

    # Assert
    assert list(df["regra"].unique()) == ["RQ008"]
    assert len(df) == 2


def test_carregar_tendencias_filtra_por_periodo(reader):
    # Act
    df = reader.carregar_tendencias(competencia_inicio="2024-12", competencia_fim="2024-12")

    # Assert
    assert list(df["data_competencia"].unique()) == ["2024-12"]
    assert len(df) == 3


def test_carregar_kpis_retorna_dict_correto(reader):
    # Act
    kpis = reader.carregar_kpis("2024-12")

    # Assert
    assert kpis["RQ008"] == 12
    assert kpis["RQ006"] == 3
    assert kpis["RQ009"] == 8


def test_carregar_delta_calcula_variacao_correta(reader):
    # Act
    delta = reader.carregar_delta("2024-12")

    # Assert
    assert delta["RQ008"] == 3   # 12 - 9
    assert delta["RQ006"] == -1  # 3 - 4


def test_carregar_delta_retorna_zero_para_primeira_competencia(reader):
    # Act
    delta = reader.carregar_delta("2024-11")

    # Assert — sem mês anterior, delta = 0
    assert delta["RQ008"] == 0
    assert delta["RQ006"] == 0


def test_carregar_registros_retorna_dataframe_via_csv(reader):
    # Act
    df = reader.carregar_registros("RQ008", "2024-12")

    # Assert
    assert not df.empty
    assert "CNS" in df.columns
    assert df.iloc[0]["NOME_PROFISSIONAL"] == "Ana Silva"


def test_carregar_registros_retorna_vazio_quando_csv_ausente(reader):
    # Act — sem arquivo para 2024-11
    df = reader.carregar_registros("RQ008", "2024-11")

    # Assert
    assert df.empty


def test_listar_competencias_para_regra_filtra_por_arquivo(reader):
    # Act
    disponiveis = reader.listar_competencias_para_regra("RQ008")

    # Assert
    assert "2024-12" in disponiveis
    assert "2024-11" not in disponiveis
```

- [ ] **Step 2: Confirmar falha**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py -v
```
Expected: FAIL com `ModuleNotFoundError: No module named 'storage.historico_reader'`

- [ ] **Step 3: Implementar `src/storage/historico_reader.py`**

```python
"""HistoricoReader — acesso ao histórico analítico (DuckDB Gold + CSVs arquivados)."""
import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

CSV_MAP: dict[str, str] = {
    "RQ003B":    "auditoria_rq003b_multiplas_unidades.csv",
    "RQ005_ACS": "auditoria_rq005_acs_tacs_incorretos.csv",
    "RQ005_ACE": "auditoria_rq005_ace_tace_incorretos.csv",
    "GHOST":     "auditoria_ghost_payroll.csv",
    "MISSING":   "auditoria_missing_registration.csv",
    "RQ006":     "auditoria_rq006_estab_fantasma.csv",
    "RQ007":     "auditoria_rq007_estab_ausente_local.csv",
    "RQ008":     "auditoria_rq008_prof_fantasma_cns.csv",
    "RQ009":     "auditoria_rq009_prof_ausente_local_cns.csv",
    "RQ010":     "auditoria_rq010_divergencia_cbo.csv",
    "RQ011":     "auditoria_rq011_divergencia_ch.csv",
}


class HistoricoReader:
    """Lê tendências do DuckDB Gold e registros individuais de CSVs arquivados."""

    def __init__(self, duckdb_path: Path, historico_dir: Path) -> None:
        self._duckdb_path = duckdb_path
        self._historico_dir = historico_dir

    def carregar_tendencias(
        self,
        regras: list[str] | None = None,
        competencia_inicio: str | None = None,
        competencia_fim: str | None = None,
    ) -> pd.DataFrame:
        """Retorna DataFrame(data_competencia, regra, total_anomalias) de gold.auditoria_resultados."""
        conditions: list[str] = []
        params: list = []
        if regras:
            placeholders = ", ".join("?" * len(regras))
            conditions.append(f"regra IN ({placeholders})")
            params.extend(regras)
        if competencia_inicio:
            conditions.append("data_competencia >= ?")
            params.append(competencia_inicio)
        if competencia_fim:
            conditions.append("data_competencia <= ?")
            params.append(competencia_fim)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"""
            SELECT data_competencia, regra, total_anomalias
            FROM gold.auditoria_resultados
            {where}
            ORDER BY data_competencia, regra
        """
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            return con.execute(sql, params).df()

    def carregar_kpis(self, competencia: str) -> dict[str, int]:
        """Retorna {regra: total} para uma competência específica."""
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            df = con.execute(
                "SELECT regra, total_anomalias FROM gold.auditoria_resultados "
                "WHERE data_competencia = ?",
                [competencia],
            ).df()
        return dict(zip(df["regra"], df["total_anomalias"].astype(int)))

    def carregar_delta(self, competencia: str) -> dict[str, int]:
        """Retorna variação de cada regra vs competência anterior (0 se não houver anterior)."""
        competencias = self.listar_competencias()
        if competencia not in competencias:
            return {}
        idx = competencias.index(competencia)
        atual = self.carregar_kpis(competencia)
        if idx == 0:
            return {regra: 0 for regra in atual}
        anterior = self.carregar_kpis(competencias[idx - 1])
        return {regra: total - anterior.get(regra, 0) for regra, total in atual.items()}

    def carregar_registros(self, regra: str, competencia: str) -> pd.DataFrame:
        """Lê CSV arquivado via DuckDB read_csv_auto. Retorna DataFrame vazio se ausente."""
        nome = CSV_MAP.get(regra)
        if not nome:
            return pd.DataFrame()
        path = self._historico_dir / competencia / nome
        if not path.exists():
            logger.warning("csv_ausente regra=%s competencia=%s", regra, competencia)
            return pd.DataFrame()
        with duckdb.connect(":memory:") as con:
            return con.execute("SELECT * FROM read_csv_auto(?)", [str(path)]).df()

    def listar_competencias(self) -> list[str]:
        """Lista competências disponíveis em gold.evolucao_metricas_mensais, ordem crescente."""
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            df = con.execute(
                "SELECT DISTINCT data_competencia "
                "FROM gold.evolucao_metricas_mensais ORDER BY data_competencia"
            ).df()
        return df["data_competencia"].tolist()

    def listar_competencias_para_regra(self, regra: str) -> list[str]:
        """Lista competências com CSV arquivado para a regra, ordem crescente."""
        nome = CSV_MAP.get(regra)
        if not nome:
            return []
        return sorted(p.parent.name for p in self._historico_dir.glob(f"*/{nome}"))
```

- [ ] **Step 4: Confirmar verde**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py -v
```
Expected: 10 testes passando

- [ ] **Step 5: Rodar suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```
Expected: todos passando, 0 erros

- [ ] **Step 6: Commit**

```bash
git add src/storage/historico_reader.py tests/storage/test_historico_reader.py
git commit -m "feat(storage): HistoricoReader — DuckDB Gold + read_csv_auto para drill-down"
```

---

## Task 3: CSV archiving — `_arquivar_csvs`

**Files:**
- Modify: `src/pipeline/stages/exportacao.py`
- Modify: `tests/pipeline/stages/test_exportacao.py`

- [ ] **Step 1: Escrever os 2 testes que falharão**

Adicionar ao final de `tests/pipeline/stages/test_exportacao.py`:

```python
def test_arquivar_csvs_copia_para_historico(tmp_path):
    # Arrange
    output_dir = tmp_path / "processed"
    output_dir.mkdir()
    (output_dir / "auditoria_rq008_prof_fantasma_cns.csv").write_text(
        "CNS,NOME\n7001234567890123,Ana\n", encoding="utf-8"
    )
    state = _state()
    state.output_path = output_dir / "Relatorio_Profissionais_CNES.csv"

    # Act
    ExportacaoStage()._arquivar_csvs(state, "2024-12")

    # Assert
    dest = output_dir / "historico" / "2024-12" / "auditoria_rq008_prof_fantasma_cns.csv"
    assert dest.exists()
    assert "Ana" in dest.read_text(encoding="utf-8")


def test_arquivar_csvs_ignora_arquivos_ausentes(tmp_path):
    # Arrange — nenhum CSV existe no output_dir
    output_dir = tmp_path / "processed"
    output_dir.mkdir()
    state = _state()
    state.output_path = output_dir / "Relatorio_Profissionais_CNES.csv"

    # Act — não deve levantar exceção
    ExportacaoStage()._arquivar_csvs(state, "2024-12")

    # Assert — diretório criado, sem erro
    assert (output_dir / "historico" / "2024-12").exists()
```

- [ ] **Step 2: Confirmar falha**

```bash
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_exportacao.py::test_arquivar_csvs_copia_para_historico tests/pipeline/stages/test_exportacao.py::test_arquivar_csvs_ignora_arquivos_ausentes -v
```
Expected: FAIL com `AttributeError: 'ExportacaoStage' object has no attribute '_arquivar_csvs'`

- [ ] **Step 3: Adicionar `import shutil`, importar `CSV_MAP` e adicionar `_arquivar_csvs` a `exportacao.py`**

No topo, junto aos demais imports, adicionar:

```python
import shutil

from storage.historico_reader import CSV_MAP
```

No final de `_persistir_historico`, antes do `logger.info`, adicionar:

```python
    self._arquivar_csvs(state, competencia)
```

Adicionar método à classe `ExportacaoStage`:

```python
def _arquivar_csvs(self, state: PipelineState, competencia: str) -> None:
    src_dir = state.output_path.parent
    dest_dir = src_dir / "historico" / competencia
    dest_dir.mkdir(parents=True, exist_ok=True)
    for nome in CSV_MAP.values():
        src = src_dir / nome
        if src.exists():
            shutil.copy2(src, dest_dir / nome)
    logger.info("csvs_arquivados competencia=%s dest=%s", competencia, dest_dir)
```

- [ ] **Step 4: Confirmar verde**

```bash
./venv/Scripts/python.exe -m pytest tests/pipeline/stages/test_exportacao.py -v
```
Expected: todos os 7 testes passando

- [ ] **Step 5: Commit**

```bash
git add src/pipeline/stages/exportacao.py tests/pipeline/stages/test_exportacao.py
git commit -m "feat(exportacao): arquiva CSVs por competencia em historico/"
```

---

## Task 4: Config + Streamlit setup

**Files:**
- Modify: `src/config.py`
- Create: `.streamlit/config.toml`
- Create: `scripts/dashboard.py`

- [ ] **Step 1: Adicionar `HISTORICO_DIR` a `config.py`**

Após a linha `OUTPUT_PATH: Path = ...`, adicionar:

```python
HISTORICO_DIR: Path = OUTPUT_PATH.parent / "historico"
```

- [ ] **Step 2: Criar `.streamlit/config.toml`**

```toml
[theme]
base = "dark"
primaryColor = "#4fa3e0"
backgroundColor = "#0e1117"
secondaryBackgroundColor = "#1a1d27"
textColor = "#fafafa"

[server]
headless = true
port = 8501
```

- [ ] **Step 3: Criar `scripts/dashboard.py`**

```python
"""CnesData Analytics — entry point do dashboard Streamlit."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import streamlit as st

import config
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
    .stDataFrame { color: #1a1a2e !important; }
}
.stDataFrame { overflow-x: auto; }
</style>
"""
st.markdown(_CSS, unsafe_allow_html=True)


@st.cache_resource
def _get_reader() -> HistoricoReader:
    return HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)


if "reader" not in st.session_state:
    st.session_state["reader"] = _get_reader()

st.sidebar.title("CnesData Analytics")
st.sidebar.caption("Presidente Epitácio/SP")
```

- [ ] **Step 4: Verificar que o servidor inicia sem erro**

```bash
./venv/Scripts/python.exe -m streamlit run scripts/dashboard.py --server.headless true &
sleep 3
curl -s http://localhost:8501 | head -5
```
Expected: resposta HTML (não erro de importação)

- [ ] **Step 5: Commit**

```bash
git add src/config.py .streamlit/config.toml scripts/dashboard.py
git commit -m "feat(dashboard): setup Streamlit — dark theme + HistoricoReader session"
```

---

## Task 5: Página 1 — Visão Geral

**Files:**
- Create: `scripts/pages/1_Visao_Geral.py`

- [ ] **Step 1: Criar `scripts/pages/1_Visao_Geral.py`**

```python
"""Página 1 — Visão Geral: KPIs do mês selecionado + tabela de todas as regras."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import pandas as pd
import streamlit as st

from storage.historico_reader import HistoricoReader

_REGRAS_META: dict[str, tuple[str, str]] = {
    "RQ008":     ("Prof Fantasma (CNS)",      "CRÍTICA"),
    "GHOST":     ("Ghost Payroll",             "CRÍTICA"),
    "RQ006":     ("Estab Fantasma",            "ALTA"),
    "RQ007":     ("Estab Ausente Local",       "ALTA"),
    "RQ009":     ("Prof Ausente Local",        "ALTA"),
    "MISSING":   ("Missing Registration",      "ALTA"),
    "RQ005_ACS": ("ACS/TACS Incorretos",       "ALTA"),
    "RQ005_ACE": ("ACE/TACE Incorretos",       "ALTA"),
    "RQ003B":    ("Múltiplas Unidades",        "MÉDIA"),
    "RQ010":     ("Divergência CBO",           "MÉDIA"),
    "RQ011":     ("Divergência CH",            "BAIXA"),
}
_SEV_ORDER = {"CRÍTICA": 0, "ALTA": 1, "MÉDIA": 2, "BAIXA": 3}
_SEV_ICON  = {"CRÍTICA": "🔴", "ALTA": "🟠", "MÉDIA": "🟡", "BAIXA": "🟢"}
_KPI_DESTAQUE = ["RQ008", "RQ006", "RQ009", "GHOST", "MISSING"]

st.title("📊 Visão Geral")

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

competencia = st.sidebar.selectbox(
    "Competência",
    options=competencias[::-1],
    index=0,
)

kpis   = reader.carregar_kpis(competencia)
deltas = reader.carregar_delta(competencia)

cols = st.columns(len(_KPI_DESTAQUE))
for i, regra in enumerate(_KPI_DESTAQUE):
    desc, sev = _REGRAS_META[regra]
    delta = deltas.get(regra, 0)
    with cols[i]:
        st.metric(
            label=f"{_SEV_ICON[sev]} {desc}",
            value=kpis.get(regra, 0),
            delta=f"+{delta}" if delta > 0 else str(delta),
            delta_color="inverse",
            help=f"Regra {regra} — Severidade: {sev}",
        )

st.divider()

rows = [
    {
        "Regra":      regra,
        "Descrição":  desc,
        "Anomalias":  kpis.get(regra, 0),
        "Δ mês":      f"+{deltas.get(regra,0)}" if deltas.get(regra,0) > 0 else str(deltas.get(regra,0)),
        "Severidade": sev,
    }
    for regra, (desc, sev) in sorted(
        _REGRAS_META.items(), key=lambda x: _SEV_ORDER[x[1][1]]
    )
]

st.dataframe(
    pd.DataFrame(rows),
    use_container_width=True,
    hide_index=True,
)
```

- [ ] **Step 2: Verificar visualmente**

```bash
./venv/Scripts/python.exe -m streamlit run scripts/dashboard.py
```
Abrir http://localhost:8501, navegar para "Visão Geral". Confirmar: KPI cards aparecem, tabela com 11 regras ordenadas por severidade.

- [ ] **Step 3: Commit**

```bash
git add scripts/pages/1_Visao_Geral.py
git commit -m "feat(dashboard): página Visão Geral — KPI cards + tabela por severidade"
```

---

## Task 6: Página 2 — Tendências

**Files:**
- Create: `scripts/pages/2_Tendencias.py`

- [ ] **Step 1: Criar `scripts/pages/2_Tendencias.py`**

```python
"""Página 2 — Tendências: gráfico de linhas Plotly multi-regra com filtros."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import plotly.express as px
import streamlit as st

from storage.historico_reader import CSV_MAP, HistoricoReader

_TODAS_REGRAS = list(CSV_MAP.keys())

_CORES: dict[str, str] = {
    "RQ008": "#e74c3c", "GHOST":     "#c0392b",
    "RQ006": "#e67e22", "RQ007":     "#d35400",
    "RQ009": "#f39c12", "MISSING":   "#e74c3c",
    "RQ003B": "#f1c40f", "RQ005_ACS": "#f1c40f", "RQ005_ACE": "#e6b800",
    "RQ010": "#f39c12", "RQ011": "#2ecc71",
}

st.title("📈 Tendências")

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

regras_sel = st.sidebar.multiselect(
    "Regras",
    options=_TODAS_REGRAS,
    default=["RQ008", "RQ006", "RQ009"],
)
comp_ini = st.sidebar.selectbox("De", options=competencias, index=0)
comp_fim = st.sidebar.selectbox("Até", options=competencias, index=len(competencias) - 1)

if not regras_sel:
    st.info("Selecione ao menos uma regra na sidebar.")
    st.stop()

df = reader.carregar_tendencias(regras_sel, comp_ini, comp_fim)

if df.empty:
    st.info("Sem dados para o período e regras selecionados.")
    st.stop()

if df["data_competencia"].nunique() < 2:
    st.info("Selecione ao menos 2 competências para visualizar a tendência.")

color_map = {r: _CORES.get(r, "#95a5a6") for r in regras_sel}
fig = px.line(
    df,
    x="data_competencia",
    y="total_anomalias",
    color="regra",
    markers=True,
    color_discrete_map=color_map,
    labels={
        "data_competencia": "Competência",
        "total_anomalias":  "Anomalias",
        "regra":            "Regra",
    },
    template="plotly_dark",
)
fig.update_layout(hovermode="x unified", legend_title_text="Regra")
st.plotly_chart(fig, use_container_width=True)

if st.checkbox("Mostrar dados brutos"):
    st.dataframe(df, use_container_width=True, hide_index=True)

st.download_button(
    "⬇ Exportar tabela",
    data=df.to_csv(index=False).encode("utf-8-sig"),
    file_name="tendencias_cnes.csv",
    mime="text/csv",
)
```

- [ ] **Step 2: Verificar visualmente**

Abrir http://localhost:8501, navegar para "Tendências". Confirmar: gráfico Plotly responsivo, multiselect funcional, download CSV funciona.

- [ ] **Step 3: Commit**

```bash
git add scripts/pages/2_Tendencias.py
git commit -m "feat(dashboard): página Tendências — Plotly multi-regra + export CSV"
```

---

## Task 7: Página 3 — Por Regra

**Files:**
- Create: `scripts/pages/3_Por_Regra.py`

- [ ] **Step 1: Criar `scripts/pages/3_Por_Regra.py`**

```python
"""Página 3 — Por Regra: drill-down de registros individuais com máscara CPF/CNS."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st

from storage.historico_reader import CSV_MAP, HistoricoReader

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

st.title("🔍 Por Regra")

reader: HistoricoReader = st.session_state["reader"]

regra = st.sidebar.selectbox(
    "Regra",
    options=list(_REGRA_DESC.keys()),
    format_func=lambda r: _REGRA_DESC[r],
)

disponiveis = reader.listar_competencias_para_regra(regra)

if not disponiveis:
    st.warning(
        f"Sem registros arquivados para **{_REGRA_DESC[regra]}**. "
        "Execute o pipeline para gerar o histórico."
    )
    st.stop()

competencia = st.sidebar.selectbox(
    "Competência",
    options=disponiveis[::-1],
    index=0,
)

kpis   = reader.carregar_kpis(competencia)
deltas = reader.carregar_delta(competencia)
valor  = kpis.get(regra, 0)
delta  = deltas.get(regra, 0)

col_metric, _ = st.columns([1, 4])
with col_metric:
    st.metric(
        label=_REGRA_DESC[regra],
        value=valor,
        delta=f"+{delta}" if delta > 0 else str(delta),
        delta_color="inverse",
    )

df = reader.carregar_registros(regra, competencia)

if df.empty:
    st.warning(f"Sem registros para {_REGRA_DESC[regra]} em {competencia}.")
    st.stop()

mostrar_completo = st.checkbox("Mostrar dados completos (CPF/CNS sem máscara)")
df_display = df.copy()
if not mostrar_completo:
    for col in df_display.select_dtypes(include="object").columns:
        upper = col.upper()
        if "CPF" in upper or "CNS" in upper:
            df_display[col] = df_display[col].apply(
                lambda v: f"***{str(v)[-4:]}" if isinstance(v, str) and len(v) >= 4 else v
            )

st.dataframe(df_display, use_container_width=True, hide_index=True)

st.download_button(
    f"⬇ Baixar CSV — {regra} / {competencia}",
    data=df.to_csv(index=False).encode("utf-8-sig"),
    file_name=f"auditoria_{regra.lower()}_{competencia}.csv",
    mime="text/csv",
)
```

- [ ] **Step 2: Verificar visualmente**

Abrir http://localhost:8501, navegar para "Por Regra". Confirmar: selectbox de regra e competência, tabela com máscara CPF/CNS, checkbox "mostrar completo" funciona, download CSV correto.

- [ ] **Step 3: Commit**

```bash
git add scripts/pages/3_Por_Regra.py
git commit -m "feat(dashboard): página Por Regra — drill-down + máscara CPF/CNS + download"
```

---

## Task 8: Requirements + README

**Files:**
- Modify: `requirements.txt`
- Modify: `README.md`

- [ ] **Step 1: Adicionar dependências a `requirements.txt`**

Adicionar ao final do arquivo:

```
streamlit>=1.32.0
plotly>=5.20.0
```

- [ ] **Step 2: Instalar**

```bash
./venv/Scripts/pip.exe install streamlit>=1.32.0 "plotly>=5.20.0"
```

- [ ] **Step 3: Adicionar seção ao `README.md`**

Após a seção de comandos existente, adicionar:

```markdown
## Dashboard Analítico

Visualização interativa de tendências e drill-down de anomalias (requer ao menos uma execução do pipeline):

```bash
./venv/Scripts/streamlit.exe run scripts/dashboard.py
```

Abre automaticamente em http://localhost:8501. Três páginas:
- **Visão Geral** — KPIs do mês selecionado + tabela por severidade
- **Tendências** — gráfico de linhas multi-regra (Plotly, responsivo)
- **Por Regra** — drill-down de registros individuais com download CSV
```

- [ ] **Step 4: Rodar suite final**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
./venv/Scripts/ruff.exe check src/ tests/
```
Expected: todos os testes passando, zero erros de lint

- [ ] **Step 5: Commit final**

```bash
git add requirements.txt README.md
git commit -m "feat(dashboard): add streamlit/plotly deps + README section"
```
