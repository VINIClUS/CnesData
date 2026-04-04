# Glosas & Métricas Avançadas no Dashboard — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expor `gold.glosas_profissional` e `gold.metricas_avancadas` no dashboard Streamlit com duas novas páginas — Glosas (drill-down por profissional com filtros) e Métricas Avançadas (KPIs e charts Plotly).

**Architecture:** `HistoricoReader` recebe dois novos métodos de leitura. Funções puras de filtragem e parsing são extraídas em `scripts/glosas_helpers.py` e `scripts/metricas_helpers.py` (padrão do `dashboard_status.py` existente) — isso permite testes sem depender do runtime Streamlit, já que módulos Python não podem importar arquivos com nome iniciando em dígito. As páginas importam esses helpers. Nenhum novo package necessário — Plotly e st-aggrid já instalados.

**Tech Stack:** Python 3.11+, DuckDB, pandas, Streamlit, Plotly Express, st-aggrid (já instalados)

---

## File Map

### Novos
| Arquivo | Responsabilidade |
|---|---|
| `scripts/glosas_helpers.py` | Funções puras: `_filtrar_glosas`, `_mascarar_pii_glosas` |
| `scripts/metricas_helpers.py` | Função pura: `_parsear_metricas` |
| `scripts/pages/4_Glosas.py` | Página Streamlit — importa `glosas_helpers`, AgGrid, download CSV |
| `scripts/pages/5_Metricas.py` | Página Streamlit — importa `metricas_helpers`, KPI cards, charts Plotly |
| `tests/scripts/test_glosas_helpers.py` | Testes de `glosas_helpers.py` |
| `tests/scripts/test_metricas_helpers.py` | Testes de `metricas_helpers.py` |

### Alterados
| Arquivo | Mudança |
|---|---|
| `src/storage/historico_reader.py` | `carregar_glosas_historicas` + parâmetro `regra`; novo método `carregar_metricas_avancadas` |
| `tests/storage/test_historico_reader.py` | Testes para o novo parâmetro e novo método |

---

## Task 1: `carregar_glosas_historicas` — adicionar filtro por `regra`

**Files:**
- Modify: `src/storage/historico_reader.py:202-225`
- Test: `tests/storage/test_historico_reader.py`

- [ ] **Step 1: Escrever testes que falham**

Adicionar ao final da classe `TestCarregarGlosasHistoricas` em `tests/storage/test_historico_reader.py`:

```python
def test_filtrar_por_regra_retorna_apenas_regra_solicitada(self, tmp_path):
    db = tmp_path / "test_glosas_regra.duckdb"
    _popular_duckdb_com_glosas(db)
    agora = datetime(2026, 3, 1)
    with duckdb.connect(str(db)) as con:
        con.execute(
            "INSERT INTO gold.glosas_profissional VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ["2026-03", "RQ008", "11111111111", "7001111111111111",
             "Carlos", "M", "2795001", "Motivo A", agora, agora, agora],
        )
        con.execute(
            "INSERT INTO gold.glosas_profissional VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ["2026-03", "RQ009", "22222222222", "7002222222222222",
             "Dora", "F", "2795002", "Motivo B", agora, agora, agora],
        )
    reader = HistoricoReader(db, tmp_path / "historico")
    df = reader.carregar_glosas_historicas(regra="RQ008")
    assert len(df) == 1
    assert df.iloc[0]["regra"] == "RQ008"

def test_filtrar_por_regra_e_competencia(self, tmp_path):
    db = tmp_path / "test_glosas_regra_comp.duckdb"
    _popular_duckdb_com_glosas(db)
    agora = datetime(2026, 3, 1)
    with duckdb.connect(str(db)) as con:
        con.execute(
            "INSERT INTO gold.glosas_profissional VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ["2026-02", "RQ008", "11111111111", "7001111111111111",
             "Carlos", "M", "2795001", "Motivo A", agora, agora, agora],
        )
        con.execute(
            "INSERT INTO gold.glosas_profissional VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            ["2026-03", "RQ008", "22222222222", "7002222222222222",
             "Dora", "F", "2795002", "Motivo B", agora, agora, agora],
        )
    reader = HistoricoReader(db, tmp_path / "historico")
    df = reader.carregar_glosas_historicas(competencia_inicio="2026-03", regra="RQ008")
    assert len(df) == 1
    assert df.iloc[0]["competencia"] == "2026-03"

def test_regra_none_retorna_todas_as_regras(self, tmp_path):
    db = tmp_path / "test_glosas_todas.duckdb"
    _popular_duckdb_com_glosas(db)
    agora = datetime(2026, 3, 1)
    with duckdb.connect(str(db)) as con:
        for regra in ("RQ008", "RQ009", "GHOST"):
            con.execute(
                "INSERT INTO gold.glosas_profissional VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                ["2026-03", regra, "11111111111", None,
                 "Profissional", "M", "2795001", "Motivo", agora, agora, agora],
            )
    reader = HistoricoReader(db, tmp_path / "historico")
    df = reader.carregar_glosas_historicas()
    assert len(df) == 3
```

- [ ] **Step 2: Rodar e confirmar falha**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py::TestCarregarGlosasHistoricas -q --tb=short
```

Esperado: 3 FAILs — `carregar_glosas_historicas() got an unexpected keyword argument 'regra'`.

- [ ] **Step 3: Adicionar parâmetro `regra` ao método**

Em `src/storage/historico_reader.py`, substituir a assinatura e corpo de `carregar_glosas_historicas`:

```python
def carregar_glosas_historicas(
    self,
    competencia_inicio: str | None = None,
    regra: str | None = None,
) -> pd.DataFrame:
    """Retorna todas as glosas de gold.glosas_profissional.

    Args:
        competencia_inicio: Filtra competencias >= este valor (YYYY-MM). None retorna todas.
        regra: Filtra por regra específica (ex.: 'RQ008'). None retorna todas.

    Returns:
        DataFrame com todas as colunas de gold.glosas_profissional.
    """
    conditions: list[str] = []
    params: list = []
    if competencia_inicio:
        conditions.append("competencia >= ?")
        params.append(competencia_inicio)
    if regra:
        conditions.append("regra = ?")
        params.append(regra)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM gold.glosas_profissional {where}"
    try:
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            return con.execute(sql, params).df()
    except duckdb.CatalogException:
        return pd.DataFrame()
```

- [ ] **Step 4: Rodar e confirmar aprovação**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py::TestCarregarGlosasHistoricas -q --tb=short
```

Esperado: todos os testes da classe passam (incluindo os pré-existentes).

- [ ] **Step 5: Commit**

```bash
git add src/storage/historico_reader.py tests/storage/test_historico_reader.py
git commit -m "feat(storage): carregar_glosas_historicas aceita filtro por regra"
```

---

## Task 2: `HistoricoReader.carregar_metricas_avancadas()`

**Files:**
- Modify: `src/storage/historico_reader.py` (append método)
- Test: `tests/storage/test_historico_reader.py` (append classe)

- [ ] **Step 1: Escrever testes que falham**

Adicionar ao final de `tests/storage/test_historico_reader.py`:

```python
def _popular_duckdb_com_metricas(path: Path) -> None:
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute("""
            CREATE TABLE gold.metricas_avancadas (
                competencia                      VARCHAR PRIMARY KEY,
                taxa_anomalia_geral              DOUBLE,
                p90_ch_total                     DOUBLE,
                proporcao_feminina_geral         DOUBLE,
                n_reincidentes                   INTEGER,
                taxa_resolucao                   DOUBLE,
                velocidade_regularizacao_media   DOUBLE,
                top_glosas_json                  VARCHAR,
                anomalias_por_cbo_json           VARCHAR,
                proporcao_feminina_por_cnes_json VARCHAR,
                ranking_cnes_json                VARCHAR,
                gravado_em                       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """)


class TestCarregarMetricasAvancadas:

    def test_retorna_dict_quando_competencia_existe(self, tmp_path):
        db = tmp_path / "test_metricas.duckdb"
        _popular_duckdb_com_metricas(db)
        with duckdb.connect(str(db)) as con:
            con.execute(
                """INSERT INTO gold.metricas_avancadas
                   (competencia, taxa_anomalia_geral, p90_ch_total,
                    proporcao_feminina_geral, n_reincidentes, taxa_resolucao,
                    velocidade_regularizacao_media, top_glosas_json,
                    anomalias_por_cbo_json, proporcao_feminina_por_cnes_json,
                    ranking_cnes_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                ["2026-03", 0.15, 40.0, 0.62, 3, 0.80, 12.5,
                 '[{"regra": "RQ008", "n": 5}]',
                 '{"225125": 3, "225170": 2}',
                 '{"2795001": 0.7}',
                 '[{"cnes": "2795001", "n": 8}]'],
            )
        reader = HistoricoReader(db, tmp_path / "historico")
        result = reader.carregar_metricas_avancadas("2026-03")
        assert result is not None
        assert result["taxa_anomalia_geral"] == pytest.approx(0.15)
        assert result["n_reincidentes"] == 3
        assert result["top_glosas_json"] == '[{"regra": "RQ008", "n": 5}]'

    def test_retorna_none_quando_competencia_ausente(self, tmp_path):
        db = tmp_path / "test_metricas_vazio.duckdb"
        _popular_duckdb_com_metricas(db)
        reader = HistoricoReader(db, tmp_path / "historico")
        assert reader.carregar_metricas_avancadas("2026-03") is None

    def test_retorna_none_quando_tabela_nao_existe(self, tmp_path):
        db = tmp_path / "test_metricas_sem_tabela.duckdb"
        with duckdb.connect(str(db)) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        reader = HistoricoReader(db, tmp_path / "historico")
        assert reader.carregar_metricas_avancadas("2026-03") is None
```

Certifique-se de que `pytest` está importado no arquivo (já deve estar no início).

- [ ] **Step 2: Rodar e confirmar falha**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py::TestCarregarMetricasAvancadas -q --tb=short
```

Esperado: 3 FAILs — `HistoricoReader has no attribute 'carregar_metricas_avancadas'`.

- [ ] **Step 3: Adicionar método ao HistoricoReader**

Adicionar após `carregar_glosas_historicas` em `src/storage/historico_reader.py`:

```python
def carregar_metricas_avancadas(self, competencia: str) -> dict | None:
    """Retorna as métricas avançadas de uma competência específica.

    Args:
        competencia: Competência no formato YYYY-MM.

    Returns:
        Dict com todas as colunas de gold.metricas_avancadas, ou None se ausente.
    """
    try:
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            df = con.execute(
                "SELECT * FROM gold.metricas_avancadas WHERE competencia = ?",
                [competencia],
            ).df()
    except duckdb.CatalogException:
        return None
    if df.empty:
        return None
    return df.iloc[0].to_dict()
```

- [ ] **Step 4: Rodar e confirmar aprovação**

```
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py::TestCarregarMetricasAvancadas -q --tb=short
```

Esperado: 3 passed.

- [ ] **Step 5: Rodar suite completa**

```
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Esperado: todos os testes passam.

- [ ] **Step 6: Commit**

```bash
git add src/storage/historico_reader.py tests/storage/test_historico_reader.py
git commit -m "feat(storage): carregar_metricas_avancadas para gold.metricas_avancadas"
```

---

## Task 3: `glosas_helpers.py` + `4_Glosas.py`

**Files:**
- Create: `scripts/glosas_helpers.py`
- Create: `scripts/pages/4_Glosas.py`
- Create: `tests/scripts/test_glosas_helpers.py`

- [ ] **Step 1: Escrever testes que falham**

Criar `tests/scripts/test_glosas_helpers.py`:

```python
"""Testes das funções auxiliares de glosas."""
import pandas as pd

from glosas_helpers import _filtrar_glosas, _mascarar_pii_glosas


def _df_base() -> pd.DataFrame:
    return pd.DataFrame({
        "regra":             ["RQ008",   "RQ009",   "GHOST"],
        "cpf":               ["11111111111", "22222222222", "33333333333"],
        "nome_profissional": ["Ana Silva", "Bruno Costa", "Carla Dias"],
        "cnes_estabelecimento": ["2795001", "2795002", "2795001"],
        "motivo":            ["A", "B", "C"],
    })


class TestFiltrarGlosas:

    def test_sem_filtro_retorna_tudo(self):
        assert len(_filtrar_glosas(_df_base(), [], "")) == 3

    def test_filtro_por_regra(self):
        result = _filtrar_glosas(_df_base(), ["RQ008", "GHOST"], "")
        assert set(result["regra"]) == {"RQ008", "GHOST"}

    def test_busca_por_nome_case_insensitive(self):
        result = _filtrar_glosas(_df_base(), [], "ana")
        assert len(result) == 1
        assert result.iloc[0]["nome_profissional"] == "Ana Silva"

    def test_busca_por_cpf(self):
        result = _filtrar_glosas(_df_base(), [], "222")
        assert len(result) == 1

    def test_regra_e_busca_sem_intersecao(self):
        result = _filtrar_glosas(_df_base(), ["RQ008"], "carla")
        assert len(result) == 0

    def test_df_vazio_retorna_vazio(self):
        df = pd.DataFrame(columns=["regra", "nome_profissional", "cpf"])
        assert _filtrar_glosas(df, ["RQ008"], "").empty


class TestMascaraGlosas:

    def _df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "cpf": ["12345678901"],
            "cns": ["7001234567890123"],
            "nome": ["Ana Silva"],
        })

    def test_mascara_cpf_e_cns(self):
        result = _mascarar_pii_glosas(self._df(), mostrar_completo=False)
        assert result.iloc[0]["cpf"].startswith("***")
        assert result.iloc[0]["cns"].startswith("***")

    def test_sem_mascara_preserva_valores(self):
        result = _mascarar_pii_glosas(self._df(), mostrar_completo=True)
        assert result.iloc[0]["cpf"] == "12345678901"

    def test_nao_muta_df_original(self):
        df = self._df()
        _mascarar_pii_glosas(df, mostrar_completo=False)
        assert df.iloc[0]["cpf"] == "12345678901"
```

- [ ] **Step 2: Rodar e confirmar falha**

```
./venv/Scripts/python.exe -m pytest tests/scripts/test_glosas_helpers.py -q --tb=short
```

Esperado: `ModuleNotFoundError: No module named 'glosas_helpers'`.

- [ ] **Step 3: Criar `scripts/glosas_helpers.py`**

```python
"""Funções puras de filtragem e mascaramento PII para a página Glosas."""
import pandas as pd


def _filtrar_glosas(df: pd.DataFrame, regras: list[str], busca: str) -> pd.DataFrame:
    if regras:
        df = df[df["regra"].isin(regras)]
    if busca:
        termo = busca.lower()
        mask = (
            df["nome_profissional"].str.lower().str.contains(termo, na=False)
            | df["cpf"].str.contains(termo, na=False)
        )
        df = df[mask]
    return df


def _mascarar_pii_glosas(df: pd.DataFrame, mostrar_completo: bool) -> pd.DataFrame:
    if mostrar_completo:
        return df
    df_display = df.copy()
    for col in ("cpf", "cns"):
        if col in df_display.columns:
            df_display[col] = df_display[col].apply(
                lambda v: f"***{str(v)[-4:]}" if isinstance(v, str) and len(v) >= 4 else v
            )
    return df_display
```

- [ ] **Step 4: Rodar e confirmar aprovação**

```
./venv/Scripts/python.exe -m pytest tests/scripts/test_glosas_helpers.py -q --tb=short
```

Esperado: 9 passed.

- [ ] **Step 5: Criar `scripts/pages/4_Glosas.py`**

```python
"""Página 4 — Glosas: drill-down individual por profissional/competência."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder

import config
from glosas_helpers import _filtrar_glosas, _mascarar_pii_glosas
from storage.historico_reader import CSV_MAP, HistoricoReader

_TODAS_REGRAS = list(CSV_MAP.keys())

st.title("Glosas por Profissional")

if "reader" not in st.session_state:
    st.session_state["reader"] = HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias_validas()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

competencia = st.sidebar.selectbox("Competência", options=competencias[::-1], index=0)
regras_sel = st.sidebar.multiselect("Regra", options=_TODAS_REGRAS, default=[])
busca = st.sidebar.text_input("Buscar por nome ou CPF")


@st.cache_data(ttl=300, show_spinner=False)
def _carregar(comp: str) -> object:
    return reader.carregar_glosas_historicas(competencia_inicio=comp)


df_raw = _carregar(competencia)
df_raw = df_raw[df_raw["competencia"] == competencia] if not df_raw.empty else df_raw
df_filtrado = _filtrar_glosas(df_raw, regras_sel, busca)

st.metric("Glosas encontradas", len(df_filtrado))

if df_filtrado.empty:
    st.info("Sem glosas para os filtros selecionados. Ajuste os filtros ou execute o pipeline.")
    st.stop()

mostrar_completo = st.checkbox("Mostrar dados completos (CPF/CNS sem máscara)")
df_display = _mascarar_pii_glosas(df_filtrado, mostrar_completo)

gb = GridOptionsBuilder.from_dataframe(df_display)
gb.configure_default_column(resizable=True, sortable=True, filter=True)
gb.configure_grid_options(domLayout="autoHeight")
AgGrid(
    df_display,
    gridOptions=gb.build(),
    use_container_width=True,
    fit_columns_on_grid_load=False,
    theme="streamlit",
    key="grid_glosas",
)

st.download_button(
    "Baixar CSV",
    data=df_filtrado.to_csv(index=False).encode("utf-8-sig"),
    file_name=f"glosas_{competencia}.csv",
    mime="text/csv",
)
```

- [ ] **Step 6: Lint**

```
./venv/Scripts/ruff.exe check scripts/glosas_helpers.py scripts/pages/4_Glosas.py tests/scripts/test_glosas_helpers.py
```

- [ ] **Step 7: Commit**

```bash
git add scripts/glosas_helpers.py scripts/pages/4_Glosas.py tests/scripts/test_glosas_helpers.py
git commit -m "feat(dashboard): pagina Glosas com filtros, AgGrid e mascaramento PII"
```

---

## Task 4: `metricas_helpers.py` + `5_Metricas.py`

**Files:**
- Create: `scripts/metricas_helpers.py`
- Create: `scripts/pages/5_Metricas.py`
- Create: `tests/scripts/test_metricas_helpers.py`

- [ ] **Step 1: Escrever testes que falham**

Criar `tests/scripts/test_metricas_helpers.py`:

```python
"""Testes das funções auxiliares de métricas avançadas."""
import pytest

from metricas_helpers import _parsear_metricas


def _raw() -> dict:
    return {
        "competencia":                   "2026-03",
        "taxa_anomalia_geral":            0.15,
        "p90_ch_total":                   40.0,
        "proporcao_feminina_geral":        0.62,
        "n_reincidentes":                 3,
        "taxa_resolucao":                 0.80,
        "velocidade_regularizacao_media":  12.5,
        "top_glosas_json":                '[{"regra": "RQ008", "n": 5}]',
        "anomalias_por_cbo_json":         '{"225125": 3, "225170": 2}',
        "proporcao_feminina_por_cnes_json": '{"2795001": 0.7}',
        "ranking_cnes_json":              '[{"cnes": "2795001", "n": 8}]',
        "gravado_em":                     None,
    }


class TestParsearMetricas:

    def test_campos_numericos_preservados(self):
        result = _parsear_metricas(_raw())
        assert result["taxa_anomalia_geral"] == pytest.approx(0.15)
        assert result["n_reincidentes"] == 3

    def test_top_glosas_e_lista_parseada(self):
        result = _parsear_metricas(_raw())
        assert isinstance(result["top_glosas"], list)
        assert result["top_glosas"][0]["regra"] == "RQ008"

    def test_anomalias_por_cbo_e_dict_parseado(self):
        result = _parsear_metricas(_raw())
        assert isinstance(result["anomalias_por_cbo"], dict)
        assert result["anomalias_por_cbo"]["225125"] == 3

    def test_ranking_cnes_e_lista_parseada(self):
        result = _parsear_metricas(_raw())
        assert isinstance(result["ranking_cnes"], list)
        assert result["ranking_cnes"][0]["cnes"] == "2795001"

    def test_proporcao_feminina_por_cnes_e_dict_parseado(self):
        result = _parsear_metricas(_raw())
        assert isinstance(result["proporcao_feminina_por_cnes"], dict)
        assert result["proporcao_feminina_por_cnes"]["2795001"] == pytest.approx(0.7)

    def test_json_nulo_retorna_estruturas_vazias(self):
        raw = _raw()
        raw["top_glosas_json"] = None
        raw["anomalias_por_cbo_json"] = None
        raw["ranking_cnes_json"] = None
        raw["proporcao_feminina_por_cnes_json"] = None
        result = _parsear_metricas(raw)
        assert result["top_glosas"] == []
        assert result["anomalias_por_cbo"] == {}
        assert result["ranking_cnes"] == []
        assert result["proporcao_feminina_por_cnes"] == {}
```

- [ ] **Step 2: Rodar e confirmar falha**

```
./venv/Scripts/python.exe -m pytest tests/scripts/test_metricas_helpers.py -q --tb=short
```

Esperado: `ModuleNotFoundError: No module named 'metricas_helpers'`.

- [ ] **Step 3: Criar `scripts/metricas_helpers.py`**

```python
"""Funções puras de parsing de métricas avançadas para a página Métricas."""
import json


def _parsear_metricas(raw: dict) -> dict:
    def _json_list(val) -> list:
        return json.loads(val) if val else []

    def _json_dict(val) -> dict:
        return json.loads(val) if val else {}

    return {
        **raw,
        "top_glosas":                  _json_list(raw.get("top_glosas_json")),
        "anomalias_por_cbo":           _json_dict(raw.get("anomalias_por_cbo_json")),
        "proporcao_feminina_por_cnes":  _json_dict(raw.get("proporcao_feminina_por_cnes_json")),
        "ranking_cnes":                _json_list(raw.get("ranking_cnes_json")),
    }
```

- [ ] **Step 4: Rodar e confirmar aprovação**

```
./venv/Scripts/python.exe -m pytest tests/scripts/test_metricas_helpers.py -q --tb=short
```

Esperado: 6 passed.

- [ ] **Step 5: Criar `scripts/pages/5_Metricas.py`**

```python
"""Página 5 — Métricas Avançadas: KPIs estatísticos e charts Plotly."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
import plotly.express as px
import streamlit as st

import config
from metricas_helpers import _parsear_metricas
from storage.historico_reader import HistoricoReader


def _pct(v) -> str:
    return f"{v:.1%}" if v is not None else "—"


def _num(v, decimais: int = 1) -> str:
    return f"{v:.{decimais}f}" if v is not None else "—"


st.title("Métricas Avançadas")

if "reader" not in st.session_state:
    st.session_state["reader"] = HistoricoReader(config.DUCKDB_PATH, config.HISTORICO_DIR)

reader: HistoricoReader = st.session_state["reader"]
competencias = reader.listar_competencias_validas()

if not competencias:
    st.warning("Nenhuma competência no DuckDB. Execute o pipeline ao menos uma vez.")
    st.stop()

competencia = st.sidebar.selectbox("Competência", options=competencias[::-1], index=0)
raw = reader.carregar_metricas_avancadas(competencia)

if raw is None:
    st.info(
        f"Sem métricas avançadas para {competencia}. "
        "Execute o pipeline para esta competência e tente novamente."
    )
    st.stop()

m = _parsear_metricas(raw)

st.subheader("KPIs")
col1, col2, col3 = st.columns(3)
col1.metric("Taxa de Anomalia Geral",       _pct(m.get("taxa_anomalia_geral")))
col2.metric("P90 Carga Horária (h/semana)", _num(m.get("p90_ch_total")))
col3.metric("Proporção Feminina Geral",     _pct(m.get("proporcao_feminina_geral")))

col4, col5, col6 = st.columns(3)
col4.metric("Reincidentes",                 str(m.get("n_reincidentes", "—")))
col5.metric("Taxa de Resolução",            _pct(m.get("taxa_resolucao")))
col6.metric("Velocidade Regularização (d)", _num(m.get("velocidade_regularizacao_media")))

if m["ranking_cnes"]:
    st.subheader("Ranking CNES por Anomalias")
    df_rank = pd.DataFrame(m["ranking_cnes"])
    st.plotly_chart(
        px.bar(df_rank, x="cnes", y="n", labels={"cnes": "CNES", "n": "Anomalias"}),
        use_container_width=True,
    )

if m["anomalias_por_cbo"]:
    st.subheader("Top 15 CBOs com Anomalias")
    df_cbo = (
        pd.DataFrame([{"cbo": k, "n": v} for k, v in m["anomalias_por_cbo"].items()])
        .sort_values("n", ascending=False)
        .head(15)
    )
    st.plotly_chart(
        px.bar(df_cbo, x="cbo", y="n", labels={"cbo": "CBO", "n": "Anomalias"}),
        use_container_width=True,
    )

if m["top_glosas"]:
    st.subheader("Top Glosas por Regra")
    st.dataframe(pd.DataFrame(m["top_glosas"]), use_container_width=True)
```

- [ ] **Step 6: Suite completa + lint**

```
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
./venv/Scripts/ruff.exe check scripts/metricas_helpers.py scripts/pages/5_Metricas.py tests/scripts/test_metricas_helpers.py
```

Esperado: todos os testes passam, sem erros de lint.

- [ ] **Step 7: Commit final**

```bash
git add scripts/metricas_helpers.py scripts/pages/5_Metricas.py tests/scripts/test_metricas_helpers.py
git commit -m "feat(dashboard): pagina Metricas Avancadas com KPIs e charts Plotly"
```
