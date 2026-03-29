# Competência Precision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Filtrar competências CNES Local pela janela de coleta (`gravado_em`), exibir cobertura real no card DuckDB e buscar range BigQuery diretamente no dashboard com cache diário.

**Architecture:** `competencia_utils.py` centraliza o cálculo do 5º dia útil (BR holidays + `lru_cache`). `HistoricoReader` ganha `listar_competencias_validas()` e `contar_competencias()`. `dashboard_status.py` ganha `_executar_range_query()` (testável) + `_consultar_range_bigquery()` (cache Streamlit). `renderizar_container_status()` recebe `cobertura: tuple[int, int]` e passa range real ao card CNES Nacional.

**Tech Stack:** Python 3.11+, DuckDB, `holidays>=0.46`, `basedosdados` (já instalado), Streamlit `@st.cache_data`.

---

## File Map

| Ação | Arquivo | Responsabilidade |
|---|---|---|
| Modificar | `requirements.txt` | Adicionar `holidays>=0.46` |
| Criar | `src/storage/competencia_utils.py` | `quinto_dia_util`, `janela_valida` |
| Modificar | `src/storage/historico_reader.py` | `listar_competencias_validas`, `contar_competencias` |
| Modificar | `scripts/dashboard_status.py` | `_executar_range_query`, `_consultar_range_bigquery`, `renderizar_container_status` atualizado |
| Modificar | `scripts/dashboard.py` | Trocar `listar_competencias` → `listar_competencias_validas`, passar `cobertura` |
| Modificar | `scripts/pages/1_Tendencias.py` | Trocar `listar_competencias` → `listar_competencias_validas` |
| Modificar | `scripts/pages/2_Por_Regra.py` | Trocar `listar_competencias` → `listar_competencias_validas` |
| Criar | `tests/storage/test_competencia_utils.py` | Testes TDD para utils |
| Modificar | `tests/storage/test_historico_reader.py` | Testes para novos métodos |
| Modificar | `tests/scripts/test_dashboard_status.py` | Testes para range BQ |

---

## Task 1 — requirements.txt: adicionar holidays

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Adicionar dependência**

Abrir `requirements.txt` e adicionar após `streamlit-aggrid==0.3.4.post3`:
```
holidays>=0.46
```

- [ ] **Step 2: Instalar**

```bash
./venv/Scripts/pip.exe install "holidays>=0.46"
```

Saída esperada: `Successfully installed holidays-X.X` (ou "already satisfied").

- [ ] **Step 3: Verificar import**

```bash
./venv/Scripts/python.exe -c "import holidays; print(holidays.__version__)"
```

Saída esperada: versão ≥ 0.46.

- [ ] **Step 4: Commit**

```bash
git add requirements.txt
git commit -m "feat(deps): adicionar holidays>=0.46 para cálculo de dias úteis BR"
```

---

## Task 2 — competencia_utils.py: quinto_dia_util + janela_valida (TDD)

**Files:**
- Create: `src/storage/competencia_utils.py`
- Create: `tests/storage/test_competencia_utils.py`

- [ ] **Step 1: Escrever testes RED**

Criar `tests/storage/test_competencia_utils.py`:

```python
"""Testes de competencia_utils — cálculo do 5º dia útil e janela de coleta."""
from datetime import date

import pytest

from storage.competencia_utils import janela_valida, quinto_dia_util


class TestQuintoDiaUtil:

    def test_janeiro_2026_com_feriado_ano_novo(self):
        # 01/01 feriado → dias úteis: 02(sex),05(seg),06(ter),07(qua),08(qui)
        assert quinto_dia_util(2026, 1) == date(2026, 1, 8)

    def test_junho_2026_sem_feriados_nacionais(self):
        # jun/2026 sem feriados: 01(seg),02(ter),03(qua),04(qui),05(sex)
        assert quinto_dia_util(2026, 6) == date(2026, 6, 5)

    def test_novembro_2024_com_finados(self):
        # 02/11/2024 = sábado (não afeta); 15/11 = sex (feriado, mas cai após 5º)
        # dias úteis: 01(sex),04(seg),05(ter),06(qua),07(qui)
        assert quinto_dia_util(2024, 11) == date(2024, 11, 7)

    def test_dezembro_2024(self):
        # dez/2024: 02(seg),03(ter),04(qua),05(qui),06(sex)
        assert quinto_dia_util(2024, 12) == date(2024, 12, 6)

    def test_lru_cache_retorna_mesmo_objeto(self):
        r1 = quinto_dia_util(2026, 6)
        r2 = quinto_dia_util(2026, 6)
        assert r1 is r2


class TestJanelaValida:

    def test_inicio_e_fim_corretos(self):
        inicio, fim = janela_valida("2026-06")
        assert inicio == quinto_dia_util(2026, 6)
        assert fim == quinto_dia_util(2026, 7)

    def test_virada_de_ano(self):
        inicio, fim = janela_valida("2026-12")
        assert inicio == quinto_dia_util(2026, 12)
        assert fim == quinto_dia_util(2027, 1)

    def test_novembro_2024_janela(self):
        inicio, fim = janela_valida("2024-11")
        assert inicio == date(2024, 11, 7)
        assert fim == date(2024, 12, 6)
```

- [ ] **Step 2: Confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_competencia_utils.py -x --tb=short -q
```

Saída esperada: `ImportError: No module named 'storage.competencia_utils'`

- [ ] **Step 3: Criar `src/storage/competencia_utils.py`**

```python
"""Utilitários de competência — janela de coleta CNES Local."""
from datetime import date, timedelta
from functools import lru_cache

import holidays

_BR = holidays.country_holidays("BR")


@lru_cache(maxsize=128)
def quinto_dia_util(ano: int, mes: int) -> date:
    """Retorna o 5º dia útil (seg–sex, sem feriados nacionais BR) do mês.

    Args:
        ano: Ano calendário.
        mes: Mês calendário 1–12.

    Returns:
        Data do 5º dia útil do mês.
    """
    dia = date(ano, mes, 1)
    count = 0
    while count < 5:
        if dia.weekday() < 5 and dia not in _BR:
            count += 1
        if count < 5:
            dia += timedelta(days=1)
    return dia


def janela_valida(competencia: str) -> tuple[date, date]:
    """Retorna (início_inclusivo, fim_exclusivo) da janela de coleta.

    Args:
        competencia: Competência no formato YYYY-MM.

    Returns:
        Tupla (inicio, fim): inicio inclusivo, fim exclusivo.
    """
    ano, mes = int(competencia[:4]), int(competencia[5:7])
    inicio = quinto_dia_util(ano, mes)
    ano_prox = ano if mes < 12 else ano + 1
    mes_prox = mes + 1 if mes < 12 else 1
    fim = quinto_dia_util(ano_prox, mes_prox)
    return inicio, fim
```

- [ ] **Step 4: Confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_competencia_utils.py -v --tb=short
```

Saída esperada: `8 passed`

- [ ] **Step 5: Lint**

```bash
./venv/Scripts/ruff.exe check src/storage/competencia_utils.py --fix
```

- [ ] **Step 6: Commit**

```bash
git add src/storage/competencia_utils.py tests/storage/test_competencia_utils.py
git commit -m "feat(storage): competencia_utils — quinto_dia_util, janela_valida com feriados BR"
```

---

## Task 3 — HistoricoReader: listar_competencias_validas + contar_competencias (TDD)

**Files:**
- Modify: `src/storage/historico_reader.py`
- Modify: `tests/storage/test_historico_reader.py`

**Contexto:** A tabela `gold.evolucao_metricas_mensais` tem coluna `gravado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP`. O helper `_popular_duckdb` nos testes **não inclui essa coluna** — precisamos de um novo helper que a inclua.

- [ ] **Step 1: Escrever testes RED**

Adicionar ao final de `tests/storage/test_historico_reader.py`:

```python
import duckdb
from datetime import datetime


def _popular_duckdb_com_timestamps(path):
    """Popula DuckDB com gravado_em controlados para testar validação temporal."""
    with duckdb.connect(str(path)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        con.execute("""
            CREATE TABLE gold.evolucao_metricas_mensais (
                data_competencia VARCHAR PRIMARY KEY,
                total_vinculos   INTEGER,
                total_ghost      INTEGER,
                total_missing    INTEGER,
                total_rq005      INTEGER,
                gravado_em       TIMESTAMP
            )
        """)
        con.execute("""
            CREATE TABLE gold.auditoria_resultados (
                data_competencia VARCHAR,
                regra            VARCHAR,
                total_anomalias  INTEGER,
                PRIMARY KEY (data_competencia, regra)
            )
        """)
        # 2024-11: janela [07/11/2024, 06/12/2024) — captura em 10/11 → válida
        con.execute(
            "INSERT INTO gold.evolucao_metricas_mensais VALUES (?,?,?,?,?,?)",
            ["2024-11", 350, 2, 3, 5, datetime(2024, 11, 10)],
        )
        # 2024-12: janela [06/12/2024, 08/01/2025) — captura em 10/12 → válida
        con.execute(
            "INSERT INTO gold.evolucao_metricas_mensais VALUES (?,?,?,?,?,?)",
            ["2024-12", 357, 3, 2, 7, datetime(2024, 12, 10)],
        )
        # 2024-10: janela [07/10/2024, 07/11/2024) — captura em 20/11 → INVÁLIDA (tarde)
        con.execute(
            "INSERT INTO gold.evolucao_metricas_mensais VALUES (?,?,?,?,?,?)",
            ["2024-10", 300, 1, 1, 3, datetime(2024, 11, 20)],
        )


@pytest.fixture
def reader_com_timestamps(tmp_path):
    db = tmp_path / "test_ts.duckdb"
    _popular_duckdb_com_timestamps(db)
    return HistoricoReader(db, tmp_path / "historico")


class TestListarCompetenciasValidas:

    def test_exclui_competencia_capturada_fora_da_janela(self, reader_com_timestamps):
        validas = reader_com_timestamps.listar_competencias_validas()
        assert "2024-10" not in validas

    def test_inclui_competencias_capturadas_dentro_da_janela(self, reader_com_timestamps):
        validas = reader_com_timestamps.listar_competencias_validas()
        assert "2024-11" in validas
        assert "2024-12" in validas

    def test_retorna_em_ordem_cronologica(self, reader_com_timestamps):
        validas = reader_com_timestamps.listar_competencias_validas()
        assert validas == sorted(validas)

    def test_retorna_lista_vazia_quando_sem_dados(self, tmp_path):
        db = tmp_path / "empty.duckdb"
        with duckdb.connect(str(db)) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
            con.execute("""
                CREATE TABLE gold.evolucao_metricas_mensais (
                    data_competencia VARCHAR PRIMARY KEY,
                    total_vinculos INTEGER,
                    total_ghost INTEGER,
                    total_missing INTEGER,
                    total_rq005 INTEGER,
                    gravado_em TIMESTAMP
                )
            """)
        r = HistoricoReader(db, tmp_path / "historico")
        assert r.listar_competencias_validas() == []


class TestContarCompetencias:

    def test_retorna_validas_e_total(self, reader_com_timestamps):
        validas, total = reader_com_timestamps.contar_competencias()
        assert total == 3   # 2024-10, 2024-11, 2024-12
        assert validas == 2  # 2024-11 e 2024-12

    def test_zeros_quando_sem_dados(self, tmp_path):
        db = tmp_path / "empty2.duckdb"
        with duckdb.connect(str(db)) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
            con.execute("""
                CREATE TABLE gold.evolucao_metricas_mensais (
                    data_competencia VARCHAR PRIMARY KEY,
                    total_vinculos INTEGER,
                    total_ghost INTEGER,
                    total_missing INTEGER,
                    total_rq005 INTEGER,
                    gravado_em TIMESTAMP
                )
            """)
        r = HistoricoReader(db, tmp_path / "historico")
        assert r.contar_competencias() == (0, 0)
```

- [ ] **Step 2: Confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py::TestListarCompetenciasValidas -x --tb=short -q
```

Saída esperada: `AttributeError: 'HistoricoReader' object has no attribute 'listar_competencias_validas'`

- [ ] **Step 3: Implementar os novos métodos em `historico_reader.py`**

Adicionar import no topo de `src/storage/historico_reader.py`, após os imports existentes:
```python
from storage.competencia_utils import janela_valida
```

Adicionar os dois métodos ao final da classe `HistoricoReader`, após `carregar_total_vinculos`:

```python
    def listar_competencias_validas(self) -> list[str]:
        """Competências com gravado_em dentro da janela de coleta CNES Local.

        Returns:
            Lista de competências YYYY-MM em ordem ascendente.
        """
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            rows = con.execute(
                "SELECT data_competencia, MIN(gravado_em) AS primeiro_gravado "
                "FROM gold.evolucao_metricas_mensais "
                "GROUP BY data_competencia ORDER BY data_competencia"
            ).fetchall()
        validas = []
        for comp, primeiro_gravado in rows:
            inicio, fim = janela_valida(comp)
            if inicio <= primeiro_gravado.date() < fim:
                validas.append(comp)
        return validas

    def contar_competencias(self) -> tuple[int, int]:
        """Retorna (válidas, total) de competências no DuckDB.

        Returns:
            Tupla (n_validas, n_total).
        """
        total = len(self.listar_competencias())
        validas = len(self.listar_competencias_validas())
        return validas, total
```

- [ ] **Step 4: Confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py -v --tb=short
```

Saída esperada: todos passando (incluindo os novos).

- [ ] **Step 5: Suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Saída esperada: todos passando.

- [ ] **Step 6: Lint**

```bash
./venv/Scripts/ruff.exe check src/storage/historico_reader.py src/storage/competencia_utils.py --fix
```

- [ ] **Step 7: Commit**

```bash
git add src/storage/historico_reader.py tests/storage/test_historico_reader.py
git commit -m "feat(storage): listar_competencias_validas e contar_competencias com janela de coleta"
```

---

## Task 4 — dashboard_status.py: range BigQuery + cobertura DuckDB

**Files:**
- Modify: `scripts/dashboard_status.py`
- Modify: `tests/scripts/test_dashboard_status.py`

**Contexto:** A função pública `renderizar_container_status` ganha parâmetro `cobertura: tuple[int, int]`. A nova função privada `_executar_range_query` contém a lógica testável; `_consultar_range_bigquery` é o wrapper com cache Streamlit.

- [ ] **Step 1: Escrever testes RED**

Adicionar ao final de `tests/scripts/test_dashboard_status.py`:

```python
import sys
import pandas as pd
from unittest.mock import MagicMock


class TestExecutarRangeBigquery:

    def test_retorna_none_quando_project_id_vazio(self):
        from dashboard_status import _executar_range_query
        assert _executar_range_query("", "3523008") is None

    def test_retorna_none_quando_id_municipio_vazio(self):
        from dashboard_status import _executar_range_query
        assert _executar_range_query("proj-123", "") is None

    def test_retorna_none_quando_bd_levanta_excecao(self, monkeypatch):
        mock_bd = MagicMock()
        mock_bd.read_sql.side_effect = Exception("BQ error")
        monkeypatch.setitem(sys.modules, "basedosdados", mock_bd)

        from importlib import reload
        import dashboard_status
        reload(dashboard_status)
        from dashboard_status import _executar_range_query

        result = _executar_range_query("proj-123", "3523008")
        assert result is None

    def test_retorna_range_quando_bd_disponivel(self, monkeypatch):
        mock_bd = MagicMock()
        mock_bd.read_sql.return_value = pd.DataFrame({
            "min_comp": ["2024-01"],
            "max_comp": ["2026-03"],
        })
        monkeypatch.setitem(sys.modules, "basedosdados", mock_bd)

        from importlib import reload
        import dashboard_status
        reload(dashboard_status)
        from dashboard_status import _executar_range_query

        result = _executar_range_query("proj-123", "3523008")
        assert result == ("2024-01", "2026-03")
```

- [ ] **Step 2: Confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/scripts/test_dashboard_status.py::TestExecutarRangeBigquery -x --tb=short -q
```

Saída esperada: `ImportError: cannot import name '_executar_range_query'`

- [ ] **Step 3: Adicionar as novas funções em `scripts/dashboard_status.py`**

Adicionar após as funções `carregar_status` e `renderizar_container_status`, antes de `_render_card`:

```python
@st.cache_data(ttl=86_400)
def _consultar_range_bigquery() -> tuple[str, str] | None:
    project_id = os.getenv("GCP_PROJECT_ID")
    id_municipio = os.getenv("ID_MUNICIPIO_IBGE7")
    if not project_id or not id_municipio:
        return None
    return _executar_range_query(project_id, id_municipio)


def _executar_range_query(project_id: str, id_municipio: str) -> tuple[str, str] | None:
    if not project_id or not id_municipio:
        return None
    try:
        import basedosdados as bd
        query = f"""
            SELECT
                MIN(CONCAT(CAST(ano AS STRING), '-',
                    LPAD(CAST(mes AS STRING), 2, '0'))) AS min_comp,
                MAX(CONCAT(CAST(ano AS STRING), '-',
                    LPAD(CAST(mes AS STRING), 2, '0'))) AS max_comp
            FROM (
                SELECT DISTINCT ano, mes
                FROM `basedosdados.br_ms_cnes.profissional`
                WHERE id_municipio = '{id_municipio}'
            )
        """
        df = bd.read_sql(query, billing_project_id=project_id)
        if df.empty or df["min_comp"].iloc[0] is None:
            return None
        return str(df["min_comp"].iloc[0]), str(df["max_comp"].iloc[0])
    except Exception:
        return None
```

- [ ] **Step 4: Atualizar `renderizar_container_status`**

Substituir a implementação atual de `renderizar_container_status` por:

```python
def renderizar_container_status(
    status: dict[str, DepStatus],
    competencias: list[str],
    cobertura: tuple[int, int],
) -> None:
    """Renderiza st.expander com cards de status das 4 dependências.

    Args:
        status: Resultado de carregar_status().
        competencias: Lista de competências válidas no DuckDB (YYYY-MM).
        cobertura: Tupla (válidas, total) de reader.contar_competencias().
    """
    algum_problema = any(s.ok is False for s in status.values())
    local_range = (
        f"{competencias[0]} → {competencias[-1]}"
        if len(competencias) >= 2
        else (competencias[0] if competencias else "—")
    )
    validas, total = cobertura
    duckdb_range = f"{validas} válidas / {total} disponíveis"

    bq_result = _consultar_range_bigquery() if status["bigquery"].ok is not None else None
    bq_range_str = f"{bq_result[0]} → {bq_result[1]}" if bq_result else "—"

    with st.expander("⚙ Status das dependências", expanded=algum_problema):
        cols = st.columns(4)
        _render_card(
            cols[0], CardInfo("CNES Local", "Firebird", local_range), status["firebird"]
        )
        _render_card(
            cols[1], CardInfo("CNES Nacional", "BigQuery", bq_range_str), status["bigquery"]
        )
        _render_card(
            cols[2], CardInfo("Histórico", "DuckDB", duckdb_range), status["duckdb"]
        )
        _render_card(cols[3], CardInfo("RH / Folha", "HR/XLSX"), status["hr"])
```

- [ ] **Step 5: Confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/scripts/test_dashboard_status.py -v --tb=short
```

Saída esperada: todos passando (6 existentes + 4 novos = 10 passed).

- [ ] **Step 6: Lint**

```bash
./venv/Scripts/ruff.exe check scripts/dashboard_status.py --fix
```

- [ ] **Step 7: Suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Saída esperada: todos passando.

- [ ] **Step 8: Commit**

```bash
git add scripts/dashboard_status.py tests/scripts/test_dashboard_status.py
git commit -m "feat(dashboard): range BigQuery ao vivo, cobertura DuckDB válidas/total"
```

---

## Task 5 — Dashboard pages: trocar listar_competencias por listar_competencias_validas

**Files:**
- Modify: `scripts/dashboard.py`
- Modify: `scripts/pages/1_Tendencias.py`
- Modify: `scripts/pages/2_Por_Regra.py`

**Contexto:** Três trocas cirúrgicas. `carregar_delta` internamente usa `listar_competencias()` — isso é intencional e não muda (delta calcula diferença entre competências consecutivas armazenadas, independente de validade).

### dashboard.py

- [ ] **Step 1: Ler `scripts/dashboard.py` para localizar as duas chamadas**

As linhas relevantes (verificar linha exata após leitura):
```python
competencias = reader.listar_competencias()          # linha ~70
renderizar_container_status(status, [])              # linha ~75 (caso vazio)
renderizar_container_status(status, competencias)    # linha ~79
```

- [ ] **Step 2: Aplicar mudanças em `scripts/dashboard.py`**

**Troca 1:** `reader.listar_competencias()` → `reader.listar_competencias_validas()`

**Troca 2:** Antes do primeiro `renderizar_container_status`, adicionar:
```python
cobertura = reader.contar_competencias()
```

**Troca 3:** Ambas as chamadas a `renderizar_container_status` ganham `cobertura`:
```python
renderizar_container_status(status, [], (0, 0))          # caso vazio
renderizar_container_status(status, competencias, cobertura)  # caso normal
```

### 1_Tendencias.py

- [ ] **Step 3: Em `scripts/pages/1_Tendencias.py`, linha 30**

Substituir:
```python
competencias = reader.listar_competencias()
```
por:
```python
competencias = reader.listar_competencias_validas()
```

### 2_Por_Regra.py

- [ ] **Step 4: Em `scripts/pages/2_Por_Regra.py`, linha 100**

Substituir:
```python
competencias = reader.listar_competencias()
```
por:
```python
competencias = reader.listar_competencias_validas()
```

- [ ] **Step 5: Suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Saída esperada: todos passando.

- [ ] **Step 6: Lint**

```bash
./venv/Scripts/ruff.exe check scripts/ --fix
```

- [ ] **Step 7: Commit**

```bash
git add scripts/dashboard.py scripts/pages/1_Tendencias.py scripts/pages/2_Por_Regra.py
git commit -m "feat(dashboard): usar listar_competencias_validas em todas as páginas"
```

---

## Self-Review — Spec Coverage

| Requisito do spec | Task |
|---|---|
| Janela de validade: 5º dia útil do mês até 5º dia útil do mês seguinte | Task 2 |
| Feriados nacionais BR no cálculo | Task 1 + Task 2 |
| `lru_cache` no cálculo de dias úteis | Task 2 |
| `listar_competencias_validas()` filtra por `gravado_em` | Task 3 |
| `contar_competencias()` retorna (válidas, total) | Task 3 |
| Card DuckDB mostra "N válidas / M disponíveis" | Task 4 |
| `_executar_range_query` testável sem Streamlit | Task 4 |
| `_consultar_range_bigquery` com `@st.cache_data(ttl=86400)` | Task 4 |
| Fallback "—" quando BQ não configurado ou query falha | Task 4 |
| Páginas dashboard usam `listar_competencias_validas()` | Task 5 |
| `renderizar_container_status` recebe `cobertura` | Task 4 + Task 5 |
