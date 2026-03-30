# Dashboard Expansão Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Adicionar 5º card DATASUS com health check, detalhes expandidos em todos os cards, expander de diretórios de saída, e toggle de escala logarítmica em Tendências.

**Architecture:** `_executar_health_check_datasus` é a função pura testável; `_verificar_datasus` é o wrapper com `@st.cache_data(ttl=3600)`. `CardInfo` ganha campo `detalhe`. `renderizar_container_status` ganha 4º param `duckdb_path` e passa `detalhe` para cada card. `renderizar_container_diretorios` é nova função em `dashboard_status.py`, chamada de `dashboard.py`.

**Tech Stack:** Python 3.11+, Streamlit, `requests` (já instalado), `st.cache_data`.

---

## File Map

| Ação | Arquivo | Responsabilidade |
|---|---|---|
| Modificar | `scripts/dashboard_status.py` | CardInfo.detalhe, _render_card, _executar_health_check_datasus, _verificar_datasus, renderizar_container_status (5 cols + duckdb_path), renderizar_container_diretorios |
| Modificar | `scripts/dashboard.py` | Passar duckdb_path, chamar renderizar_container_diretorios |
| Modificar | `scripts/pages/1_Tendencias.py` | Checkbox de escala logarítmica |
| Modificar | `tests/scripts/test_dashboard_status.py` | Testes TDD para _executar_health_check_datasus |
| Modificar | `CLAUDE.md` | Documentar DATASUS_AUTH_TOKEN |

---

## Task 1 — TDD: `_executar_health_check_datasus`

**Files:**
- Modify: `tests/scripts/test_dashboard_status.py`
- Modify: `scripts/dashboard_status.py`

**Contexto:** `_executar_health_check_datasus(url, token)` é uma função pura que faz um GET HTTP e retorna `DepStatus`. Import de `requests` é lazy (dentro do `try`). `sys`, `MagicMock`, `patch`, `pd`, `pytest` já estão importados no arquivo de testes.

- [ ] **Step 1: Adicionar testes ao final de `tests/scripts/test_dashboard_status.py`**

```python
class TestExecutarHealthCheckDatasus:

    def test_retorna_ok_true_quando_resposta_2xx(self, monkeypatch):
        mock_requests = MagicMock()
        mock_requests.get.return_value.status_code = 200
        monkeypatch.setitem(sys.modules, "requests", mock_requests)
        from importlib import reload
        import dashboard_status
        reload(dashboard_status)
        from dashboard_status import _executar_health_check_datasus
        assert _executar_health_check_datasus("https://url", "token-abc").ok is True

    def test_retorna_false_token_invalido_quando_401(self, monkeypatch):
        mock_requests = MagicMock()
        mock_requests.get.return_value.status_code = 401
        monkeypatch.setitem(sys.modules, "requests", mock_requests)
        from importlib import reload
        import dashboard_status
        reload(dashboard_status)
        from dashboard_status import _executar_health_check_datasus
        result = _executar_health_check_datasus("https://url", "token-abc")
        assert result.ok is False
        assert "token" in result.erro

    def test_retorna_false_inacessivel_quando_excecao(self, monkeypatch):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = Exception("timeout")
        monkeypatch.setitem(sys.modules, "requests", mock_requests)
        from importlib import reload
        import dashboard_status
        reload(dashboard_status)
        from dashboard_status import _executar_health_check_datasus
        result = _executar_health_check_datasus("https://url", "token-abc")
        assert result.ok is False
        assert "inacessível" in result.erro

    def test_retorna_false_com_codigo_quando_503(self, monkeypatch):
        mock_requests = MagicMock()
        mock_requests.get.return_value.status_code = 503
        monkeypatch.setitem(sys.modules, "requests", mock_requests)
        from importlib import reload
        import dashboard_status
        reload(dashboard_status)
        from dashboard_status import _executar_health_check_datasus
        result = _executar_health_check_datasus("https://url", "token-abc")
        assert result.ok is False
        assert "503" in result.erro
```

- [ ] **Step 2: Confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/scripts/test_dashboard_status.py::TestExecutarHealthCheckDatasus -x --tb=short -q
```

Saída esperada: `ImportError: cannot import name '_executar_health_check_datasus'`

- [ ] **Step 3: Adicionar `_executar_health_check_datasus` + `_verificar_datasus` em `scripts/dashboard_status.py`**

Inserir APÓS `_consultar_range_bigquery` e ANTES de `_render_card`:

```python
@st.cache_data(ttl=3_600)
def _verificar_datasus() -> DepStatus:
    token = os.getenv("DATASUS_AUTH_TOKEN")
    if not token:
        return DepStatus(ok=None)
    return _executar_health_check_datasus(
        "https://apidadosabertos.saude.gov.br/v1/cnes/estabelecimentos",
        token,
    )


def _executar_health_check_datasus(url: str, token: str) -> DepStatus:
    try:
        import requests
        headers = {"Authorization": f"Bearer {token}"}
        r = requests.get(url, headers=headers, params={"limit": 1}, timeout=5)
        if r.status_code < 300:
            return DepStatus(ok=True)
        if r.status_code == 401:
            return DepStatus(ok=False, erro="token inválido")
        return DepStatus(ok=False, erro=f"HTTP {r.status_code}")
    except Exception:
        return DepStatus(ok=False, erro="inacessível")
```

- [ ] **Step 4: Confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/scripts/test_dashboard_status.py::TestExecutarHealthCheckDatasus -v --tb=short
```

Saída esperada: `4 passed`

- [ ] **Step 5: Suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Saída esperada: todos passando.

- [ ] **Step 6: Lint**

```bash
./venv/Scripts/ruff.exe check scripts/dashboard_status.py tests/scripts/test_dashboard_status.py --fix
```

- [ ] **Step 7: Commit**

```bash
git add scripts/dashboard_status.py tests/scripts/test_dashboard_status.py
git commit -m "feat(dashboard): _executar_health_check_datasus com cache — 5º card DATASUS"
```

---

## Task 2 — `dashboard_status.py`: CardInfo.detalhe + render expandido + container de diretórios

**Files:**
- Modify: `scripts/dashboard_status.py`

**Contexto:** Sem novos testes — funções de render são Streamlit-dependentes. A suite existente confirma que nada quebrou.

- [ ] **Step 1: Adicionar campo `detalhe` ao dataclass `CardInfo`**

Localizar o dataclass `CardInfo` (linhas ~20-26) e substituir por:

```python
@dataclass
class CardInfo:
    """Informações de exibição para um card de status."""

    titulo: str
    fonte: str
    range_str: str | None = None
    detalhe: str | None = None
```

- [ ] **Step 2: Atualizar `_render_card` para renderizar `detalhe`**

Substituir a função `_render_card` completa por:

```python
def _render_card(col, info: CardInfo, s: DepStatus) -> None:
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
            f"Range: <strong>{info.range_str}</strong></div>"
            if info.range_str
            else ""
        )
        detalhe_html = (
            f'<div style="font-size:9px;margin-top:2px;color:#777">{info.detalhe}</div>'
            if info.detalhe
            else ""
        )
        st.markdown(
            f'<div style="border:1px solid #2d4a7a;border-radius:6px;'
            f'padding:8px;background:#0d1b2a">'
            f'<div style="font-size:10px;color:#888">{info.titulo}</div>'
            f"<div>{icon} <strong>{info.fonte}</strong></div>"
            f'<div style="color:#888;font-size:11px">{label}{ts_str}</div>'
            f"{range_html}{detalhe_html}</div>",
            unsafe_allow_html=True,
        )
```

- [ ] **Step 3: Substituir `renderizar_container_status` completo**

Localizar a função `renderizar_container_status` e substituir completamente por:

```python
def renderizar_container_status(
    status: dict[str, DepStatus],
    competencias: list[str],
    cobertura: tuple[int, int],
    duckdb_path: Path,
) -> None:
    """Renderiza st.expander com cards de status das 5 dependências.

    Args:
        status: Resultado de carregar_status().
        competencias: Lista de competências válidas no DuckDB (YYYY-MM).
        cobertura: Tupla (válidas, total) de reader.contar_competencias().
        duckdb_path: Caminho do arquivo DuckDB (para detalhe do card).
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
    datasus_status = _verificar_datasus()
    token_detalhe = "token: configurado" if os.getenv("DATASUS_AUTH_TOKEN") else "sem token"
    hr_path = os.getenv("FOLHA_HR_PATH", "")
    hr_detalhe = Path(hr_path).name if hr_path else "não configurado"
    with st.expander("⚙ Status das dependências", expanded=algum_problema):
        cols = st.columns(5)
        _render_card(
            cols[0],
            CardInfo("CNES Local", "Firebird", local_range,
                     Path(os.getenv("DB_PATH", "")).name or "—"),
            status["firebird"],
        )
        _render_card(
            cols[1],
            CardInfo("CNES Nacional", "BigQuery", bq_range_str,
                     os.getenv("GCP_PROJECT_ID", "—")),
            status["bigquery"],
        )
        _render_card(
            cols[2],
            CardInfo("Histórico", "DuckDB", duckdb_range, duckdb_path.name),
            status["duckdb"],
        )
        _render_card(
            cols[3], CardInfo("RH / Folha", "HR/XLSX", None, hr_detalhe), status["hr"]
        )
        _render_card(
            cols[4],
            CardInfo("API DATASUS", "DATASUS", "apidadosabertos.saude.gov.br", token_detalhe),
            datasus_status,
        )
```

- [ ] **Step 4: Adicionar `renderizar_container_diretorios` ao final do módulo** (antes de `_ler_last_run`)

```python
def renderizar_container_diretorios(
    output_path: Path,
    historico_dir: Path,
    duckdb_path: Path,
) -> None:
    """Renderiza expander com os diretórios de saída configurados.

    Args:
        output_path: Caminho do CSV principal de saída.
        historico_dir: Diretório de histórico de CSVs.
        duckdb_path: Caminho do arquivo DuckDB.
    """
    with st.expander("📁 Diretórios de Saída", expanded=False):
        st.markdown(f"**DuckDB:** `{duckdb_path}`")
        st.markdown(f"**Histórico:** `{historico_dir}`")
        st.markdown(f"**CSV saída:** `{output_path}`")
```

- [ ] **Step 5: Suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Saída esperada: todos passando.

- [ ] **Step 6: Lint**

```bash
./venv/Scripts/ruff.exe check scripts/dashboard_status.py --fix
```

- [ ] **Step 7: Commit**

```bash
git add scripts/dashboard_status.py
git commit -m "feat(dashboard): CardInfo.detalhe, 5 cards, renderizar_container_diretorios"
```

---

## Task 3 — `dashboard.py` + `CLAUDE.md`

**Files:**
- Modify: `scripts/dashboard.py`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Ler `scripts/dashboard.py`** para localizar as linhas exatas antes de editar.

- [ ] **Step 2: Atualizar import em `scripts/dashboard.py`**

Localizar a linha:
```python
from dashboard_status import carregar_status, renderizar_container_status, REGRAS_FONTE
```

Substituir por:
```python
from dashboard_status import (
    carregar_status,
    renderizar_container_status,
    renderizar_container_diretorios,
    REGRAS_FONTE,
)
```

- [ ] **Step 3: Atualizar chamada na rota de erro (sem competências)**

Localizar:
```python
    renderizar_container_status(status, [], (0, 0))
    st.stop()
```

Substituir por:
```python
    renderizar_container_status(status, [], (0, 0), config.DUCKDB_PATH)
    renderizar_container_diretorios(config.OUTPUT_PATH, config.HISTORICO_DIR, config.DUCKDB_PATH)
    st.stop()
```

- [ ] **Step 4: Atualizar chamada na rota normal + adicionar container de diretórios**

Localizar:
```python
renderizar_container_status(status, competencias, cobertura)
```

Substituir por:
```python
renderizar_container_status(status, competencias, cobertura, config.DUCKDB_PATH)
renderizar_container_diretorios(config.OUTPUT_PATH, config.HISTORICO_DIR, config.DUCKDB_PATH)
```

- [ ] **Step 5: Atualizar `CLAUDE.md` — adicionar `DATASUS_AUTH_TOKEN`**

Localizar a linha:
```
| `FOLHA_HR_PATH` | no | Path to HR payroll spreadsheet |
```

Inserir após ela:
```
| `DATASUS_AUTH_TOKEN` | no | Bearer token para apidadosabertos.saude.gov.br |
```

- [ ] **Step 6: Suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Saída esperada: todos passando.

- [ ] **Step 7: Lint**

```bash
./venv/Scripts/ruff.exe check scripts/dashboard.py --fix
```

- [ ] **Step 8: Commit**

```bash
git add scripts/dashboard.py CLAUDE.md
git commit -m "feat(dashboard): renderizar_container_diretorios, duckdb_path no status container"
```

---

## Task 4 — `1_Tendencias.py`: escala logarítmica

**Files:**
- Modify: `scripts/pages/1_Tendencias.py`

- [ ] **Step 1: Ler `scripts/pages/1_Tendencias.py`** para localizar as linhas exatas.

- [ ] **Step 2: Adicionar checkbox de escala log na sidebar**

Localizar:
```python
comp_fim = st.sidebar.selectbox("Até", options=competencias, index=len(competencias) - 1)
```

Inserir logo após:
```python
escala_log = st.sidebar.checkbox("Escala logarítmica", value=False)
```

- [ ] **Step 3: Aplicar escala log no gráfico**

Localizar:
```python
fig.update_xaxes(type="category", tickangle=-45)
```

Inserir logo após:
```python
if escala_log:
    fig.update_yaxes(type="log")
```

- [ ] **Step 4: Adicionar nota sobre zeros**

Localizar:
```python
st.plotly_chart(fig, use_container_width=True)
```

Inserir logo após:
```python
if escala_log:
    st.caption("Valores zero não são exibidos em escala logarítmica.")
```

- [ ] **Step 5: Suite completa**

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

Saída esperada: todos passando.

- [ ] **Step 6: Lint**

```bash
./venv/Scripts/ruff.exe check scripts/pages/1_Tendencias.py --fix
```

- [ ] **Step 7: Commit**

```bash
git add scripts/pages/1_Tendencias.py
git commit -m "feat(tendencias): toggle de escala logarítmica no eixo Y"
```

---

## Self-Review — Spec Coverage

| Requisito | Task |
|---|---|
| `_executar_health_check_datasus` pura + testável | Task 1 |
| `_verificar_datasus` com `@st.cache_data(ttl=3600)` | Task 1 |
| 401 sem token → ok=False com "token inválido" | Task 1 |
| Exceção → ok=False com "inacessível" | Task 1 |
| outro HTTP code → ok=False com "HTTP NNN" | Task 1 |
| Sem `DATASUS_AUTH_TOKEN` → ok=None (não executa request) | Task 1 |
| `CardInfo.detalhe` renderizado em `_render_card` | Task 2 |
| `renderizar_container_status` — 5 cols, duckdb_path param | Task 2 |
| Detalhe por card: DB_PATH, GCP_PROJECT_ID, duckdb.name, FOLHA_HR_PATH, token | Task 2 |
| `renderizar_container_diretorios` em `dashboard_status.py` | Task 2 |
| Chamadas em `dashboard.py` atualizadas | Task 3 |
| `CLAUDE.md` documenta `DATASUS_AUTH_TOKEN` | Task 3 |
| Checkbox "Escala logarítmica" em `1_Tendencias.py` | Task 4 |
| Caption quando escala log ativa | Task 4 |
