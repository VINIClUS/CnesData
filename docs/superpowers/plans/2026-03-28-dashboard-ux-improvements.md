# Dashboard UX Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Corrigir 3 problemas de UX no dashboard Streamlit: default de tendências, tabs na tela Por Regra, e contexto nos KPIs da Visão Geral quando valores são zero.

**Architecture:** Quatro tasks independentes ordenadas por dependência. Task 1 e Task 2 são completamente independentes. Task 3 depende do método adicionado na Task 2. Task 4 é independente das demais. Performance (Task 5) usa plano separado existente.

**Tech Stack:** `streamlit>=1.32.0`, `duckdb`, `pandas`. Zero novas dependências.

---

## File Map

| Ação | Arquivo | Responsabilidade |
|---|---|---|
| Modificar | `scripts/pages/2_Tendencias.py:31` | Mudar `default` de 3 regras para todas |
| Modificar | `src/storage/historico_reader.py` | Adicionar `carregar_total_vinculos()` |
| Modificar | `tests/storage/test_historico_reader.py` | 2 testes para `carregar_total_vinculos` |
| Modificar | `scripts/pages/1_Visao_Geral.py` | Adicionar métrica de vínculos + callout contextual |
| Modificar | `scripts/pages/3_Por_Regra.py` | Reescrever com `st.tabs()` — 11 tabs |

---

## Task 1 — Tendências: default todas as regras

**Files:**
- Modify: `scripts/pages/2_Tendencias.py:31`

- [ ] **Step 1: Alterar o default do multiselect**

Em `scripts/pages/2_Tendencias.py`, linha 31, alterar:

```python
# Antes
regras_sel = st.sidebar.multiselect(
    "Regras",
    options=_TODAS_REGRAS,
    default=["RQ008", "RQ006", "RQ009"],
)

# Depois
regras_sel = st.sidebar.multiselect(
    "Regras",
    options=_TODAS_REGRAS,
    default=_TODAS_REGRAS,
)
```

- [ ] **Step 2: Verificar no browser**

```bash
streamlit run scripts/dashboard.py
```

Navegar para "Tendências" — multiselect deve abrir com todas as 11 regras marcadas.

- [ ] **Step 3: Commit**

```bash
git add scripts/pages/2_Tendencias.py
git commit -m "fix(dashboard): tendencias abre com todas as regras selecionadas por padrao"
```

---

## Task 2 — HistoricoReader: método `carregar_total_vinculos`

**Files:**
- Modify: `src/storage/historico_reader.py`
- Modify: `tests/storage/test_historico_reader.py`

- [ ] **Step 1: Escrever testes RED**

Abrir `tests/storage/test_historico_reader.py` e adicionar ao final do arquivo (após o último teste existente):

```python
def test_carregar_total_vinculos_retorna_valor_correto(reader):
    assert reader.carregar_total_vinculos("2024-12") == 357


def test_carregar_total_vinculos_retorna_zero_quando_competencia_ausente(reader):
    assert reader.carregar_total_vinculos("2099-01") == 0
```

O valor `357` vem da fixture `_popular_duckdb` — linha:
`con.execute("INSERT INTO gold.evolucao_metricas_mensais VALUES ('2024-12',357,3,2,7)")`

- [ ] **Step 2: Confirmar RED**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py::test_carregar_total_vinculos_retorna_valor_correto -x --tb=short -q
```

Saída esperada: `AttributeError: 'HistoricoReader' object has no attribute 'carregar_total_vinculos'`

- [ ] **Step 3: Implementar método em `HistoricoReader`**

Abrir `src/storage/historico_reader.py` e adicionar após o método `listar_competencias_para_regra` (última linha do arquivo):

```python
    def carregar_total_vinculos(self, competencia: str) -> int:
        """Retorna total de vínculos processados para uma competência.

        Args:
            competencia: Competência no formato YYYY-MM.

        Returns:
            Total de vínculos ou 0 se a competência não existir.
        """
        with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
            df = con.execute(
                "SELECT total_vinculos FROM gold.evolucao_metricas_mensais "
                "WHERE data_competencia = ?",
                [competencia],
            ).df()
        return int(df["total_vinculos"].iloc[0]) if not df.empty else 0
```

- [ ] **Step 4: Confirmar GREEN**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py -v --tb=short
```

Saída esperada: todos os testes passando (os 10 existentes + os 2 novos).

- [ ] **Step 5: Lint**

```bash
./venv/Scripts/ruff.exe check src/storage/historico_reader.py --fix
```

Saída esperada: sem erros.

- [ ] **Step 6: Commit**

```bash
git add src/storage/historico_reader.py tests/storage/test_historico_reader.py
git commit -m "feat(historico-reader): adicionar carregar_total_vinculos"
```

---

## Task 3 — Visão Geral: métrica de vínculos + callout contextual

**Files:**
- Modify: `scripts/pages/1_Visao_Geral.py`

Depende da Task 2 estar concluída.

- [ ] **Step 1: Adicionar chamada ao novo método**

Abrir `scripts/pages/1_Visao_Geral.py`. Após as linhas:

```python
kpis   = reader.carregar_kpis(competencia)
deltas = reader.carregar_delta(competencia)
```

Adicionar:

```python
total_vinculos = reader.carregar_total_vinculos(competencia)
```

- [ ] **Step 2: Adicionar métrica de vínculos no cabeçalho**

Substituir o bloco de colunas KPI existente (que começa com `cols = st.columns(len(_KPI_DESTAQUE))`):

```python
# Antes
cols = st.columns(len(_KPI_DESTAQUE))
for i, regra in enumerate(_KPI_DESTAQUE):
    ...
```

```python
# Depois — adicionar coluna de vínculos antes dos KPI cards
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
    delta = deltas.get(regra, 0)
    with cols[i]:
        st.metric(
            label=f"{_SEV_ICON[sev]} {desc}",
            value=kpis.get(regra, 0),
            delta=f"+{delta}" if delta > 0 else str(delta),
            delta_color="inverse",
            help=f"Regra {regra} — Severidade: {sev}",
        )
```

- [ ] **Step 3: Adicionar callout contextual após o segundo `st.divider()`**

Após o bloco de KPI cards e antes da tabela resumo, inserir:

```python
st.divider()

if total_vinculos == 0:
    st.warning(
        "Pipeline rodou mas não processou vínculos. Verifique os logs."
    )
elif not kpis:
    st.warning(
        "Dados de auditoria não encontrados para esta competência. Reexecute o pipeline."
    )
elif sum(kpis.values()) == 0:
    st.info(
        "Nenhuma anomalia detectada nesta competência. "
        "Se esperava resultados, verifique se o pipeline rodou com dados nacionais (BigQuery habilitado)."
    )
```

- [ ] **Step 4: Verificar no browser**

```bash
streamlit run scripts/dashboard.py
```

Navegar para "Visão Geral" — deve exibir o card "Vínculos processados" no topo. Quando todos os KPIs forem 0 e vínculos > 0, o `st.info` deve aparecer.

- [ ] **Step 5: Commit**

```bash
git add scripts/pages/1_Visao_Geral.py
git commit -m "fix(visao-geral): adicionar metrica de vinculos e callout contextual quando KPIs zerados"
```

---

## Task 4 — Por Regra: tabs horizontais

**Files:**
- Modify: `scripts/pages/3_Por_Regra.py`

- [ ] **Step 1: Reescrever `3_Por_Regra.py` com tabs**

Substituir o conteúdo completo do arquivo:

```python
"""Página 3 — Por Regra: drill-down com tabs horizontais por regra."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import streamlit as st

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

st.title("🔍 Por Regra")

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

tabs = st.tabs([label for label, _ in _TABS])

for tab, (_, regra) in zip(tabs, _TABS):
    with tab:
        valor = kpis.get(regra, 0)
        delta = deltas.get(regra, 0)

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
            st.warning(
                f"Sem registros arquivados para **{_REGRA_DESC[regra]}** em {competencia}. "
                "Verifique se o pipeline rodou para essa competência."
            )
            continue

        mostrar_completo = st.checkbox(
            "Mostrar dados completos (CPF/CNS sem máscara)",
            key=f"mask_{regra}",
        )
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
            key=f"dl_{regra}",
        )
```

- [ ] **Step 2: Verificar no browser**

```bash
streamlit run scripts/dashboard.py
```

Navegar para "Por Regra" — deve exibir 11 tabs horizontais. Sidebar só exibe o selectbox de Competência. Cada tab mostra `st.metric` + tabela logo abaixo sem gap.

- [ ] **Step 3: Verificar que widgets dentro de tabs não colidem**

Trocar de tab várias vezes e confirmar que o checkbox de máscara e o botão de download são independentes por regra (garantido pelos `key=f"mask_{regra}"` e `key=f"dl_{regra}"`).

- [ ] **Step 4: Lint**

```bash
./venv/Scripts/ruff.exe check scripts/pages/3_Por_Regra.py --fix
```

Saída esperada: sem erros.

- [ ] **Step 5: Commit**

```bash
git add scripts/pages/3_Por_Regra.py
git commit -m "feat(por-regra): substituir selectbox por tabs horizontais — uma tab por regra"
```

---

## Task 5 — Performance: executar plano existente

O plano `docs/superpowers/plans/2026-03-28-performance-optimizations.md` já está completo com código e testes. Executar via `superpowers:executing-plans` ou `superpowers:subagent-driven-development`.

Tasks nesse plano (independentes, nesta ordem):
1. **Task 1** — Remover `TIPO_UNIDADE` e `COD_MUNICIPIO` de `_COLUNAS_TEXTO` (`src/processing/transformer.py`)
2. **Task 2** — `CachingVerificadorCnes` com cache JSON TTL 24h (`src/analysis/verificacao_cache.py`)
3. **Task 3** — `ThreadPoolExecutor` para fetches BigQuery em paralelo (`src/main.py`)
4. **Task 4** — Cache pickle TTL 1h para resultados BigQuery (`src/ingestion/cnes_nacional_adapter.py`)

**Nota:** Task 4 depende do `CACHE_DIR` adicionado na Task 2 — executar em ordem.

---

## Self-Review — Cobertura do Spec

| Requisito do spec | Task que implementa |
|---|---|
| Tendências default = todas as regras | Task 1 ✅ |
| `carregar_total_vinculos` em HistoricoReader | Task 2 ✅ |
| Visão Geral — métrica vínculos + callout | Task 3 ✅ |
| Por Regra — tabs horizontais | Task 4 ✅ |
| Performance — 4 tasks do plano existente | Task 5 (referência) ✅ |
| `key=` único por widget dentro de tabs | Task 4, Step 1 ✅ |
| `kpis`/`deltas` carregados 1x fora do loop de tabs | Task 4, Step 1 ✅ |
| Callout só quando `kpis` não-vazio e sum==0 | Task 3, Step 3 ✅ |

## Quick Reference

```bash
# Rodar testes novos
./venv/Scripts/python.exe -m pytest tests/storage/test_historico_reader.py -v

# Suite completa
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short

# Dashboard
streamlit run scripts/dashboard.py
```
