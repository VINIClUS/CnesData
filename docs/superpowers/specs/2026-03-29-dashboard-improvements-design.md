# Dashboard Improvements — Design Spec

**Data:** 2026-03-29
**Escopo:** `scripts/dashboard.py`, `scripts/pages/`, `src/storage/historico_reader.py`

---

## Objetivo

Consolidar a home vazia com a Visão Geral, adicionar diagnóstico de configuração em tempo real, corrigir a exibição equivocada de "0" para regras sem fonte de dados, carregar listas pesadas em segundo plano, e tornar as tabelas interativas com filtros e colunas móveis.

---

## 1. Estrutura de Páginas

**Antes:**
```
dashboard.py        ← home vazia (só branding)
pages/1_Visao_Geral.py
pages/2_Tendencias.py
pages/3_Por_Regra.py
```

**Depois:**
```
dashboard.py        ← home = Visão Geral + container de status
pages/2_Tendencias.py  (renomeada para 1_Tendencias.py)
pages/3_Por_Regra.py   (renomeada para 2_Por_Regra.py)
```

`1_Visao_Geral.py` é removida. Seu conteúdo migra para `dashboard.py`. A numeração das páginas restantes é reajustada.

---

## 2. Container Retrátil de Status das Dependências

**Posição:** `st.expander` no topo do conteúdo principal de `dashboard.py`, acima dos KPIs.

**Comportamento de abertura:**
- Expandido por padrão se qualquer dependência estiver vermelha ou amarela.
- Recolhido por padrão se todas estiverem verdes.

**Layout interno:** grid 4 colunas (`st.columns(4)`), um card por dependência.

### Dependências monitoradas

O dashboard **não faz conexões live** — verifica apenas presença de variáveis no `.env` e lê `last_run.json`.

| Card | Fonte | Verificação |
|---|---|---|
| CNES Local | Firebird | `DB_PATH`, `DB_PASSWORD`, `FIREBIRD_DLL` presentes no `.env` |
| CNES Nacional | BigQuery | `GCP_PROJECT_ID` presente no `.env` |
| Histórico | DuckDB | `DUCKDB_PATH` existe no filesystem |
| RH / Folha | HR XLSX | `FOLHA_HR_PATH` definido e arquivo existe |

### Estados por card

| Ícone | Label | Condição |
|---|---|---|
| 🟢 | `con.` + timestamp última execução | Config válida e última execução bem-sucedida |
| 🟡 | `não configurada` | Variável ausente no `.env` (opcional — sem erro) |
| 🔴 | `erro: <código ou mensagem curta>` | Config presente mas falhou na verificação |

**Timestamp** lido de um arquivo de metadados `data/cache/last_run.json` gravado pelo pipeline ao final de cada execução. Formato:
```json
{
  "firebird":  {"ts": "2026-03-28T14:32:00", "ok": true},
  "bigquery":  {"ts": null, "ok": false, "erro": "credentials not found"},
  "duckdb":    {"ts": "2026-03-28T14:32:00", "ok": true},
  "hr":        {"ts": null, "ok": null}
}
```

**Range de competências validadas** (linha inferior de cada card relevante):
- CNES Local: `min → max` de `reader.listar_competencias()` (todas as competências no DuckDB refletem execuções locais).
- CNES Nacional: extraído de `last_run.json["bigquery"]["competencia_range"]` gravado pelo pipeline. Exibe `—` se BigQuery não rodou.

Para não bloquear a UI, o range é lido via `@st.cache_data(ttl=300)`.

O campo `competencia_range` é adicionado ao `last_run.json` pelo pipeline quando a fonte roda com sucesso:
```json
"bigquery": {"ts": "2026-03-28T14:32:00", "ok": true, "competencia_range": ["2024-01", "2026-01"]}
```

---

## 3. Distinção "0 anomalias" vs "regra não executada"

### Problema
Regras que dependem de BigQuery (RQ006–RQ011) ou HR (GHOST, MISSING) mostram `0` quando a fonte não foi executada. O usuário não sabe se é "zero anomalias" ou "não rodou".

### Solução
Adicionar coluna `fonte_disponivel: bool` calculada no dashboard, baseada no status do container:

```python
REGRAS_FONTE: dict[str, str] = {
    "RQ008": "firebird", "GHOST": "hr",      "RQ006": "bigquery",
    "RQ007": "bigquery", "RQ009": "bigquery", "MISSING": "hr",
    "RQ005_ACS": "firebird", "RQ005_ACE": "firebird",
    "RQ003B": "firebird", "RQ010": "bigquery", "RQ011": "bigquery",
}
```

Quando `fonte_disponivel=False`:
- KPI card: exibe `—` no lugar do número, com `help="BigQuery não configurado"`.
- Tabela resumo: célula `—`, opacidade reduzida via CSS.
- Por Regra tab: exibe callout amarelo `⚠ Fonte de dados não disponível para esta regra`.

---

## 4. Tabelas com AG Grid (`streamlit-aggrid`)

**Nova dependência:** `streamlit-aggrid>=0.3.4`

### Comportamento padrão para todas as tabelas de auditoria

```python
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_default_column(
    resizable=True, sortable=True, filter=True,
    wrapText=False, autoHeight=False,
)
gb.configure_grid_options(enableRangeSelection=False, domLayout="autoHeight")
AgGrid(df, gridOptions=gb.build(), use_container_width=True,
       fit_columns_on_grid_load=False, theme="streamlit")
```

**Funcionalidades habilitadas:**
- Redimensionar colunas (arrastar borda do header)
- Mover colunas (arrastar header)
- Filtros por coluna: texto (`contains`), número (`=`, `>`, `<`), set (lista de valores únicos)
- Ordenação multi-coluna (Shift+clique)
- Busca global via input acima da grid

**PII masking** mantido: colunas CPF/CNS continuam mascaradas via pré-processamento no DataFrame antes de entregar ao AgGrid (não depende do componente).

### Tabela de resumo (Visão Geral)
Mesmas opções + coluna `Anomalias` formatada com `cellStyle` condicional (vermelho se >0, cinza se `—`).

---

## 5. Carregamento em Segundo Plano (Por Regra)

### Problema
Cada tab em `3_Por_Regra.py` lê um CSV de auditoria ao renderizar. Com 11 tabs e arquivos grandes, o carregamento inicial trava a UI.

### Solução: lazy load com cache em session_state

```python
# Padrão por tab
key = f"df_{regra}_{competencia}"
if key not in st.session_state:
    with st.spinner(f"Carregando {regra}..."):
        st.session_state[key] = reader.carregar_registros(regra, competencia)
df = st.session_state[key]
```

**Resultado:** cada tab carrega o CSV apenas uma vez (na primeira ativação). Mudança de competência invalida o cache limpando as chaves prefixadas com `df_`.

### Download de listas em segundo plano (funcionários/estabelecimentos)

Para os CSVs de download de listas longas (Por Regra), o botão de download fica disponível somente após o carregamento lazy da tab. Não há download pré-carregado — o dado já está em `session_state` quando o botão aparece.

---

## 6. Eixo X do Gráfico (Tendências)

**Problema:** Plotly formata `data_competencia` (string `YYYY-MM`) como datetime, podendo exibir dias ou horas.

**Fix:** manter `data_competencia` como `str` no eixo X — não converter para datetime. Configurar:

```python
fig.update_xaxes(type="category")
```

Isso garante que o eixo exiba apenas os valores exatos `YYYY-MM` disponíveis, sem interpolação de datas intermediárias.

---

## 7. Gravação de `last_run.json` pelo Pipeline

`src/pipeline/stages/exportacao.py` (`_persistir_historico`) grava ao final de cada execução:

```python
import json, datetime
from pathlib import Path

def _gravar_last_run(config, resultados: dict[str, dict]) -> None:
    path = config.CACHE_DIR / "last_run.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(resultados, ensure_ascii=False, indent=2), encoding="utf-8")
```

O `ExportacaoStage` passa os resultados de cada fonte. Se uma fonte não rodou, grava `{"ts": null, "ok": null}`.

---

## 8. Novo módulo: `scripts/dashboard_status.py`

Centraliza a lógica de diagnóstico de configuração, isolando-a de `dashboard.py`:

```
scripts/
  dashboard.py             ← home page (usa dashboard_status)
  dashboard_status.py      ← lógica de verificação de deps e last_run
  pages/
    1_Tendencias.py
    2_Por_Regra.py
```

`dashboard_status.py` expõe:
- `carregar_status() -> dict[str, DepStatus]` — lê `last_run.json` + verifica config
- `renderizar_container_status(status)` — renderiza o `st.expander` com os cards

---

## 9. Impacto em Performance

| Mudança | Impacto |
|---|---|
| Status container com `@st.cache_data(ttl=300)` | Evita re-leitura do DuckDB a cada rerun |
| Lazy load de CSVs em `session_state` | Cada CSV carrega 1x por sessão |
| `st.cache_resource` para `HistoricoReader` | Já existente — mantido |
| AgGrid com `domLayout="autoHeight"` | Renderiza apenas linhas visíveis |
| `last_run.json` é arquivo local simples | Leitura instantânea, sem I/O DB |

---

## 10. Arquivos Afetados

| Ação | Arquivo |
|---|---|
| Modificar (absorve Visão Geral) | `scripts/dashboard.py` |
| Remover | `scripts/pages/1_Visao_Geral.py` |
| Renomear + modificar | `scripts/pages/2_Tendencias.py` → `1_Tendencias.py` |
| Renomear + modificar | `scripts/pages/3_Por_Regra.py` → `2_Por_Regra.py` |
| Criar | `scripts/dashboard_status.py` |
| Modificar | `src/pipeline/stages/exportacao.py` (gravar `last_run.json`) |
| Modificar | `src/config.py` (expor `LAST_RUN_PATH`) |
| `requirements.txt` / `pyproject.toml` | Adicionar `streamlit-aggrid>=0.3.4` |
