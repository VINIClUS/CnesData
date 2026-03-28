# CnesData Analytics Dashboard — Design Spec

**Data:** 2026-03-28
**Objetivo:** App Streamlit local multi-página que consome DuckDB Gold + CSVs arquivados para análise histórica de anomalias CNES.

---

## 1. Contexto e Motivação

O pipeline já produz `gold.auditoria_resultados` e `gold.evolucao_metricas_mensais` no DuckDB. Falta uma interface para explorar esses dados sem precisar abrir o Excel toda vez. O público-alvo é o desenvolvedor/analista que quer:

- Confirmar que a última rodada produziu resultados coerentes
- Identificar tendências (qual regra está melhorando ou piorando?)
- Fazer drill-down em registros individuais por competência e regra

---

## 2. Pré-condições e Correções no Pipeline

Antes de construir o dashboard, dois bugs e uma extensão no `ExportacaoStage` precisam ser corrigidos:

### 2a. Bug: `competencia_stem` sempre retorna `"CNES"`

**Localização:** `src/pipeline/stages/exportacao.py:_persistir_historico`

```python
# ANTES (bugado)
competencia_stem = (
    state.output_path.stem.split("_")[-1]
    if "_" in state.output_path.stem
    else "desconhecida"
)

# DEPOIS (correto)
competencia_stem = state.competencia_str  # ex.: "2024-12"
```

### 2b. Extensão: `gravar_auditoria` para todas as 11 regras

Hoje só grava GHOST, MISSING, RQ005. Adicionar ao final de `_persistir_historico`:

```python
loader.gravar_auditoria(competencia_stem, "RQ003B", len(state.df_multi_unidades))
loader.gravar_auditoria(competencia_stem, "RQ005_ACS", len(state.df_acs_incorretos))
loader.gravar_auditoria(competencia_stem, "RQ005_ACE", len(state.df_ace_incorretos))
loader.gravar_auditoria(competencia_stem, "RQ006", len(state.df_estab_fantasma))
loader.gravar_auditoria(competencia_stem, "RQ007", len(state.df_estab_ausente))
loader.gravar_auditoria(competencia_stem, "RQ008", len(state.df_prof_fantasma))
loader.gravar_auditoria(competencia_stem, "RQ009", len(state.df_prof_ausente))
loader.gravar_auditoria(competencia_stem, "RQ010", len(state.df_cbo_diverg))
loader.gravar_auditoria(competencia_stem, "RQ011", len(state.df_ch_diverg))
```

### 2c. Arquivo histórico de CSVs

Após `_exportar_csvs`, copiar todos os CSVs de auditoria para:

```
data/processed/historico/{competencia}/auditoria_{regra}.csv
```

Mapeamento exato (chave usada em `gravar_auditoria` → nome do CSV em `data/processed/`):

| Chave DuckDB | CSV fonte |
|---|---|
| `RQ003B` | `auditoria_rq003b_multiplas_unidades.csv` |
| `RQ005_ACS` | `auditoria_rq005_acs_tacs_incorretos.csv` |
| `RQ005_ACE` | `auditoria_rq005_ace_tace_incorretos.csv` |
| `GHOST` | `auditoria_ghost_payroll.csv` |
| `MISSING` | `auditoria_missing_registration.csv` |
| `RQ006` | `auditoria_rq006_estab_fantasma.csv` |
| `RQ007` | `auditoria_rq007_estab_ausente_local.csv` |
| `RQ008` | `auditoria_rq008_prof_fantasma_cns.csv` |
| `RQ009` | `auditoria_rq009_prof_ausente_local_cns.csv` |
| `RQ010` | `auditoria_rq010_divergencia_cbo.csv` |
| `RQ011` | `auditoria_rq011_divergencia_ch.csv` |

Criar o diretório `historico/{competencia}/` se não existir. Copiar apenas arquivos não-vazios (os que `_exportar_se_nao_vazio` criou).

---

## 3. Nova Classe: `HistoricoReader`

**Arquivo:** `src/storage/historico_reader.py`

Interface pública:

```python
class HistoricoReader:
    def __init__(self, duckdb_path: Path, historico_dir: Path) -> None: ...

    def carregar_tendencias(
        self,
        regras: list[str] | None = None,
        competencia_inicio: str | None = None,
        competencia_fim: str | None = None,
    ) -> pd.DataFrame:
        """Retorna DataFrame com colunas: data_competencia, regra, total_anomalias."""

    def carregar_kpis(self, competencia: str) -> dict[str, int]:
        """Retorna totais por regra para uma competência específica."""

    def carregar_delta(self, competencia: str) -> dict[str, int]:
        """Retorna variação de cada regra vs competência anterior."""

    def carregar_registros(self, regra: str, competencia: str) -> pd.DataFrame:
        """Lê CSV arquivado via DuckDB read_csv_auto. Retorna DataFrame vazio se ausente."""

    def listar_competencias(self) -> list[str]:
        """Lista competências disponíveis no DuckDB, ordem cronológica crescente."""
```

`carregar_tendencias`, `carregar_kpis`, `carregar_delta` leem de `gold.auditoria_resultados`.
`carregar_registros` usa `duckdb.connect(':memory:').execute("SELECT * FROM read_csv_auto(?)", [path])`.
`listar_competencias` lê de `gold.evolucao_metricas_mensais ORDER BY data_competencia`.

---

## 4. Estrutura de Arquivos

```
scripts/
  dashboard.py                  ← st.set_page_config + injeção de CSS
  pages/
    1_Visao_Geral.py
    2_Tendencias.py
    3_Por_Regra.py
src/
  storage/
    historico_reader.py         ← novo
.streamlit/
  config.toml                   ← tema base + configurações do server
data/
  processed/
    historico/
      2024-12/
        auditoria_rq008_prof_fantasma_cns.csv
        auditoria_rq006_estab_fantasma.csv
        ...
```

---

## 5. Páginas

### 5a. `dashboard.py` — Entry point

- `st.set_page_config(page_title="CnesData Analytics", layout="wide", initial_sidebar_state="expanded")`
- Injeta CSS global via `st.markdown(..., unsafe_allow_html=True)`:
  - `prefers-color-scheme: dark` media query com variáveis CSS para sobreescrever cores do Streamlit
  - Estilos de severity tags (badges coloridos por nível)
  - Responsividade: `max-width` no container principal em telas < 900px
- Instancia `HistoricoReader` e armazena em `st.session_state` (evita reconectar a cada rerun)

### 5b. `1_Visao_Geral.py`

**Sidebar (compartilhado via `dashboard.py`):**
- Selectbox "Competência": `listar_competencias()` em ordem decrescente, default = última

**Conteúdo:**
1. Cabeçalho: competência selecionada + data de geração + total de vínculos
2. **KPI cards** (`st.columns(5)`): uma card por regra principal (RQ008, RQ006, RQ009, RQ010, GHOST). Cada card mostra:
   - Valor atual em destaque (fonte grande)
   - Delta vs mês anterior com seta colorida (▲ vermelho / ▼ verde / → cinza)
   - Borda esquerda colorida por severidade
3. **Tabela resumo** (`st.dataframe` ou `st.table`): todas as 11 regras, colunas: Regra | Descrição | Anomalias | Δ mês | Severidade. Coluna Severidade renderizada como badge colorido via `st.dataframe` com `column_config.TextColumn`.

**UX:**
- Se `listar_competencias()` retornar lista vazia: `st.warning("Nenhuma competência encontrada no DuckDB. Execute o pipeline ao menos uma vez.")`
- KPI cards com `help=` tooltip descrevendo a regra

### 5c. `2_Tendencias.py`

**Sidebar:**
- Multiselect "Regras": todas as 11 regras, default = [RQ008, RQ006, RQ009]
- Date range com dois selectboxes "De / Até competência"

**Conteúdo:**
1. **Gráfico de linhas Plotly** (`plotly.express.line`):
   - `use_container_width=True` — responsivo
   - x = `data_competencia`, y = `total_anomalias`, color = `regra`
   - Hover: competência + regra + total + delta
   - Cores fixas por severidade: CRÍTICA=vermelho, ALTA=laranja, MÉDIA=amarelo, BAIXA=verde
   - `template="plotly_dark"` se sistema em dark mode (detectado via `st_javascript` ou fallback via `.streamlit/config.toml`)
2. **Toggle "Mostrar dados brutos"** (`st.checkbox`): expande `st.dataframe` com os dados do gráfico
3. **Botão "Exportar tabela"**: `st.download_button` com CSV dos dados filtrados

**UX:**
- Se período selecionado tiver só 1 competência: `st.info("Selecione ao menos 2 competências para ver tendência.")`
- Legenda clicável do Plotly para mostrar/ocultar regras individualmente

### 5d. `3_Por_Regra.py`

**Sidebar:**
- Selectbox "Regra": 11 opções com código + descrição curta
- Selectbox "Competência": apenas competências que têm arquivo CSV arquivado para a regra selecionada (via `Path.glob`)

**Conteúdo:**
1. Cabeçalho: `{N} registros — {regra} / {competência}`
2. `st.dataframe(df, use_container_width=True)`: todos os campos do CSV. Colunas CNS/CPF mascaradas por padrão (últimos 4 dígitos visíveis) com checkbox "Mostrar dados completos".
3. `st.download_button("⬇ Baixar CSV", ...)`: download do DataFrame filtrado

**UX:**
- Se CSV ausente para a combinação regra+competência: `st.warning("Sem registros arquivados para {regra} em {competência}. Verifique se o pipeline rodou para essa competência.")`
- `st.metric` no topo mostrando contagem + delta vs competência anterior para a regra selecionada

---

## 6. Tema e Dark Mode

**`.streamlit/config.toml`:**

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

**CSS injetado via `st.markdown`** para suporte a `prefers-color-scheme`:

```css
@media (prefers-color-scheme: light) {
  :root {
    --sev-critica: #ff4444;
    --sev-alta: #ff8800;
    ...
  }
  .stApp { background-color: #ffffff !important; color: #1a1a2e !important; }
  /* overrides de tabelas, sidebar, etc. */
}
```

O `config.toml` define dark como base. O CSS injeta overrides para light mode quando o sistema preferir claro. Isso garante que o app acompanha a preferência do sistema sem toggle manual.

---

## 7. Responsividade

- Todos os gráficos Plotly: `use_container_width=True`
- KPI grid: `st.columns([1,1,1,1,1])` — Streamlit reflow automático em telas menores
- Tabelas: `use_container_width=True`
- CSS: `max-width: 100%; overflow-x: auto` em containers de tabela para scroll horizontal em mobile
- `.streamlit/config.toml`: `layout = "wide"` permite uso total da largura disponível

---

## 8. Dependências

Adicionar a `requirements.txt`:

```
streamlit>=1.32.0
plotly>=5.20.0
```

---

## 9. Execução

```bash
streamlit run scripts/dashboard.py
# Abre automaticamente em http://localhost:8501
```

Adicionar ao `README.md`:

```bash
# Dashboard analítico (requer pipeline executado ao menos uma vez)
streamlit run scripts/dashboard.py
```

---

## 10. Testes

- `tests/storage/test_historico_reader.py` — 8–10 testes unitários com DuckDB `:memory:` + CSVs temporários:
  - `test_carregar_tendencias_filtra_por_regra`
  - `test_carregar_tendencias_filtra_por_periodo`
  - `test_carregar_kpis_retorna_dict_por_regra`
  - `test_carregar_delta_calcula_variacao_correta`
  - `test_carregar_delta_retorna_zero_quando_sem_anterior`
  - `test_carregar_registros_via_read_csv_auto`
  - `test_carregar_registros_retorna_vazio_quando_arquivo_ausente`
  - `test_listar_competencias_ordem_cronologica`

- `tests/pipeline/stages/test_exportacao.py` — 3 novos testes:
  - `test_persistir_historico_usa_competencia_str` (fix do bug)
  - `test_persistir_historico_grava_todas_11_regras`
  - `test_arquivar_csvs_cria_diretorio_historico`

- Pages Streamlit: não testadas com pytest — validação manual via `streamlit run`.

---

## 11. Fora de Escopo

- Autenticação / multi-usuário
- Deploy em servidor remoto
- Notificações / alertas automáticos
- Edição de dados pelo dashboard
- Bronze/Silver layer (roadmap #20, aguarda dados PEC/SIAH)
