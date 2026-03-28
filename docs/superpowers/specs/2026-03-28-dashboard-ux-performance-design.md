# Dashboard UX + Performance — Design Spec

**Data:** 2026-03-28
**Escopo:** 4 melhorias independentes no dashboard Streamlit e pipeline de performance.

---

## 1. Tendências — default todas as regras

**Arquivo:** `scripts/pages/2_Tendencias.py:31`

**Mudança:** `default=["RQ008", "RQ006", "RQ009"]` → `default=_TODAS_REGRAS`

**Motivação:** O usuário quer ver o panorama completo ao abrir a aba, não um subconjunto arbitrário.

---

## 2. Por Regra — tabs horizontais

**Arquivo:** `scripts/pages/3_Por_Regra.py`

### Antes
- Sidebar: selectbox "Regra" + selectbox "Competência" (filtrada por regra)
- Gap visual entre seletor e tabela

### Depois
- **Sidebar:** apenas `st.selectbox("Competência", listar_competencias()[::-1])` — uma competência para todas as tabs
- **Área principal:** `st.tabs(labels)` com 11 tabs
  - Labels curtos: `"🔴 RQ-008"`, `"🔴 Ghost"`, `"🟠 RQ-006"`, `"🟠 RQ-007"`, `"🟠 RQ-009"`, `"🟠 Missing"`, `"🟠 RQ-005a"`, `"🟠 RQ-005b"`, `"🟡 RQ-003B"`, `"🟡 RQ-010"`, `"🟢 RQ-011"`
  - Dentro de cada tab: `st.metric` + checkbox máscara + `st.dataframe` + `st.download_button`
  - Se CSV ausente: `st.warning(...)` dentro da própria tab (não bloqueia as demais)

**Benefícios:**
- Todas as regras visíveis de uma vez sem dropdown
- Zero gap — tab ativa e conteúdo são contíguos
- Sem sidebar secundária — competência é seletor único global

### Mapeamento de tabs
| Tab label | Regra | Severidade |
|---|---|---|
| 🔴 RQ-008 | Prof Fantasma (CNS) | CRÍTICA |
| 🔴 Ghost | Ghost Payroll | CRÍTICA |
| 🟠 RQ-006 | Estab Fantasma | ALTA |
| 🟠 RQ-007 | Estab Ausente Local | ALTA |
| 🟠 RQ-009 | Prof Ausente Local | ALTA |
| 🟠 Missing | Missing Registration | ALTA |
| 🟠 RQ-005a | ACS/TACS Incorretos | ALTA |
| 🟠 RQ-005b | ACE/TACE Incorretos | ALTA |
| 🟡 RQ-003B | Múltiplas Unidades | MÉDIA |
| 🟡 RQ-010 | Divergência CBO | MÉDIA |
| 🟢 RQ-011 | Divergência CH | BAIXA |

---

## 3. Visão Geral — contexto quando KPIs são zero

**Arquivos:** `src/storage/historico_reader.py`, `scripts/pages/1_Visao_Geral.py`

### Problema
Pipeline rodou sem dados nacionais → `df_prof_fantasma`, `df_cbo_diverg` etc. ficam vazios → `gravar_auditoria` grava 0 para todas as regras → KPI cards mostram 0 sem contexto → usuário não sabe se o pipeline rodou ou se não há anomalias.

### Solução

**A. Novo método `HistoricoReader.carregar_total_vinculos(competencia: str) -> int`**

```python
def carregar_total_vinculos(self, competencia: str) -> int:
    with duckdb.connect(str(self._duckdb_path), read_only=True) as con:
        df = con.execute(
            "SELECT total_vinculos FROM gold.evolucao_metricas_mensais "
            "WHERE data_competencia = ?",
            [competencia],
        ).df()
    return int(df["total_vinculos"].iloc[0]) if not df.empty else 0
```

**B. `1_Visao_Geral.py` — cabeçalho e callout contextual**

Após o selectbox de competência:
1. `total_vinculos = reader.carregar_total_vinculos(competencia)`
2. Exibir `st.metric("Vínculos processados", total_vinculos)` no cabeçalho (antes dos KPI cards)
3. Lógica de callout (em ordem de prioridade):
   - `total_vinculos == 0` → `st.warning("Pipeline rodou mas não processou vínculos. Verifique os logs.")`
   - `kpis` vazio (auditoria não gravada) e `total_vinculos > 0` → `st.warning("Dados de auditoria não encontrados. Reexecute o pipeline.")`
   - `kpis` não-vazio e `sum(kpis.values()) == 0` e `total_vinculos > 0` → `st.info("Nenhuma anomalia detectada. Se esperava resultados, verifique se o pipeline rodou com dados nacionais (BigQuery habilitado).")`

---

## 4. Performance — executar plano existente

O plano detalhado em `docs/superpowers/plans/2026-03-28-performance-optimizations.md` cobre 4 tasks independentes ordenadas por impacto:

| Task | Mudança | Impacto estimado |
|---|---|---|
| Task 1 | Remover `TIPO_UNIDADE` e `COD_MUNICIPIO` de `_COLUNAS_TEXTO` | Mínimo (dead code) |
| Task 2 | `CachingVerificadorCnes` — cache JSON TTL 24h | Alto (elimina HTTP redundante no cascade_resolver) |
| Task 3 | `ThreadPoolExecutor` para BigQuery | Médio (~40% redução no fetch nacional) |
| Task 4 | Cache pickle TTL 1h para BigQuery | Alto (elimina 15–60s em re-execuções) |

Essas tasks têm código completo no plano existente e serão executadas via `superpowers:executing-plans` sem reescrever.

---

## 5. Fora de Escopo

- Redesign do tema ou paleta de cores
- Autenticação / controle de acesso
- Novos tipos de gráfico em Tendências
- Mudanças no pipeline além das 4 tasks de performance já especificadas
