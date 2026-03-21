# CnesData — Roadmap de Desenvolvimento

Base estabilizada em 2026-03-21. Pipeline canônico em camadas, 198 testes unitários passando. Todos os Work Packages concluídos.

## Work Packages

### ✅ WP-001 — hr_client.py (Parser de RH)
**Módulo:** `src/ingestion/hr_client.py`
**Objetivo:** Parsear planilhas de folha de pagamento e ponto eletrônico (.xlsx/.csv) com validação estrita de schema antes de carregar em DataFrame.
**Dependências:** nenhuma
**Regras de Negócio:** precondição para WP-003 e WP-004
**Critério de Aceite:** ✅
- `tests/ingestion/test_hr_client.py` — 21 testes passando (mock em `pd.read_excel` / `pd.read_csv`)
- Rejeita arquivos com colunas obrigatórias ausentes (CPF, NOME, STATUS) — mensagem lista ausentes
- Normaliza CPF: remove `.`, `-`, espaços; preserva None
- Detecta e loga CPFs inválidos (comprimento ≠ 11 ou nulo) sem remover registros
- Imutabilidade: `.copy()` antes de transformar; original não é mutado
**Complexidade:** M

---

### ✅ WP-002 — web_client.py (Cliente BigQuery via Base dos Dados)
**Módulo:** `src/ingestion/web_client.py`
**Objetivo:** Buscar dados CNES nacionais via `basedosdados` (BigQuery) para cross-check com o banco Firebird local.
**Dependências:** `basedosdados`, `google-cloud-bigquery`, `GCP_PROJECT_ID` no `.env`
**Chave de JOIN descoberta:** `LFCES018.COD_CNS` ↔ `br_ms_cnes.profissional.cartao_nacional_saude` (CNS 15 dígitos — confirmado 2026-03-21)
**Regras de Negócio:** ghost professionals (CNS local ausente no nacional), missing local registration, divergência de CBO e CH
**Critério de Aceite:** ✅
- `tests/ingestion/test_web_client.py` — 17 testes passando (mock em `bd.read_sql`)
- 4 métodos: `fetch_estabelecimentos`, `fetch_profissionais`, `fetch_profissionais_por_estabelecimento`, `fetch_equipes`
- Exceções tipadas: `CnesWebError`, `CnesWebAuthError`, `CnesWebQuotaError`
- Particionamento por `ano`/`mes` em todos os WHEREs
- Imutabilidade garantida via `.copy()`
**Complexidade:** M

---

### ✅ WP-003 — Ghost Payroll (Folha Fantasma)
**Módulo:** `src/analysis/rules_engine.py` — `detectar_folha_fantasma(df_cnes, df_rh)`
**Objetivo:** Identificar profissionais ativos no CNES mas inativos ou ausentes na folha de RH.
**Dependências:** WP-001 ✅
**Critério de Aceite:** ✅
- `TestGhostPayroll` — 10 testes passando
- Coluna `MOTIVO_GHOST`: `'AUSENTE_NO_RH'` | `'INATIVO_NO_RH'`
- Preserva todas as colunas originais do CNES no resultado
- Retorna vazio quando não há anomalias ou df_cnes vazio
**Complexidade:** M

---

### ✅ WP-004 — Missing Registration (Registro Ausente)
**Módulo:** `src/analysis/rules_engine.py` — `detectar_registro_ausente(df_cnes, df_rh)`
**Objetivo:** Identificar profissionais ativos no RH mas ausentes no CNES local.
**Dependências:** WP-001 ✅
**Critério de Aceite:** ✅
- `TestMissingRegistration` — 9 testes passando
- Apenas STATUS='ATIVO' no RH gera anomalia
- STATUS='INATIVO' e STATUS='AFASTADO' não são falsos positivos
- Preserva colunas do RH no resultado
**Complexidade:** M

---

### ✅ WP-005 — Integração das Regras de Cruzamento no main.py
**Módulo:** `src/main.py`
**Objetivo:** Incorporar Ghost Payroll e Missing Registration ao pipeline principal.
**Dependências:** WP-003 ✅, WP-004 ✅
**Critério de Aceite:** ✅
- `tests/test_main.py` — 15 testes passando (todos I/O mockados)
- Pipeline gera até 6 CSVs: principal + RQ-003-B + RQ-005 ACS + RQ-005 ACE + ghost + missing
- Cross-check HR é condicional: apenas quando `FOLHA_HR_PATH` está no `.env`
- Helper `_exportar_se_nao_vazio` elimina repetição dos 5 blocos `if not df.empty`
- `config.py`: `FOLHA_HR_PATH: Path | None` adicionado (opcional)
**Complexidade:** P

---

### ✅ WP-006 — evolution_tracker.py (Snapshots Históricos)
**Módulo:** `src/analysis/evolution_tracker.py`
**Objetivo:** Criar snapshots datados dos relatórios de auditoria para medir a evolução das inconsistências.
**Dependências:** WP-005 ✅
**Critério de Aceite:** ✅
- `tests/analysis/test_evolution_tracker.py` — 33 testes passando
- `Snapshot`: `data_competencia`, `total_vinculos`, `total_ghost`, `total_missing`, `total_rq005`
- `Delta`: variações + `tendencia` (`MELHORA` | `PIORA` | `ESTAVEL`)
- Persistência JSON em `data/snapshots/snapshot_{YYYY-MM}.json`
- `historico_completo(dir)` retorna todos os deltas consecutivos em ordem cronológica
- Integrado em `main.py`: snapshot salvo automaticamente ao final de cada execução
- `config.py`: `SNAPSHOTS_DIR: Path` adicionado
**Complexidade:** G

---

### ✅ WP-007 — report_generator.py (Relatórios Segmentados)
**Módulo:** `src/export/report_generator.py`
**Objetivo:** Gerar relatórios Excel (.xlsx) multi-aba com formatação e recomendações de correção por registro.
**Dependências:** WP-005 ✅
**Critério de Aceite:** ✅
- `tests/export/test_report_generator.py` — 25 testes passando
- 6 abas possíveis: Principal + Ghost_Payroll + Missing_Registro + Multi_Unidades + ACS_TACS_Incorretos + ACE_TACE_Incorretos
- Abas de auditoria criadas apenas quando DataFrame não-vazio
- Coluna RECOMENDACAO: preenchida, sem nulos, específica por tipo de anomalia
- Cabeçalho formatado: negrito, cor azul escuro, largura de coluna ajustada
- Arquivo abre sem erro no Excel pt-BR (engine openpyxl, encoding implícito)
- Integrado em `main.py`: `Relatorio_Profissionais_CNES.xlsx` gerado a cada execução
**Complexidade:** G

---

## Priorização

| Prioridade | WP | Justificativa |
|---|---|---|
| 1 | WP-001 | Desbloqueia WP-003 e WP-004; risco mais alto (parsing de xlsx real) |
| 2 | WP-003 | Regra de maior impacto operacional (folha fantasma = desvio de recurso público) |
| 3 | WP-004 | Complementa WP-003; juntos fecham o cruzamento CNES × RH |
| 4 | WP-005 | Integração; baixa complexidade, alto retorno imediato |
| 5 | WP-002 | Enriquecimento útil mas não bloqueia as auditorias principais |
| 6 | WP-006 | Snapshots precisam de pelo menos 2 execuções para gerar delta útil |
| 7 | WP-007 | Excel formatado é melhoria de entrega; CSVs já são operacionais |

## Estado Atual da Base

| Módulo | Status |
|---|---|
| `ingestion/cnes_client.py` | ✅ Implementado e testado |
| `processing/transformer.py` | ✅ RQ-002 + RQ-003 implementados e testados |
| `analysis/rules_engine.py` | ✅ RQ-003-B + RQ-005 implementados e testados |
| `export/csv_exporter.py` | ✅ Implementado |
| `ingestion/hr_client.py` | ✅ WP-001 — parser xlsx/csv, 21 testes |
| `ingestion/web_client.py` | ✅ WP-002 — BigQuery via basedosdados, 17 testes |
| Ghost Payroll | ✅ WP-003 — 10 testes |
| Missing Registration | ✅ WP-004 — 9 testes |
| Evolution Tracker | ✅ WP-006 — 33 testes, JSON snapshots |
| Report Generator | ✅ WP-007 — 25 testes, Excel multi-aba |
