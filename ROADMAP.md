# CnesData — Roadmap de Desenvolvimento

Base estabilizada em 2026-03-21. Pipeline canônico em camadas, 271 testes unitários passando. Todos os Work Packages concluídos.

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

### ✅ WP-008 — Repository/Protocol Pattern (Camada de Ingestão Padronizada)
**Módulos:** `src/ingestion/base.py`, `src/ingestion/schemas.py`, `src/ingestion/cnes_local_adapter.py`, `src/ingestion/cnes_nacional_adapter.py`
**Objetivo:** Eliminar o acoplamento direto entre regras de negócio e backends (Firebird/BigQuery), estabelecendo contratos de interface via PEP 544 Protocols e um schema canônico de colunas que ambos os adapters devem produzir.
**Dependências:** WP-002 ✅
**Critério de Aceite:** ✅
- `tests/ingestion/test_base.py` — 5 testes (`isinstance()` estrutural com Protocols)
- `tests/ingestion/test_cnes_local_adapter.py` — 25 testes (mapeamento de colunas, deduplicação, cache)
- `tests/ingestion/test_cnes_nacional_adapter.py` — 14 testes (schema, CH_TOTAL, SUS, COD_MUNICIPIO 6 dígitos)
- `base.py`: 3 Protocols `@runtime_checkable` — `EstabelecimentoRepository`, `ProfissionalRepository`, `EquipeRepository`
- `schemas.py`: `SCHEMA_PROFISSIONAL`, `SCHEMA_ESTABELECIMENTO`, `SCHEMA_EQUIPE` como `Final[tuple[str, ...]]`
- `CnesLocalAdapter`: cache interno em `_extrair()` — `extrair_profissionais()` chamado uma única vez por execução
- `CnesNacionalAdapter`: levanta `ValueError` explícito para `competencia=None`; `CPF=None` e `NOME_PROFISSIONAL=None` (indisponível no BigQuery)
**Complexidade:** G

---

### ✅ WP-009 — Cross-check Local × Nacional (RQ-006 a RQ-011)
**Módulos:** `src/analysis/rules_engine.py` (6 novas funções), `src/main.py` (integração + 6 exports condicionais), `src/config.py` (`COMPETENCIA_ANO`, `COMPETENCIA_MES`)
**Objetivo:** Reconciliar dados locais (Firebird) com a base nacional CNES (BigQuery) usando CNES como chave para estabelecimentos e CNS como chave para profissionais, detectando fantasmas, ausências e divergências de atributos.
**Dependências:** WP-007 ✅, WP-008 ✅
**Critério de Aceite:** ✅
- `tests/analysis/test_cross_check.py` — 24 testes (edge cases: DataFrame vazio, múltiplos vínculos mesmo CNS, tolerância CH)
- `tests/test_main.py` reescrito com `contextlib.ExitStack` — 16 testes (inclui `TestCrossCheckNacional`)
- 6 regras implementadas: `detectar_estabelecimentos_fantasma`, `detectar_estabelecimentos_ausentes_local`, `detectar_profissionais_fantasma`, `detectar_profissionais_ausentes_local`, `detectar_divergencia_cbo`, `detectar_divergencia_carga_horaria`
- Pipeline gera até 11 CSVs de auditoria por execução (condicionais em `_exportar_se_nao_vazio`)
- Fix crítico: `.dropna().astype(str).str.strip()` para compatibilidade com dtype `float64` em DataFrames vazios
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

## Priorização

| Prioridade | WP | Justificativa |
|---|---|---|
| 8 | WP-008 | Desbloqueia WP-009; elimina acoplamento infra-negócio |
| 9 | WP-009 | Cross-check nacional amplia cobertura de auditoria |

## Estado Atual da Base

| Módulo | Status | Testes |
|---|---|---|
| `ingestion/cnes_client.py` | ✅ Implementado e testado | — |
| `ingestion/hr_client.py` | ✅ WP-001 — parser xlsx/csv | 21 |
| `ingestion/web_client.py` | ✅ WP-002 — BigQuery via basedosdados | 17 |
| `ingestion/base.py` | ✅ WP-008 — Protocols PEP 544 | 5 |
| `ingestion/schemas.py` | ✅ WP-008 — schema canônico | — |
| `ingestion/cnes_local_adapter.py` | ✅ WP-008 — adapter Firebird | 25 |
| `ingestion/cnes_nacional_adapter.py` | ✅ WP-008 — adapter BigQuery | 14 |
| `processing/transformer.py` | ✅ RQ-002 + RQ-003 | — |
| `analysis/rules_engine.py` | ✅ RQ-003-B + RQ-005 + RQ-006–011 | 24 (cross-check) |
| `analysis/evolution_tracker.py` | ✅ WP-006 — JSON snapshots | 33 |
| `export/csv_exporter.py` | ✅ Implementado | — |
| `export/report_generator.py` | ✅ WP-007 — Excel multi-aba | 25 |
| Ghost Payroll | ✅ WP-003 | 10 |
| Missing Registration | ✅ WP-004 | 9 |
| `main.py` (orquestração) | ✅ WP-005 + WP-009 — 11 exports condicionais | 16 |
