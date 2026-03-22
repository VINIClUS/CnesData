# CnesData — Roadmap de Desenvolvimento

Base estabilizada em 2026-03-22. Pipeline canônico em camadas, 345 testes unitários passando. Todos os Work Packages concluídos.

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
| 10 | WP-010 | CLI para execução por colegas sem editar .env |
| 11 | WP-011 | Relatório Excel completo com RESUMO e cross-check |
| 12 | WP-012 | Código limpo e README para onboarding |
| 13 | WP-013 | Execução mensal automatizada |
| 14 | WP-014 | Validação de dados e correção de 5 defeitos |
| 15 | WP-015 | Relatórios legíveis com nomes de cargo |

---

### Cleanup — Remoção de Código Legado
**Data:** 2026-03-22
**Removidos:**
- `src/cnes_exporter.py` (deprecated desde WP-005)
- `tests/test_exporter_integration.py` (testava módulo removido)
- `src/exemplos/` (material didático, não usado no pipeline)

**Adicionados:**
- `tests/test_pipeline_integration.py` (integração real via main.py + CLI)
- `README.md` reescrito com docs CLI e guia de onboarding

---

### ✅ WP-010 — CLI Enhancement (argparse)
**Módulo:** `src/cli.py`
**Objetivo:** Interface de linha de comando com argumentos opcionais que sobrescrevem valores do .env sem editar o arquivo.
**Dependências:** WP-009 ✅
**Critério de Aceite:** ✅
- `tests/test_cli.py` — ~15 testes passando
- Argumentos: `-c`/`--competencia`, `-o`/`--output-dir`, `--skip-nacional`, `--skip-hr`, `-v`/`--verbose`, `--version`
- `CliArgs` dataclass frozen com 5 campos
- Validação de competência: formato YYYY-MM, ano 2000-2099, mês 1-12
- 100% retrocompatível: sem argumentos = comportamento idêntico ao anterior
**Complexidade:** M

---

### ✅ WP-011 — Report Generator Upgrade (RESUMO + Cross-check tabs)
**Módulo:** `src/export/report_generator.py`
**Objetivo:** Expandir relatório Excel para incluir aba RESUMO executivo e abas para as 6 regras de cross-check (RQ-006 a RQ-011).
**Dependências:** WP-009 ✅
**Critério de Aceite:** ✅
- `gerar_relatorio()` aceita `dict[str, pd.DataFrame]` (não kwargs posicionais)
- Aba RESUMO sempre presente: indicadores gerais + tabela de anomalias com severidade colorida
- Até 13 abas: RESUMO + Principal + 11 auditorias
- Coluna RECOMENDACAO em todas as abas de auditoria
- Nomes de aba ≤ 31 caracteres (limite Excel)
**Complexidade:** G

---

### ✅ WP-012 — Cleanup e README para Colegas
**Módulos removidos:** `src/cnes_exporter.py`, `tests/test_exporter_integration.py`, `src/exemplos/`
**Módulos adicionados:** `tests/test_pipeline_integration.py`
**Objetivo:** Remover código morto, reescrever README com docs CLI e guia de onboarding para colegas.
**Critério de Aceite:** ✅
- `cnes_exporter.py` e `src/exemplos/` removidos
- `README.md` com seções: Início Rápido, CLI, Regras de Auditoria, Saídas, Estrutura
- Nenhum import referenciando módulos removidos
**Complexidade:** P

---

### ✅ WP-013 — Automação PowerShell (Execução Mensal)
**Módulos:** `scripts/Run-CnesAudit.ps1`, `scripts/Schedule-CnesAudit.ps1`
**Objetivo:** Scripts para execução automatizada com auto-detecção de competência, rotação de logs, e agendamento via Windows Task Scheduler.
**Critério de Aceite:** ✅
- `Run-CnesAudit.ps1`: auto-competência (mês - 2), rotação de logs (max 6), resumo de execução, exit code correto, notificação de erro opcional
- `Schedule-CnesAudit.ps1`: registra tarefa mensal no Task Scheduler (dia 15 às 07:00)
**Complexidade:** M

---

### ✅ WP-014 — Validação de Dados Reais + Correções
**Módulos alterados:** `transformer.py`, `cnes_local_adapter.py`, `rules_engine.py`, `main.py`, `data_dictionary.md`
**Objetivo:** Executar pipeline com dados reais, validar resultados estatisticamente, e corrigir os 5 ALERTAs identificados.
**Critério de Aceite:** ✅
- ALERTA-1: CPF `zfill(11)` no transformer — corrige 28 CPFs
- ALERTA-2: CNES `zfill(7)` no local adapter — corrige 8 CNES
- ALERTA-3: RQ-009 `cnes_excluir` — remove 393 falsos positivos de cascata
- ALERTA-4: RQ-007 `tipos_excluir` — remove 48 consultórios de outros mantenedores
- ALERTA-5: TIPO_UNIDADE=50 válido para ACE/TACE — remove 18 falsos positivos
- `VALIDACAO_DADOS.md` gerado com status final
**Complexidade:** G

---

### ✅ WP-015 — CBO Enrichment (Descrições de Cargo)
**Módulos alterados:** `cnes_client.py`, `transformer.py`, `rules_engine.py`, `main.py`
**Objetivo:** Adicionar coluna DESCRICAO_CBO com nomes legíveis de cargo extraídos da tabela NFCES026 do Firebird.
**Critério de Aceite:** ✅
- `extrair_lookup_cbo()` em `cnes_client.py` — dict CBO→descrição via NFCES026 (`COD_CBO`, `DESCRICAO`)
- `transformer.py`: parâmetro opcional `cbo_lookup`, coluna `DESCRICAO_CBO`
- `detectar_divergencia_cbo`: colunas `DESCRICAO_CBO_LOCAL` e `DESCRICAO_CBO_NACIONAL`
- Fallback `"CBO NAO CATALOGADO"` para códigos não encontrados
- 100% retrocompatível: sem lookup = sem coluna (testes existentes inalterados)
**Complexidade:** M

---

## Estado Atual da Base

| Módulo | Status | Testes |
|---|---|---|
| `cli.py` | ✅ WP-010 — argparse CLI | ~15 |
| `config.py` | ✅ centralizado | 10 |
| `main.py` (orquestração) | ✅ WP-005 + WP-009 — 11 exports condicionais | 16+ |
| `ingestion/cnes_client.py` | ✅ extração Firebird + lookup CBO | 15 |
| `ingestion/hr_client.py` | ✅ WP-001 — parser xlsx/csv | 21 |
| `ingestion/web_client.py` | ✅ WP-002 — BigQuery via basedosdados | 17 |
| `ingestion/base.py` | ✅ WP-008 — Protocols PEP 544 | 5 |
| `ingestion/schemas.py` | ✅ WP-008 — schema canônico | — |
| `ingestion/cnes_local_adapter.py` | ✅ WP-008 — adapter Firebird | 29 |
| `ingestion/cnes_nacional_adapter.py` | ✅ WP-008 — adapter BigQuery | 14 |
| `processing/transformer.py` | ✅ RQ-002 + RQ-003 + CBO enrichment | ~25 |
| `analysis/rules_engine.py` | ✅ RQ-003-B + RQ-005 + RQ-006–011 | 24 (cross-check) + 30+ (local) |
| `analysis/evolution_tracker.py` | ✅ WP-006 — JSON snapshots | 33 |
| `export/csv_exporter.py` | ✅ Implementado | — |
| `export/report_generator.py` | ✅ WP-011 — Excel RESUMO + 13 abas | 25+ |
| Ghost Payroll | ✅ WP-003 | 10 |
| Missing Registration | ✅ WP-004 | 9 |
| `scripts/Run-CnesAudit.ps1` | ✅ WP-013 — automação mensal | manual |
| `scripts/Schedule-CnesAudit.ps1` | ✅ WP-013 — agendamento Task Scheduler | manual |
