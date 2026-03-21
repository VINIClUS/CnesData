# CnesData — Roadmap de Desenvolvimento

Base estabilizada em 2026-03-21. Pipeline canônico em camadas, 68 testes unitários passando.

## Work Packages

### WP-001 — hr_client.py (Parser de RH)
**Módulo:** `src/ingestion/hr_client.py`
**Objetivo:** Parsear planilhas de folha de pagamento e ponto eletrônico (.xlsx/.csv) com validação estrita de schema antes de carregar em DataFrame.
**Dependências:** nenhuma
**Regras de Negócio:** precondição para WP-003 e WP-004
**Critério de Aceite:**
- `tests/ingestion/test_hr_client.py` passa sem banco/arquivo real (fixtures com DataFrames mockados)
- Rejeita arquivos com colunas obrigatórias ausentes (CPF, nome, status)
- Normaliza CPF para 11 dígitos (remove formatação)
- Detecta e loga registros com CPF inválido
**Complexidade:** M

---

### WP-002 — web_client.py (Cliente DATASUS)
**Módulo:** `src/ingestion/web_client.py`
**Objetivo:** Buscar dados abertos do DATASUS via HTTP com retry exponencial e timeout configurável.
**Dependências:** nenhuma
**Regras de Negócio:** enriquecimento de CBO e validação de CNES via tabela nacional
**Critério de Aceite:**
- `tests/ingestion/test_web_client.py` usa `unittest.mock.patch` no `requests.get`
- Retry com backoff exponencial (3 tentativas, timeout 30s)
- Levanta exceção descritiva após esgotar tentativas
**Complexidade:** M

---

### WP-003 — Ghost Payroll (Folha Fantasma)
**Módulo:** `src/analysis/rules_engine.py` — nova função `detectar_folha_fantasma()`
**Objetivo:** Identificar profissionais ativos no CNES mas inativos ou ausentes na folha de RH.
**Dependências:** WP-001 (hr_client.py deve estar implementado)
**Regras de Negócio:** cruzamento CNES × RH por CPF; ativo no CNES = vínculo presente; ativo no RH = registro com status ativo na competência vigente
**Critério de Aceite:**
- `tests/analysis/test_rules_engine.py` — classe `TestGhostPayroll`
- Detecta CPF presente no CNES e ausente no RH
- Detecta CPF presente em ambos mas inativo no RH
- Retorna DataFrame vazio quando não há anomalias
**Complexidade:** M

---

### WP-004 — Missing Registration (Registro Ausente)
**Módulo:** `src/analysis/rules_engine.py` — nova função `detectar_registro_ausente()`
**Objetivo:** Identificar profissionais que constam na folha de RH mas estão ausentes ou desatualizados no CNES local.
**Dependências:** WP-001
**Regras de Negócio:** CPF no RH com status ativo mas ausente em LFCES021; ou presente mas com STATUSMOV ≠ '2'
**Critério de Aceite:**
- `tests/analysis/test_rules_engine.py` — classe `TestMissingRegistration`
- Detecta CPF no RH ausente no CNES
- Não falso-positiva em profissionais com afastamento temporário documentado
**Complexidade:** M

---

### WP-005 — Integração das Regras de Cruzamento no main.py
**Módulo:** `src/main.py`
**Objetivo:** Incorporar os relatórios de Ghost Payroll e Missing Registration ao pipeline principal, exportando CSVs de auditoria segmentados.
**Dependências:** WP-003, WP-004
**Regras de Negócio:** relatórios apenas quando `not df.empty` (evitar arquivos vazios)
**Critério de Aceite:**
- Pipeline completo gera até 5 CSVs de auditoria (principal + 4 regras)
- Testes de integração E2E atualizados (`test_exporter_integration.py` migrado para os novos módulos e `cnes_exporter.py` removido)
**Complexidade:** P

---

### WP-006 — evolution_tracker.py (Snapshots Históricos)
**Módulo:** `src/analysis/evolution_tracker.py`
**Objetivo:** Criar snapshots datados dos relatórios de auditoria para medir a evolução das inconsistências ao longo das competências CNES.
**Dependências:** WP-005
**Regras de Negócio:** cada execução salva snapshot com timestamp de competência; métrica principal = delta de anomalias entre competências consecutivas
**Critério de Aceite:**
- `tests/analysis/test_evolution_tracker.py` — valida estrutura do snapshot e cálculo do delta
- Snapshot inclui: data_competencia, total_vinculos, total_ghost, total_missing, total_rq005
- Delta negativo = melhoria; delta positivo = regressão
**Complexidade:** G

---

### WP-007 — report_generator.py (Relatórios Segmentados)
**Módulo:** `src/export/report_generator.py`
**Objetivo:** Gerar relatórios Excel (.xlsx) multi-aba com formatação condicional, segmentados por tipo de inconsistência e com recomendações de correção por registro.
**Dependências:** WP-005
**Regras de Negócio:** cada aba = uma regra violada; coluna "RECOMENDACAO" com ação sugerida por tipo
**Critério de Aceite:**
- `tests/export/test_report_generator.py` — valida estrutura do .xlsx (abas, colunas, sem nulos em RECOMENDACAO)
- Arquivo gerado abre sem erro no Excel pt-BR
- Cada registro violado tem RECOMENDACAO não vazia
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
| `ingestion/hr_client.py` | ⏳ WP-001 |
| `ingestion/web_client.py` | ⏳ WP-002 |
| Ghost Payroll | ⏳ WP-003 |
| Missing Registration | ⏳ WP-004 |
| Evolution Tracker | ⏳ WP-006 |
| Report Generator | ⏳ WP-007 |
