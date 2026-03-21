# Walkthrough — Data Discovery: Banco CNES Firebird

## Objetivo
Reverse-engineering completo do schema do `CNES.GDB` (Firebird) para Presidente Epitácio (IBGE `354130`), com geração da Query Master para o `cnes_client.py`.

---

## Scripts de Profiling Executados

### Script 1 — Inventário de Schema
**Arquivo:** [scripts/db_profiling_01_schema_discovery.py](file:///c:/Users/CPD/Projetos/CnesData/scripts/db_profiling_01_schema_discovery.py)

- 250 tabelas, 1787 colunas inventariadas
- PKs/FKs confirmadas via `RDB$RELATION_FIELDS` e `RDB$REF_CONSTRAINTS`
- Tabelas críticas identificadas: `LFCES018`, `LFCES004`, `LFCES021`, `LFCES048`, `LFCES060`

| Tabela | Função | PK Declarada |
|---|---|---|
| `LFCES018` | Profissionais | `PROF_ID` ✅ |
| `LFCES004` | Estabelecimentos | `UNIDADE_ID` ✅ |
| `LFCES021` | Vínculos Prof×Estab | 5 campos ✅ |
| `LFCES048` | Membros de Equipe | ❌ sem PK |
| `LFCES060` | Equipes de Saúde | ❌ sem PK |

---

### Script 2b — Domínios e Status
**Arquivo:** [scripts/db_profiling_02b_domains_fixed.py](file:///c:/Users/CPD/Projetos/CnesData/scripts/db_profiling_02b_domains_fixed.py)

**Achados:**
- `STATUSMOV = '2'` = registro ativo (confirmado)
- `LFCES020` = inventário de equipamentos (não histórico de vínculos)
- `NFCES088` = Profissional × Programa CAPS (não domínio de tipo de unidade)
- `LFCES004.CODMUNGEST` = `354130` (6 dígitos), sem mismatch com `LFCES048.COD_MUN`

---

### Script 3 — Domínios NFCES e Auditoria
**Arquivo:** [scripts/db_profiling_03_domains_lookup_and_audit.py](file:///c:/Users/CPD/Projetos/CnesData/scripts/db_profiling_03_domains_lookup_and_audit.py)

**🔴 Correção Crítica de CBOs:**
| CBO | Suposição Inicial | Realidade (NFCES026) |
|---|---|---|
| `516220` | ~~ACS~~ | **CUIDADOR EM SAÚDE** |
| `515320` | ~~ACE~~ | **CONSELHEIRO TUTELAR** |
| `515105` | — | ✅ AGENTE COMUNITÁRIO DE SAÚDE |
| `515140` | — | ✅ AGENTE DE COMBATE ÀS ENDEMIAS |

**Outros achados:**
- IND_VINC decodificado via NFCES058 (12 subtipos mapeados)
- 24 profissionais com vínculos em 2+ unidades (padrão CAPS + Res. Terapêutica)
- 0 profissionais com vínculos duplicados na mesma unidade

---

### Script 4 — Auditoria Final e Query Master
**Arquivo:** [scripts/db_profiling_04_final_audit_and_master_query.py](file:///c:/Users/CPD/Projetos/CnesData/scripts/db_profiling_04_final_audit_and_master_query.py)

**Resultados de Auditoria:**
| Grupo | CBOs | TP_UNID válidos | Anomalias |
|---|---|---|---|
| ACS/TACS | `515105`, `322255` | `01`, `02`, `15` | ✅ 0 |
| ACE/TACE | `515140`, `322210`, `322260` | `02`, `50`, `69`, `22`, `15` | ✅ 0 |

**CBOs no banco do município:**
- `515105` (ACS): 71 profissionais, todos em `TP_UNID=02` ✅
- `515140` (ACE): 18 profissionais, todos em `TP_UNID=50` ✅
- `516220` (Cuidador): 7 profissionais, 11 vínculos no CAPS ✅

**Query Master:** 367 vínculos retornados com sucesso.

---

## Quirks do Firebird Descobertos

| Problema | Causa | Solução |
|---|---|---|
| `ORDER BY 1` falha em queries de RDB$ | Firebird não aceita `ORDER BY` posicional em system tables | Usar nome de coluna literal |
| `TRIM()` falha em subqueries de RDB$ | Função não disponível no contexto de system queries | Usar `.strip()` no Python |
| `UnicodeEncodeError` no logger | stdout Windows usa cp1252 | `sys.stdout.reconfigure(encoding='utf-8')` |
| `CD_SEGMENT` inacessível via LEFT JOIN | Quirk do Firebird com aliases curtos em JOINs aninhados | Remover da query; buscar em subquery separada se necessário |
| `pd.read_sql` + LEFT JOIN = erro -501 | pandas tenta fechar cursor já fechado | Usar `cursor.execute()` + `fetchall()` diretamente |

---

## Próximos Passos: Sprint 1

Com o Data Dictionary v0.5 finalizado, a próxima etapa é refatorar `src/ingestion/cnes_client.py` com:

1. A **Query Master validada** como SQL base
2. Decodificação de `IND_VINC` em Python (dicionário já mapeado)
3. Flag `MULTIPLOS_VINCULOS` para os 24 profissionais multi-unidade
4. Regras de qualidade RQ-001 a RQ-005 implementadas como validações pandas
