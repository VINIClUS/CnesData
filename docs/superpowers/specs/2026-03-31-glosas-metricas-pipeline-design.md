# Glosas por Profissional, Métricas Avançadas e Otimização do Pipeline — Design Spec

**Date:** 2026-03-31
**Status:** Approved
**Scope:** Registro de glosas individuais por competência, SEXO no schema canônico,
otimização nacional (TTL + fingerprint), métricas estatísticas avançadas e atualização dos relatórios.

---

## Problema

1. O Gold armazena apenas **contagens agregadas** por regra/competência — não há rastreabilidade por profissional.
2. `SEXO` é extraído pelo Firebird mas descartado no adapter, ficando ausente em todos os relatórios.
3. O `IngestaoNacionalStage` consulta o BigQuery em toda execução, mesmo sem mudança nos dados locais.
4. As métricas disponíveis são exclusivamente contagens brutas — sem taxas, distribuições, reincidência ou comparação inter-competências.

---

## Solução

Extensões cirúrgicas por módulo, sem redesign de arquitetura:

- `SCHEMA_PROFISSIONAL` recebe `SEXO`
- Três novas tabelas DuckDB Gold
- Split de `AuditoriaStage` em Local + Nacional
- `ProcessamentoStage` movido antes do nacional
- `IngestaoNacionalStage` com TTL + fingerprint
- Novo `GlosasBuilder` e `MetricasStage`
- Relatório Excel com aba de métricas avançadas

---

## Modelo de Dados

### SCHEMA_PROFISSIONAL (ingestion/schemas.py)

`SEXO` adicionado após `NOME_PROFISSIONAL`. Registros `FONTE=NACIONAL` terão `None`
(BigQuery não expõe sexo).

```python
SCHEMA_PROFISSIONAL: Final[tuple[str, ...]] = (
    "CNS", "CPF", "NOME_PROFISSIONAL", "SEXO",
    "CBO", "CNES", "TIPO_VINCULO", "SUS",
    "CH_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS", "CH_HOSPITALAR",
    "FONTE",
)
```

### gold.glosas_profissional

Uma linha por (competência, regra, profissional). Sem PRIMARY KEY para evitar
complexidade com campos nullable — upsert via DELETE+INSERT por (competencia, regra).

```sql
CREATE TABLE IF NOT EXISTS gold.glosas_profissional (
    competencia             VARCHAR NOT NULL,
    regra                   VARCHAR NOT NULL,
    cpf                     VARCHAR,
    cns                     VARCHAR,
    nome_profissional       VARCHAR,
    sexo                    VARCHAR(1),
    cnes_estabelecimento    VARCHAR,
    motivo                  VARCHAR,
    criado_em_firebird      TIMESTAMP,
    criado_em_pipeline      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em_pipeline  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
```

> `criado_em_firebird`: NULL enquanto `LFCES018`/`LFCES021` não tiverem timestamps
> mapeados (apenas 9 das 60 colunas documentadas no `data_dictionary.md`).

### gold.cache_nacional

```sql
CREATE TABLE IF NOT EXISTS gold.cache_nacional (
    competencia        VARCHAR PRIMARY KEY,
    fingerprint_local  VARCHAR NOT NULL,
    gravado_em         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
```

### gold.metricas_avancadas

```sql
CREATE TABLE IF NOT EXISTS gold.metricas_avancadas (
    competencia                      VARCHAR PRIMARY KEY,
    taxa_anomalia_geral              DOUBLE,
    p90_ch_total                     DOUBLE,
    proporcao_feminina_geral         DOUBLE,
    n_reincidentes                   INTEGER,
    taxa_resolucao                   DOUBLE,
    velocidade_regularizacao_media   DOUBLE,
    top_glosas_json                  VARCHAR,
    anomalias_por_cbo_json           VARCHAR,
    proporcao_feminina_por_cnes_json VARCHAR,
    ranking_cnes_json                VARCHAR,
    gravado_em                       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)
```

Campos JSON armazenam listas de objetos serializados. Exemplos de schema interno:

- `top_glosas_json`: `[{"cpf": "...", "nome": "...", "total": 3}]`
- `anomalias_por_cbo_json`: `[{"cbo": "515105", "descricao": "ACS", "total": 12, "taxa": 0.08}]`
- `ranking_cnes_json`: `[{"cnes": "2345678", "nome": "UBS ...", "total_anomalias": 5, "indice_conformidade": 0.92}]`
- `proporcao_feminina_por_cnes_json`: `[{"cnes": "2345678", "proporcao_f": 0.65, "total": 20}]`

---

## Fluxo do Pipeline

### Nova ordem das stages

```
IngestaoLocalStage
ProcessamentoStage          ← movido antes do nacional
IngestaoNacionalStage       ← TTL + fingerprint + on-demand
AuditoriaLocalStage         ← split: RQ-003-B, RQ-005, Ghost, Missing
AuditoriaNacionalStage      ← split: RQ-006 a RQ-011
MetricasStage               ← novo
ExportacaoStage             ← atualizado
```

`ProcessamentoStage` não depende de dados nacionais. Movê-lo antes permite ao
`IngestaoNacionalStage` usar `df_processado` para computar o fingerprint local.

### PipelineState — novos campos

```python
nacional_validado: bool = False   # True se BigQuery foi consultado nesta execução
fingerprint_local: str = ""       # SHA256 computado em IngestaoNacionalStage
```

---

## Componente: IngestaoNacionalStage (enhanced)

### Lógica de decisão

```
1. Se executar_nacional=False → skip
2. fingerprint = SHA256(sorted(CPF+CBO+CNES de df_processado))
3. cache = SELECT FROM gold.cache_nacional WHERE competencia = ?
4. Se cache existe
       AND fingerprint == cache.fingerprint_local
       AND (now - cache.gravado_em).days < NACIONAL_CACHE_TTL_DIAS
   → skip BigQuery (CnesNacionalAdapter retorna pickle existente)
5. Senão → query BigQuery normalmente
6. Upsert gold.cache_nacional com fingerprint + gravado_em=now
7. state.nacional_validado = True
```

`NACIONAL_CACHE_TTL_DIAS = 7` em `config.py` (configurável via `.env`).

O `CnesNacionalAdapter` já possui cache pickle por competência. O novo TTL+fingerprint
é uma camada de decisão acima: evita instanciar o adapter quando o cache é válido.

---

## Componente: AuditoriaLocalStage

Arquivo: `src/pipeline/stages/auditoria_local.py`

Executa sempre (independente de `executar_nacional`):

- `detectar_multiplas_unidades` → `state.df_multi_unidades`
- `auditar_lotacao_acs_tacs` → `state.df_acs_incorretos`
- `auditar_lotacao_ace_tace` → `state.df_ace_incorretos`
- `detectar_folha_fantasma` → `state.df_ghost`
- `detectar_registro_ausente` → `state.df_missing`

---

## Componente: AuditoriaNacionalStage

Arquivo: `src/pipeline/stages/auditoria_nacional.py`

Executa apenas se `df_prof_nacional` ou `df_estab_nacional` não-vazios.
Mantém lógica de cascade resolver (RQ-006) e exclusão de CNES do RQ-007 no RQ-009.

- `detectar_estabelecimentos_fantasma` → `state.df_estab_fantasma`
- `detectar_estabelecimentos_ausentes_local` → `state.df_estab_ausente`
- `detectar_profissionais_fantasma` → `state.df_prof_fantasma`
- `detectar_profissionais_ausentes_local` → `state.df_prof_ausente`
- `detectar_divergencia_cbo` → `state.df_cbo_diverg`
- `detectar_divergencia_carga_horaria` → `state.df_ch_diverg`

`auditoria.py` atual é removido e substituído pelos dois arquivos acima.

---

## Componente: GlosasBuilder

Arquivo: `src/analysis/glosas_builder.py`

### Interface

```python
def construir_glosas(
    competencia: str,
    state: PipelineState,
    criado_em_pipeline: datetime,
) -> pd.DataFrame:
    """Retorna DataFrame com schema gold.glosas_profissional para INSERT."""
```

### Mapeamento de colunas por regra

| Regra | CPF | CNS | NOME | SEXO | CNES | MOTIVO |
|---|---|---|---|---|---|---|
| RQ003B | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| RQ005_ACS | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| RQ005_ACE | ✅ | ✅ | ✅ | ✅ | ✅ | — |
| GHOST | ✅ | ✅ | ✅ | ✅ | ✅ | MOTIVO_GHOST |
| MISSING | ✅ | — | ✅ | — | — | — |
| RQ008 | — | ✅ | ✅ | — | ✅ | — |
| RQ009 | — | ✅ | ✅ | — | ✅ | — |
| RQ010 | — | ✅ | — | — | ✅ | `CBO_LOCAL != CBO_NACIONAL` |
| RQ011 | — | ✅ | — | — | ✅ | `DELTA_CH` |

Cada regra tem uma função privada `_extrair_<regra>(df) -> pd.DataFrame`.
`pd.concat` de todos os blocos não-vazios com coluna `regra` preenchida.

---

## Componente: MetricasStage

Arquivos:
- `src/analysis/metricas_avancadas.py` — funções puras (sem DuckDB, testáveis isoladamente)
- `src/pipeline/stages/metricas.py` — stage que orquestra cálculo + persistência

### Funções puras (metricas_avancadas.py)

```python
def calcular_taxa_anomalia(df_vinculos, df_glosas) -> float
    # len(CPF/CNS únicos em df_glosas) / len(df_vinculos)

def calcular_p90_ch(df_vinculos) -> float
    # df_vinculos["CH_TOTAL"].quantile(0.90)

def calcular_proporcao_feminina(df_vinculos) -> float
    # SEXO=="F" / total com SEXO não-nulo

def calcular_proporcao_feminina_por_cnes(df_vinculos) -> list[dict]

def calcular_top_glosas(df_glosas, n: int = 10) -> list[dict]
    # group by cpf/cns+nome, top N por contagem

def calcular_anomalias_por_cbo(df_vinculos, df_glosas, cbo_lookup) -> list[dict]

def calcular_ranking_cnes(df_estab_local, df_glosas, df_vinculos) -> list[dict]
    # indice_conformidade = 1 - (anomalias_cnes / vinculos_cnes)

def calcular_reincidencia(competencia, df_glosas_historico) -> int
    # profissionais com mesma (cpf/cns + regra) em >= 2 competências consecutivas

def calcular_taxa_resolucao(comp_anterior, comp_atual, df_glosas_historico) -> float
    # % de (cpf/cns + regra) da comp_anterior ausentes na comp_atual

def calcular_velocidade_regularizacao(df_glosas_historico) -> float
    # média de competências entre primeira e última ocorrência de glosas já resolvidas
```

Métricas temporais leem `gold.glosas_profissional` via `HistoricoReader` — sem
reprocessar dados do Firebird.

### MetricasStage

1. Constrói `df_glosas` via `GlosasBuilder.construir_glosas()`
2. Persiste em `gold.glosas_profissional` via `DatabaseLoader.gravar_glosas()`
3. Carrega histórico de glosas das competências anteriores via `HistoricoReader`
4. Calcula todas as métricas
5. Persiste em `gold.metricas_avancadas` via `DatabaseLoader.gravar_metricas_avancadas()`

---

## Componente: DatabaseLoader (novos métodos)

Os três métodos existentes permanecem inalterados. Adicionados:

```python
def gravar_glosas(self, competencia: str, regra: str, df: pd.DataFrame) -> None:
    """DELETE + INSERT para (competencia, regra). Preserva criado_em_pipeline original."""

def gravar_cache_nacional(self, competencia: str, fingerprint: str) -> None:
    """UPSERT em gold.cache_nacional."""

def gravar_metricas_avancadas(self, competencia: str, metricas: dict) -> None:
    """INSERT OR REPLACE em gold.metricas_avancadas."""
```

`inicializar_schema()` passa a criar as 3 novas tabelas além das 2 existentes.

---

## Componente: ExportacaoStage (mudanças)

### CSVs

- `vinculos_processados.csv`: `SEXO` incluída automaticamente via propagação do schema
- Todos os CSVs de auditoria com dados de profissionais recebem `SEXO` sem alteração explícita

### Excel (report_generator.py)

- `SEXO` presente em todas as abas de profissionais (propagação automática)
- Nova aba **"Métricas Avançadas"** inserida após RESUMO:
  - Bloco 1: Indicadores gerais (taxa anomalia, P90 CH, proporção feminina, reincidentes, taxa resolução)
  - Bloco 2: Top-10 profissionais com mais glosas
  - Bloco 3: Top-10 CBOs por volume de anomalias
  - Bloco 4: Ranking CNES por índice de conformidade

---

## Arquivos Alterados / Criados

### Novos
| Arquivo | Responsabilidade |
|---|---|
| `src/analysis/glosas_builder.py` | DataFrames de regras → schema glosas |
| `src/analysis/metricas_avancadas.py` | Funções puras de cálculo de métricas |
| `src/pipeline/stages/auditoria_local.py` | RQ-003-B, RQ-005, Ghost, Missing |
| `src/pipeline/stages/auditoria_nacional.py` | RQ-006 a RQ-011 |
| `src/pipeline/stages/metricas.py` | Orquestração de métricas + persistência |

### Alterados
| Arquivo | Mudança |
|---|---|
| `src/ingestion/schemas.py` | `SEXO` em `SCHEMA_PROFISSIONAL` |
| `src/ingestion/cnes_local_adapter.py` | Propagar `SEXO` no mapping |
| `src/ingestion/cnes_nacional_adapter.py` | Preencher `SEXO=None` |
| `src/pipeline/state.py` | `nacional_validado`, `fingerprint_local` |
| `src/pipeline/stages/ingestao_nacional.py` | TTL + fingerprint + skip logic |
| `src/storage/database_loader.py` | 3 novos métodos + 3 novas tabelas no DDL |
| `src/storage/historico_reader.py` | Método para carregar glosas históricas |
| `src/export/report_generator.py` | Aba Métricas Avançadas |
| `src/pipeline/stages/exportacao.py` | Chamar MetricasStage outputs, novo CSV_MAP |
| `src/main.py` | Nova ordem de stages, remover AuditoriaStage |
| `config.py` | `NACIONAL_CACHE_TTL_DIAS` |

### Removidos
| Arquivo | Substituído por |
|---|---|
| `src/pipeline/stages/auditoria.py` | `auditoria_local.py` + `auditoria_nacional.py` |

---

## Testes

Cada novo módulo tem suite própria em `tests/`:

| Teste | Estratégia |
|---|---|
| `test_glosas_builder.py` | DataFrames sintéticos por regra → assert colunas + valores |
| `test_metricas_avancadas.py` | DataFrames determinísticos → assert métricas calculadas |
| `test_metricas_stage.py` | Mock DatabaseLoader + HistoricoReader |
| `test_ingestao_nacional_ttl.py` | Mock DuckDB + datetime → assert skip/fetch por cenário |
| `test_auditoria_local_stage.py` | Mock PipelineState → assert DataFrames de resultado |
| `test_auditoria_nacional_stage.py` | Mock PipelineState com dados nacionais sintéticos |

Todos os testes existentes de `auditoria.py` são migrados para os dois novos arquivos.

---

## Fora de Escopo

- Mapeamento completo das 60 colunas do `LFCES018` (necessário para `criado_em_firebird`)
- Dashboard Streamlit para visualização das métricas avançadas
- API de consulta às glosas históricas
- Notificações/alertas automáticos por threshold de anomalia
