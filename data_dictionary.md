# Dicionário de Dados — CNES Firebird (CNES.GDB)
> **Versão:** 0.5 — Iteração 5 (validação de dados + CBO enrichment)
> **Status:** ✅ CONCLUÍDO — RQ-001 a RQ-012 documentados, zero-padding, filtragem de cascata, CBO enrichment implementados

---

## Tabelas Mapeadas (Schema Parcial — Inferido do Código)

| Tabela Técnica | Nome de Negócio Inferido | Chave Primária Inferida | Papel no Modelo |
|---|---|---|---|
| `LFCES018` | **Cadastro de Profissionais (PF)** | `PROF_ID` | Entidade raiz: dados pessoais do indivíduo |
| `LFCES004` | **Estabelecimentos de Saúde** | `UNIDADE_ID` | Entidade raiz: unidade física/CNES |
| `LFCES021` | **Vínculos Prof → Estabelecimento** | [(PROF_ID, UNIDADE_ID)](file:///c:/Users/CPD/Projetos/CnesData/scripts/db_profiling_04_final_audit_and_master_query.py#69-80) | Tabela de fatos: CBO, carga horária, lotação |
| `LFCES048` | **Membros de Equipe de Saúde** | [(CPF_PROF, COD_CBO, COD_MUN, SEQ_EQUIPE)](file:///c:/Users/CPD/Projetos/CnesData/scripts/db_profiling_04_final_audit_and_master_query.py#69-80) | Bridge: liga profissional à equipe |
| `LFCES060` | **Equipes de Saúde (ESF/NASF/etc)** | [(SEQ_EQUIPE, COD_AREA, COD_MUN)](file:///c:/Users/CPD/Projetos/CnesData/scripts/db_profiling_04_final_audit_and_master_query.py#69-80) | Entidade: dados da equipe (INE, área, segmento) |
| `NFCES026` | **Domínio CBO (Classificação Brasileira de Ocupações)** | `COD_CBO` | Lookup: código → descrição do cargo |

> [!NOTE]
> As PKs acima são **inferidas** pelos JOINs do [cnes_exporter.py](file:///c:/Users/CPD/Projetos/CnesData/src/cnes_exporter.py). Precisam ser confirmadas
> via `RDB$RELATION_FIELDS` e `RDB$INDICES` (ver Script 1 abaixo).

---

## Colunas-Chave Documentadas (Tipos CONFIRMADOS via `RDB$FIELDS`)

### `LFCES018` — Profissionais (60 colunas)

| Coluna | Tipo Real | Tamanho | Obrig. | Descrição |
|---|---|---|---|---|
| `PROF_ID` | VARYING | 16 | ✅ | **PK** — chave surrogada interna |
| `CPF_PROF` | VARYING | 11 | ✅ | CPF sem formatação — chave de JOIN com LFCES048 |
| `COD_CNS` | VARYING | 60 | ❌ | **Cartão Nacional de Saúde (15 dígitos)** — chave de JOIN com BigQuery (`br_ms_cnes.profissional.cartao_nacional_saude`). Confirmado via amostra: 5/5 profissionais com valores de 15 dígitos. |
| `NOME_PROF` | VARYING | 60 | ✅ | Nome completo |
| `NO_SOCIAL` | VARYING | 60 | ❌ | Nome social (novo campo LGPD) |
| `STATUS` | TEXT | 1 | ❌ | Status do registro (**domínio desconhecido — descobrir**) |
| `STATUSMOV` | TEXT | 1 | ✅ | Status de movimentação — `2` na amostra |
| `DATA_NASC` | DATE | — | ❌ | Data de nascimento |
| `SEXO` | TEXT | 1 | ❌ | Sexo (M/F) |

### `LFCES004` — Estabelecimentos (61 colunas)

| Coluna | Tipo Real | Tamanho | Obrig. | Descrição |
|---|---|---|---|---|
| `UNIDADE_ID` | VARYING | 31 | ✅ | **PK** — chave surrogada interna |
| `CNES` | VARYING | 7 | ❌ | Código CNES nacional (indexado) |
| `CNPJ_MANT` | VARYING | 14 | ❌ | CNPJ da entidade mantenedora |
| `NOME_FANTA` | VARYING | 60 | ✅ | Nome fantasia |
| `TP_UNID_ID` | VARYING | 2 | ❌ | **FK** → tabela nomenclatura (tipo de unidade: UBS, CSII, etc.) |
| `CODMUNGEST` | VARYING | **7** | ✅ | ⚠️ Código IBGE com 7 dígitos de espaço — verificar se armazena 6 ou 7 |
| `CNPJ_MANT` | VARYING | 14 | ❌ | CNPJ da mantenedora |
| `STATUS` | TEXT | 1 | ❌ | Status do registro |
| `STATUSMOV` | TEXT | 1 | ✅ | Status de movimentação |
| `CO_NATUREZ` | VARYING | 4 | ❌ | **FK** → natureza jurídica |
| `CO_TIPO_ES` | VARYING | 3 | ❌ | **FK** → tipo de estabelecimento (detalhe) |

### `LFCES021` — Vínculos (20 colunas)

| Coluna | Tipo Real | Tamanho | Obrig. | Descrição |
|---|---|---|---|---|
| `UNIDADE_ID` | VARYING | 31 | ✅ | **PK+FK** → LFCES004 |
| `PROF_ID` | VARYING | 16 | ✅ | **PK+FK** → LFCES018 |
| `COD_CBO` | VARYING | 6 | ✅ | **PK+FK** — código de ocupação |
| `IND_VINC` | VARYING | 6 | ✅ | **PK+FK** — tipo de vínculo (ex: `010101` = CLT?) — **domínio a mapear** |
| `TP_SUS_NAO` | TEXT | 1 | ✅ | **PK** — `S`=SUS / `N`=Não-SUS |
| `CGHORAOUTR` | LONG | 4 | ❌ | Carga horária outras atividades |
| `CG_HORAAMB` | LONG | 4 | ❌ | Carga horária ambulatorial |
| `CGHORAHOSP` | LONG | 4 | ❌ | Carga horária hospitalar |
| `STATUS` | TEXT | 1 | ❌ | Status do vínculo |
| `STATUSMOV` | TEXT | 1 | ❌ | ⚠️ `NULL` na amostra — diferente de LFCES018 |

### `LFCES048` — Membros de Equipe (5 colunas)

| Coluna | Tipo Real | Tamanho | Obrig. | Descrição |
|---|---|---|---|---|
| `COD_MUN` | VARYING | **6** | ✅ | ⚠️ 6 dígitos — vs LFCES004.CODMUNGEST (7) — verificar mismatch |
| `CPF_PROF` | VARYING | 11 | ✅ | Chave de join com LFCES018 (sem FK declarada) |
| `COD_CBO` | VARYING | 6 | ✅ | CBO do membro nesta equipe |
| `COD_AREA` | VARYING | 4 | ✅ | Área (FK implícita → LFCES060) |
| `SEQ_EQUIPE` | LONG | 4 | ✅ | Sequencial da equipe (FK implícita → LFCES060) |

### `LFCES060` — Equipes de Saúde (10 colunas)

| Coluna | Tipo Real | Tamanho | Obrig. | Descrição |
|---|---|---|---|---|
| `COD_MUN` | VARYING | 6 | ✅ | Município |
| `COD_AREA` | VARYING | 4 | ❌ | Área de atuação |
| `SEQ_EQUIPE` | LONG | 4 | ✅ | Sequencial da equipe |
| `TP_EQUIPE` | VARYING | 2 | ✅ | Tipo: `70`=ESF, `71`=ESB, `76`=EAP — **domínio a mapear** |
| `CNES` | VARYING | 7 | ✅ | CNES da unidade (join com LFCES004.CNES, **não** UNIDADE_ID) |
| `CD_SEGMENT` | VARYING | 2 | ❌ | Segmento: `02`=ESF? — **domínio a mapear** |
| `DS_SEGMENT` | VARYING | 60 | ❌ | Descrição do segmento (ex: "ESF ALTO DA MINA") |
| `DS_AREA` | VARYING | 60 | ❌ | Nome da equipe (ex: "ESF ILHA DE SANTANA I") |
| `INE` | VARYING | 10 | ❌ | Identificador Nacional de Equipe |

### `NFCES026` — Domínio CBO (Classificação Brasileira de Ocupações)

| Coluna | Tipo | Descrição |
|---|---|---|
| `COD_CBO` | VARYING(6) | Código CBO (chave) |
| `DESCRICAO` | VARYING | Descrição do cargo (ex: "AGENTE COMUNITARIO DE SAUDE") |

Usada como lookup para enriquecimento: `extrair_lookup_cbo()` em `cnes_client.py` gera `dict[str, str]` CBO→descrição. Coluna `DESCRICAO_CBO` adicionada a todos os relatórios pelo transformer. Confirmado via `data/discovery/03_nfces026_cbo_descricao.csv`.

---

## Schema BigQuery Confirmado — `br_ms_cnes.profissional`

> **Confirmado em 2026-03-21 contra a tabela real.** Erros anteriores (`id_cbo`, `indicador_sus`) corrigidos.

| Coluna BigQuery | Tipo | Mapeamento → Schema Padronizado |
|---|---|---|
| `ano` | INTEGER | (partição — filtro WHERE, não mapear) |
| `mes` | INTEGER | (partição — filtro WHERE, não mapear) |
| `id_municipio` | STRING | (filtro WHERE, não mapear) |
| `id_estabelecimento_cnes` | STRING | → `CNES` |
| `cartao_nacional_saude` | STRING | → `CNS` |
| `nome` | STRING | → `NOME_PROFISSIONAL` |
| `cbo_2002` | STRING | → `CBO` ⚠️ (não `id_cbo` — campo inexistente) |
| `tipo_vinculo` | STRING | → `TIPO_VINCULO` |
| `indicador_atende_sus` | INTEGER | → `SUS` via `.map({1: "S", 0: "N"})` ⚠️ (não `indicador_sus`) |
| `carga_horaria_ambulatorial` | INTEGER | → `CH_AMBULATORIAL` |
| `carga_horaria_outros` | INTEGER | → `CH_OUTRAS` |
| `carga_horaria_hospitalar` | INTEGER | → `CH_HOSPITALAR` |
| `sigla_uf` | STRING | (não usado) |
| `id_municipio_6_residencia` | STRING | (não usado) |
| `cbo_2002_original` | STRING | (não usado — preferir `cbo_2002` tratado) |
| `cbo_1994` | STRING | (não usado) |
| `indicador_estabelecimento_terceiro` | INTEGER | (não usado) |
| `indicador_vinculo_contratado_sus` | INTEGER | (não usado) |
| `indicador_vinculo_autonomo_sus` | INTEGER | (não usado) |
| `indicador_vinculo_outros` | INTEGER | (não usado) |
| `indicador_atende_nao_sus` | INTEGER | (não usado — inverso de `indicador_atende_sus`) |
| `id_registro_conselho` | STRING | (não usado) |
| `tipo_conselho` | STRING | (não usado) |

**`CPF` indisponível no BigQuery** — campo `CPF` do schema canônico recebe `None` na fonte nacional.

---

## Schema BigQuery Confirmado — `br_ms_cnes.estabelecimento`

> **Confirmado em 2026-03-21.** Tabela tem 204 colunas; apenas as relevantes para o schema são listadas.

| Coluna BigQuery | Tipo | Mapeamento → Schema Padronizado |
|---|---|---|
| `ano` | INTEGER | (partição — filtro WHERE) |
| `mes` | INTEGER | (partição — filtro WHERE) |
| `id_municipio` | STRING | (filtro WHERE — 7 dígitos) |
| `id_municipio_6` | STRING | → `COD_MUNICIPIO` (já 6 dígitos) |
| `id_estabelecimento_cnes` | STRING | → `CNES` |
| `cnpj_mantenedora` | STRING | → `CNPJ_MANTENEDORA` ⚠️ (não `cnpj` — campo inexistente) |
| `id_natureza_juridica` | STRING | → `NATUREZA_JURIDICA` ⚠️ (não `natureza_juridica`) |
| `tipo_unidade` | STRING | → `TIPO_UNIDADE` |
| `indicador_vinculo_sus` | INTEGER | → `VINCULO_SUS` via `.map({1: "S", 0: "N"})` |
| `tipo_gestao` | STRING | (não usado no schema) |

**`NOME_FANTASIA` indisponível no BigQuery** — campo `NOME_FANTASIA` do schema canônico recebe `None` na fonte nacional.

## Schema BigQuery Confirmado — `br_ms_cnes.equipe`

> **Confirmado em 2026-03-21.** Tabela tem 24 colunas.

| Coluna BigQuery | Tipo | Mapeamento potencial |
|---|---|---|
| `id_estabelecimento_cnes` | STRING | → `CNES` |
| `id_equipe` | STRING | → possível `INE` (18 chars — formato diferente do Firebird) |
| `tipo_equipe` | STRING | → `TIPO_EQUIPE` |
| `equipe` | STRING | → `NOME_EQUIPE` |
| `area` | STRING | (área de atuação) |
| `id_municipio` | STRING | (filtro WHERE) |

> [!NOTE]
> O adapter nacional não implementa `listar_equipes()` — a tabela `equipe` do BigQuery usa `id_equipe` (18 chars) como identificador, que não casa diretamente com o `INE` de 10 chars do Firebird. Cross-check de equipes requer análise adicional do formato de junção.

## Tabelas Prioritárias Não Exploradas

| Tabela | Hipótese | Colunas | Prioridade |
|---|---|---|---|
| `LFCES020` | Vínculos históricos / Competência | 11 | 🔴 Alta |
| `NFCES005` | Domínios principais (20 colunas!) | 20 | 🔴 Alta |
| `NFCES088` | Tipo de estabelecimento (13 colunas) | 13 | 🔴 Alta — mapear `TP_UNID_ID` |
| `LFCES000` | Tabela-mãe de estabelecimentos (40 colunas) | 40 | 🟡 Média |
| `LFCES027` | Leitos/Serviços? (46 colunas) | 46 | 🟡 Média |

---

## Regras de Qualidade Mapeadas

### RQ-001 — Filtro de Status ✅ RESOLVIDA
- **Descoberta:** `STATUSMOV = '2'` é o estado ativo em todas as três tabelas principais.
- **Conclusão:** A query atual não precisa de filtro adicional de STATUS — todos os 813 vínculos no banco local já são `STATUSMOV='2'`.
- **Monitoramento:** Em versões futuras com carga de competência, adicionar `WHERE vinc.STATUSMOV = '2'`.

### RQ-002 — CPF Nulo ou Inválido em LFCES018
- **Condição:** `CPF_PROF IS NULL OR LENGTH(TRIM(CPF_PROF)) != 11`
- **Impacto:** Quebra JOIN com LFCES048; profissional some do relatório.
- **Ação:** Logar `WARNING`; excluir da carga e registrar no log de auditoria.

### RQ-012 — Normalização de Zero-Padding (CPF e CNES)
- **CPF:** Firebird pode retornar CPFs com 9-10 dígitos quando o valor começa com zero. Pipeline aplica `zfill(11)` no transformer ANTES do RQ-002. Corrigido em 2026-03-22 (ALERTA-1 da validação).
- **CNES:** Firebird pode retornar CNES com 6 dígitos. Pipeline aplica `zfill(7)` no CnesLocalAdapter. Corrigido em 2026-03-22 (ALERTA-2).

### RQ-003 — Vínculo sem Carga Horária ("Vínculo Zumbi")
- **Condição:** `CG_HORAAMB = 0 AND CGHORAOUTR = 0 AND CGHORAHOSP = 0`
- **Impacto:** Profissional no cadastro sem horas declaradas.
- **Ação:** Flag `STATUS_CH = 'ATIVO_SEM_CH'`.

### RQ-004 — Múltiplos Vínculos na Mesma Unidade ✅ RESOLVIDA
- **Descoberta:** 0 casos encontrados no banco atual.
- **Conclusão:** A PK composta de 5 campos de LFCES021 garante não duplicatas de vínculos por (Prof+Unidade+CBO+IND_VINC+SUS).
- **Monitoramento:** Manter verificação na pipeline de transformação.

### RQ-003-B — Profissionais com Vínculos em Múltiplas Unidades 🔍 MAPEADO
- **Descoberta:** **24 profissionais** com vínculos em 2+ unidades (2 deles com 3 unidades: KATIA MIZUKI BORDIN e LAYLA IZABELLY KONDO).
- **Padrão dominante:** Profissionais com carga mínima (2h) na Residência Terapêutica + carga principal no CAPS — parece estrutural.
- **Ação:** Exportar `03_rq003_multiplas_unidades.csv` para revisão de RH.

> [!CAUTION]
> **ERRO CRÍTICO DE CBO CORRIGIDO:** Os CBOs em versões anteriores estavam errados.
> `516220` = **"CUIDADOR EM SAÚDE"** (não ACS!) e `515320` = **"CONSELHEIRO TUTELAR"** (não ACE!)

### RQ-005 — Auditoria de Lotação ✅ REGRAS CONFIRMADAS PELO USUÁRIO

#### Grupo ACS (Atenção Básica de Saúde da Família)
| CBO | Cargo | Lotação Correta (TP_UNID_ID) |
|---|---|---|
| `515105` | Agente Comunitário de Saúde (ACS) | `01`, `02`, `15` |
| `322255` | Técnico em Agente Comunitário de Saúde (TACS) | `01`, `02`, `15` |

#### Grupo ACE (Vigilância e Controle de Endemias)
| CBO | Cargo | Lotação Correta (TP_UNID_ID) | Observação |
|---|---|---|---|
| `515140` | Agente de Combate às Endemias (legado) | `02`, `69`, `22`, `15`, `50` | CBO em transição |
| `322210` | Técn. Agente de Combate (legado) | `02`, `69`, `22`, `15`, `50` | CBO em transição |
| `322260` | Técn. em Agente de Combate às Endemias (TACE) | `02`, `69`, `22`, `15`, `50` | CBO atual |

> Tipo 50 = COVEPE / órgão de gestão de vigilância epidemiológica. Lotação administrativa válida para ACE/TACE em Presidente Epitácio (confirmado pela validação de dados 2026-03-22).

#### Grupo Saúde Mental (lotação CORRETA confirmada)
| CBO | Cargo | Lotação Correta |
|---|---|---|
| `516220` | Cuidador em Saúde | CAPS (70) / Res. Terapêutica (70) ✅ |

**SQL de Auditoria ACS/TACS fora de unidade correta:**
```sql
SELECT prof.CPF_PROF, prof.NOME_PROF, vinc.COD_CBO, est.NOME_FANTA, est.TP_UNID_ID
FROM LFCES021 vinc
JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
WHERE vinc.COD_CBO IN ('515105', '322255')   -- ACS e TACS
  AND est.TP_UNID_ID NOT IN ('01', '02', '15')
  AND est.CODMUNGEST = '354130'
```

**SQL de Auditoria ACE/TACE fora de unidade correta:**
```sql
WHERE vinc.COD_CBO IN ('515140', '322210', '322260')  -- ACE e TACE
  AND est.TP_UNID_ID NOT IN ('02', '69', '22', '15', '50')
  AND est.CODMUNGEST = '354130'
```

### RQ-006 — Estabelecimentos Fantasma (local sem correspondência nacional) ✅ IMPLEMENTADO
- **Condição:** CNES presente em `df_local` mas ausente em `df_nacional` (base BigQuery)
- **Chave de JOIN:** `CNES` (7 dígitos)
- **Impacto:** Estabelecimento ativo no Firebird local mas sem cadastro na base nacional DATASUS.
- **Output:** `auditoria_rq006_estab_fantasma.csv` — subconjunto de `df_local` sem correspondência
- **Função:** `detectar_estabelecimentos_fantasma(df_local, df_nacional)`

### RQ-007 — Estabelecimentos Ausentes no Local (nacional sem correspondência local) ✅ IMPLEMENTADO
- **Condição:** CNES presente em `df_nacional` mas ausente em `df_local`
- **Chave de JOIN:** `CNES` (7 dígitos)
- **Impacto:** Estabelecimento registrado no DATASUS nacional mas não no banco Firebird local.
- **Output:** `auditoria_rq007_estab_ausente_local.csv` — subconjunto de `df_nacional` sem correspondência
- **Função:** `detectar_estabelecimentos_ausentes_local(df_local, df_nacional, tipos_excluir=None)`

> **Exclusão de escopo (implementada 2026-03-22):** Consultórios Isolados (TIPO_UNIDADE=22) são excluídos do RQ-007 pois pertencem a outros mantenedores (profissionais autônomos), não à prefeitura. Parâmetro `tipos_excluir` na função.

### RQ-008 — Profissionais Fantasma por CNS (local sem correspondência nacional) ✅ IMPLEMENTADO
- **Condição:** CNS presente em `df_local` mas ausente em `df_nacional`
- **Chave de JOIN:** `CNS` (15 dígitos — `LFCES018.COD_CNS` ↔ `br_ms_cnes.profissional.cartao_nacional_saude`)
- **Impacto:** Vínculo ativo no Firebird local sem correspondência na base CNES nacional.
- **Nota:** Registros com CNS nulo em `df_local` são excluídos da comparação.
- **Output:** `auditoria_rq008_prof_fantasma_cns.csv` — subconjunto de `df_local` (com CNS) sem correspondência
- **Função:** `detectar_profissionais_fantasma(df_local, df_nacional)`

### RQ-009 — Profissionais Ausentes no Local por CNS (nacional sem correspondência local) ✅ IMPLEMENTADO
- **Condição:** CNS presente em `df_nacional` mas ausente em `df_local`
- **Chave de JOIN:** `CNS` (15 dígitos)
- **Impacto:** Profissional registrado no DATASUS nacional mas sem vínculo no Firebird local.
- **Nota:** Registros com CNS nulo em `df_nacional` são excluídos da comparação.
- **Output:** `auditoria_rq009_prof_ausente_local_cns.csv` — subconjunto de `df_nacional` (com CNS) sem correspondência
- **Função:** `detectar_profissionais_ausentes_local(df_local, df_nacional, cnes_excluir=None)`

> **Filtragem de cascata (implementada 2026-03-22):** Profissionais cujo CNES já consta no resultado do RQ-007 (estabelecimento ausente no local) são excluídos do RQ-009 para evitar falsos positivos em cascata. Parâmetro `cnes_excluir` na função.

### RQ-010 — Divergência de CBO entre Local e Nacional ✅ IMPLEMENTADO
- **Condição:** Mesmo par (CNS + CNES) com `CBO_LOCAL ≠ CBO_NACIONAL`
- **Chave de JOIN:** `(CNS, CNES)` — inner join entre local e nacional
- **Impacto:** Profissional cadastrado com ocupação diferente nas duas fontes — possível erro de registro.
- **Output:** `auditoria_rq010_divergencia_cbo.csv` — colunas `CNS`, `CNES`, `CBO_LOCAL`, `CBO_NACIONAL`, `DESCRICAO_CBO_LOCAL`, `DESCRICAO_CBO_NACIONAL`
- **Função:** `detectar_divergencia_cbo(df_local, df_nacional, cbo_lookup=None)`

### RQ-011 — Divergência de Carga Horária entre Local e Nacional ✅ IMPLEMENTADO
- **Condição:** Mesmo par (CNS + CNES) com `|CH_LOCAL - CH_NACIONAL| > tolerancia` (padrão: 0h)
- **Chave de JOIN:** `(CNS, CNES)` — inner join entre local e nacional; usa `CH_TOTAL` de ambas as fontes
- **Impacto:** Carga horária declarada difere entre Firebird e BigQuery além da tolerância aceita.
- **Parâmetro:** `tolerancia: int = 0` — diferença mínima em horas para ser considerada divergência
- **Output:** `auditoria_rq011_divergencia_ch.csv` — colunas `CNS`, `CNES`, `CH_LOCAL`, `CH_NACIONAL`, `DELTA_CH`
- **Função:** `detectar_divergencia_carga_horaria(df_local, df_nacional, tolerancia=0)`

---

## Grafo de Relacionamentos (**CONFIRMADO** via `RDB$`)

```
                    NFCES088 (Domínio TP_UNID)
                         ▲ FK
LFCES018 (Profissional)  │
    │ PK=PROF_ID          │
    │ FK: PROF_ID ────────┤
    ▼                     │
LFCES021 (Vínculo)        │
  PK=(UNIDADE_ID,          │
      PROF_ID,             │
      COD_CBO,      ───► LFCES004 (Estabelecimento)
      IND_VINC,             PK=UNIDADE_ID
      TP_SUS_NAO)           COD_MUN→354130
    │                       CNES (7 chars)
    │ CPF_PROF + COD_CBO + COD_MUN (⚠️ sem FK declarada)
    ▼
LFCES048 (Membro Equipe)        ⚠️ SEM PK
    │ SEQ_EQUIPE + COD_AREA + COD_MUN (⚠️ sem FK declarada)
    ▼
LFCES060 (Equipe)               ⚠️ SEM PK
  join via CNES → LFCES004.CNES (não UNIDADE_ID!)
```

> [!WARNING]
> **Dois joins sem FK declarada** no caminho crítico do relatório:
> 1. `LFCES021 → LFCES048` via `CPF_PROF` — sem garantia referencial
> 2. `LFCES048 → LFCES060` via `SEQ_EQUIPE+COD_AREA+COD_MUN` — sem PK destino

---

## Decodificação IND_VINC (CONFIRMADA via NFCES058)

| IND_VINC | CD | TP | Sub | Descrição | Hab. |
|---|---|---|---|---|---|
| `010101` | 01 | 01 | 01 | **SERVIDOR PRÓPRIO** (estatutário efetivo, RJU) | S |
| `010102` | 01 | 01 | 02 | Servidor Cedido (RJU, cedido por outro ente) | S |
| `010202` | 01 | 02 | 02 | Empregado Público CLT Próprio (prazo indet.) | S |
| `010203` | 01 | 02 | 03 | Empregado Público CLT Cedido | S |
| `010301` | 01 | 03 | 01 | **CONTRATADO TEMPORÁRIO PÚBLICO** (lei específica) | S |
| `010302` | 01 | 03 | 02 | Contratado Temporário Privado (CLT prazo det.) | S |
| `010403` | 01 | 04 | 03 | Servidor Público Próprio (cargo comissão) | S |
| `010500` | 01 | 05 | 00 | **CLT PRIVADO** (prazo indet., entidade privada) | S |
| `020900` | 02 | 09 | 00 | Autônomo (sem intermediacão) | S |
| `021000` | 02 | 10 | 00 | Autônomo Pessoa Física | S |
| `060101` | 06 | 01 | 01 | Estagiário | S |
| `070101` | 07 | 01 | 01 | Bolsista | S |

> **No município 354130:** `010500` (289) = CLT via entidade privada, `010101` (280) = Servidor efetivo, `020900` (105) = Autônomo

---

## Validação CODMUNGEST ✅ SEM MISMATCH
- `LFCES004.CODMUNGEST = '354130'` (6 dígitos, mesmo sem padding)
- `LFCES048.COD_MUN = '354130'` (6 dígitos, 197 membros de equipe)
- **Join funciona corretamente.**

---

## Tipos de Equipe Relevantes (NFCES046 CONFIRMADO)

| TP_EQUIPE | Sigla | Nome Completo |
|---|---|---|
| `70` | ESF | Equipe de Saúde da Família |
| `71` | ESB | Equipe de Saúde Bucal |
| `72` | EMULTI | Equipe Multiprofissional APS |
| `76` | EAP | Equipe de Atenção Primária |
| `49` | EAP | Equipe de Atenção Primária (antigo) |
| `01` | ESF | ESF clássica |
| `04` | EACS | Equipe de Agentes Comunitários |

---

## Progresso de Descoberta ✅ CONCLUÍDO

- [x] **Script 1** — 250 tabelas, 1787 colunas, PKs/FKs confirmadas
- [x] **Script 2b** — STATUS ativo=STATUSMOV='2', TP_UNID_ID mapeados, NFCES088 é Prof×CAPS
- [x] **Script 3** — Domínios NFCES, IND_VINC decodificado, CBOs corrigidos, 24 profs multi-unidade
- [x] **Script 4** — Auditoria ACS/ACE limpa (0 anomalias), Query Master validada (367 vínculos)

---

## Query Master Validada (base do cnes_client.py)

**Resultado:** 367 vínculos, ~330 profissionais únicos, múltiplos estabelecimentos.

```sql
SELECT
  prof.CPF_PROF AS CPF,
  prof.NOME_PROF AS NOME_PROFISSIONAL,
  prof.NO_SOCIAL AS NOME_SOCIAL,
  prof.SEXO,
  prof.DATA_NASC AS DATA_NASCIMENTO,
  vinc.COD_CBO AS CBO,
  vinc.IND_VINC AS COD_VINCULO,
  vinc.TP_SUS_NAO_SUS AS SUS_NAO_SUS,
  (COALESCE(vinc.CG_HORAAMB,0)+COALESCE(vinc.CGHORAOUTR,0)+COALESCE(vinc.CGHORAHOSP,0)) AS CARGA_HORARIA_TOTAL,
  COALESCE(vinc.CG_HORAAMB,0) AS CH_AMBULATORIAL,
  COALESCE(vinc.CGHORAOUTR,0) AS CH_OUTRAS,
  COALESCE(vinc.CGHORAHOSP,0) AS CH_HOSPITALAR,
  est.CNES AS COD_CNES,
  est.NOME_FANTA AS ESTABELECIMENTO,
  est.TP_UNID_ID AS COD_TIPO_UNIDADE,
  est.CODMUNGEST AS COD_MUN_GESTOR,
  eq.INE AS COD_INE_EQUIPE,
  eq.DS_AREA AS NOME_EQUIPE,
  eq.TP_EQUIPE AS COD_TIPO_EQUIPE   -- CD_SEGMENT/DS_SEGMENT inaccessible via LEFT JOIN alias
FROM LFCES021 vinc
INNER JOIN LFCES004 est  ON est.UNIDADE_ID=vinc.UNIDADE_ID
INNER JOIN LFCES018 prof ON prof.PROF_ID=vinc.PROF_ID
LEFT JOIN LFCES048 me   ON me.CPF_PROF=prof.CPF_PROF
                       AND me.COD_CBO=vinc.COD_CBO
                       AND me.COD_MUN=est.CODMUNGEST
LEFT JOIN LFCES060 eq   ON eq.SEQ_EQUIPE=me.SEQ_EQUIPE
                       AND eq.COD_AREA=me.COD_AREA
                       AND eq.COD_MUN=me.COD_MUN
WHERE est.CODMUNGEST='354130'
  AND est.CNPJ_MANT='55293427000117'
ORDER BY prof.NOME_PROF, vinc.COD_CBO
```

> [!NOTE]
> `CD_SEGMENT` e `DS_SEGMENT` existem em `LFCES060` (confirmado via `RDB$`) mas retornam
> erro `-206 Column unknown` quando acessados via alias curto em `LEFT JOIN` aninhado no Firebird.
> Se necessário, recuperar em subquery separada após a carga principal.
