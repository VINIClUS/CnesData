# Dicionário de Dados — Gold Schema v2

- **Versão:** 2.0
- **Data:** 2026-04-21
- **Status:** Implementado em `010_gold_v2_fresh` (2026-04-22)
- **Autor:** Claude (co-design com Vinícius Andre, session 2026-04-21)
- **Spec de referência:** `docs/superpowers/specs/2026-04-21-gold-v2-implementation-design.md`
- **Migration:** `packages/cnes_infra/src/cnes_infra/alembic/versions/010_gold_v2_fresh.py`
- **Contracts:** `packages/cnes_contracts/` (pydantic models + JSON Schema em `docs/contracts/schemas/`)

Dicionários correlatos (fontes que alimentam Gold v2):

- `docs/data-dictionary-cnes.md` — CNES Firebird local (LFCES*) + nacional (NFCES*)
- `docs/data-dictionary-firebird-bigquery.md` — CNES BigQuery nacional
- `docs/data-dictionary-sihd-hospital.md` — SIHD2 local (TB_AIH/TB_HAIH/TB_PA/TB_HPA)
- `docs/data-dictionary-sia.md` — SIASUS DBFs (105 arquivos introspectados)
- `docs/data-dictionary-bpa.md` — BPA-Mag (BPAMAG.GDB + layout TXT exportação)

---

## 1. Visão geral

Gold v2 adota **star schema híbrido** em duas camadas:

1. **`landing.raw_extractions`** — evidência imutável de auditoria. Cada
   extração (Parquet em MinIO) registrada com `sha256`, `row_count`,
   `schema_version`, `extracao_ts`, `job_id`, `fonte_sistema`.
2. **`gold.*`** — star schema compacto (7 dims + 4 fatos + 1 view
   materializada). Surrogate keys `INT4`, valores em centavos `BIGINT`,
   partitioning por `sk_competencia`.

Garantias de auditabilidade: todo fato carrega `fonte_sistema`, `job_id`,
`extracao_ts` — rastreamento reverso até Parquet raw em MinIO via
`landing.raw_extractions.object_key`.

Byte budget: redução ~65% vs Gold atual (UUID(16) → INT4(4) em chaves,
JSONB redundante removido de fatos, tipos nativos compactos — `SMALLINT`,
`CHAR(N)`, `DATE` onde não há componente temporal).

Multi-tenant: `landing.tenant_id CHAR(6)` IBGE6. Em `gold.*` isolamento
via `set_tenant_id()` ContextVar + RLS Postgres sobre `sk_municipio`.

---

## 2. Layer `landing`

### 2.1 `landing.raw_extractions`

```sql
CREATE SCHEMA IF NOT EXISTS landing;

CREATE TABLE landing.raw_extractions (
    id              UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID          NOT NULL,
    fonte_sistema   TEXT          NOT NULL,
    tenant_id       CHAR(6)       NOT NULL,
    competencia     INT4          NOT NULL,
    tipo_extracao   TEXT          NOT NULL,
    object_key      TEXT          NOT NULL,
    row_count       INT4          NOT NULL,
    sha256          CHAR(64)      NOT NULL,
    schema_version  SMALLINT      NOT NULL,
    extracao_ts     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    ingested_ts     TIMESTAMPTZ,
    CONSTRAINT uniq_source_comp UNIQUE (fonte_sistema, tenant_id, competencia, tipo_extracao),
    CONSTRAINT chk_fonte_sistema CHECK (fonte_sistema IN (
        'CNES_LOCAL', 'CNES_NACIONAL', 'SIHD',
        'SIA_APA', 'SIA_BPI', 'BPA_C', 'BPA_I'
    )),
    CONSTRAINT chk_competencia_yyyymm CHECK (
        competencia BETWEEN 200001 AND 209912
        AND (competencia % 100) BETWEEN 1 AND 12
    )
);

CREATE INDEX ix_landing_tenant_comp ON landing.raw_extractions (tenant_id, competencia);
CREATE INDEX ix_landing_job          ON landing.raw_extractions (job_id);
CREATE INDEX ix_landing_pending      ON landing.raw_extractions (ingested_ts)
    WHERE ingested_ts IS NULL;
```

Semântica: `extracao_ts` marca geração pelo edge agent; `ingested_ts`
NULL indica pendente de consumo pelo `data_processor`. Zero UPDATE/DELETE
em produção — `ingested_ts` é o único campo mutável (flag ciclo de
vida).

---

## 3. Layer `gold` — Dimensões

Schema `gold`. Surrogate keys `INT4 GENERATED ALWAYS AS IDENTITY`.

### 3.1 `gold.dim_profissional`

`cpf_hash` = SHA256 truncado em 11 chars. Fontes merged via JSONB
(nunca sobrescrever).

```sql
CREATE TABLE gold.dim_profissional (
    sk_profissional     INT4         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cpf_hash            CHAR(11)     NOT NULL UNIQUE,
    nome                TEXT         NOT NULL,
    cns                 CHAR(15),
    sk_cbo_principal    INT4,
    fontes              JSONB        NOT NULL DEFAULT '{}'::JSONB,
    criado_em           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    atualizado_em       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_profissional_cbo
        FOREIGN KEY (sk_cbo_principal) REFERENCES gold.dim_cbo (sk_cbo)
);

CREATE INDEX ix_profissional_cbo ON gold.dim_profissional (sk_cbo_principal);
```

### 3.2 `gold.dim_estabelecimento`

`cnes CHAR(7)` inclui DV. `tp_unid` é código de tipo de unidade SIGTAP.

```sql
CREATE TABLE gold.dim_estabelecimento (
    sk_estabelecimento  INT4         GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cnes                CHAR(7)      NOT NULL UNIQUE,
    nome                TEXT         NOT NULL,
    cnpj_mantenedora    CHAR(14),
    tp_unid             SMALLINT     NOT NULL,
    sk_municipio        INT4         NOT NULL,
    fontes              JSONB        NOT NULL DEFAULT '{}'::JSONB,
    criado_em           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    atualizado_em       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_estab_municipio
        FOREIGN KEY (sk_municipio) REFERENCES gold.dim_municipio (sk_municipio)
);

CREATE INDEX ix_estab_municipio ON gold.dim_estabelecimento (sk_municipio);
CREATE INDEX ix_estab_tp_unid   ON gold.dim_estabelecimento (tp_unid);
```

### 3.3 `gold.dim_procedimento_sus`

SIGTAP canônico, 10 dígitos com DV. Vigência por competência para
rastrear procedimentos descontinuados.

```sql
CREATE TABLE gold.dim_procedimento_sus (
    sk_procedimento            INT4      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cod_sigtap                 CHAR(10)  NOT NULL UNIQUE,
    descricao                  TEXT      NOT NULL,
    complexidade               SMALLINT,
    financiamento              CHAR(3),
    modalidade                 CHAR(3),
    competencia_vigencia_ini   INT4,
    competencia_vigencia_fim   INT4,
    CONSTRAINT chk_complexidade  CHECK (complexidade IN (1, 2, 3)),
    CONSTRAINT chk_financiamento CHECK (financiamento IN ('MAC', 'FAE', 'PAB', 'VISA')),
    CONSTRAINT chk_modalidade    CHECK (modalidade IN ('AMB', 'HOSP', 'APAC'))
);

CREATE INDEX ix_procedimento_vigencia
    ON gold.dim_procedimento_sus (competencia_vigencia_ini, competencia_vigencia_fim);
CREATE INDEX ix_procedimento_financ ON gold.dim_procedimento_sus (financiamento);
```

Domínios: `complexidade` 1=baixa/2=média/3=alta. `financiamento` MAC
(Média/Alta Complexidade) / FAE (Fundo Ações Estratégicas) / PAB (Piso
Atenção Básica) / VISA (Vigilância Sanitária). `modalidade` AMB /
HOSP / APAC.

### 3.4 `gold.dim_cbo`

```sql
CREATE TABLE gold.dim_cbo (
    sk_cbo       INT4      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cod_cbo      CHAR(6)   NOT NULL UNIQUE,
    descricao    TEXT      NOT NULL
);
```

### 3.5 `gold.dim_cid10`

```sql
CREATE TABLE gold.dim_cid10 (
    sk_cid       INT4      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cod_cid      CHAR(4)   NOT NULL UNIQUE,
    descricao    TEXT      NOT NULL,
    capitulo     SMALLINT  NOT NULL,
    CONSTRAINT chk_capitulo CHECK (capitulo BETWEEN 1 AND 22)
);

CREATE INDEX ix_cid_capitulo ON gold.dim_cid10 (capitulo);
```

### 3.6 `gold.dim_municipio`

`teto_pab_cents` descoberto no SIA `CADMUN.DBF` (`TETOPAB`) — mantido em
Gold para JOIN em análises de cobertura sem descer ao DBF raw.

```sql
CREATE TABLE gold.dim_municipio (
    sk_municipio          INT4      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ibge6                 CHAR(6)   NOT NULL UNIQUE,
    ibge7                 CHAR(7)   NOT NULL UNIQUE,
    nome                  TEXT      NOT NULL,
    uf                    CHAR(2)   NOT NULL,
    populacao_estimada    INT4,
    teto_pab_cents        BIGINT
);

CREATE INDEX ix_municipio_uf ON gold.dim_municipio (uf);
```

### 3.7 `gold.dim_competencia`

Pré-populada 2020-01 → 2040-12 em migration inicial. `sk_competencia`
sequencial (202001→1, 202002→2, ...) usado como range boundary em
partitioning de fatos. Nunca renumerar sk existente.

```sql
CREATE TABLE gold.dim_competencia (
    sk_competencia      INT4      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    competencia         INT4      NOT NULL UNIQUE,
    ano                 SMALLINT  NOT NULL,
    mes                 SMALLINT  NOT NULL,
    qtd_dias_uteis      SMALLINT,
    inicio_coleta       DATE,
    fim_coleta          DATE,
    CONSTRAINT chk_mes_valido CHECK (mes BETWEEN 1 AND 12)
);

CREATE INDEX ix_competencia_ano_mes ON gold.dim_competencia (ano, mes);
```

---

## 4. Layer `gold` — Fatos

Todos particionados via `PARTITION BY RANGE (sk_competencia)` nativo
Postgres. PK composta inclui `sk_competencia`.

### 4.1 `gold.fato_vinculo_cnes`

Origem: CNES `LFCES021` (TB_CARGA_HORARIA_SUS) + join `LFCES060` para
equipe ESF/NASF.

```sql
CREATE TABLE gold.fato_vinculo_cnes (
    sk_vinculo           BIGINT        GENERATED ALWAYS AS IDENTITY,
    sk_profissional      INT4          NOT NULL,
    sk_estabelecimento   INT4          NOT NULL,
    sk_cbo               INT4          NOT NULL,
    sk_competencia       INT4          NOT NULL,
    carga_horaria_sem    SMALLINT,
    ind_vinc             CHAR(6),
    sk_equipe            INT4,
    job_id               UUID          NOT NULL,
    fonte_sistema        TEXT          NOT NULL,
    extracao_ts          TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (sk_competencia, sk_vinculo),
    CONSTRAINT fk_vinculo_prof  FOREIGN KEY (sk_profissional)    REFERENCES gold.dim_profissional (sk_profissional),
    CONSTRAINT fk_vinculo_estab FOREIGN KEY (sk_estabelecimento) REFERENCES gold.dim_estabelecimento (sk_estabelecimento),
    CONSTRAINT fk_vinculo_cbo   FOREIGN KEY (sk_cbo)             REFERENCES gold.dim_cbo (sk_cbo),
    CONSTRAINT fk_vinculo_comp  FOREIGN KEY (sk_competencia)     REFERENCES gold.dim_competencia (sk_competencia),
    CONSTRAINT chk_fonte_cnes   CHECK (fonte_sistema IN ('CNES_LOCAL', 'CNES_NACIONAL'))
) PARTITION BY RANGE (sk_competencia);

CREATE INDEX ix_vinculo_estab_comp ON gold.fato_vinculo_cnes (sk_estabelecimento, sk_competencia);
CREATE INDEX ix_vinculo_prof_comp  ON gold.fato_vinculo_cnes (sk_profissional, sk_competencia);
```

### 4.2 `gold.fato_producao_ambulatorial`

Produção unificada SIA (APA, BPI) + BPA (BPA-C, BPA-I). Cross-source
dedup em `fontes_reportadas JSONB` (regras em §9).

```sql
CREATE TABLE gold.fato_producao_ambulatorial (
    sk_producao            BIGINT        GENERATED ALWAYS AS IDENTITY,
    sk_profissional        INT4          NOT NULL,
    sk_estabelecimento     INT4          NOT NULL,
    sk_procedimento        INT4          NOT NULL,
    sk_competencia         INT4          NOT NULL,
    sk_cid_principal       INT4,
    qtd                    INT4          NOT NULL,
    valor_aprov_cents      BIGINT        NOT NULL DEFAULT 0,
    dt_atendimento         DATE,
    job_id                 UUID          NOT NULL,
    fonte_sistema          TEXT          NOT NULL,
    extracao_ts            TIMESTAMPTZ   NOT NULL,
    fontes_reportadas      JSONB,
    PRIMARY KEY (sk_competencia, sk_producao),
    CONSTRAINT fk_prod_prof      FOREIGN KEY (sk_profissional)    REFERENCES gold.dim_profissional (sk_profissional),
    CONSTRAINT fk_prod_estab     FOREIGN KEY (sk_estabelecimento) REFERENCES gold.dim_estabelecimento (sk_estabelecimento),
    CONSTRAINT fk_prod_proc      FOREIGN KEY (sk_procedimento)    REFERENCES gold.dim_procedimento_sus (sk_procedimento),
    CONSTRAINT fk_prod_comp      FOREIGN KEY (sk_competencia)     REFERENCES gold.dim_competencia (sk_competencia),
    CONSTRAINT fk_prod_cid       FOREIGN KEY (sk_cid_principal)   REFERENCES gold.dim_cid10 (sk_cid),
    CONSTRAINT chk_fonte_amb     CHECK (fonte_sistema IN ('SIA_APA', 'SIA_BPI', 'BPA_C', 'BPA_I')),
    CONSTRAINT chk_qtd_positivo  CHECK (qtd > 0),
    CONSTRAINT chk_valor_nao_neg CHECK (valor_aprov_cents >= 0)
) PARTITION BY RANGE (sk_competencia);

CREATE INDEX ix_prod_estab_comp ON gold.fato_producao_ambulatorial (sk_estabelecimento, sk_competencia);
CREATE INDEX ix_prod_prof_comp  ON gold.fato_producao_ambulatorial (sk_profissional, sk_competencia);
CREATE INDEX ix_prod_proc_comp  ON gold.fato_producao_ambulatorial (sk_procedimento, sk_competencia);
CREATE INDEX ix_prod_dedup      ON gold.fato_producao_ambulatorial (
    sk_estabelecimento, sk_profissional, sk_procedimento, sk_competencia, dt_atendimento
);
```

### 4.3 `gold.fato_internacao`

Somente AIHs fechadas (`TB_HAIH`). `TB_AIH` em movimento não alimenta
Gold para evitar dupla contagem.

```sql
CREATE TABLE gold.fato_internacao (
    sk_aih                      BIGINT        GENERATED ALWAYS AS IDENTITY,
    num_aih                     CHAR(13)      NOT NULL,
    sk_profissional_solicit     INT4,
    sk_estabelecimento          INT4          NOT NULL,
    sk_competencia              INT4          NOT NULL,
    sk_cid_principal            INT4,
    dt_internacao               DATE          NOT NULL,
    dt_saida                    DATE,
    valor_total_cents           BIGINT,
    job_id                      UUID          NOT NULL,
    fonte_sistema               TEXT          NOT NULL DEFAULT 'SIHD',
    extracao_ts                 TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (sk_competencia, sk_aih),
    CONSTRAINT fk_aih_prof     FOREIGN KEY (sk_profissional_solicit) REFERENCES gold.dim_profissional (sk_profissional),
    CONSTRAINT fk_aih_estab    FOREIGN KEY (sk_estabelecimento)      REFERENCES gold.dim_estabelecimento (sk_estabelecimento),
    CONSTRAINT fk_aih_comp     FOREIGN KEY (sk_competencia)          REFERENCES gold.dim_competencia (sk_competencia),
    CONSTRAINT fk_aih_cid      FOREIGN KEY (sk_cid_principal)        REFERENCES gold.dim_cid10 (sk_cid),
    CONSTRAINT chk_fonte_sihd  CHECK (fonte_sistema = 'SIHD'),
    CONSTRAINT chk_datas_aih   CHECK (dt_saida IS NULL OR dt_saida >= dt_internacao)
) PARTITION BY RANGE (sk_competencia);

CREATE INDEX ix_aih_numero     ON gold.fato_internacao (num_aih);
CREATE INDEX ix_aih_estab_comp ON gold.fato_internacao (sk_estabelecimento, sk_competencia);
CREATE INDEX ix_aih_cid        ON gold.fato_internacao (sk_cid_principal);
```

### 4.4 `gold.fato_procedimento_aih`

Procedimentos executados em cada AIH (`TB_HPA`). `sk_aih` é FK lógico
cross-partition, validado pelo transformer (não enforced como constraint).

```sql
CREATE TABLE gold.fato_procedimento_aih (
    sk_proc_aih              BIGINT        GENERATED ALWAYS AS IDENTITY,
    sk_aih                   BIGINT        NOT NULL,
    sk_procedimento          INT4          NOT NULL,
    sk_profissional_exec     INT4,
    sk_competencia           INT4          NOT NULL,
    qtd                      INT4          NOT NULL DEFAULT 1,
    valor_cents              BIGINT,
    job_id                   UUID          NOT NULL,
    extracao_ts              TIMESTAMPTZ   NOT NULL,
    PRIMARY KEY (sk_competencia, sk_proc_aih),
    CONSTRAINT fk_pa_proc     FOREIGN KEY (sk_procedimento)      REFERENCES gold.dim_procedimento_sus (sk_procedimento),
    CONSTRAINT fk_pa_prof     FOREIGN KEY (sk_profissional_exec) REFERENCES gold.dim_profissional (sk_profissional),
    CONSTRAINT fk_pa_comp     FOREIGN KEY (sk_competencia)       REFERENCES gold.dim_competencia (sk_competencia),
    CONSTRAINT chk_qtd_aih    CHECK (qtd > 0),
    CONSTRAINT chk_valor_aih  CHECK (valor_cents IS NULL OR valor_cents >= 0)
) PARTITION BY RANGE (sk_competencia);

CREATE INDEX ix_pa_aih       ON gold.fato_procedimento_aih (sk_aih);
CREATE INDEX ix_pa_proc_comp ON gold.fato_procedimento_aih (sk_procedimento, sk_competencia);
```

### 4.5 Partition template (anual)

Script roda em job anual (novembro do ano anterior). Ex: ano 2026
assume `sk_competencia` 202601=73 e 202701=85.

```sql
CREATE TABLE gold.fato_vinculo_cnes_2026
    PARTITION OF gold.fato_vinculo_cnes
    FOR VALUES FROM (73) TO (85);

CREATE TABLE gold.fato_producao_ambulatorial_2026
    PARTITION OF gold.fato_producao_ambulatorial
    FOR VALUES FROM (73) TO (85);

CREATE TABLE gold.fato_internacao_2026
    PARTITION OF gold.fato_internacao
    FOR VALUES FROM (73) TO (85);

CREATE TABLE gold.fato_procedimento_aih_2026
    PARTITION OF gold.fato_procedimento_aih
    FOR VALUES FROM (73) TO (85);
```

Partições >3 anos podem ser detached + movidas para tier secundário
via `pg_cron` + `pg_dump`.

---

## 5. Materialized View `gold.view_auditoria_producao`

`FULL OUTER JOIN` entre 3 fatos principais por tupla
`(sk_profissional, sk_estabelecimento, sk_competencia)`. Refresh semanal
`CONCURRENTLY` (requer índice UNIQUE — aplicado).

```sql
CREATE MATERIALIZED VIEW gold.view_auditoria_producao AS
SELECT
    COALESCE(fv.sk_profissional, fpa.sk_profissional, fi.sk_profissional_solicit)  AS sk_profissional,
    COALESCE(fv.sk_estabelecimento, fpa.sk_estabelecimento, fi.sk_estabelecimento) AS sk_estabelecimento,
    COALESCE(fv.sk_competencia, fpa.sk_competencia, fi.sk_competencia)             AS sk_competencia,
    COUNT(DISTINCT fv.sk_vinculo)                                                  AS qtd_vinculos,
    COUNT(DISTINCT fpa.sk_producao)                                                AS qtd_producao_amb,
    COUNT(DISTINCT fi.sk_aih)                                                      AS qtd_aih,
    COALESCE(SUM(fpa.valor_aprov_cents), 0)                                        AS valor_producao_cents,
    COALESCE(SUM(fi.valor_total_cents), 0)                                         AS valor_aih_cents
FROM gold.fato_vinculo_cnes fv
FULL OUTER JOIN gold.fato_producao_ambulatorial fpa
    USING (sk_profissional, sk_estabelecimento, sk_competencia)
FULL OUTER JOIN gold.fato_internacao fi
    ON  fi.sk_profissional_solicit = COALESCE(fv.sk_profissional, fpa.sk_profissional)
    AND fi.sk_estabelecimento      = COALESCE(fv.sk_estabelecimento, fpa.sk_estabelecimento)
    AND fi.sk_competencia          = COALESCE(fv.sk_competencia, fpa.sk_competencia)
GROUP BY 1, 2, 3;

CREATE UNIQUE INDEX ix_vap_key
    ON gold.view_auditoria_producao (sk_profissional, sk_estabelecimento, sk_competencia);
CREATE INDEX ix_vap_comp ON gold.view_auditoria_producao (sk_competencia);
```

Refresh:

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY gold.view_auditoria_producao;
```

Scheduling: weekly cron via `central_api` admin endpoint, domingo 03:00
UTC após fim de semana sem writes pesados.

---

## 6. Byte Budget

Comparação Gold atual vs proposto (storage lógico, overhead Postgres +
TOAST excluídos).

| Tabela | Bytes/row atual | Bytes/row proposto | Economia / 1M rows |
|---|---|---|---|
| `fato_vinculo_cnes` | ~250 (3×UUID + VARCHAR + TIMESTAMPTZ + JSONB) | ~80 (4×INT4 + SMALLINT + CHAR(6) + INT4 + UUID + TEXT + TIMESTAMPTZ) | ~170 MB |
| `fato_producao_ambulatorial` | N/A (novo) | ~85 | baseline |
| `fato_internacao` | N/A (novo) | ~90 | baseline |
| `fato_procedimento_aih` | N/A (novo) | ~60 | baseline |
| `dim_profissional` | ~200 | ~100 | ~100 MB / 10k rows |
| `dim_estabelecimento` | ~200 | ~120 | ~80 MB / 1k rows |
| `dim_procedimento_sus` | N/A (novo) | ~80 | baseline |
| `dim_cbo` | N/A (novo) | ~50 | baseline |
| `dim_cid10` | N/A (novo) | ~60 | baseline |
| `dim_municipio` | ~170 | ~60 | ~110 MB / 5.7k rows |
| `dim_competencia` | N/A (novo) | ~22 | baseline |

**Extrapolação produção:** 10 competências × 100 municípios ×
1M rows/competência ≈ **1 bilhão de linhas em fatos**.

- Gold atual (≈250 B/row): **~1.3 TB**.
- Gold v2 (≈80 B/row avg ponderado): **~400 GB**.
- Economia ≈ **65%**.

Ganhos secundários: índices `INT4` ~4× menores que `UUID`; JOINs mais
rápidos (cache-friendly); partition pruning por `sk_competencia`
elimina I/O em queries filtradas por ano/mês.

---

## 7. Query motriz (auditoria)

Valida se o schema sustenta o caso de uso primário: identificar
profissionais × estabelecimento × competência com alto volume
financeiro (produção + internação).

```sql
SELECT
    sk_profissional,
    sk_estabelecimento,
    sk_competencia,
    qtd_vinculos,
    qtd_producao_amb,
    qtd_aih,
    valor_producao_cents + valor_aih_cents AS total_cents
FROM gold.view_auditoria_producao
WHERE sk_competencia BETWEEN :ini AND :fim
ORDER BY total_cents DESC
LIMIT 100;
```

**Dry-run conceitual:**

- Sem MV: 3×`fato_*` range scan + 3 GROUP BY — custo O(n log n).
- Com MV refresh semanal: custo O(linhas filtradas por
  `sk_competencia`), tipicamente ≤10k/ano em município pequeno —
  latência <100ms. Paginação por keyset `(total_cents DESC,
  sk_profissional ASC)`.

---

## 8. Cross-source dedup SIA + BPA

Chave canônica:

```
(sk_estabelecimento, sk_profissional, sk_procedimento, sk_competencia, dt_atendimento)
```

Regras:

1. Match entre SIA e BPA para mesma tupla → **1 linha** em
   `fato_producao_ambulatorial`.
2. `fonte_sistema` recebe fonte primária por precedência:
   - **SIA > BPA** (SIA é submissão oficial, BPA é captação local).
   - SIA_APA > SIA_BPI (APAC cobre alta complexidade).
   - BPA_I > BPA_C (individualizado é granular).
3. `fontes_reportadas JSONB` lista todas as fontes + valores divergentes:

```json
{
  "SIA_APA":  {"qtd": 3, "valor_aprov_cents": 15000},
  "SIA_BPI":  {"qtd": 3, "valor_aprov_cents": 15000},
  "BPA_I":    {"qtd": 4, "valor_aprov_cents": 20000}
}
```

4. Divergências (qtd ou valor diferentes) são auditáveis — preservadas
   em `fontes_reportadas` sem tentativa de reconciliar.

Ingestão ordenada: SIA primeiro, depois BPA. Row mapper BPA faz UPSERT
condicional — se chave dedup existe, UPDATE `fontes_reportadas` com
merge JSON; senão INSERT.

---

## 9. Naming conventions

- **Idioma:** PT-BR em tabelas/colunas/docstrings de domínio. SQL
  keywords em inglês maiúsculo.
- **Prefixos:** `landing.raw_*` (imutável) / `gold.dim_*` / `gold.fato_*`
  / `gold.view_*`.
- **Surrogate keys:** `sk_` prefix, `INT4` em dimensões, `BIGINT` em
  fatos. Nome descritivo completo (`sk_profissional`, não `sk_prof`).
- **Hashes/códigos naturais:** colunas explícitas (`cpf_hash`,
  `cod_sigtap`, `cod_cid`, `cnes`, `ibge6`) para debug e reconciliação.
- **Valores monetários:** **sempre** `BIGINT` em centavos com sufixo
  `_cents`. Nunca `NUMERIC(N,2)`.
- **Datas:** `DATE` quando só a data importa; `TIMESTAMPTZ` UTC para
  eventos (`extracao_ts`, `criado_em`, `atualizado_em`).
- **Boolean-like:** `SMALLINT`/`CHAR(N)` + CHECK ao invés de `BOOLEAN`
  quando domínio pode evoluir >2 valores.
- **Partitioning:** sempre por `sk_competencia` range anual.
- **Índices:** `ix_<tabela-curta>_<colunas>`. UNIQUE: `uniq_...`.
- **FKs:** `fk_<tabela-curta>_<ref>`. Check: `chk_<descricao>`.

---

## 10. Integrações novas — SIA + BPA

**Status:** Implementado (spec `docs/superpowers/specs/2026-04-23-bpa-sia-pipeline-design.md`, branch `feat/bpa-sia-pipeline`, 2026-04-23).

### 10.1 `dump_agent_go` (edge)

Novas intents em `apps/dump_agent_go/internal/intent/`:

- `sia_apa` — lê `E:/siasus/S_APA.DBF` via biblioteca Go `dbase`
  (DBF reader, encoding cp1252). `fonte_sistema=SIA_APA`.
- `sia_bpi` — lê `S_BPI.DBF`. `fonte_sistema=SIA_BPI`.
- `bpa_c` — lê `BPAMAG.GDB` via `fbdriver` Go (Firebird 1.5.5); fallback
  operacional: TXT de remessa (layout 50B linhas).
  `fonte_sistema=BPA_C`.
- `bpa_i` — idem, 352B linhas. `fonte_sistema=BPA_I`.

Cada intent produz 1 Parquet por competência, registrado em
`landing.raw_extractions`.

> **Local test fixture available:** `docs/fixtures/firebird/` — embedded
> FB 1.5.6 DLL for local/CI tests against BPA-Mag GDBs. Setup via
> `python scripts/fb156_setup.py`. x86-only; consumers must run 32-bit.
> See fixture README for consumer patterns.

### 10.2 `data_processor` (worker)

Novos row mappers em `apps/data_processor/app/mappers/`:

- `sia_apa_mapper.py` — APA_* → `fato_producao_ambulatorial` subtipo
  SIA_APA + FK resolution (CPF hash / CNES / cod_sigtap / CID).
- `sia_bpi_mapper.py` — BPI_* → subtipo SIA_BPI.
- `bpa_c_mapper.py` — BPA-C agregado sem paciente. Chave natural
  `(estab, competência, cbo, idade, procedimento)`.
- `bpa_i_mapper.py` — BPA-I granular com CNS profissional + paciente
  (paciente out-of-scope nesta spec).

### 10.3 `central_api`

Sem mudança estrutural — rotas presigned URL minting genéricas já
suportam qualquer `fonte_sistema`. Adicionar validação no Pydantic
schema para aceitar novos valores (`SIA_APA`, `SIA_BPI`, `BPA_C`,
`BPA_I`).

### 10.4 Campos obrigatórios extraídos

Do Layout_Exportacao_BPA (ver `docs/data-dictionary-bpa.md`):

- `cbc-mvm` → `competencia`
- `cbc-cgccpf` → `cnpj_mantenedora` (quando PJ)
- `prd-cnes` → resolve `sk_estabelecimento`
- `prd-cbo` → resolve `sk_cbo`
- `prd-pa` → resolve `sk_procedimento` (SIGTAP 10 dígitos)
- `prd-cmp` → `sk_competencia`
- `prd-qt` → `qtd`
- `Prd_dtaten` (BPA-I) → `dt_atendimento`
- `Prd_cnsmed` (BPA-I) → resolve `sk_profissional` via CNS quando CPF
  ausente

Do SIA DBFs (ver `docs/data-dictionary-sia.md`):

- `APA_CNES` / `BPI_CNES` → `sk_estabelecimento`
- `APA_CMP` / `BPI_CMP` → `sk_competencia`
- `APA_PROC` / `BPI_PROC` → `sk_procedimento`
- `APA_CNS_MEDEXE` / `BPI_CNSMED` → `sk_profissional`
- `APA_CID` → `sk_cid_principal`
- `APA_VAPROV` / `BPI_VAPROV` × 100 → `valor_aprov_cents`
- `APA_DTINI` / `BPI_DTATEN` → `dt_atendimento`

---

## 11. Exit criteria

Matching spec §12:

- [ ] `docs/data-dictionary-gold-v2.md` completo (este arquivo),
      review pelo autor
- [ ] `docs/data-dictionary-sia.md` completo com 105 DBFs + schema por
      DBF prioritária (Task 5 — feito)
- [ ] `docs/data-dictionary-bpa.md` completo com layout TXT
      introspectado + fluxo operacional (Task 7 — feito)
- [ ] `docs/data-dictionary-cnes.md`, `data-dictionary-sihd-hospital.md`,
      `data-dictionary-firebird-bigquery.md` atualizados com §Gold v2 Mapping
- [ ] `docs/migration-plan-gold-v2.md` com revs Alembic 010-015 +
      rollback
- [ ] Byte budget tabelado para cada tabela (§6)
- [ ] Query motriz `view_auditoria_producao` documentada + dry-run
      comentado (§7)
- [ ] Zero código / migration executada — spec é paper only
- [ ] User review approved

---

**Fim da proposta Gold v2.** Próximo passo pós-approval: spec
`migration-plan-gold-v2.md` com Alembic revs 010-015 detalhadas e
plano de dual-write/shadow validation ≥2 competências antes de
descartar schema legado.
