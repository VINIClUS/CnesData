# Migration Plan — Gold Schema v2

- **Data:** 2026-04-21
- **Status:** Design — não executar sem aprovação
- **Spec:** `docs/superpowers/specs/2026-04-21-gold-schema-refactor-sia-bpa-design.md`
- **Target DB:** Postgres (central) — schemas `gold` + `landing`

## Convenções

- Alembic script_location: `packages/cnes_infra/src/cnes_infra/alembic/versions/`
- Revision numerada sequencial (próxima = `010`, já que 009 é agent_version de Plan C)
- Cada rev é atômica; rollback via `alembic downgrade`

## Revisões propostas

### `010_gold_dim_compartilhadas_novas`

**Conteúdo:**
- CREATE TABLE gold.dim_profissional_new (sk_profissional INT4 identity, cpf_hash CHAR(11), ...)
- CREATE TABLE gold.dim_estabelecimento_new (...)
- CREATE TABLE gold.dim_procedimento_sus (sk_procedimento INT4 identity, cod_sigtap CHAR(10), ...)
- CREATE TABLE gold.dim_cbo (sk_cbo INT4 identity, cod_cbo CHAR(6), ...)
- CREATE TABLE gold.dim_cid10 (...)
- CREATE TABLE gold.dim_municipio (...)
- CREATE TABLE gold.dim_competencia (...)

**Rollback:** DROP TABLE gold.dim_*_new + gold.dim_procedimento_sus + gold.dim_cbo.

**Risco:** nenhum — coexiste com gold antigo.

---

### `011_gold_popular_dims_iniciais`

**Conteúdo:**
```sql
INSERT INTO gold.dim_profissional_new (cpf_hash, nome, cns, fontes, criado_em)
SELECT cpf, nome, cns, fontes, criado_em FROM gold.dim_profissional;

INSERT INTO gold.dim_estabelecimento_new (cnes, nome, cnpj_mantenedora, tp_unid, sk_municipio, fontes)
SELECT cnes, nome, cnpj_mant, tp_unid_id, sk_municipio, fontes
FROM gold.dim_estabelecimento
JOIN gold.dim_municipio_lookup ON ...
```

Backfill dim_municipio + dim_cbo via sincronização inicial contra fontes domínio
(NFCES005 + CADMUN SIA + TB_MUN SIHD reconciliados).

**Rollback:** TRUNCATE gold.dim_*_new.

---

### `012_gold_fatos_novos_particionados`

**Conteúdo:**
- CREATE TABLE gold.fato_vinculo_cnes (sk_vinculo BIGINT identity, sk_profissional INT4, ...) PARTITION BY RANGE (sk_competencia)
- Criar partições iniciais para competências 202401-202612 (24 ranges)
- CREATE TABLE gold.fato_producao_ambulatorial (...) PARTITION BY RANGE
- CREATE TABLE gold.fato_internacao (...) PARTITION BY RANGE
- CREATE TABLE gold.fato_procedimento_aih (...) PARTITION BY RANGE

**Rollback:** DROP TABLE gold.fato_* CASCADE.

---

### `013_gold_migrar_fato_vinculo`

**Conteúdo:**
- INSERT INTO gold.fato_vinculo_cnes SELECT ... FROM gold.fato_vinculo (antigo) com sk_* resolvidos via JOIN nas novas dims
- data_processor atualiza para dual-write (escrever tanto em fato_vinculo antigo quanto fato_vinculo_cnes novo)
- Validar paridade: `SELECT count(*) FROM fato_vinculo` vs `SELECT count(*) FROM fato_vinculo_cnes` → igual ±0.1%

**Rollback:** TRUNCATE gold.fato_vinculo_cnes + revert data_processor dual-write code.

**Risco:** médio — dual-write complica código. Manter por 2 competências máximo.

---

### `014_gold_view_auditoria`

**Conteúdo:**
```sql
CREATE MATERIALIZED VIEW gold.view_auditoria_producao AS
SELECT ... FROM gold.fato_vinculo_cnes fv
FULL OUTER JOIN gold.fato_producao_ambulatorial fpa USING (sk_profissional, sk_estabelecimento, sk_competencia)
FULL OUTER JOIN gold.fato_internacao fi ON ...
GROUP BY ROLLUP (1, 2, 3);

CREATE UNIQUE INDEX ix_vap_key
    ON gold.view_auditoria_producao (sk_profissional, sk_estabelecimento, sk_competencia);
```

Cron semanal no central_api para REFRESH CONCURRENTLY.

**Rollback:** DROP MATERIALIZED VIEW gold.view_auditoria_producao.

---

### `015_gold_drop_tabelas_antigas` (DESTRUTIVA)

**Pre-flight obrigatório:**
- [ ] Executar `SELECT count(*)` nas _old vs nas novas — paridade ≥ 99.9%
- [ ] `pg_dump --schema=gold` executado + armazenado em bucket retention 30d
- [ ] data_processor stable há ≥ 2 competências sem anomalia
- [ ] Shadow E2E gold ≥ 7 consecutivas verdes (paridade dados)

**Conteúdo:**
```sql
DROP TABLE gold.fato_vinculo;
DROP TABLE gold.dim_profissional;
DROP TABLE gold.dim_estabelecimento;
ALTER TABLE gold.dim_profissional_new RENAME TO dim_profissional;
ALTER TABLE gold.dim_estabelecimento_new RENAME TO dim_estabelecimento;
```

**Rollback:** restaurar via pg_dump (~30min para DB de 100GB). Not reversible via alembic downgrade.

**Risco:** alto — destrutivo. Só após todo pipeline confirmar paridade.

---

## Timeline recomendada pós-approval

| Semana | Rev | Ação | Responsável |
|---|---|---|---|
| 1 | 010, 011 | Aplicar em staging + dev local | Backend eng |
| 2 | 012 | Criar fatos particionados + popular competências teste | Backend eng |
| 3 | 013 | Dual-write data_processor + primeira competência dual | Data eng |
| 4 | — | Validar paridade 1ª competência | Data eng |
| 5 | — | Segunda competência dual + validação | Data eng |
| 6 | 014 | MatView + cron refresh | Backend eng |
| 7 | 015 | Drop destrutivo + pg_dump pre-flight | DBA |
| 8 | — | Consumer Gold v2 ativo em queries de auditoria | Audit team |

## Risk matrix

| Rev | Destrutivo | Rollback | Downtime estimado |
|---|---|---|---|
| 010 | não | alembic downgrade | 0 |
| 011 | não (idempotent) | TRUNCATE | <5min |
| 012 | não | DROP CASCADE | 0 |
| 013 | não | TRUNCATE + revert code | <10min |
| 014 | não | DROP MATVIEW | 0 |
| 015 | sim | pg_dump restore (~30min) | 10-30min |

## Integração SIA + BPA (pós-015)

Após gold v2 estabilizado, adicionar:
- Adapter Go: `dump_agent_go` novos intents `sia_apa`, `sia_bpi`, `bpa_c`, `bpa_i`
- Mappers data_processor: `sia_bpi_row_mapper.py`, `bpa_producao_row_mapper.py`
- Fontes: `fato_producao_ambulatorial.fonte_sistema` IN (SIA_APA, SIA_BPI, BPA_C, BPA_I)
- Dedup cross-source: chave (sk_estab, sk_prof, sk_procedimento, sk_competencia, dt_atendimento)

---

**Fim migration plan.** Execução real em Spec C futura.
