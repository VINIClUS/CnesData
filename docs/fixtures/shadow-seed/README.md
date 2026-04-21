# Shadow Seed Fixtures (Synthetic)

Fixtures sinteticas geradas com faker para rodar shadow E2E CI contra Firebird docker.

**Nota:** versao atual e SINTETICA (faker-populated ~100 rows/tabela) para viabilizar CI
reproduzivel em qualquer maquina. Fixtures REAIS anonimizados do piloto PE sao gerados
pelo operador local via `scripts/seed_anonymize.py` + upload (nao incluido neste repo por
ora — precisa acesso FB CNES + fbclient.dll).

## Reproducibilidade

```bash
.venv/Scripts/python.exe scripts/gen_shadow_seed_sql.py --seed 42 --rows-per-table 100 \
  --output docs/fixtures/shadow-seed/cnes_seed.sql
```

Output byte-identico dado mesmo seed + faker version.

## Tabelas seed

- `LFCES004` (estabelecimentos) — 100 rows, municipio 354130
- `LFCES018` (profissionais) — 100 rows, nomes pt_BR faker
- `LFCES021` (vinculos) — ~150-200 rows (prof x estab combinacoes)

## Execucao em docker

```bash
docker compose -f docker-compose.shadow.yml up -d
bash scripts/seed_restore.sh docs/fixtures/shadow-seed/
```

`seed_restore.sh` executa `cnes_seed.sql` via `isql-fb` dentro do container.

## Privacidade

Dados 100% sinteticos. Zero risco vazamento.

## Evolucao

Quando fixture real for gerado (PE synthetic anonimizado), swap `cnes_seed.sql`
+ update manifest. CI continua funcionando sem mudancas.
