# Reference Data Fixtures

CSVs for seeding public dimensions (non-tenant data).

## Files

| File | Purpose | Official source |
|---|---|---|
| `cbo2002.csv` | Classificação Brasileira de Ocupações (CBO 2002) | Ministério do Trabalho |
| `cid10.csv` | Classificação Internacional de Doenças 10ª ed. | DATASUS |
| `ibge_municipios.csv` | Municípios brasileiros (IBGE6/IBGE7 + UF + população) | IBGE API |
| `sigtap_2026.csv` | Tabela SIGTAP vigente | DATASUS (tabwin) |

## Current state

Stub files with minimal rows for CI smoke tests. Full production sourcing
is an operations task — replace with complete CSVs before loading gold
data at scale.

## Seed usage

```bash
python scripts/seed_dims_publicos.py --db-url "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test"
```

Idempotent via `ON CONFLICT (cod_*) DO NOTHING`.
