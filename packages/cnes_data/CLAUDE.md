# cnes_data — Persistência, ingestão e object stores

## Responsabilidade

Persistência, ingestão e object stores. Fronteiras canônicas: docs/architecture.md.

## Dependências

Dependências locais declaradas: cnes-contracts, cnes-domain, cnes-observability.
Não importar apps, cnes_runtime ou implementações de outra capability fora
do grafo aprovado. Exceção: cnes_runtime é a camada externa de composição.

## Convenções

- Preservar contratos, isolamento de tenant e semântica dos adapters existentes.
- Testes em tests/; mocks somente em fronteiras de I/O.
- Aplicam-se as regras e limites do CLAUDE.md raiz.
- Variáveis de ambiente: .env.example; nunca registrar segredos.

## Persistência

- Migrações: recurso cnes_data:alembic; revisions publicadas são imutáveis.
- Config Alembic explícita precede DB_URL; não importar Runtime.
- Toda consulta Postgres deve manter set_tenant_id e RLS.
- Gold v2 preserva plain INSERT para vínculos e merge JSONB de fontes nas dims.
- ETL e national são extras opcionais; não carregá-los no import raiz.
