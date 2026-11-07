# cnes_db_migrator — Runner Alembic

Executa uma vez `alembic upgrade head` usando o recurso `cnes_data:alembic`.
Não mantém estado nem executa retry próprio. Integra-se às stacks e ao CI
existentes; o deploy deste repositório não usa Kubernetes.

## Contrato

- `DB_URL` é obrigatório; ausente, registra o erro e encerra com código 1.
- URL é passada explicitamente ao Config do Alembic e precede o fallback de Data.
- Em sucesso registra `migration_complete target=head`.
- Histórias e IDs de migração são imutáveis após merge.
- Não gerar revisions nem executar contra produção durante refatorações.

## Dependências e arquivos

- Dependência local: cnes-data; nenhuma dependência de Runtime ou da API.
- Entrypoint: src/cnes_db_migrator/run.py (`cnes-migrate`).
- Config para uso manual: packages/cnes_data/alembic.ini.
- Recursos: packages/cnes_data/alembic/, incluídos no wheel.
- Testes: tests/test_run.py; integração de schema no CI com PostgreSQL.

Aplicam-se as instruções do CLAUDE.md raiz. Operação: ops/README.md e
runbooks canônicos em docs/runbooks/.
