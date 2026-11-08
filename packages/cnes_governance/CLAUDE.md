# cnes_governance — Sinks de auditoria operacional

## Responsabilidade

Sinks de auditoria operacional. Fronteiras canônicas: docs/architecture.md.

## Dependências

Dependências locais declaradas:

- cnes-contracts
- cnes-data

Não importar apps nem Runtime. Respeitar o grafo de docs/architecture.md.

## Convenções

- Preservar contratos, isolamento de tenant e semântica dos adapters existentes.
- Testes em tests/; mocks somente em fronteiras de I/O.
- Aplicam-se as regras e limites do CLAUDE.md raiz.
- Variáveis de ambiente: .env.example; nunca registrar segredos.
