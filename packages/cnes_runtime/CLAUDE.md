# cnes_runtime — Configuração e composição de runtime

## Responsabilidade

Configuração e composição de runtime. Fronteiras canônicas: docs/architecture.md.

## Dependências

Dependências locais declaradas:

- cnes-contracts
- cnes-control-plane
- cnes-data
- cnes-governance

Camada externa de composição; nenhuma capability depende de Runtime. Não importar apps.

## Convenções

- Preservar contratos, isolamento de tenant e semântica dos adapters existentes.
- Testes em tests/; mocks somente em fronteiras de I/O.
- Aplicam-se as regras e limites do CLAUDE.md raiz.
- Variáveis de ambiente: .env.example; nunca registrar segredos.

## Composição e operação

- profiles.py e settings_types.py pertencem à configuração, não ao Domain.
- artifacts/ possui decisões de release/retenção de artefatos operacionais.
- Não oferecer tipos de configuração completos a capacidades que só precisam de um campo.
