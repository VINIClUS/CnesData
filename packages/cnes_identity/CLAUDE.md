# cnes_identity — Identidade, autenticação e certificados

## Responsabilidade

Identidade, autenticação e certificados. Fronteiras canônicas: docs/architecture.md.

## Dependências

Dependências locais declaradas: cnes-contracts, cnes-domain, cnes-data.
Não importar apps, cnes_runtime ou implementações de outra capability fora
do grafo aprovado. Exceção: cnes_runtime é a camada externa de composição.

## Convenções

- Preservar contratos, isolamento de tenant e semântica dos adapters existentes.
- Testes em tests/; mocks somente em fronteiras de I/O.
- Aplicam-se as regras e limites do CLAUDE.md raiz.
- Variáveis de ambiente: .env.example; nunca registrar segredos.
