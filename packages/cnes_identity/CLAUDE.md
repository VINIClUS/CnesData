# cnes_identity — Identidade, autenticação e certificados

## Responsabilidade

Identidade, autenticação e certificados. Fronteiras canônicas: docs/architecture.md.

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

## Fronteira de identidade

- certificates.py e refresh_contracts.py possuem os modelos e invariantes de identidade.
- Leads públicos: DTOs em Contracts e stores/port em Data; não pertencem a Identity.
- local_auth recebe apenas as interfaces de membership e tenant de que precisa.
- Não importar Control Plane ou Runtime, inclusive sob TYPE_CHECKING.
