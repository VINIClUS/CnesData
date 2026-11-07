# cnes_runtime — Configuração e composição de runtime

## Responsabilidade

Configuração e composição de runtime. Fronteiras canônicas: docs/architecture.md.

## Dependências

Dependências locais declaradas: cnes-domain, cnes-data, cnes-governance, cnes-control-plane.
Camada externa de composição: usa as capabilities declaradas para montar
o runtime. Nenhuma capability pode depender deste pacote. Não importar apps.

## Convenções

- Preservar contratos, isolamento de tenant e semântica dos adapters existentes.
- Testes em tests/; mocks somente em fronteiras de I/O.
- Aplicam-se as regras e limites do CLAUDE.md raiz.
- Variáveis de ambiente: .env.example; nunca registrar segredos.
