# cnes_control_plane — Controle, billing transacional e execução

## Responsabilidade

Controle, billing transacional e execução. Fronteiras canônicas: docs/architecture.md.

## Dependências

Dependências locais declaradas: cnes-contracts, cnes-domain, cnes-data, cnes-observability.
Não importar apps, cnes_runtime ou implementações de outra capability fora
do grafo aprovado. Exceção: cnes_runtime é a camada externa de composição.

## Convenções

- Preservar contratos, isolamento de tenant e semântica dos adapters existentes.
- Testes em tests/; mocks somente em fronteiras de I/O.
- Aplicam-se as regras e limites do CLAUDE.md raiz.
- Variáveis de ambiente: .env.example; nunca registrar segredos.

## Transações

- persistence/ e billing/ compartilham a fronteira transacional existente.
- Não separar quota, run, capacidade ou revogação em escritas não atômicas.
- Preservar chaves, CAS, leases e eventos de outbox.
- Modo billing disabled não cria clientes de rede nem lê segredos Stripe.
