# cnes_control_plane — Controle, billing transacional e execução

## Responsabilidade

Controle, billing transacional e execução. Fronteiras canônicas: docs/architecture.md.

## Dependências

Dependências locais declaradas:

- cnes-contracts
- cnes-data
- cnes-observability

Não importar apps nem Runtime. Respeitar o grafo de docs/architecture.md.

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

## Fronteira operacional

- model/ e orchestration/ contêm os estados, transições e planejamento operacionais.
- billing/ possui regras e adapters; ports/ define as interfaces consumidas aqui.
- Código puro de execução/quotas não pertence a cnes_domain.
- Compartilhar Membership, OutboxEvent e o vocabulário de erros via Contracts.
- ports/audit.py e ports/serving.py pertencem aos consumidores; sinks são estruturais.
- Não importar Runtime para interpretar perfis; validar apenas a configuração de billing.
