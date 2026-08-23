# CnesData — Stripe Billing, Entitlements e Revogação

**Status:** aprovado para planejamento

**Data:** 2026-08-16

**Dependência:**
[Data Plane Parquet e Orquestração](2026-08-16-parquet-data-plane-orchestration-design.md)

## 1. Objetivo

Adicionar assinatura e cobrança ao perfil SaaS AWS por meio da Stripe, mantendo o
perfil local open-source sem licença ou validação remota obrigatória.

Stripe é a fonte da verdade financeira. O CnesData mantém uma projeção de
entitlements no DynamoDB para autorizar operações com baixa latência, aplicar quotas
e revogar compute ou publicação quando necessário.

## 2. Decisões centrais

| Tema | Decisão |
|---|---|
| Perfil local | `BILLING_MODE=disabled`, sem chamadas à Stripe |
| Perfil AWS | `BILLING_MODE=stripe` |
| Provedor inicial | Stripe Billing |
| Checkout | Stripe Checkout hospedado |
| Autogestão | Stripe Customer Portal |
| Fonte financeira | Subscription e invoices da Stripe |
| Autorização runtime | Entitlement projection no DynamoDB |
| Features booleanas | Stripe Entitlements espelhados localmente |
| Quotas numéricas | `PlanVersion` interna e imutável |
| Cache | Curto e local; Redis não é introduzido |
| Revogação crítica | Leitura consistente + invalidação de fences ativos |
| Logs operacionais | CloudWatch Logs |
| Auditoria de domínio | S3 versionado com Object Lock |

## 3. Não objetivos

- Não cobrar nem limitar instalações locais open-source.
- Não armazenar número de cartão ou outros dados PCI no CnesData.
- Não consultar a Stripe em cada request.
- Não codificar quotas em JWT ou claims de longa duração.
- Não usar CloudFront invalidation como mecanismo de autorização.
- Não adicionar Redis somente para cache de assinatura.
- Não suportar múltiplos provedores de pagamento no primeiro release.
- Não definir preços comerciais nesta especificação.

## 4. Modelo de domínio

### 4.1 Billing account

Um `BillingAccount` representa a organização responsável pelo pagamento. Ele pode
possuir um ou mais tenants conforme a `PlanVersion`.

Campos mínimos:

```text
billing_account_id
stripe_customer_id
owner_user_id
status
created_at
updated_at
```

O tenant referencia `billing_account_id`. Isso evita acoplar diretamente município,
usuário e customer da Stripe.

### 4.2 PlanVersion

`PlanVersion` é imutável depois de publicada:

```text
plan_version_id
plan_key
stripe_product_id
stripe_price_ids
features
max_tenants
max_agents
max_runs_per_period
max_concurrency
retention_days
athena_scan_budget_bytes
effective_from
```

Mudança de limites cria nova versão. Assinaturas existentes continuam ligadas à
versão contratada até upgrade, downgrade ou migração explícita.

### 4.3 EntitlementSnapshot

O snapshot runtime contém:

```text
billing_account_id
stripe_subscription_id
subscription_status
plan_version_id
features
quotas
period_start
period_end
grace_until
valid_until
entitlement_version
updated_at
source_event_id
```

`entitlement_version` aumenta em toda alteração que possa modificar acesso, quota ou
fencing. O snapshot é a autoridade runtime; a Stripe continua sendo a autoridade
financeira usada para reconciliá-lo.

## 5. Mapeamento Stripe

| Stripe | CnesData |
|---|---|
| Customer | BillingAccount |
| Product | Família de plano |
| Price | Preço e periodicidade de uma PlanVersion |
| Subscription | Contrato ativo do BillingAccount |
| Active Entitlement | Feature habilitada |
| Invoice | Evidência de cobrança e pagamento |

Stripe Entitlements representa features booleanas. Quotas numéricas permanecem na
`PlanVersion`, porque precisam de semântica e enforcement próprios do CnesData.

## 6. Lifecycle de assinatura

| Estado | Política CnesData |
|---|---|
| `trialing` | Acesso conforme plano de trial |
| `active` | Acesso completo conforme PlanVersion |
| `cancel_at_period_end=true` | Acesso completo até `period_end` |
| `past_due` | Grace period configurado na PlanVersion |
| `incomplete` | Sem provisionamento de compute |
| `incomplete_expired` | Sem acesso pago |
| `unpaid` | Read-only; sem novos agents ou runs |
| `paused` | Read-only; sem novos agents ou runs |
| `canceled` | Read-only conforme retenção; sem novo compute |
| `admin_revoked` | Bloqueio imediato, inclusive de publicação em andamento |

Perda de assinatura não apaga datasets automaticamente. Retenção, exportação e
deleção seguem política explícita e auditável.

## 7. Fluxo de checkout

1. Usuário autenticado cria ou seleciona um BillingAccount.
2. API valida que o usuário é billing owner.
3. API cria Checkout Session com idempotency key interna.
4. Metadata contém somente IDs opacos necessários para correlação.
5. Usuário conclui pagamento na página hospedada da Stripe.
6. Redirect de sucesso mostra estado pendente, sem liberar acesso por si só.
7. Webhook confirmado atualiza Subscription e EntitlementSnapshot.
8. O frontend observa o novo estado pelo endpoint de billing.

O redirect nunca é usado como prova de pagamento.

## 8. Webhooks

O endpoint usa body raw e valida `Stripe-Signature` com a biblioteca oficial. Eventos
iniciais:

- `checkout.session.completed`;
- `customer.subscription.created`;
- `customer.subscription.updated`;
- `customer.subscription.deleted`;
- `customer.subscription.paused`;
- `customer.subscription.resumed`;
- `invoice.paid`;
- `invoice.payment_failed`;
- `invoice.payment_action_required`;
- `entitlements.active_entitlement_summary.updated`.

### 8.1 Idempotência

Antes de processar, o handler grava condicionalmente um item por `stripe_event_id`.
Evento já `PROCESSED` retorna HTTP 200 sem reaplicar efeitos. Evento `PROCESSING`
vencido pode ser retomado por recovery worker.

### 8.2 Ordem

O handler não presume entrega ordenada. Em eventos capazes de mudar acesso, busca o
estado atual da Subscription e dos active entitlements na Stripe, recalcula a projeção
e faz conditional update do snapshot.

### 8.3 Resposta

O endpoint valida, persiste o evento e responde rapidamente. Efeitos secundários são
processados de forma assíncrona. Falha transitória retorna erro para permitir retry da
Stripe. Um recovery job consulta eventos não entregues dentro da janela suportada.

## 9. Entitlement gate

O domínio expõe `EntitlementGate` com operações orientadas a casos de uso:

```text
authorize_create_run
authorize_register_agent
authorize_analytics_query
authorize_serving_access
authorize_tenant_creation
```

Processors não conhecem Stripe. Eles recebem um `RunAuthorization` imutável com:

```text
billing_account_id
plan_version_id
entitlement_version
max_concurrency
budget_reservation_id
authorized_at
```

O publisher revalida entitlement e fence antes de tornar um run ativo.

## 10. Quotas e budgets

Quotas possuem reserva atômica no DynamoDB para impedir oversubscription por requests
concorrentes. Criar um run executa uma transação que:

1. lê entitlement válido;
2. verifica quota e budget do período;
3. cria reservation idempotente;
4. incrementa uso reservado;
5. cria o run autorizado.

Conclusão converte reserva em uso consumido. Cancelamento libera a parte não
consumida. Reconciliação periódica corrige reservations abandonadas.

`max_concurrency` do plano limita fan-out, mas o deployment pode impor limite menor.
O cliente nunca eleva concorrência acima do menor limite aplicável.

## 11. Revogação e runs em andamento

Revogação normal, como cancelamento ao fim do período, só entra em vigor em
`period_end`. Revogação imediata executa:

1. atualizar EntitlementSnapshot e incrementar `entitlement_version`;
2. marcar novos comandos como não autorizados;
3. localizar runs não terminais do BillingAccount;
4. definir `CANCEL_REQUESTED` e incrementar seus fencing tokens;
5. solicitar cancelamento das execuções Step Functions;
6. impedir que tasks antigas publiquem outputs;
7. emitir `entitlement.revoked` e `run.cancel_requested` na auditoria.

Workers podem terminar escrita temporária depois da revogação, mas o fence inválido
impede publicação. Lifecycle remove temporários órfãos.

## 12. Cache e invalidação

Não existe cache distribuído obrigatório.

| Operação | Política |
|---|---|
| Criar run | DynamoDB consistent read, sem cache |
| Registrar agent | DynamoDB consistent read, sem cache |
| Reservar budget | DynamoDB transaction, sem cache |
| Publicar run | Revalidação consistente, sem cache |
| Leitura de UI | Cache local de até 60 segundos |
| Emitir URL/cookie assinado | Revalidação ou cache de até 60 segundos |

Cache keys incluem `billing_account_id` e `entitlement_version`. Mudança no snapshot
publica `entitlement.changed` via DynamoDB Streams. Consumidores long-lived removem a
versão anterior. Lambdas podem simplesmente usar o TTL curto.

URLs e cookies assinados têm duração curta. Cancelamento impede novas emissões; o
resíduo máximo é a duração da credencial já emitida. CloudFront invalidation não é
usada para revogar autorização, pois remover conteúdo do cache não invalida uma
credencial de acesso.

## 13. Serving e conteúdo histórico

Objetos de serving permanecem privados. O BFF resolve tenant, BillingAccount,
membership e entitlement antes de emitir acesso. Status read-only pode permitir
consulta e exportação de histórico dentro da retenção, mas nunca iniciar novo compute.

`admin_revoked` bloqueia serving imediatamente, exceto endpoints explícitos de billing,
suporte e recuperação de conta.

## 14. Logs e auditoria

CloudWatch Logs recebe logs operacionais estruturados. Esses logs não são fonte de
verdade de billing.

Eventos de domínio append-only incluem:

- billing account criado ou transferido;
- Checkout Session criada;
- webhook recebido, duplicado, aplicado ou falho;
- assinatura ativada, alterada, pausada ou cancelada;
- entitlement concedido ou revogado;
- quota reservada, consumida, liberada ou negada;
- run cancelado por entitlement;
- serving access negado por assinatura.

O audit writer grava S3 versionado com Object Lock. DynamoDB Streams complementa a
trilha de mutações, mas eventos explícitos preservam ator, motivo e contexto de negócio.

## 15. Segurança

- Stripe secret key e webhook secret ficam no AWS Secrets Manager.
- Endpoint exige TLS e valida assinatura sobre body não modificado.
- Nenhum dado de cartão entra em logs, DynamoDB ou S3.
- Metadata enviada à Stripe contém IDs opacos, não dados municipais sensíveis.
- Billing owner e administradores usam autorização separada de tenant viewer.
- Webhook handler usa role mínima para billing tables e audit events.
- Checkout e Customer Portal usam URLs de retorno allowlisted.
- Mudanças administrativas exigem reason code e audit event.

## 16. Falhas e reconciliação

| Falha | Comportamento |
|---|---|
| Stripe indisponível durante request | Não criar checkout; retornar erro retryable |
| Webhook atrasado | Último snapshot vale até `valid_until` |
| Webhook duplicado | Deduplicar por event ID e retornar 200 |
| Webhook fora de ordem | Buscar estado atual na Stripe antes de projetar |
| DynamoDB indisponível | Falhar fechado para novo compute e publicação |
| Invalidation consumer falha | TTL curto limita staleness; gates críticos não usam cache |
| Cancelamento de workflow falha | Fence já invalidado impede publicação |
| Audit sink indisponível | Outbox durável retenta; evento crítico não é descartado |

Um reconciliation job periódico compara Stripe e EntitlementSnapshot para contas
ativas, corrige drift com conditional write e registra a correção em auditoria.

## 17. Testes

- Unit tests para cada status Stripe e política de acesso.
- Contract tests do EntitlementGate e quota reservations.
- Testes de assinatura válida e inválida do webhook.
- Testes de eventos duplicados, reorder e retry concorrente.
- Testes com Stripe test clocks para trial, renewal, failure e cancellation.
- Testes de cancelamento ao fim do período e cancelamento imediato.
- Race tests de duas reservas consumindo a última unidade de quota.
- Teste de cache stale provando que gates críticos consultam DynamoDB.
- Teste end-to-end de revogação invalidando fence de run ativo.
- Teste provando que perfil local funciona com billing desabilitado e sem secrets.

## 18. Observabilidade

Métricas mínimas:

- webhook latency e failure rate;
- eventos duplicados e recovery backlog;
- drift encontrado pelo reconciliation job;
- entitlement checks denied por motivo;
- quota reservations ativas e expiradas;
- runs cancelados por revogação;
- idade do EntitlementSnapshot;
- falhas de audit outbox.

Alertas existem para webhook failures persistentes, snapshots stale, drift recorrente
e audit outbox acumulando.

## 19. Migração e rollout

1. Introduzir BillingAccount, PlanVersion e EntitlementSnapshot sem enforcement.
2. Integrar Stripe em sandbox e processar webhooks em shadow mode.
3. Validar deduplicação, reorder e reconciliation com test clocks.
4. Ativar entitlement checks apenas em logs para tenants internos.
5. Ativar gates de criação de run e agent registration.
6. Ativar quotas e budget reservations.
7. Ativar revogação de publicação e cancelamento de workflows.
8. Habilitar produção para um plano e expandir após observabilidade estável.

O perfil local mantém `BILLING_MODE=disabled` durante todas as fases.

## 20. Critérios de aceitação

- Instalação local funciona sem Stripe, Secrets Manager ou chamadas externas.
- Redirect de checkout não libera acesso sem webhook confirmado.
- Eventos duplicados não duplicam efeitos.
- Eventos fora de ordem convergem para o estado atual da Stripe.
- Operações caras fazem leitura consistente e não dependem de cache stale.
- Cancelamento ao fim do período preserva acesso até `period_end`.
- Revogação imediata bloqueia novos comandos e impede publicação de runs ativos.
- Falha ao cancelar Step Functions não permite commit por fence antigo.
- Perda de assinatura não apaga datasets silenciosamente.
- Todos os efeitos financeiros e de autorização geram audit events.

## 21. Referências externas

- Stripe subscription webhooks:
  <https://docs.stripe.com/billing/subscriptions/webhooks>
- Stripe Entitlements:
  <https://docs.stripe.com/billing/entitlements>
- Stripe webhook security:
  <https://docs.stripe.com/webhooks>
- Stripe webhook recovery and deduplication:
  <https://docs.stripe.com/webhooks/process-undelivered-events>
- Stripe subscription cancellation:
  <https://docs.stripe.com/billing/subscriptions/cancel>
