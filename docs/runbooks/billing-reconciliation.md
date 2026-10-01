# Reconciliação e observabilidade de billing

Opera o worker de billing com `BILLING_MODE=stripe`. A Stripe é a fonte financeira; o
`EntitlementSnapshot` no DynamoDB é a autoridade em runtime. O worker mantém os dois
alinhados por três caminhos: inbox de webhooks, recovery e reconciliação.

## Comandos

O parser `billing-worker` é ligado na Task 17 (#327). Esta seção descreve a semântica final;
até lá, as funções correspondentes existem em `cnes_infra.billing` sem entrypoint de CLI.

| Comando | Efeito |
|---|---|
| `billing-worker inbox --limit 100` | Processa até 100 eventos recuperáveis do inbox de webhooks |
| `billing-worker recover` | Recovery por `events.list` da Stripe, com cursor retomável |
| `billing-worker reconcile --limit 100` | Reconciliação de até 100 contas (seção abaixo) |

## Reconciliação

Para cada conta com Stripe Customer (listagem paginada `list_stripe_accounts`, revalidada por
leitura forte), o reconciler lê o snapshot com leitura forte, busca o estado atual da
assinatura na Stripe, mapeia para a `PlanVersion` imutável pelo price e compara só os campos
financeiros e de acesso:

- `subscription_status`, `stripe_subscription_id`, `plan_version_id`
- `features`, `quotas`
- `period_start`, `period_end`, `cancel_at_period_end`, `grace_until`

Não são comparados `valid_until`, `updated_at`, `source_event_id` e `entitlement_version`.

### Sem drift

Nenhuma escrita; a versão do snapshot não incrementa. Só métricas.

### Com drift

Um CAS (`compare_and_set_snapshot`) grava versão+1 com `billing.reconciliation_drift` e
`billing.reconciliation_corrected` na mesma transação. O audit usa ator `system:reconciler`,
razão `stripe_projection_drift`, hashes SHA-256 dos campos comparados do snapshot anterior e
do novo, e IDs Stripe opacos. Não contém segredo nem payload.

CAS perdido: o reconciler relê com leitura forte e busca a Stripe de novo. Se o projector já
corrigiu, conta drift sem correção e audita só `billing.reconciliation_drift`, com id
determinístico (replay idempotente).

### Conta revogada

`admin_revoked` nunca é sobrescrito. Drift em conta revogada é auditado e não corrigido. Uma
conta revogada cuja assinatura segue ativa na Stripe continua contando em `ReconciliationDrift`
a cada execução; cancele a assinatura na Stripe ao revogar para encerrar o alarme.

### Perda de acesso

Se o snapshot vigente (o corrigido, ou o atual quando não há drift) não dá acesso FULL pela
`EntitlementPolicy`, o reconciler delega ao mesmo serviço da revogação imediata: fence de Runs,
cancelamento no executor e finalização. Não grava `admin_revoked`. O progresso fica por versão
em `REVOCATION#<versão>`; a chamada é idempotente e versão já COMPLETE retorna sem efeito.

Perdem acesso FULL:

- status `canceled`, `unpaid`, `paused`, `incomplete` ou `incomplete_expired`;
- `past_due` com grace expirado;
- fim de período com `cancel_at_period_end=true`.

`cancel_at_period_end=true` antes de `period_end` continua FULL.

### Cursor

O item `BILLING#SYSTEM / RECONCILIATION#STRIPE` guarda `position` (última conta confirmada),
`version`, `updated_at` e `last_completed_at`.

- O cursor é persistido após cada conta, só depois de a correção e o enforcement concluírem.
- Crash: a execução recomeça na próxima conta não confirmada. A conta em curso é reavaliada,
  já sem drift, então não há segundo CAS (corrige uma única vez). Se o enforcement não
  completou, é retomado.
- Fim da listagem: `position` é removida e `last_completed_at` é gravado; a próxima execução
  inicia novo ciclo.

### Falhas

Falha numa conta (erro retryable ou de dependência, `stripe_price_unmapped`,
`stripe_subscription_ambiguous` etc.): a conta fica em `failed`, o lote para e devolve
`next_cursor` igual à última conta confirmada. O cursor não avança além dela. Uma falha
permanente numa conta bloqueia o ciclo até correção manual; investigar pelo log
`billing_reconcile_failed`.

Com falha na primeira conta de um ciclo, `next_cursor` volta `None` (a posição inicial); use
`failed > 0` para distinguir de ciclo concluído.

Conta sem snapshot: ignorada, contada em `examined` e sem drift. O snapshot inicial é
responsabilidade do webhook ou do recovery.

### Concorrência

Duas execuções simultâneas disputam o CAS do cursor. A perdedora falha com
`reconciliation_cursor_contended` e pode ser re-executada.

## Replay seguro

- inbox: ids de evento Stripe; o inbox usa fence por claim.
- recover: cursor por ciclo.
- reconcile: conforme a seção anterior.

Todos são idempotentes. Audit duplicado é no-op no outbox.

## Métricas EMF

Namespace `CnesData/Billing`. Dimensões permitidas: apenas `Environment`, `EventType`,
`Reason` e `SubscriptionStatus`. Nunca tenant, conta ou ID de evento. Métrica com nome,
dimensão ou unidade fora do contrato é descartada com log `billing_metric_rejected reason=...`.

| Métrica | Unidade | Emissor |
|---|---|---|
| `WebhookLatencyMs` | Milliseconds | Sink pronto; emissão ligada na Task 17 (#327) |
| `WebhookFailures` | Count | Sink pronto; emissão ligada na Task 17 (#327) |
| `WebhookDuplicates` | Count | Sink pronto; emissão ligada na Task 17 (#327) |
| `RecoveryBacklog` | Count | Sink pronto; emissão ligada na Task 17 (#327) |
| `ReconciliationDrift` | Count | Reconcile, nesta entrega: uma por execução, valor = drifts encontrados, inclusive 0 |
| `EntitlementChecksDenied` | Count | Sink pronto; emissão ligada na Task 17 (#327) |
| `QuotaReservationsActive` | Count | Sink pronto; emissão ligada na Task 17 (#327) |
| `QuotaReservationsExpired` | Count | Sink pronto; emissão ligada na Task 17 (#327) |
| `RunsCanceledByRevocation` | Count | Enforcement do reconcile, nesta entrega, quando fenceia Runs |
| `AuditOutboxFailures` | Count | Sink pronto; emissão ligada na Task 17 (#327) |
| `EntitlementSnapshotAgeSeconds` | Seconds | Sink pronto; emissão ligada na Task 17 (#327) |

O sink é `CloudWatchBillingMetrics` (`cnes_infra.billing.metrics`).

## Alarmes iniciais

Contrato dos alarmes. O IaC (CloudWatch alarms e dashboards) está fora de escopo e exige a
especificação de deploy separada; a lacuna está registrada no EPIC #95.

| Alarme | Significado | Primeira ação |
|---|---|---|
| `WebhookFailures >= 5` em 5 minutos | Webhooks falhando na verificação ou no projetor | Inspecionar o inbox e os logs; rodar `billing-worker inbox --limit 100` após corrigir a causa |
| `RecoveryBacklog >= 100` por 10 minutos | Eventos recuperáveis acumulando sem processamento | Verificar se o worker está ativo; rodar `billing-worker inbox --limit 100` e depois `billing-worker recover` |
| `EntitlementSnapshotAgeSeconds > 300` ativo por 10 minutos | Snapshots sem atualização; risco de runtime com estado velho | Conferir entrega de webhooks da Stripe e executar `billing-worker recover` |
| `ReconciliationDrift >= 1` em três execuções seguidas | Projeção divergente da Stripe de forma persistente | Ler os audits `billing.reconciliation_drift` e investigar por que o projector não converge |
| `AuditOutboxFailures >= 1` por 5 minutos | Audit durável não está sendo gravado | Verificar o outbox e permissões da tabela; reprocessar após corrigir (audit duplicado é no-op) |

## Inventário de audit events

Eventos duráveis (outbox), iguais ao `AUDIT_EVENT_INVENTORY` de
`packages/cnes_infra/tests/billing/test_audit_inventory.py`.

| Categoria | Eventos |
|---|---|
| Conta | `billing_account.created`, `billing_account.tenant_linked`, `billing_account.customer_attached` |
| Transferência | `billing_account.transferred` |
| Checkout | `checkout.session_created` |
| Webhook | `billing.webhook_failed_final` |
| Subscription | `subscription.status_changed` |
| Entitlement | `entitlement.changed`, `entitlement.revoked` |
| Quota | `quota.reserved`, `quota.consumed`, `quota.released` |
| Autorização de Run | `run.authorized` |
| Revogação e cancelamento de Run | `run.cancel_requested`, `run.canceled` |
| Reconciliação | `billing.reconciliation_drift`, `billing.reconciliation_corrected` |

Log-only até a Task 17 (#327), emitidos como linha `billing_audit ...` e sem audit durável:
`run_execution.bind_failed` e `entitlement.shadow_denied`. Serving denial chega na Task 17.

## Limitação conhecida

Com `BILLING_MODE=stripe` e `BILLING_ENFORCEMENT_MODE` em `off` ou `shadow`,
`create_unmetered_run` não confere o snapshot. Runs criados após uma revogação ou perda de
acesso não são fenceados: não há enforcement nesses modos. Use `enforce` onde o corte de
acesso for requisito.
