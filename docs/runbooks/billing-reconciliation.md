# Reconciliação e observabilidade de billing

Opera o worker de billing com `BILLING_MODE=stripe`. A Stripe é a fonte financeira; o
`EntitlementSnapshot` no DynamoDB é a autoridade em runtime. O worker mantém os dois
alinhados por três caminhos: inbox de webhooks, recovery e reconciliação.

## Comandos

Cada comando roda um ciclo limitado e idempotente e termina; a repetição é do scheduler. Exit
0 conclui o ciclo (inclusive com falhas por conta, contadas em `failed`); 1 só quando o estado
durável não registrou o retry (cursor disputado, dependência indisponível antes do primeiro
registro, erro de billing não tratado); 2 para entrada ou configuração inválida.

| Comando | Efeito |
|---|---|
| `billing-worker inbox --limit 100` | Processa até 100 eventos recuperáveis do inbox de webhooks |
| `billing-worker recover` | Recovery por `events.list` da Stripe, com cursor retomável |
| `billing-worker reconcile --limit 100` | Reconciliação de até 100 contas (seção abaixo) |
| `billing-worker revoke-pending --limit 100` | Retoma revogações incompletas de até 100 contas (seção própria) |
| `billing-worker release-expired-reservations --limit 100` | Recupera até 100 reservas vencidas (seção própria) |

### Enforcer por modo

`BILLING_ENFORCEMENT_MODE` decide quem age na perda de acesso (projector, reconcile e
revoke-pending usam a mesma instância):

- `enforce`: `ImmediateRevocationService` real (fence, cancelamento no Step Functions,
  finalização). Exige `AWS_STATE_MACHINE_ARN`.
- `shadow`: só grava o audit durável `entitlement.shadow_access_loss`; nenhum Run é fenceado.
- `off`: nenhum enforcement; `revoke-pending` é no-op (`billing_worker_skipped ...
  reason=enforcement_off`).
- `BILLING_MODE=disabled`: o worker nem compõe.

## Reconciliação

Para cada conta com Stripe Customer (listagem paginada `list_stripe_accounts`, revalidada por
leitura forte), o reconciler lê o snapshot com leitura forte, busca o estado atual da
assinatura na Stripe, mapeia para a `PlanVersion` imutável pelo price e compara só os campos
financeiros e de acesso:

- `subscription_status`, `stripe_subscription_id`, `plan_version_id`
- `features`, `quotas`
- `period_start`, `period_end`, `cancel_at_period_end`, `grace_until`

Não são comparados `valid_until`, `updated_at`, `source_event_id` e `entitlement_version`.

Assinatura substituída: se a assinatura do snapshot estiver encerrada (`canceled` ou
`incomplete_expired`), o reconciler consulta a assinatura viva do Customer e usa essa; só
mantém o estado encerrado quando não há assinatura viva única (`stripe_subscription_ambiguous`).
Assim um webhook perdido da nova assinatura não tira o acesso de quem recontratou.

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
cancelamento no executor e finalização. Não grava `admin_revoked`. Antes de cada página de
fencing o serviço relê o snapshot com leitura forte. O fence é irreversível, então, se a versão
mudou, o serviço para de fencear Runs novas e sempre conclui cancelamento e liquidação das Runs
já fenceadas (log `access_loss_superseded`). Em seguida relê o snapshot: se a versão nova ainda
não dá acesso FULL (e não é `admin_revoked`), aplica o enforcement dessa versão na mesma chamada,
até 3 vezes; acima disso falha com `access_loss_enforcement_unstable`.

Se o snapshot vigente dá acesso FULL, o reconciler chama `resume_pending`: retoma o progresso de
revogação incompleto mais recente da conta. Se o progresso é de uma versão anterior, não fenceia
Runs novas e só conclui as já fenceadas; se é da versão vigente (um enforcement concorrente em
curso), continua normalmente. Assim uma queda no meio do fencing seguida de reativação não deixa
Runs fenceadas sem cancelamento. O `revoke-pending` reusa esse ponto de entrada.

Contas `admin_revoked` não são delegadas: a revogação administrativa conduz as próprias fases. O
progresso fica por versão em `REVOCATION#<versão>`; a chamada é idempotente e versão já COMPLETE
retorna sem efeito.

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

Só a indisponibilidade de dependência interrompe o lote: `BillingDependencyError`
(`dynamodb_unavailable`) e `stripe_unavailable`. Nesse caso as contas seguintes falhariam do mesmo
jeito, então o lote para, a conta fica em `failed`, e `next_cursor` é a última conta confirmada;
o cursor não avança além dela. Com falha na primeira conta de um ciclo, `next_cursor` volta
`None` (a posição inicial); use `failed > 0` para distinguir de ciclo concluído.

Qualquer outra falha específica da conta segue o fluxo: a conta fica em `failed`, é logada como
`billing_reconcile_failed billing_account_id=... code=...` e o cursor avança além dela. A conta é
reexaminada no próximo ciclo; o CAS e o progresso de revogação são idempotentes, então a correção
continua única. Exemplos: `stripe_price_unmapped`, `stripe_subscription_ambiguous`,
`stripe_request_rejected`, `stripe_state_invalid` (objeto Stripe malformado na leitura ou no
mapeamento), `reconciliation_cas_exhausted`, `access_loss_enforcement_unstable` e
`revocation_progress_contended`. Uma conta que falha em todo ciclo aparece repetidamente nesse
log. Exceções inesperadas (defeitos) propagam e encerram a execução sem avançar o cursor.

Conta sem snapshot: ignorada, contada em `examined` e sem drift. O snapshot inicial é
responsabilidade do webhook ou do recovery.

### Concorrência

Duas execuções simultâneas disputam o CAS do cursor. A perdedora falha com
`reconciliation_cursor_contended` e pode ser re-executada.

## Delegação do projector

Depois de gravar o snapshot, o projector compara o nível de acesso (`SERVING_ACCESS` na
`EntitlementPolicy`) antes e depois. Uma transição de FULL para não-FULL chama
`enforce_access_loss`. `cancel_at_period_end=true` antes de `period_end` continua FULL e não
delega. Uma falha do enforcement é logada (`stripe_projection_enforcement_failed`) e não desfaz
o snapshot; o reconcile e o revoke-pending retomam.

Evento sem mudança: quando os campos comparados (`COMPARED_FIELDS`, os mesmos da
reconciliação) não mudam e a validade não aumenta, o projector conclui o claim do inbox com uma
ConditionCheck na versão atual, sem gravar snapshot. A versão não incrementa, então fences e
enforcement não veem mudanças espúrias.

## Revogações pendentes (`revoke-pending`)

Pagina as contas com Stripe Customer (`list_stripe_accounts`, a mesma listagem do reconcile)
com cursor próprio em `BILLING#SYSTEM / REVOCATION#PENDING` e chama `resume_pending` em cada
uma. O progresso é por conta e versão (`REVOCATION#<versão>`), então não há índice de pendentes
a manter; contas sem progresso incompleto custam uma Query.

- Retoma revogações administrativas e por perda de acesso. Uma versão nova do snapshot só
  interrompe o fence de Runs novas quando dá acesso FULL (ou o snapshot sumiu); uma versão nova
  ainda negada, inclusive `admin_revoked`, continua fenceando sob o progresso armazenado.
- Cancelamento no executor: usa o `execution_ref` do registro de dispatch mesmo com o lease
  expirado, desde que o dispatch não seja TERMINAL.
- Run em `PUBLISHING` durante o fence (publicação negada após a revogação): vai para `FAILED`
  (único terminal legal a partir de `PUBLISHING` na CND) na mesma transação que grava o evento
  `run.failed` e libera a reserva sem decrementar `consumed_runs`. Contado em
  `failed_publications`.
- Falhas: igual ao reconcile. Só `BillingDependencyError` interrompe o lote sem avançar; outra
  falha da conta conta em `failed` (`billing_revoke_pending_failed`) e o cursor avança.

## Reservas vencidas (`release-expired-reservations`)

Lê uma página do índice de reservas vencidas (`QUOTA_RESERVATION#DUE`) e, com leitura forte:

- reserva de Run sem Run criado: libera (devolve o Run e o scan reservado);
- Run em estado terminal: consome sem decrementar `consumed_runs`;
- Run ainda não terminal (inclusive `PUBLISHING` com publicação negada repetidamente): renova
  o lease; a reserva sai do `RESERVED` quando o `revoke-pending` leva o Run a `FAILED`;
- capacidade (agente/tenant) que ficou `RESERVED` após falha da segunda escrita ou do consumo:
  consome quando há prova de posse da própria reserva (marcador do agente ou link do tenant),
  senão libera.

Emite `QuotaReservationsExpired` com o número de liberadas.

## Replay seguro

- inbox: ids de evento Stripe; o inbox usa fence por claim.
- recover: cursor por ciclo.
- reconcile: conforme a seção anterior.
- revoke-pending: progresso por versão com CAS; cursor próprio.
- release-expired-reservations: transições condicionadas ao payload lido.

Todos são idempotentes. Audit duplicado é no-op no outbox.

## Customer órfão

`POST /api/v1/billing/accounts` cria o Customer no Stripe e depois o anexa à conta. Uma queda
entre os dois deixa o Customer órfão. O replay com a mesma `idempotency_key` o recupera:

- antes de criar, o gateway busca `metadata['billing_account_id']` e reusa o Customer não
  excluído mais antigo da conta (a Stripe descarta chaves de idempotência após 24 h);
- a busca indexa com atraso de cerca de 1 minuto; nessa janela, a chave `customer:<conta>`
  devolve o mesmo Customer;
- busca com mais de uma página falha fechado (`stripe_customers_unbounded`, 503) sem criar;
- o `StripeClient` repete falhas de rede até 2 vezes com a mesma chave, o que evita a maior
  parte dos órfãos por resposta perdida.

Cliente que perdeu a `idempotency_key`: uma chave nova, enviada pelo dono ou por um `gestor` do
tenant vinculado, devolve a mesma conta (201). A rota resolve o link reverso
`TENANT#<t>/BILLING_ACCOUNT` com leitura forte, aplica o mesmo controle de dono/`gestor` com link
direto forte e segue o mesmo caminho do Customer acima (chave `customer:<conta>` + busca por
metadata), sem criar outro Customer. Sinal em log:
`billing_account_recovered billing_account_id=...`. Após essa recuperação, 409
`billing_tenant_conflict` no `POST /accounts` só ocorre com `TENANT#<t>/BILLING_ACCOUNT`
pendente, ausente ou divergente do link direto: investigue o índice antes de qualquer correção
manual.

Sinais em log, sem ação automática (o Customer sobra no Stripe, sem anexo):

- `stripe_customer_duplicates billing_account_id=... count=N chosen=...`: duplicatas já
  existentes;
- `billing_customer_orphaned billing_account_id=... stripe_customer_id=...`: outro Customer foi
  anexado na corrida.

Antes de apagar um órfão no Stripe, confirme que ele não tem assinatura nem é o Customer da conta.

Limites conhecidos:

- um 500 da Stripe que não criou o Customer fica guardado na chave: a busca não acha nada e os
  replays recebem 503 até a chave expirar (até 24 h);
- um Customer achado pela busca mas já mapeado a outra conta (metadata editada à mão na Stripe)
  falha fechado com 502 `stripe_request_rejected`.

## Métricas EMF

Namespace `CnesData/Billing`. Dimensões permitidas: apenas `Environment`, `EventType`,
`Reason` e `SubscriptionStatus`. Nunca tenant, conta ou ID de evento. Métrica com nome,
dimensão ou unidade fora do contrato é descartada com log `billing_metric_rejected reason=...`.

| Métrica | Unidade | Emissor |
|---|---|---|
| `WebhookLatencyMs` | Milliseconds | Webhook: `now - created` do evento aceito pela primeira vez (duplicados não contam) |
| `WebhookFailures` | Count | Webhook: assinatura inválida, payload grande demais ou dependência indisponível |
| `WebhookDuplicates` | Count | Webhook: evento já recebido (disposição duplicada) |
| `RecoveryBacklog` | Count | `inbox`: eventos vencidos vistos e não aplicados no ciclo (limitado por `--limit`) |
| `ReconciliationDrift` | Count | Reconcile, nesta entrega: uma por execução, valor = drifts encontrados, inclusive 0 |
| `EntitlementChecksDenied` | Count | Gate de entitlement (`enforce`), por `Reason` |
| `QuotaReservationsActive` | Count | Sem emissor (follow-up) |
| `QuotaReservationsExpired` | Count | `release-expired-reservations`: reservas liberadas |
| `RunsCanceledByRevocation` | Count | Projector e reconcile (`stripe_access_loss`), revoke admin (`admin_revoked`) e revoke-pending (`revocation_resumed`) |
| `AuditOutboxFailures` | Count | Audit best-effort (gates, callbacks, serving) que falhou ao gravar |
| `EntitlementSnapshotAgeSeconds` | Seconds | Sem emissor (follow-up) |

O sink é `CloudWatchBillingMetrics` (`cnes_infra.billing.metrics`), com `Environment` de
`BILLING_METRICS_ENVIRONMENT`; sem a variável as métricas são descartadas. O sink escreve no
stdout por um logger próprio (`cnes_infra.billing.metrics.emf`, sem propagação) com o formatter
JSON, então o documento EMF sai numa linha JSON qualquer que seja a configuração de logging do
processo (worker ou API).
`QuotaReservationsActive` e `EntitlementSnapshotAgeSeconds` ainda não têm emissor (sem ponto
natural sem varredura dedicada); ficam como follow-up.

## Alarmes iniciais

Contrato dos alarmes. O IaC (CloudWatch alarms e dashboards) está fora de escopo e exige a
especificação de deploy separada; a lacuna está registrada no EPIC #95.

| Alarme | Significado | Primeira ação |
|---|---|---|
| `WebhookFailures >= 5` em 5 minutos | Webhooks rejeitados na rota (assinatura, tamanho ou inbox indisponível); falhas do projetor aparecem no inbox e em `RecoveryBacklog` | Inspecionar o inbox e os logs; rodar `billing-worker inbox --limit 100` após corrigir a causa |
| `RecoveryBacklog >= 50` por 10 minutos (com `inbox --limit 100`) | Eventos recuperáveis acumulando sem processamento | Verificar se o worker está ativo; rodar `billing-worker inbox --limit 100` e depois `billing-worker recover` |
| `EntitlementSnapshotAgeSeconds > 300` ativo por 10 minutos (inativo até existir emissor) | Snapshots sem atualização; risco de runtime com estado velho | Conferir entrega de webhooks da Stripe e executar `billing-worker recover` |
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
| Revogação e cancelamento de Run | `run.cancel_requested`, `run.canceled`, `run.failed` |
| Execução | `run_execution.bind_failed` |
| Shadow | `entitlement.shadow_denied`, `entitlement.shadow_access_loss` |
| Serving | `serving.denied` |
| Reconciliação | `billing.reconciliation_drift`, `billing.reconciliation_corrected` |

Nenhum evento é só log: `run_execution.bind_failed`, `entitlement.shadow_denied` e
`serving.denied` são gravados no outbox por um audit best-effort (falha de gravação vira
`AuditOutboxFailures` e não muda a decisão). `serving.denied` grava um evento por tenant,
dataset, motivo e hora (id determinístico); negações repetidas na mesma hora são no-op.

## Limitação conhecida

Com `BILLING_MODE=stripe` e `BILLING_ENFORCEMENT_MODE` em `off` ou `shadow`,
`create_unmetered_run` não confere o snapshot. Runs criados após uma revogação ou perda de
acesso não são fenceados: não há enforcement nesses modos. Use `enforce` onde o corte de
acesso for requisito.
