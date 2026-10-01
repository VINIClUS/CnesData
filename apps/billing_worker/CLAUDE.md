# billing_worker — dreno do inbox e recovery de webhooks Stripe

## Executive Summary

CLI de ciclo único (`billing-worker inbox|recover`) para execução agendada
(EventBridge/ECS task ou cron). Compõe os adapters de `cnes_infra.billing` e
delega a `WebhookRecovery`. Não expõe HTTP e não mantém estado próprio.

## Role

Executor agendado do reprocessamento de eventos Stripe armazenados no inbox
DynamoDB e da reconciliação por cursor da API de Events. Cada invocação faz
um ciclo limitado e termina; a repetição é responsabilidade do scheduler.

## Functionalities

- `billing-worker inbox [--limit N]` — drena até N (1..100, padrão 100) eventos
  vencidos via `WebhookRecovery.drain_inbox`.
- `billing-worker recover` — drena e processa UMA página do cursor de Events
  via `WebhookRecovery.run` com `STRIPE_RECOVERY_*`.
- Modo `BILLING_MODE=disabled`: no-op com exit 0; nenhuma sessão, Secrets
  Manager, DynamoDB ou cliente Stripe é criado.
- Exit codes: 0 ciclo concluído; 1 falha retryable (`BillingError`,
  `SecretProviderError` retryable); 2 entrada ou configuração inválida.

## Cadência (agendamento é IaC, fora deste app)

- `inbox --limit 100`: a cada 1 min. O backoff do inbox vai de 30 s a 1 h e o
  lease de processamento é 300 s (`STRIPE_PROCESSING_LEASE_SECONDS`, constante).
- `recover`: a cada 5–15 min; cada chamada processa uma página do cursor
  (`STRIPE_RECOVERY_BATCH_SIZE`) dentro da janela `STRIPE_RECOVERY_LOOKBACK_HOURS`.
- Execuções concorrentes são seguras: claims e cursor usam condição DynamoDB.

## Limitations

- Eventos que o dreno não consegue liquidar permanecem vencidos no inbox
  durável e são retomados no próximo ciclo; `RecoveryResult` não tem contador
  de não liquidados, então isso NÃO gera exit 1. Handoff durável para
  `FAILED_RETRYABLE` conta como sucesso.
- Uma página do cursor por invocação de `recover`.
- Sem auditoria de regras nem acesso a Postgres/Firebird.

## Requirements

| Variável | Obrigatória | Uso |
|---|---|---|
| `PROFILE`, `BILLING_MODE` | sim | validadas por `BillingSettings` (local exige `TENANT_ID`, proíbe stripe) |
| `AWS_REGION` | stripe | região da sessão boto3 |
| `AWS_CONTROL_PLANE_TABLE` | stripe | tabela DynamoDB de billing |
| `DYNAMODB_ENDPOINT_URL` | não | endpoint local; vazio vira `None` |
| `STRIPE_SECRET_KEY_SECRET_ARN` | stripe | ARN do segredo da API key |
| `STRIPE_WEBHOOK_SECRET_SECRET_ARN` | stripe | ARN do segredo do webhook |
| `BILLING_RETURN_ORIGINS`, `BILLING_SUCCESS_URL`, `BILLING_CANCEL_URL`, `BILLING_PORTAL_RETURN_URL` | stripe | exigidas pelo gateway |
| `STRIPE_RECOVERY_LOOKBACK_HOURS`, `STRIPE_RECOVERY_BATCH_SIZE` | não | padrão 72 / 100 |

Credenciais AWS vêm da cadeia padrão do boto3 (role da task).

## Module Map

- `src/billing_worker/worker.py` — `BillingWorker`, `RecoveryRunner`, `build_worker`.
- `src/billing_worker/main.py` — argparse, mapeamento de exit codes, logs.
- `tests/test_worker.py` — composição, delegação, exit codes, ausência de vazamento.

## Gotchas

- Decisão: o worker NÃO usa `AwsRuntimeSettings.from_mapping`, que exige todo o
  env da API/processor (state machine, buckets, OIDC). Lê apenas os mesmos nomes
  de variável para região, tabela e endpoint.
- `StripeRuntimeSettings` é validada antes de criar qualquer sessão ou cliente.
- O worker nunca vê valores de segredo; somente `build_stripe_billing` os lê.
  Logs carregam apenas `code=` e contadores.
- `main.py` importa `botocore` (via `SecretProviderError`), mas `boto3.Session`
  só é criada no modo stripe: em disabled nenhum cliente AWS ou Stripe existe.
- `PermanentBillingError` no ciclo também sai com 1: o scheduler repete e o
  alarme operacional vem do log `billing_worker_cycle_failed code=...`.
- Sem `PROFILE`, `BillingSettings` assume o profile local e exige `TENANT_ID`
  (exit 2 `code=tenant_id_required`); o worker agendado sempre define
  `PROFILE=aws`.
