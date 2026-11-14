# Canary de modo em produção (#499)

Prova unit, recovery e audit na rota PROD real com um tenant sintético reservado.
Não é a canary audit pré-close do #471 e não usa as roles nem o storage dela. O contrato
vigente é o pacote v4 ratificado no #499 e o amendment de runtime de 2026-11-13.
Execução real depende do wiring de #473/#474 e do produtor shared (infra-ansible #55).

## Identidades e namespace

- Tenant `prod-canary-<canary_id>` (32 hex da activation). O prefixo `prod-canary-`, em
  qualquer caixa ou sufixo, é inacessível ao caminho normal: API, recovery GSI4,
  `coordinator.resume` e admissão recusam com `SyntheticContextRequired`.
- Runs `unit-<owner[:16]>`, `recovery-a-<owner[:16]>`, `recovery-b-<owner[:16]>`.
- Quatro roles nullable/disabled por default: `mode_seed`, `mode_dispatch`,
  `mode_audit_launch`, `mode_observe`. Sessões `mc-<code>-<40 hex>`.
- Control `PROD_CANARY_CONTROL#<owner>`, record `PROD_CANARY#<owner>#<phase>`, tickets
  `PROD_CANARY_EFFECT#<owner>#<effect_id>`, binding por dispatch `PROD_CANARY#<dispatch_id>`.
  Tetos de promoção: 8 unit tasks, 8 recovery resumes, 1 hang. Seal tenta até 64 CAS.

## Ordem na promoção

1. Keeper adquirido e activation autenticada; F ainda nulo.
2. `application-prepare` com `mode=mode-canary`: `admit_prepare` fecha o envelope de 12
   campos e `validate_mode_canary_prepare` aceita só os oito endereços
   `module.runtime_iam.aws_iam_role{,_policy}.mode_*[0]`. Initial: create/no-op;
   established: update/no-op. Todo o resto no-op. Trust e policy precisam ser os bytes
   `jsonencode` derivados do input admitido (espelho de `runtime-iam/mode-canary.tf`), e os
   hashes do output são conferidos contra esses bytes. Grant de RunTask (curinga, caixa ou
   `NotAction`) só no Launch exato de `mode_dispatch`/`mode_audit_launch`. `prepare_receipt`
   exige dois readbacks IAM concordantes, espaçados de 10 s ou mais, dentro do prazo original.
3. `mode_dispatch` grava o control ACTIVE (`initialize_control`) antes do seed.
4. Seed com `mode_seed`: `build_mode_seed` produz o artifact `mode-seed` antes das
   chamadas; `execute_mode_seed` usa só PutItem/GetItem e PutObject/GetObject condicionais.
   Run nasce PROCESSING (estado sintético declarado), unidades PENDING do `plan_run`.
5. Unit após B: `execute_mode_canary("unit", ...)` confere máquina e revisão unit na ASL e
   chama `dispatch_unit` do context com o binding da fase. Só aceita SFN SUCCEEDED com
   task STOPPED/exit 0 na revisão e output real.
6. Recovery após C-recovery: RunTask da revisão exata do target do Scheduler (retries 0,
   comando `recover-once`, rede do target), Count 1, sem ExecuteCommand, só
   `PROD_CANARY_BINDING` no ambiente e `clientToken` determinístico. Ticket admitido antes;
   resposta perdida vira UNKNOWN e a reentrada repete o mesmo token.
7. Audit após C-audit: RunTask com `mode_audit_launch` e só `PROD_CANARY_AUDIT_OBSERVE`.
   A task loga `mode_canary_audit_event` (hash do corpo, chaves e a retenção configurada em
   `configured_retention_mode`/`configured_retain_until`, sem ler o Object Lock real) e
   `mode_canary_audit_cursor` (digests do cursor, CAS, wrap, contadores, ids ausentes)
   só para tenants reservados, também nas invocações periódicas sem override.
8. Seal ACTIVE→SEALED antes de buscar quiescência; release só com ledger sem pending/unknown.

## Freeze simulado

Com `frozen=true`, seed e dispatch são negados antes de qualquer efeito
(`ModeCanaryDenied operation=<op>`). Audit é admitido e o resultado traz
`freeze_proof_sha256` com as negações de seed/unit/recovery registradas.

## Leitura do resultado

`mode-result` tem 21 campos fechados. `pending` e `unknown` só crescem entre reentradas;
F, `started_at` e `common` não mudam. Códigos de pendência:

| Código | Significado |
|---|---|
| `unit_task_not_stopped` / `recovery_task_not_stopped` | Sem task STOPPED observada |
| `unit_output_missing` | Exit 0 sem output de unidade |
| `unit_execution_not_succeeded` | SFN não terminou SUCCEEDED na máquina PROD |
| `<modo>_task_failed` | Exit diferente do esperado (3 em `hang_no_dispatch`, senão 0) |
| `routing_after_missing` / `routing_changed` | Readback pós-execução ausente ou divergente |
| `run_task:<token>` (unknown) | RunTask sem resposta ou com falha; repetir o mesmo token |

## Limites

- Roles runtime PROD são cross-tenant; o gate limita inputs, não há isolamento IAM por tenant.
- A task audit não vê o `VersionId` do objeto; `mode_observe` confirma versão e retenção.
- Readiness (`run_mode_readiness.py`, workflow manual) ainda não existe.
