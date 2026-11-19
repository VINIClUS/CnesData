# Freeze de custo (#477)

O que muda quando o freeze agregado de custo está ativo e como provar o efeito sem depender do
Budget real. Este runbook não faz deploy e não dispara o Budget.

## Envelope

- Produto CnesData até USD 8 por mês; teto operacional agregado de USD 15, incluindo KMS,
  evidência e WAF, com a Budget action levando ao freeze (EPIC #465, seção Custos).
- O Budget e a ação de freeze têm dono externo (`personal-infra-live`). O CnesData só recebe
  `shared_outputs.budget_freeze` (`budget_arn`, `action_arn`, `policy_arn`) em
  `infra/opentofu/env/prod/variables.tf`, validados por forma pelo gate de
  [infrastructure-plan.md](infrastructure-plan.md).
- A reconciliação real do Budget para USD 15 está pendente (EXT-5/BACKEND-FREEZE). A
  configuração integrada em personal-infra-live#1 não demonstra aplicação real.

## Efeito do freeze

O freeze anexa a policy `cnesdata-budget-freeze` às roles alvo. Com ela anexada:

- Promoção aborta: o preâmbulo lê as policies anexadas e recusa com `freeze=present`
  (`scripts/promotion/_api_static_preamble.py`). Listagem truncada ou ARN de freeze
  desconhecido também recusam.
- Custo novo para: as ações de custo das roles alvo ficam negadas pela policy, cujo conteúdo
  é do produtor externo.
- Continuam: entrega de audit ao objeto com Object Lock, leitura do serving e backup (PITR do
  control plane e versionamento dos buckets).

## Athena

O Athena do operador tem corte próprio, que não depende deste freeze. Disable manual e
reativação com o alarme de cutoff em `OK` estão em [athena-operator.md](athena-operator.md).

## Drill

O drill prova o efeito, não o Budget.

1. Freeze simulado da canary de modo (`frozen=true`), conforme
   [mode-canary.md](mode-canary.md#freeze-simulado): seed e dispatch negados com
   `ModeCanaryDenied`, audit admitido e `freeze_proof_sha256` no resultado.
2. Preâmbulo da promoção com a policy de freeze anexada à role lida: recusa `freeze=present`
   antes de qualquer efeito. Coberto em `tests/production/delivery/test_api_static_preamble.py`.
3. Na mesma janela, leitura do serving e entrega de audit continuam.

A leitura das policies anexadas é só leitura:

```bash
aws iam list-attached-role-policies --role-name "$ROLE" --output json
```

A classificação da role do fence operator no freeze é da #472 (D5 da EPIC #465), fora deste
drill.

## Evidência

O resultado entra nos registros do `PRE-17` e do `SMK-06` (o freeze não para o audit), no schema
de [production-promotion.md](production-promotion.md#schema-de-evidência). O artefato guarda o
hash do resultado da canary, o código da recusa do preâmbulo e os booleanos de audit e leitura,
sem ARN nem conta.
