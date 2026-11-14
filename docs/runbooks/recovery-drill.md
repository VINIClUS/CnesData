# Drill de recovery e audit (#477)

Prova em produção os itens SMK-06, SMK-07 e SMK-08 de
[production-promotion.md](production-promotion.md): falha forçada de recovery e de audit,
recovery travado e recoveries sobrepostas. Roda no estágio Produção da #474, depois das duas
aceitações da Phase C, com o tenant sintético reservado de [mode-canary.md](mode-canary.md).

O drill não faz deploy, não aplica plan, não edita item do DynamoDB e não para execução à mão.
A regra de [aws-runtime-validation.md](aws-runtime-validation.md#procedimento-de-recovery) vale
inteira: o CAS do dispatch e o fencing das unidades são a proteção contra replay.

## Valores de produção

Fonte: `docs/superpowers/specs/2026-08-29-cnesdata-production-operations-design.md` (§6 e §7.2).

- Lease das unidades e deadline do PID 1: `AWS_PROCESSOR_LEASE_SECONDS=3600`.
- `recover-once` a cada 30 minutos, batch 100, `MaximumRetryAttempts=0` no Scheduler.
- `dispatch-outbox-once` independente a cada 30 minutos, com deadline de 60 s no PID 1.

## Pré-condições

- Aprovação explícita do dono na #474, com o formato de evidência e a limpeza de
  [infrastructure-plan.md](infrastructure-plan.md#capability-checks-em-aws-real).
- Phase C aceita para recovery e para audit, com as rotas lidas forte.
- Identidades, tickets e tetos da canary de modo (8 unit tasks, 8 recovery resumes, 1 hang)
  conforme [mode-canary.md](mode-canary.md). O drill não cria role nem tenant fora deles.

## SMK-06: falha forçada de recovery e de audit

1. Forçar a falha de recovery só pelo mecanismo do pacote v4 do #499 descrito em
   [mode-canary.md](mode-canary.md). Este drill não cria outro.
2. Esperado: a passada termina com exit 1 e `processor_recovery_completed` com `failed > 0`,
   sem afetar runs de tenant normal.
3. Na mesma janela, o `dispatch-outbox-once` entrega o audit a um objeto com Object Lock:
   `mode_observe` confirma versão e retenção.
4. Com o freeze simulado da canary (`frozen=true`), seed e dispatch são negados antes de
   qualquer efeito e o audit continua admitido, com `freeze_proof_sha256` no resultado.

## SMK-07: recovery travado

1. Usar o único hang permitido pela canary (`hang_no_dispatch`, exit esperado 3).
2. Esperado: o PID 1 chega ao deadline igual ao lease, cancela o trabalho, sai com código
   diferente de zero e a task está `STOPPED` no ECS até o deadline.
3. Log do deadline e alarme disparado entram na evidência.

## SMK-08: recoveries sobrepostas

1. Dois runs distintos da canary (`recovery-a-…` e `recovery-b-…`) com passadas sobrepostas,
   repetidas ou duplicadas.
2. Esperado: no máximo uma task Fargate de unidade ativa em toda a janela. A passada perdedora
   adia sem `StartExecution`; o CAS do dispatch só repete chamada no mesmo run.
3. Cada passada é limitada sozinha. Não existe teto global de concorrência de recovery, e toda
   task-hour de unidade, recovery e audit conta para a meta monitorada de 100 horas.

Leitura de tasks ativas, só leitura:

```bash
aws ecs list-tasks --cluster "$CLUSTER" --desired-status RUNNING --output json
```

## Evidência

Um registro por item (`SMK-06`, `SMK-07`, `SMK-08`) no schema de
[production-promotion.md](production-promotion.md#schema-de-evidência). O artefato redigido
guarda códigos de saída, contagens de tasks ativas, booleanos de log e alarme e o hash do
resultado da canary. Nada de tenant, run, ARN ou corpo de evento.

## Falha no meio do drill

Rota divergente ou partial apply mantém o fence fechado. A convergência segue
[rollback.md](rollback.md) ou um plan novo revisado da Phase C; nunca uma edição manual.
