# Rollback, drain e poda do processor (#475)

Rollback da release AWS atual (N+1) para a anterior (N), drain do trabalho antigo e poda das
revisões e digests que saíram da retenção. Os scripts ficam em `scripts/promotion/`:
`rollback.py`, `drain.py` e `prune_revisions.py`. Nenhum deles muda dados do DynamoDB ou do
S3 para trás, nem edita state ou o control plane à mão.

## Pré-condições

- Activation anterior autenticada pelo caminho histórico: `prepare_phase_b(reference, ...)`
  chama `verify_activation(historical=True)` com o `ext2_post_upload_receipt_v1` da activation
  **anterior**. Receipt, artefato ou log original ausente ou expirado falha fechado. Não há
  nova assinatura nem refresh por idade.
- Activation atual e a evidência da Phase B dela.
- Fence `CLOSED` por `close_and_drain`, com o documento de close-drain em mãos.

## 1. API e estático, sem esperar o processor

1. O host gate do infra-ansible executa `rollback-previous`: upstream do Nginx e digest GHCR
   anteriores. O CnesData não edita o host.
2. Com o handoff `recovery_required` do host, `restore_api_static` restaura `index.html` e
   `theme-init.js` da versão anterior dentro de `min(recovery_deadline, choice_at + 300 s)`.
   Outro status do host recusa sem tocar no bucket.
3. `rollback_release` grava o registro do restore antes de iniciar a convergência do
   processor. A convergência segue em paralelo, com o fence fechado.

## 2. Rotas do processor

1. `rollback_context(prior, current, phase_b)` liga a activation anterior à atual:
   - a unit anterior é o `active_before` da Phase A atual e está no conjunto retido;
   - recovery e audit anteriores são as rotas registradas na Phase B atual;
   - a evidência da Phase B é da activation atual.
2. `tfvars` dá as quatro variáveis do plan: unit e Schedulers anteriores e o conjunto retido
   atual, que mantém a unit atual para as execuções em voo. `image_digest` não muda.
3. `tofu plan` e auditoria independente com `phase=rollback`, `plan_sha256`, `checkpoint` e
   `source_sha`.
4. `execute_reviewed_rollback` revisa a cópia 0400 do plan. Um só plan troca a máquina, os dois
   Schedulers e os dois grants `ecs:RunTask`. Plan parcial, nova revisão ECS, mudança no grant
   da state machine ou rota diferente da anterior recusam o plan inteiro.
5. O documento de close-drain precisa ser da activation atual (`activation_sha256`); outro
   recusa antes de ler o plan. Logo antes e depois do apply, o fence precisa estar fechado na
   mesma promoção, sem permit.
   Depois do apply: state limpo, checkpoint que avançou e as três rotas anteriores lidas forte.
6. Apply com falha exige plan novo e nova auditoria.

A reabertura do fence depois do rollback não é feita por estes scripts. Ela exige decisão do
dono, com as rotas anteriores lidas forte e o state limpo.

## 3. Drain

- Uma execução Standard mantém a definição com que começou. `drain_old_work` lista as
  execuções RUNNING iniciadas antes da troca de rota e as tasks ECS RUNNING de revisões
  antigas do processor.
- Drenado exige duas leituras limpas seguidas. Uma nova leitura só começa antes do prazo, então
  a segunda cai no máximo um intervalo de poll (5 s) depois dele. Prazo vencido devolve as
  revisões em voo, e revisões, digests e grants antigos ficam.
- Cancelamento só com `CancelApproval`: aprovador, motivo e cada execução como ARN exato,
  tenant e run. Nunca curinga. O cancelamento usa `StepFunctionsExecutor.cancel` e entra na
  evidência.

## 4. Poda

- Só depois do drain. Sem drain, `execute_prune` não lê nem apaga nada.
- As referências vêm de `PruneReferences.from_drain(result, prior)`: atual, retidas, em voo e
  drenado saem do `DrainResult` e do alvo dele. A anterior tem uma revisão por família, fora da
  atual, com a unit no conjunto retido; vazia só quando o conjunto retido é só a unit atual.
- O inventário lê o ECR e as três famílias de task definition até o fim da paginação. Página sem
  fim, revisão fora do contrato ou referência sem digest tagueado tornam o inventário
  incompleto, e nada é podado.
- `processor_retention` decide: até 5 digests por até 30 dias, nunca o atual, o anterior, os
  em voo ou os retidos. Um digest só sai quando todas as revisões que o usam são mais antigas
  que a atual e a anterior e não estão protegidas.
- Ordem: leitura, decisão, segunda leitura igual, deregister das revisões, `BatchDeleteImage`
  por digest e leitura final. Mudança entre leituras, falha de delete, leitura final incompleta
  ou recurso que volta na leitura final param a poda.
- `retained_after` propõe o conjunto retido reduzido às unit atual, anterior e em voo. Os
  grants antigos só saem por um novo plan revisado com esse conjunto.

## ECR

- `promote_ecr` grava o manifest com a tag imutável `release_id` e confere digest e tag no
  readback. A mesma tag no mesmo digest é idempotente; em outro digest, recusa.
- Não há regra de lifecycle por tag. A regra untagged de 7 dias continua para push
  intermediário.
- O adaptador real do registry precisa aceitar a tag em `put_manifest`, devolver `tags` em
  `describe_image` e devolver `None` em `describe_image(tag)` quando a tag não existe.

## Estágio Produção

O ensaio de rollback roda numa segunda promoção (N+1 para N): digest anterior do API e task
definition unit, com evidência redigida. O entrypoint do frontend cita o rollback estático real
da #473.
