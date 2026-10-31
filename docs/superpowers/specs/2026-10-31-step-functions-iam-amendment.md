# #385 — amendment v4: revisão de routing, bootstrap e contrato executável

**Status:** contrato de planejamento. Este arquivo autoriza a revisão do plano; não autoriza
edição de código, teste, fixture, provider, cloud ou deploy.

**Data:** 2026-10-31
**Base de leitura:** `issue-385-next-plan.md`, `issue-385-root-rulings.md`,
`issue-385-routing-contract-review.md`, `issue-385-runtime-iam-inventory.md`,
`issue-385-aws-research.md` e `issue-385-aws-action-mapping.md`.

Esta versão preserva v1, v2 e v3. Ela congela a normalização dos inputs e o driver nativo de
verificação, além do inventário de paths de teste, para que os contratos existentes acompanhem o
novo forwarding, outputs e roles sem apagar os oráculos atuais.

## Efeito sobre o plano anterior

Este amendment prevalece sobre qualquer trecho do plano anterior que trate `candidate` como
entrada da ASL, use `latest`/família sem revisão, exija uma state machine durante o bootstrap,
proíba `env/prod/variables.tf` ou `env/prod/outputs.tf`, use a fixture genérica como prova de
produção, ou trate `TransactWriteItems` como ação IAM. Os drafts históricos, inclusive v1,
permanecem intactos e não são reescritos.

O contrato agora separa três coisas:

- `processor_active_unit_task_definition_arn` é a única revisão que a ASL referencia;
- `processor_retained_unit_task_definition_arns` é o conjunto finito de revisões autorizadas e
  protegidas durante registro, drain e rollback. Uma candidata registrada em Phase A entra nesse
  conjunto antes de qualquer execução; ela não vira `active` automaticamente;
- a policy `ecs:RunTask` contém exatamente a união da revisão unit candidata produzida pelo
  `image_digest` (quando houver) e `retained`, deduplicada. `active` precisa estar em `retained`
  e, portanto, já fica coberta sem ser uma fonte separada de ARN. Não há ARN de família, `latest`,
  revisão recovery/audit ou ARN estrangeiro.

`active` e `retained` são inputs da raiz, provenientes da activation manifest/revisão de promoção.
A candidata unit é produzida diretamente pelo `processor_image_digest` através de
`module.processing.task_definition_arns["unit"]`; ela não é input adicional, não é descoberta AWS
e não precisa estar no manifest antes da Phase A. A raiz passa os dois inputs ao módulo processing;
o próprio módulo compõe candidate ∪ retained para o grant.

## Contrato de inputs, validação e bootstrap

### Forma dos inputs

Adicionar à raiz de produção, com defaults que permitam a primeira avaliação:

```hcl
processor_active_unit_task_definition_arn  = null
processor_retained_unit_task_definition_arns = []
```

O tipo efetivo do segundo input é `set(string)` com `nullable = false` e default vazio quando essa
forma for suportada pelo OpenTofu; na fronteira, `null` legado é normalizado para conjunto vazio.
Duplicatas textuais são normalizadas silenciosamente pelo set e a ordem de entrada nunca altera
JSON, policy, ASL ou hash. Não rejeitar duplicatas antes ou depois da conversão para set. O módulo
`processing` recebe os mesmos valores, com `active` nulo permitido.

Cada ARN deve ser um ARN ECS completo, qualificado por revisão decimal, na região `us-east-2`,
na conta de `shared_outputs.account_id`, com família exatamente pertencente ao namespace de
unit `${name_prefix}-processor-unit` (a validação deve escapar o prefixo, não aceitar prefixos
parecidos). A revisão é positiva e obrigatória. A validação rejeita família sem revisão,
`latest`, URI de imagem, ARN de outra conta/região, ARN de outro serviço, ARN de recovery/audit e
caracteres fora do formato de família ECS. O conjunto final deve ser finito e conter somente ARNs
desse formato.

As validações precisam produzir diagnóstico explícito e estável para pelo menos:
`task_definition_arn_shape`, `task_definition_arn_account`, `task_definition_arn_region`,
`task_definition_arn_family`, `task_definition_arn_revision` e
`active_must_be_retained`. Não executar lookup AWS para validar identidade.

### Semântica nula

- `active = null` é o bootstrap. O cluster, a role de Step Functions e as roles ECS podem ser
  planejados; a state machine não existe e `module.processing.state_machine_arn`/o output raiz
  correspondente são `null`. As tasks unit/recovery preservam desde a Phase A a variável
  determinística `AWS_STATE_MACHINE_ARN` já prevista no contrato #384; ela não é derivada do
  output nulo e não faz a task depender da própria machine. A igualdade com o ARN produzido é
  verificada depois da primeira criação real.
- Nesse bootstrap, `retained` vazio é a forma inicial. Um `retained` não vazio é permitido
  somente para a Phase A que já registrou revisões unit qualificadas e traz a activation manifest;
  ainda assim não há state machine nem execução. Isso permite autorizar a candidata registrada
  sem inventar `active`.
- `active != null` exige `active ∈ retained`, state machine real e ARN de machine não nulo.
  Tasks unit/recovery continuam recebendo o ARN determinístico de `var.state_machine_arn`; após a
  criação ele deve ser igual ao output de `aws_sfn_state_machine`, nunca um literal estrangeiro.
  O estado `audit` não recebe essa variável.
- `image_digest = null` continua sem task definitions. Inputs de revisão ainda são validados;
  não se fabrica uma revisão para tornar o plan verde.

## Cluster e routing

`processing/step_functions.tf` cria um `aws_ecs_cluster` real, sempre que o módulo é composto,
e exporta `cluster_arn`. Não criar ECS service, desired count, autoscaling, execution role,
Container Insights, scheduler, controller ou segunda instância de módulo. O parâmetro
`Parameters.Cluster` da ASL usa diretamente o ARN produzido desse recurso.

A ASL usa somente `active`. O `candidate` produzido pode estar na policy junto com `retained`, mas
mudar uma candidata com `active` fixo não muda a definição ASL. O output `state_machine_arn` é o ARN
produzido pela machine `STANDARD` chamada `cnesdata-prod`, ou `null` no bootstrap.

### Promotion phases

**Phase A — register/authorize.** Sem exigir fechamento do fence, registra as três revisões do
digest aprovado, inclui a nova revisão unit no conjunto `retained` e dual-autoriza old/new pela
policy. Mantém a ASL, `TaskDefinition` active, schedulers e routes exatamente iguais. Nenhum
recurso de scheduler é criado ou ativado por #385.

**Phase B — route.** Depois de verificação da mesma activation manifest, drain e fence, altera
somente `processor_active_unit_task_definition_arn` para uma revisão já registrada e já presente
em `retained`. Não registra revisão ECS, não muda digest, recovery, audit, env comum, cluster,
policy, scheduler ou route. Grants antigos permanecem até drain/rollback completo.

O teste nativo deve provar a transição sobre uma mesma execução mockada (Phase A apply/estado,
seguida de Phase B plan) quando o harness suportar esse seam. Dois plans independentes do zero
não são aceitos como prova de “sem nova revisão”; se o harness não oferecer o seam, o teste deve
falhar como `setup_error` e o próximo passo é adaptar apenas o harness de teste, sem reduzir o
oráculo.

## Retry e ASL congelados

O único retry autorizado é o seguinte, no estado `RunProcessor`:

```json
{
  "Retry": [
    {
      "ErrorEquals": ["AmazonECS.Unknown"],
      "IntervalSeconds": 3,
      "BackoffRate": 2,
      "MaxAttempts": 3,
      "MaxDelaySeconds": 5,
      "JitterStrategy": "FULL"
    }
  ]
}
```

Não incluir `Catch`, `States.ALL`, logging configuration/payload, Distributed Map, estado de
wave ou estados NORMALIZE/RECONCILE/MATERIALIZE. `AmazonECS.Unknown` cobre a falha otimizada do
ECS `.sync` quando `RunTask.Failures` não está vazio. Os números acima são limites finitos do
produto, escolhidos com o exemplo oficial de jitter; não são defaults exigidos pela AWS.

A Task `RunProcessor` define `TimeoutSeconds = 3600`, alinhado ao lease de produção.
O runtime usa esse lease no `DeadlinePolicy` de `run-unit`
(`apps/data_processor/src/data_processor/main.py`). O limite da integração cobre também a
espera externa pela task. `States.Timeout` não entra no retry e continua sem `Catch`.
Essa decisão limita a duração externa sem alterar validators ou deadlines do runtime.

## IAM corrigido

### Roles e grafo

O novo input nullable `runtime-iam.state_machine_arn` aceita somente `null` ou o ARN
literal `arn:aws:states:us-east-2:${account_id}:stateMachine:${name_prefix}`. Conta, região,
nome, serviço, forma e qualificação divergentes falham com
`state_machine_arn_identity=expected_own_unqualified_machine`. Isso mantém o trust restrito
também na composição isolada. O input não nulo e determinístico já existente de processing
é distinto do output condicional produzido pela machine; tasks nunca dependem desse output.
Active não nulo sem a role Step Functions falha com `step_functions_role_required`.

`runtime-iam` cria a role de Step Functions e exporta `step_functions_role_id` e
`step_functions_role_arn`; ela não referencia `module.processing` nem contém `ecs:RunTask`.
`processing` anexa a policy de `RunTask` à role recebida por ID, com resources somente
`candidate produzido ∪ retained`, e a policy de `DescribeTasks`/`StopTask` usa apenas o wildcard
indispensável ao endpoint AWS. A raiz mantém uma única chamada a cada módulo.

A role API é externa e já é produzida por `shared_outputs.roles_anywhere.api_role_arn`
(`env/prod/shared-outputs.schema.json`, `env/prod/variables.tf`). #385 não cria
`aws_iam_role` API. A composição recebe somente esse ARN, extrai o nome com validação de ARN e
anexa desde o bootstrap a policy DDB/S3 da API. O bloco de ações Step Functions é condicional:
fica ausente enquanto `active` é nulo e inclui `DescribeStateMachine` (e as ações preparatórias de
execution) quando a machine existe.

### Matriz mínima por modo

| role | ações permitidas | recursos/condições obrigatórios |
|---|---|---|
| API | `states:DescribeStateMachine`; contrato preparatório `StartExecution`, `DescribeExecution`, `StopExecution`; DDB `Query`/`GetItem`; S3 `GetObject` e `ListBucket` | machine exata; execution ARNs da machine; `gsi1`/tabela e LeadingKeys definidos; `reconciliation/*` e `serving/*`; prefixos equivalentes em `ListBucket`; nenhuma escrita audit/raw |
| unit | boot `DescribeStateMachine`; DDB `GetItem`, `Query`, `ConditionCheckItem`, `DeleteItem`, `PutItem`, `UpdateItem`; SFN `StartExecution`, `DescribeExecution`, `StopExecution`; S3 `GetObject`, `PutObject`, `ListBucket` | machine/execution exatos; tabela e índices produzidos; namespaces de tenant/outbox/fence/semaphore; raw/tmp e destinos finais autorizados; `ListBucket` com prefixos |
| recovery | boot/SFN e DDB da unit; os mesmos child actions transacionais necessários; S3 `GetObject`, `PutObject`, `ListBucket` | `gsi4`, tabela e namespaces do coordinator; somente `tmp/*`, `reconciliation/*`, `serving/*` no caminho de resume; sem `raw/*` |
| audit | `s3:GetBucketObjectLockConfiguration`; DDB `Query` em `gsi6`, `GetItem`, `UpdateItem`; S3 `PutObject`, `PutObjectRetention`, `GetObject`, `GetObjectRetention` | `gsi6`, cursor/evento; bucket audit e `audit/*`; sem delete, list ou bypass |

Não existe `dynamodb:TransactWriteItems` como grant independente. Uma transação usa somente os
child actions correspondentes (`ConditionCheckItem`, `DeleteItem`, `PutItem`, `UpdateItem`).
`dynamodb:EnclosingOperation` pode restringir Put/Update/Delete à chamada transacional, mas não
substitui grants e não deve ser aplicado a `ConditionCheckItem` como se fosse universal.

O sink de audit envia Object Lock inline no próprio `PutObject`; por isso a policy efetiva mantém
`s3:PutObject` e `s3:PutObjectRetention`, mesmo sem chamada separada `put_object_retention`.

### Matriz efetiva independente

Antes de comparar o plano, o teste deve declarar esta matriz como dados independentes da policy
renderizada. Os nomes entre `⟨…⟩` são valores de outputs/manifest já validados; não são
wildcards que possam ser ampliados pela implementação.

| role | action | resource | condition exigida |
|---|---|---|---|
| Step Functions | `ecs:RunTask` | cada ARN de `⟨candidate produzido ∪ retained⟩` | nenhuma revisão fora do conjunto |
| Step Functions | `ecs:DescribeTasks`, `ecs:StopTask` | `*` | somente esta role contém o wildcard indispensável ao endpoint |
| Step Functions | `iam:PassRole` | exatamente `⟨unit role⟩` e `⟨execution role⟩` | `iam:PassedToService = ecs-tasks.amazonaws.com` |
| Step Functions | `events:PutTargets`, `events:PutRule`, `events:DescribeRule` | `⟨StepFunctionsGetEventsForECSTaskRule ARN⟩` | nenhuma |
| Step Functions | logs delivery oficial (`CreateLogDelivery`, `GetLogDelivery`, `UpdateLogDelivery`, `DeleteLogDelivery`, `ListLogDeliveries`, `CreateLogStream`, `PutLogEvents`, `PutResourcePolicy`, `DescribeResourcePolicies`, `DescribeLogGroups`) | `*` | nenhuma |
| API | `states:DescribeStateMachine`, `states:StartExecution` | `⟨machine ARN⟩` | anexar apenas com `active != null` |
| API | `states:DescribeExecution`, `states:StopExecution` | `arn:aws:states:us-east-2:⟨account⟩:execution:cnesdata-prod:*` | nenhuma |
| API | `dynamodb:Query` | `⟨table ARN⟩/index/gsi1` | `dynamodb:LeadingKeys = USER#*` |
| API | `dynamodb:GetItem` | `⟨table ARN⟩` | `dynamodb:LeadingKeys = TENANT#*` |
| API | `s3:GetObject` | `⟨data bucket ARN⟩/reconciliation/*`, `⟨data bucket ARN⟩/serving/*` | nenhuma |
| API | `s3:ListBucket` | `⟨data bucket ARN⟩` | `s3:prefix` somente `reconciliation/*`, `serving/*` |
| unit | `states:DescribeStateMachine`, `states:StartExecution` | `⟨machine ARN⟩` | nenhuma |
| unit | `states:DescribeExecution`, `states:StopExecution` | `⟨execution ARN prefix⟩` | nenhuma |
| unit/recovery | DDB `GetItem`, `Query`, `ConditionCheckItem`, `DeleteItem`, `PutItem`, `UpdateItem` | `⟨table ARN⟩` e somente índices `⟨gsi1/gsi4 conforme modo⟩` | LeadingKeys dos namespaces usados pelo modo; sem `Scan` |
| unit | `s3:GetObject` | `⟨data bucket ARN⟩/raw/*`, `/tmp/*` e destinos finais autorizados | nenhuma |
| recovery | `s3:GetObject` | `⟨data bucket ARN⟩/tmp/*`, `/reconciliation/*`, `/serving/*` | não incluir `/raw/*` |
| unit/recovery | `s3:PutObject` | somente `⟨data bucket ARN⟩` nos destinos finais do modo | nenhuma |
| unit/recovery | `s3:ListBucket` | `⟨data bucket ARN⟩` | `s3:prefix` somente os prefixes do modo |
| audit | `s3:GetBucketObjectLockConfiguration` | `⟨audit bucket ARN⟩` | nenhuma |
| audit | `dynamodb:Query` | `⟨table ARN⟩/index/gsi6` | `dynamodb:LeadingKeys = OUTBOX#PENDING` |
| audit | `dynamodb:GetItem`, `dynamodb:UpdateItem` | `⟨table ARN⟩` | somente partições cursor/evento autorizadas |
| audit | `s3:PutObject`, `s3:PutObjectRetention`, `s3:GetObject`, `s3:GetObjectRetention` | `⟨audit bucket ARN⟩/audit/*` | sem delete, list ou bypass |

Os patterns `/raw/*` acima são prefixos de objeto, escritos como
`⟨data bucket ARN⟩/raw/*`; a barra inicial na célula é apenas legibilidade. O teste deve expandir
os símbolos a partir dos outputs esperados e rejeitar qualquer statement que acrescente action,
resource ou condition. `dynamodb:EnclosingOperation` permanece opcional: se não for escolhido
por uma policy, não deve ser inventado pelo teste; se escolhido, só pode aparecer nos child
actions Put/Update/Delete e com o valor transacional definido na documentação do contrato.

## Polaridade dos validators

Os validators e fixtures existentes são imutáveis. A fixture genérica continua
`AssignPublicIp=DISABLED` e é usada apenas como positivo do contrato neutro. A definição
renderizada de produção deve ser `AssignPublicIp=ENABLED`, subnets públicas exatas, security
group exato, lease `3600` e concorrência `1`; ela é o positivo de
`validate_production_state_machine` e o negativo esperado do validator genérico. Os testes devem
registrar esses resultados separadamente, sem alterar `step_functions.py`, fixtures ou
`ProductionNetworkPolicy`.

## Outputs e paths autorizados

Além dos outputs de #384, o contrato produz:

- `module.processing.cluster_arn`, `module.processing.state_machine_arn`;
- `module.runtime_iam.step_functions_role_id`, `module.runtime_iam.step_functions_role_arn`;
- outputs públicos equivalentes na raiz (`processor_cluster_arn`,
  `processor_state_machine_arn`, `processor_step_functions_role_id`,
  `processor_step_functions_role_arn`). O state machine output é nulo no bootstrap; os demais
  existem quando seus módulos existem.

Paths de código permitidos por este amendment:

**Criar:**

- `infra/opentofu/modules/processing/step_functions.tf`
- `infra/opentofu/modules/processing/state_machine.asl.json`
- `infra/opentofu/modules/runtime-iam/step-functions.tf`
- `tests/production/infra/test_processor_revision_contract.py`
- `tests/production/infra/test_step_functions_iam.py`
- `tests/production/infra/test_step_functions_iam_asl.py` e
  `tests/production/infra/test_step_functions_iam_runtime.py`, somente para separar
  os testes do mesmo contrato ASL/runtime e manter cada arquivo abaixo de 500 linhas;
  não introduzem runner, fixture de produto ou comportamento adicional.
- `tests/production/infra/_step_functions_iam_contracts.py` (helper isolado; mantém o teste
  principal abaixo do limite atual em vez de inflá-lo)
- opcional, somente se o slice de revisão precisar de dados puros separados:
  `tests/production/infra/_processor_orchestration_contracts.py`

**Modificar:**

- `infra/opentofu/env/prod/main.tf`, `infra/opentofu/env/prod/variables.tf`,
  `infra/opentofu/env/prod/outputs.tf`;
- `infra/opentofu/modules/processing/variables.tf`, `outputs.tf`, `ecs.tf`;
- `infra/opentofu/modules/runtime-iam/variables.tf`, `outputs.tf`, `task-roles.tf`;
- `tests/production/infra/test_composition.py` — somente inventário de outputs raiz e forwarding
  estreito dos dois módulos;
- `tests/production/infra/test_static_web_composition.py` — somente o inventário independente de
  outputs e os tipos `aws_ecs_cluster`/`aws_sfn_state_machine` dentro da allowlist de
  `processing`; preservar intactos a lista e os oráculos de recursos static-web. O output
  de machine nulo no bootstrap segue o contrato de nulidade já exigido;
- `tests/production/infra/test_processing_revisions.py` — somente valores produtores, outputs e
  contagem/identidade de roles revisados;
- `tests/production/infra/test_processing_runtime.py` — somente wiring do ARN determinístico,
  output nulo no bootstrap e igualdade após a criação da machine;
- `tests/production/infra/test_ecr_ecs.py` — preservar o teste base do módulo runtime-iam sem
  policies de aplicação e adicionar no root os positivos/negativos das novas policies;
- `tests/production/infra/test_processing_ecs.py` — somente contagens/identidades de roles,
  mapas de task roles e famílias estáveis;
- `tests/production/infra/_processing_native.py` — somente forwarding de `active`/`retained`,
  digest e overrides necessários ao plan nativo;
- `tests/production/infra/_processing_identities.py` — somente identidades determinísticas para
  novos roles/outputs no mock nativo;
- `tests/production/infra/_composition_contracts.py` — somente se necessário para compartilhar
  a extração de outputs, sem alterar a política de provider;
- `tests/production/infra/conftest.py` — condicional, apenas se o fixture existente precisar
  encaminhar os novos inputs ao harness; manter markers, isolamento e todos os defaults AWS.
- `.trivyignore`, somente para um achado aceito por design, com motivo e expiração.

Qualquer modificação de teste/helper acima deve ser limitada a inventário, forwarding, identidade,
output e policy da composição #385. Não apagar o oráculo base do módulo: a ausência de policies de
aplicação no módulo isolado continua coberta, enquanto a diferença de policies anexadas no root
ganha asserções positivas e negativas independentes. Não alterar validators, fixtures JSON,
`ProductionNetworkPolicy`, providers, locks, módulos de rede/buckets/control-plane/registry.
Não criar controller, scheduler, serviço ECS, segunda instância, output derivado de lookup AWS ou
placeholder de ARN. Qualquer outro path exige novo amendment.

## Gates e fronteiras

- #384 está integrada no baseline `7dd83bc5ba27e1e1e70164f6dfdd8bee18825b74`; o issue pode
  permanecer **OPEN** por administração do backlog e isso não bloqueia este trabalho documental
  offline. A preimage histórica `1a4d6ba416c6c6d010e285a2daac4b8956002b46`, o `source.tar`
  `8a0bf02372f456e01ba4f44c6a4d62ecd0148ebf7531fcf73e29463a50dea6b2` e o HEAD
  `e9d012b635b741d03649a28c3247eae5dff82d9e` ficam registrados como evidência antiga, não como
  base. O gate desta revisão é readback do baseline e dos contratos; não há requisito de fechar
  #384. Operação, deploy, apply e chamadas cloud permanecem explicitamente congelados.
- #371/PROD-006 aceito: PR #399 head
  `26991429fd8b2f74128539a99b13fd90666e64f3`, merge `88ea303cee1088b9abefe6f3e18022744a149993`,
  Deploy develop verde `38169504531`.
- #375/PROD-010 aceito: PR #425 head
  `d1dfd1f6ae90dddd1a4252a9f8bfd684f8ee26aa`, merge `709394c09969001fa41df5d885f10c9e821b06c9`,
  árvore `de30c09b98e91aba134c89a36e040a3bab2fb11f`, Deploy develop verde `40592568914`.
- Base de pesquisa atual: `origin/develop@7dd83bc5ba27e1e1e70164f6dfdd8bee18825b74`.
  Qualquer divergência de dependência exige nova leitura e não autoriza rebase silencioso.

Este amendment não executa bootstrap, apply, plan de provider real, scheduler, Docker, SDK/AWS,
deploy ou alteração em fonte. O plano executável a seguir começa por um slice nativo pequeno e
parável; os slices posteriores não podem ser iniciados se o contrato do slice anterior falhar.
