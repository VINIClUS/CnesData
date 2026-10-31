# #385 — amendment v5: IAM literal, routing e preflight nativo

**Status:** contrato de escopo aceito pelo controller para o Slice 3. Esta atualização é
somente documental; implementação requer despacho separado. Não executa testes, cloud ou deploy.

**Data:** 2026-10-31
**Base de leitura:** `issue-385-next-plan.md`, `issue-385-root-rulings.md`,
`issue-385-routing-contract-review.md`, `issue-385-runtime-iam-inventory.md`,
`issue-385-aws-research.md` e `issue-385-aws-action-mapping.md`.

Os drafts históricos permanecem intactos. Esta revisão incorpora as erratas do plano IAM v3,
o preflight v4 e a correção de observação v5 aceitos; preserva normalização, oráculos e histórico.
Readback de implementação: HEAD `610970bbb63fdcb17503c22da90a7173af0241a4` (Slice 2 integrado).

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
A candidata unit é produzida diretamente pelo `image_digest` através de
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

**Phase B — route.** Depois da mesma activation manifest, drain e fence, altera active para uma
revisão registrada e já presente em retained. Há dois oráculos distintos:

- Ativação inicial (active=null → ARN): cria
  `module.processing.aws_sfn_state_machine.processor[0]` e atualiza
  `module.runtime_iam.aws_iam_role_policy.api[0]` para incluir as quatro ações SFN.
- Promoção com machine estabelecida: muda somente o routing/definição de
  `module.processing.aws_sfn_state_machine.processor[0]`; nenhuma policy muda.

Ambos mantêm digest, revisões ECS, recovery/audit, env comum, cluster, schedulers e routes.
Retained conserva old e candidate; grants antigos permanecem até decisão explícita de
drain/rollback. A ativação inicial não pode ser julgada pelo oráculo machine-only da promoção.

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

O input nullable existente `runtime-iam.state_machine_arn` aceita somente `null` ou o ARN
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

### Símbolos e condições exatas

T = module.control_plane.table_arn; I1/I4/I6 = seus index_arns["gsi1"/"gsi4"/"gsi6"].
D = module.data_buckets.data_bucket_arn; A = module.data_buckets.audit_bucket_arn.
M = `arn:aws:states:us-east-2:<account_id>:stateMachine:cnesdata-prod`.
E = `arn:aws:states:us-east-2:<account_id>:execution:cnesdata-prod:*`.
R = `arn:aws:events:us-east-2:<account_id>:rule/StepFunctionsGetEventsForECSTaskRule`.
U/X são os outputs ARN das roles unit/execution. Não criar nem consultar R na AWS.

- LKlike(values): `ForAllValues:StringLike` em `dynamodb:LeadingKeys` e
  `Null: {"dynamodb:LeadingKeys": "false"}`.
- LKeq(values): `ForAllValues:StringEquals` e o mesmo guard `Null=false`.
- Prefix(values): `StringLike: {"s3:prefix": values}`.
- Pass: `StringEquals: {"iam:PassedToService": "ecs-tasks.amazonaws.com"}`.
- K = `["TENANT#*", "TENANT#*#RUN#*", "TENANT#_GLOBAL", "ENV#prod#FENCE",
  "ENV#prod#SEMAPHORE"]`. Preservar a lista literal, inclusive sua sobreposição.
- Data5 = `[raw/*, tmp/*, normalized/*, reconciliation/*, serving/*]`.
- Publish4 = `[tmp/*, normalized/*, reconciliation/*, serving/*]`.
  Em recursos de objeto, D/Data5 e D/Publish4 expandem cada prefixo como `${D}/<prefixo>`.
  ListBucket usa somente D, sem `/*`, com a condição Prefix correspondente.
- DDB6 = `dynamodb:GetItem`, `dynamodb:Query`, `dynamodb:ConditionCheckItem`,
  `dynamodb:PutItem`, `dynamodb:UpdateItem`, `dynamodb:DeleteItem`.
- SFN_M = `states:DescribeStateMachine`, `states:StartExecution`.
- SFN_E = `states:DescribeExecution`, `states:StopExecution`.
- Event3 = `events:PutTargets`, `events:PutRule`, `events:DescribeRule`.
- Logs10 = `logs:CreateLogDelivery`, `logs:GetLogDelivery`, `logs:UpdateLogDelivery`,
  `logs:DeleteLogDelivery`, `logs:ListLogDeliveries`, `logs:CreateLogStream`,
  `logs:PutLogEvents`, `logs:PutResourcePolicy`, `logs:DescribeResourcePolicies`,
  `logs:DescribeLogGroups`.
- Audit4 = `s3:PutObject`, `s3:PutObjectRetention`, `s3:GetObject`, `s3:GetObjectRetention`.
- Nenhuma: Condition ausente ou vazia, normalizada à mesma tupla vazia.

### Matriz efetiva independente

O teste declara esta matriz antes de ler policies renderizadas. Recursos vêm dos produtores
verificados, nunca da policy observada. API* significa somente `active != null`; esse marcador
não é uma condição IAM. Todas as quatro ações SFN da API, incluindo DescribeStateMachine, ficam
ausentes em active=null; a policy DDB/S3 da API permanece anexada desde o bootstrap.

| Role/policy | Actions | Resources | Condition |
|---|---|---|---|
| SFN/common | iam:PassRole | U, X | Pass |
| SFN/common | Event3 | R | Nenhuma |
| SFN/common | Logs10 | * | Nenhuma |
| SFN/ECS | ecs:RunTask | candidate unit produzido ∪ retained | Nenhuma |
| SFN/ECS | ecs:DescribeTasks, ecs:StopTask | * | Nenhuma |
| API* | SFN_M | M | Nenhuma |
| API* | SFN_E | E | Nenhuma |
| API | dynamodb:Query | I1 | LKlike([USER#*]) |
| API | dynamodb:GetItem | T | LKlike([TENANT#*]) |
| API | s3:GetObject | D/reconciliation/*, D/serving/* | Nenhuma |
| API | s3:ListBucket | D | Prefix([reconciliation/*, serving/*]) |
| unit/recovery | SFN_M | M | Nenhuma |
| unit/recovery | SFN_E | E | Nenhuma |
| unit/recovery | DDB6 | T somente | LKlike(K) |
| unit/recovery | dynamodb:UpdateItem | T somente | LKeq([ENV#prod#ATTEMPTS]) |
| recovery | dynamodb:Query | I4 somente | LKeq([RUN_RECOVERABLE]) |
| unit | s3:GetObject | D/Data5 | Nenhuma |
| unit | s3:PutObject | D/Publish4 | Nenhuma |
| unit | s3:ListBucket | D | Prefix(Data5) |
| recovery | s3:GetObject, s3:PutObject | D/Publish4 | Nenhuma |
| recovery | s3:ListBucket | D | Prefix(Publish4) |
| audit | dynamodb:Query | I6 somente | LKeq([OUTBOX#PENDING]) |
| audit | dynamodb:GetItem, dynamodb:UpdateItem | T | LKeq([TENANT#_GLOBAL, OUTBOX#CURSOR]) |
| audit | s3:GetBucketObjectLockConfiguration | A | Nenhuma |
| audit | Audit4 | A/audit/* | Nenhuma |

Unit não recebe índice; recovery recebe Query em I4 em statement separado do grant da tabela.
ATTEMPTS também é statement separado: somente UpdateItem, sem GetItem, Query, PutItem,
DeleteItem ou ConditionCheckItem nessa partição. Audit não recebe SFN. Recovery não recebe raw.
Somente audit acessa A/audit/* e a configuração de Object Lock de A. Nenhum modo recebe delete
S3, list sem prefixo, DDB Scan ou grants implícitos fora da matriz.

Nunca emitir `dynamodb:TransactWriteItems` como Action; transações usam seus child actions.
Slice 3 omite `dynamodb:EnclosingOperation` integralmente. PutObjectRetention continua necessário
para Object Lock inline em PutObject, mesmo sem chamada separada put_object_retention.
Exigir Allow e Version 2012-10-17; rejeitar NotAction/NotResource/Principal e campos inesperados.
Rejeitar grants/statements repetidos ANTES da conversão em set/frozenset; isso não altera a
normalização por set do input retained. Comparar Actions/Resources/Conditions por igualdade exata.

### Proveniência das correções de IAM

Em packages/cnes_infra/src/cnes_infra/control_plane/, dynamodb_environment_gate.py:87-96 e
dynamodb_execution_quota.py:84-105 produzem/atualizam ENV#prod#ATTEMPTS. O admission é ligado por
aws/runtime.py:125 e consumido em domain/orchestration/dispatch_admission.py:197; unit e recovery
são montados em apps/data_processor/src/data_processor/aws_composition.py:90-170.
RUN_RECOVERABLE vem de dynamodb_run_codec.py:34 e da Query dynamodb_adapter.py:309-318.
OUTBOX#PENDING está em dynamodb_publication.py:73,273-303; cursor em dynamodb_outbox_cursor.py.

Unit after_persist chama coordinator.resume (aws_composition.py:148); coordinator.py:360-390
publica unidades. publisher.py:96-110,194-196 lê todos os output_manifests e verifica destinos
finais sem filtro de estágio. stage_processor.py:144,194,239 produz normalized/reconciliation/
serving. Em infra/object_store/s3.py:74,157,187-191, stat/replay usam GetObject e promote lê tmp e
grava destino. recovery.py existe, é ligado em aws_composition.py:158-170 e delega recover/resume
sem StageProcessor; isso não exclui publicação normalized. Corrigir grants, não o runtime.
As referências detalhadas e os arquivos AWS arquivados permanecem no plano v3 e no oráculo literal.

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

Escopo futuro: Slice 2 existente é Modificar (não Criar); esta atualização não edita fontes.

**Criar**, sob `tests/production/infra/`:

- `_root_native_identities.py`: seeds computed-only, checagens, merge e renderer existente.
- `test_root_native_identities.py`: 63 casos congelados, sem edição nesta correção; SHA-256
  `a03a6515442862a67546e5e0a326383173a6479f86af9445596f882181f6fe82`.
- `test_root_mock_defaults.py`: novos testes puros da preservação separada; sem runner.
- `_root_native_observation.py`: extração JSON, evidência e checagem de produtores; sem subprocess.
- `_processor_orchestration_contracts.py`, somente se necessário para dados puros já autorizados.

**Modificar — produto**, mantendo o escopo IAM/routing existente:

- `infra/opentofu/env/prod/main.tf`, `variables.tf`, `outputs.tf`;
- `infra/opentofu/modules/processing/variables.tf`, `outputs.tf`, `ecs.tf`, `step_functions.tf`,
  `state_machine.asl.json`;
- `infra/opentofu/modules/runtime-iam/variables.tf`, `outputs.tf`, `task-roles.tf`,
  `step-functions.tf`.

**Modificar — testes/helpers**, todos sob `tests/production/infra/`:

- `test_processor_revision_contract.py`: revisão/normalização e seeds em positivos e negativos.
- `test_step_functions_iam.py`: grants comuns/ECS e merge de seeds por run stateful.
- `test_step_functions_iam_asl.py`, `test_step_functions_iam_runtime.py`: mesmos contratos
  ASL/runtime, isolamento e oráculos de ativação/promoção; sem novo runner/produto.
- `_step_functions_iam_contracts.py`: extração e igualdade literal independente de grants.
- `test_composition.py`: inventário/forwarding; tipos e acesso ao relatório nativo, escopos
  positivos explícitos e assertions equivalentes de runs/contagens/diagnósticos abaixo.
- `test_static_web_composition.py`: inventário; incluir aws_iam_role_policy na allowlist de
  processing; schema próprio antes do plan e merge por fronteira; preservar STATIC_TYPES.
- `test_processing_revisions.py`: produtores/outputs e contagem/identidade de roles.
- `test_processing_runtime.py`: ARN determinístico, bootstrap nulo e igualdade após criação.
- `test_ecr_ecs.py`: primeira discovery seeded, merge dos overrides IAM e grants root exatos;
  manter execução/pull/logs e isolamento runtime-iam sem policies de aplicação.
- `test_processing_ecs.py`: roles/mapas/famílias; seeds na chamada direta do digest inválido;
  preservar o runner isolado de quatro inputs, sem seeds root nem novos opcionais.
- `_processing_native.py`: schema antes da primeira discovery, merge/render, checagem de
  produtores, forwarding e flags opcionais do init offline de schema em workspace copiado.
- `_processing_identities.py`: reutilizar/exportar apenas checagens existentes de config/computed;
  preservar contratos de identidade de repository/network/task/service.
- `_composition_contracts.py`: extração compartilhada estreita, se necessária; sem mudar provider.
- `conftest.py`: schema/seeds, JSON/verbose, SharedValidator, relatório; isolamento/markers.
  Só native_contract_result: mkdir exclusivo ANTES try; try/finally cobre execução/observação/yield
  e remove só diretório novo próprio. Executor hash/move stale à evidência antes de rerun; nunca
  adotar/apagar preexistente. Persistir raws antes da rejeição; controle de produtor verifica
  erro/raw original e remoção do diretório próprio, sem ampliar cleanup a outras fixtures.
- `test_control_plane.py`: seeds apenas no run root, JSON/verbose e relatório do plan misto;
  manter isolated_report textual e todos os oráculos de proteção/mutação.
- `test_data_buckets.py`: seeds apenas no run root, JSON/verbose/relatório; substituir somente
  scaffolding textual obsoleto, preservando casos de módulo, validação e proteção.
- `test_cognito.py`: forwarding de identidades somente na raiz de produção, sem mudar oráculos.
- `test_network.py`: forwarding somente na raiz de produção; raiz sintética/guards intactos.
- `_aws_mock_defaults.py`: somente extrair constante hostname nos dois locais existentes,
  mantendo bytes avaliados de MOCK_AWS_PROVIDERS exatos; sem parser/mutação de HCL.

`.trivyignore` mantém apenas a autorização anterior: achado aceito por design, motivo e expiração.
Nenhum outro path é autorizado. Não mudar validators, fixtures JSON, ProductionNetworkPolicy,
providers, locks, módulos de rede/buckets/control-plane/registry, CI ou floor nesta etapa.
Não criar controller, scheduler, serviço ECS, módulo duplicado ou lookup AWS. Runtime IAM
isolado omite runtime_grants e conserva ausência de policies de aplicação; runtime_grants=null.
Runtime-iam possui policies comuns/runtime/API; processing possui ECS candidate∪retained.
A role API permanece externa. Nenhuma policy runtime-iam concede ecs:RunTask.

### Preflight computed-only antes da primeira discovery

Seeds puros da raiz: oito targets/quinze campos ARN/id computados; preservação de defaults é
etapa separada abaixo. Prefixar targets com `module.`; não sobrescrever configurados ou outputs:

| Target | Campos |
|---|---|
| control_plane.aws_dynamodb_table.control_plane | arn |
| data_buckets.aws_s3_bucket.data | arn, id |
| data_buckets.aws_s3_bucket.audit | arn, id |
| runtime_iam.aws_iam_role.execution | arn, id |
| runtime_iam.aws_iam_role.task["unit"] | arn, id |
| runtime_iam.aws_iam_role.task["recovery"] | arn, id |
| runtime_iam.aws_iam_role.task["audit"] | arn, id |
| runtime_iam.aws_iam_role.step_functions[0] | arn, id |

Seeds do contrato de fixture: account=123456789012 (VALID_SHARED_OUTPUTS), região us-east-2 e
name_prefix=cnesdata-prod (prod/providers.tf). Tabela: nome cnesdata-prod-control-plane; ARN
`arn:aws:dynamodb:us-east-2:123456789012:table/cnesdata-prod-control-plane`.
Buckets: cnesdata-prod-data e cnesdata-prod-audit; ARNs `arn:aws:s3:::cnesdata-prod-data` e
`arn:aws:s3:::cnesdata-prod-audit`; id igual ao nome. Roles: cnesdata-prod-processor-execution,
cnesdata-prod-processor-unit, cnesdata-prod-processor-recovery, cnesdata-prod-processor-audit e
cnesdata-prod-step-functions; path=/, ARN `arn:aws:iam::123456789012:role/` + nome; id=nome,
não unique_id. Names vêm do contrato independente de main.tf e
runtime-iam/task-roles.tf/step-functions.tf; não alegar derivação de um plan ainda não executado.
Negativos mantêm seus inputs malformados e usam seeds válidos canônicos; não reparar inputs.

Obter show-config/version e schema ANTES de renderizar a primeira discovery. Reutilizar o
_provider_schema provider-only existente, uma vez em tofu_artifacts; remover a chamada tardia.
Exigir fonte registry.opentofu.org/hashicorp/aws, versão selecionada igual ao lock (6.54.0),
lock bytes intactos. Seleção/lock são gates de _provider_schema em tofu_artifacts e na cópia
static; seu retorno é schema puro, sem campo de versão inventado. root_identity_seeds consome
somente schema já verificado; evidência do gate real acompanha a proveniência separadamente.
Testes de seeds não fingem validar seleção ausente no schema nem alteram fixture_contract.
Exigir provider default aws sem alias, região us-east-2, recurso managed/endereço exato, schema
computed=true e type=string, sem expressão configurada para cada campo seeded.
Registrar por campo value/source, expressões configuradas, atributo schema, seleção provider e
origem do contrato; não fabricar original_computed_value. Copiar workspace static exige seu
próprio config/schema/lock; usar flags do mirror offline no quarto argumento opcional do helper.
Não reinicializar scratch exclusivo nem baixar/atualizar provider para satisfazer a fixture.
Interfaces estreitas: root_identity_seeds(configuration, schema, fixture_contract),
merge_identity_records(*groups), assert_root_identity_producers(plan, seeds). Reutilizar o único
_override_hcl, movido ao helper puro com reexport em _processing_native; sem import reverso.
Mesmo target/campo/valor coalesce preservando proveniência; divergência falha setup. Renderizar
um bloco efetivo por target; nunca competir overrides file/run. Após cada plan root positivo,
validar nomes/path/account avaliados, conjunto de targets e campos seeded. Para índices, observar
o ARN real da tabela e o conjunto de nomes em global_secondary_index; não alegar index_arns
interno avaliado, ausente no test_plan. Derivar ARN esperado como <table ARN real>/index/<nome>
e compará-lo aos recursos concretos das policies reais nos testes IAM downstream por igualdade
exata. Nunca fabricar outputs avaliados nem substituir essa prova IAM por source grep.
Isso impede que seeds ocultem mutação de nome/path. Dados configurados não substituem essa prova.
Aplicar antes de TODA discovery root: conftest composição/shared, processing inicial/tasks/null/
digests, stateful, registry inicial, revisões positivas/negativas e static. Em suites mistas,
injetar só no run root explícito control_plane/data_buckets/cognito/network, nunca globalmente.
Excluir SYNTHETIC_SUITE network e módulos isolados/proteção; preservar headers/defaults/aliases.
SFN_ROLE_OVERRIDE usa arn+id compartilhado; digest inválido mantém todos os diagnósticos originais.
Preservação separada: preserve_root_mock_defaults(records, configuration, schema, default_context).
Contexto explícito {value: str, source: str}; só data/audit bucket_regional_domain_name computed
string não configurado, validado mesmo se explícito; copiar entradas sem mutação. Adicionar apenas
campo ausente; manter value/source explícitos e rejeição normal de conflitos. Proveniência inclui
producer_address, provider_config_key, schema_attribute, mock_default_source e expressões.
Seeds seguem 8/15; bundle .seeds puro e .overrides 8/17 separados; render usa overrides,
checker usa seeds, inclusive processing/stateful, sem quinto argumento ou projeção de overrides.
Static aplica data A/B antes da preservação; audit usa default custom local, não o global. Extrair
constantes sem alterar bytes MOCK_AWS_PROVIDERS/aliases/headers A/B; provar igualdade avaliada.
Exigir auditoria dos testes, GO de comportamento, RED de assertion e auditoria dos helpers.

### Observação estruturada da mesma invocação

Usar JSON/verbose nas invocações existentes de conftest.native_contract_result,
conftest.validate_shared_outputs, test_control_plane.plan_report e test_data_buckets.plan_report.
Nenhuma segunda observação, runner genérico ou resumo de sucesso fabricado é permitida.
observe_root_result(result, seeds, observation_spec, evidence_context) retorna NativeRootReport:
result original, statuses terminais e observations (plans/summary/diagnostics reais). Não lança
subprocesso. Salvar streams, argv/rc, suites e seeds antes de parse/check; label único por chamada.
Rejeitar JSON inválido, runs ambíguos e terminais duplicados; reter identidade testfile/run.

O spec declara os positivos root explicitamente. Cada um exige rc=0, status pass, summary real
bem-sucedido, exatamente um plan não vazio próprio e assert_root_identity_producers antes de
retornar a fixture. Ausência de plan positivo é setup_error, nunca missing_grants. Pass de
expect_failures não é observação positiva. Negativos continuam na raiz completa, sem projeção.
SharedValidator recebe (cases, positive_roots) e retorna NativeRootReport; False na tupla não
implica positivo: null/missing-field também usam False e devem falhar nativamente.

Preservar todos os nomes/runs/status/counts e diagnósticos, adaptando somente o transporte:

- Composição: evaluate_composition_contract, pass, summary real 1/0/0/0 (pass/fail/error/skip).
- Shared válido: enabled_true/false, ambos planos checados; summary real 2/0/0/0, rc=0.
- Shared inválido: invalid_* exatos, expect_failures=[var.shared_outputs], nenhum positivo;
  todos pass, summary real len(cases)/0/0/0, rc=0, nenhum diagnóstico error inesperado.
- Null: rc!=0, passed=0, failed+errored=1, skipped=0 e diagnóstico nativo literal
  `required variable may not be set to null`; não fabricar summary se estiver ausente.
- account_baseline_ausente: rc!=0 e `attribute "account_baseline" is required`; falha pré-run
  não exige plan/summary inexistente. Erros vêm apenas de summary/detail severity=error ou stderr
  original, nunca texto de suite/plan/input. Não substituir falha nativa por checagem Python.
- Control-plane: cinco runs originais pass; só raiz_agenda_control_plane exige plan de produtores.
  Preservar isolated_report textual, prevent_destroy, mutações e todos os asserts HCL/config.
- Data-buckets: raiz + sete positivos de módulo + INVALID_NAME_RUNS; todos pass e summary real
  8+len(INVALID_NAME_RUNS)/0/0/0. Só raiz_agenda_data_buckets exige plan de produtores.

Manter checagens de provider-header e as mensagens exatas. Relatórios negativos preservam rc,
streams e diagnósticos, sem default de sucesso. Ausência de evidência obrigatória falha o teste.
O laboratório provider-free 1.13.2 aceito fundamenta preservar negativos root, não prova produção.

Limites: arquivos<=500/largura100, funções<=50, parâmetros<=4, nesting<=3, complexidade<=10;
observer<=200. Recalcular budgets pós-defaults; data-buckets/network em 500, registry perto de 500.
Não remover assertions/casos/comentários nem ampliar paths para caber; conservar relatório isolado.
Fixture RED/GREEN antecede missing_policy/missing_grants em plan válido; 14 negativos baratos e
auditoria nativa uma vez permanecem. Nenhum setup_error prova IAM; gate fullinfra segue abaixo.
Extrações mecânicas só em _processing_native.py e test_ecr_ecs.py, após defaults aceitos e focados
GREEN completos; revalidar hashes/ASTs/cinco alvos/budgets após essa correção antes de editar.
Processing: _successful_plans→_native_plan_events; _identity_overrides→_network_identity_records.
Registry: _successful_plan usa o coletor; _runtime_identities→_partition_runtime_roles;
_role_policy_bindings→_assert_inline_policy_block. Preservar assinaturas/retornos/gates/mensagens,
nomes, ordem, regex e normalização; gates processing/registry distintos, sem observer mais estrito.
Antes de fullinfra: equivalência old/new, métricas reais, mesmos focados, Ruff/Pyright e review do
controller. Se budgets/alvos mudarem, reavaliar; sem compressão, terceiro path ou alterar oráculos.

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
- Base da pesquisa original: `origin/develop@7dd83bc5ba27e1e1e70164f6dfdd8bee18825b74`.
  Qualquer divergência de dependência exige nova leitura e não autoriza rebase silencioso.

Este amendment não executa bootstrap, apply, plan de provider real, scheduler, Docker, SDK/AWS,
deploy ou alteração em fonte. O plano executável a seguir começa por um slice nativo pequeno e
parável; os slices posteriores não podem ser iniciados se o contrato do slice anterior falhar.
