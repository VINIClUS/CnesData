# Validação do runtime AWS (`PROFILE=aws`)

Valida a aplicação (`central_api` e `data_processor`) no profile `aws`, sobre DynamoDB, S3,
Step Functions Standard e ECS Fargate (EPIC #94, AWS-010…014). Nada aqui cria recurso nem faz
deploy. O texto descreve o código de `develop`; onde plano e código divergem, vale o código.

**Bloqueado pela especificação de deployment**
(`docs/superpowers/plans/2026-08-31-cnesdata-production-*.md`): criação de recursos, documentos
de policy IAM, KMS, rede, alarmes e agendamento. Não há IaC do profile `aws` no repo;
`deploy/aws/raw/` pertence ao caminho raw legado (#294).

## Gate em emuladores

O job `aws-runtime-integration` de `.github/workflows/python-quality.yml` sobe `dynamodb-local` e
`localstack` (profile `aws-test`), roda a suíte AWS-014 (`tests/integration/aws/`) e sempre
executa o step `Stop AWS emulators` (`docker compose --profile aws-test down -v` sob
`if: always()`). Credenciais dummy existem só no step do pytest. O job herda os gatilhos do
workflow: PR que toca `packages/`, `apps/central_api/`, `apps/data_processor/` ou `tests/`,
nightly (que lê o arquivo de `main`) e `workflow_dispatch`. Torná-lo obrigatório é regra de
ruleset, decisão do dono do repo.

Reprodução local, com projeto compose próprio para não tocar em outras stacks:

```bash
docker compose -p cnesdata-aws-gate --profile aws-test up -d --wait dynamodb-local localstack
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
AWS_ENDPOINT_URL=http://127.0.0.1:4566 DYNAMODB_ENDPOINT_URL=http://127.0.0.1:18000 \
  uv run pytest -m "dynamodb_local and s3_integration" tests/integration/aws -q
docker compose -p cnesdata-aws-gate --profile aws-test down -v
```

`DB_URL` precisa existir no ambiente (placeholder; o `.env` local cobre). O LocalStack 4.11.1
pinado não tem ECS: a suíte cria as state machines nele e as valida pela composição, mas atende
`StartExecution`, `DescribeExecution` e `StopExecution` em processo, com semântica Standard.
Trocar o digest é decisão de licença (comentário em `docker-compose.yml`), não bump de rotina.
O bucket de audit, com retenção `COMPLIANCE`, só some no `down -v`.

## Variáveis de ambiente

Fonte: `packages/cnes_infra/src/cnes_infra/aws/settings.py`. Valor inválido derruba o boot com
`AwsRuntimeConfigurationError(<código>)`.

| Variável | Regra | Código |
|---|---|---|
| `PROFILE` | `aws` minúsculo; `AWS` roteia, mas não valida | `profile_must_be_aws` |
| `AUTH_MODE` | `oidc` | `auth_mode_must_be_oidc` |
| `OIDC_ISSUER` | `https`, sem `?`/`#`; `http` só com `AWS_ENDPOINT_URL` | `oidc_issuer_invalid` |
| `OIDC_AUDIENCE` | obrigatória | `missing=OIDC_AUDIENCE` |
| `AWS_REGION` | obrigatória | `missing=AWS_REGION` |
| `AWS_CONTROL_PLANE_TABLE` | obrigatória | `missing=AWS_CONTROL_PLANE_TABLE` |
| `AWS_DATA_BUCKET` | obrigatória | `missing=AWS_DATA_BUCKET` |
| `AWS_AUDIT_BUCKET` | obrigatória | `missing=AWS_AUDIT_BUCKET` |
| `AWS_STATE_MACHINE_ARN` | obrigatória | `missing=AWS_STATE_MACHINE_ARN` |
| `AWS_PROCESSOR_CONTAINER_NAME` | obrigatória | `missing=AWS_PROCESSOR_CONTAINER_NAME` |
| `AWS_AUDIT_RETENTION_DAYS` | obrigatória, ≥ 1, sem teto | `audit_retention_days_invalid` |
| `AWS_SERVING_URL_TTL_SECONDS` | default 300, 30–900 | `serving_ttl_out_of_range` |
| `AWS_PROCESSOR_MAX_CONCURRENCY` | default 8, 1–40 | `processor_concurrency_invalid` |
| `AWS_PROCESSOR_LEASE_SECONDS` | default 300, 30–3600 | `processor_lease_invalid` |
| `AWS_PROCESSOR_RECOVERY_BATCH_SIZE` | default 100, 1–1000 | `recovery_batch_invalid` |
| `DYNAMODB_ENDPOINT_URL`, `AWS_ENDPOINT_URL` | só em emulador; produção deixa vazias | — |

- Variável numérica presente e vazia não usa o default: falha com `integer_invalid=<VAR>`.
- `DB_URL` segue obrigatório no import de `cnes_infra.config` (placeholder, não usado).
- Credenciais vêm só da provider chain (role da task). A task definition não define
  `AWS_ACCESS_KEY_ID` nem `AWS_SECRET_ACCESS_KEY`: o boto3 prefere as variáveis à role.
- A API recusa `RAW_BACKEND` e `RAW_AWS_*` no mesmo processo (`raw_backend=forbidden`).
- `AWS_PROCESSOR_MAX_CONCURRENCY` é o teto de deployment de cada onda.
  `AWS_PROCESSOR_LEASE_SECONDS` é o lease do dispatch e o `LEASE_SECONDS` das unidades.
- A task `recover-once` recebe o conjunto inteiro, audit incluído, porque usa a mesma
  composição.

Reproduzir a validação com o ambiente exato de uma task, sem chamar a AWS (para no primeiro
erro):

```bash
uv run python - <<'PY'
import os

from cnes_infra.aws.settings import AwsRuntimeSettings

AwsRuntimeSettings.from_mapping(os.environ)
PY
```

## Health e readiness

- `GET /api/v1/system/health` é só liveness. No profile `aws` responde sempre 200 com
  `status=ok`, sem identidade e sem sondar DynamoDB, S3 ou Step Functions. É a rota do
  `HEALTHCHECK` da imagem da API; um 200 não prova acesso atual à AWS.
- A readiness real é o boot. A API (lifespan) e cada task do processor fazem, nesta ordem:
  `GetObjectLockConfiguration` no `AWS_AUDIT_BUCKET` (falha com `object_lock=disabled` ou com o
  `ClientError` da AWS) e `DescribeStateMachine` com `validate_state_machine` (próxima seção).
- DynamoDB não é sondado no boot. Discovery OIDC e JWKS são lazy: o primeiro request
  autenticado busca `<OIDC_ISSUER>/.well-known/openid-configuration` (timeout 5 s, cache de
  chaves de 600 s).
- Boot falho na API: a exceção do lifespan derruba o uvicorn antes de abrir a porta, e o
  traceback sai em texto no stderr.
- Boot falho no processor: uma linha JSON `processor_entrypoint_failed` só com
  `exception_type`, e exit 1. O código do erro não vai para o log; diagnosticar com a
  reprodução de settings acima e com os checks abaixo.

Checks de leitura antes de subir uma revisão:

```bash
aws s3api get-object-lock-configuration --bucket "$AWS_AUDIT_BUCKET"
uv run python - <<'PY'
import os

import boto3

from cnes_infra.executor.step_functions import validate_state_machine

validate_state_machine(
    boto3.client("stepfunctions", region_name=os.environ["AWS_REGION"]),
    os.environ["AWS_STATE_MACHINE_ARN"],
    os.environ["AWS_PROCESSOR_CONTAINER_NAME"],
    int(os.environ["AWS_PROCESSOR_LEASE_SECONDS"]),
)
PY
```

## State machine compatível

`validate_state_machine` (`packages/cnes_infra/src/cnes_infra/executor/step_functions.py`)
recusa com `IncompatibleStateMachine(<código>)`:

| Exigência | Código |
|---|---|
| `type` `STANDARD`; Express é recusado | `workflow_must_be_standard` |
| um único `Map`, que é o `StartAt` | `single_map_required`, `map_must_be_start_state` |
| `ItemProcessor.ProcessorConfig.Mode` `INLINE`; Distributed é recusado | `map_must_be_inline` |
| `ItemsPath` `$.unit_ids` | `map_items_must_be_unit_ids` |
| `ItemSelector` exato (abaixo) | `map_item_selector_mismatch` |
| `MaxConcurrencyPath` `$.max_concurrency` | `map_concurrency_must_be_explicit` |
| sem `Catch` nem `ToleratedFailure*` no Map e na Task | `unit_failures_must_propagate` |
| uma única Task `arn:aws:states:::ecs:runTask.sync` | `single_ecs_sync_task_required` |
| `LaunchType` `FARGATE` | `launch_type_must_be_fargate` |
| `TaskDefinition` presente | `task_definition_required` |
| `Subnets` não vazio | `subnets_required` |
| `AssignPublicIp` `DISABLED` | `assign_public_ip_mismatch` |
| um só override para `AWS_PROCESSOR_CONTAINER_NAME` | `processor_container_override_missing` |
| o override só define `Environment` | `container_override_must_only_set_environment` |
| nenhuma variável duplicada no `Environment` | `duplicate_environment_variable` |
| `LEASE_SECONDS` literal igual a `AWS_PROCESSOR_LEASE_SECONDS` | `lease_seconds_mismatch` |
| exatamente as sete variáveis do envelope | `environment_bindings_mismatch` |

O `ItemSelector` exato leva os quatro IDs do input e o `unit_id` de `$$.Map.Item.Value`.
Não são validados `Cluster`, `SecurityGroups`, conteúdo e revisão da task definition, roles e
`Retry`. Erro de API no `DescribeStateMachine` vira `ProcessorExecutionUnavailable(<código>)`.
Definição de referência:
`packages/cnes_infra/tests/fixtures/step_functions/standard_inline_ecs.json`.

O input da execução carrega só `tenant_id`, `run_id`, `wave_id`, `dispatch_id`, `unit_ids` e
`max_concurrency`. O nome da execução é o `dispatch_id`: um replay com o mesmo input recebe
`ExecutionAlreadyExists` e reaproveita o ARN, e input diferente falha com
`execution_name_conflict`. Mudar `AWS_PROCESSOR_LEASE_SECONDS` exige mudar o literal da state
machine na mesma entrega, senão API e processor não sobem.

## Envelope da task de unidade

A state machine entrega sete variáveis ao container, sem argumentos de comando, sobre o
`ENTRYPOINT ["python", "-m", "data_processor.main"]`
(`apps/data_processor/src/data_processor/aws_entrypoint.py`):

| Variável | Binding | Validação |
|---|---|---|
| `TENANT_ID` | `$.tenant_id` | não vazia (`missing=TENANT_ID`) |
| `RUN_ID` | `$.run_id` | não vazia (`missing=RUN_ID`) |
| `WAVE_ID` | `$.wave_id` | 16 hex minúsculos (`invalid=WAVE_ID`) |
| `DISPATCH_ID` | `$.dispatch_id` | 16 hex minúsculos (`invalid=DISPATCH_ID`) |
| `UNIT_ID` | `$.unit_id` | não vazia (`missing=UNIT_ID`) |
| `EXECUTION_OWNER` | `$$.Execution.Id` | prefixo `arn:aws:states:` (`invalid=EXECUTION_OWNER`) |
| `LEASE_SECONDS` | literal (`AWS_PROCESSOR_LEASE_SECONDS`) | 30–3600 (`invalid=LEASE_SECONDS`) |

Com `UNIT_ID`, a task processa exatamente uma unidade, e qualquer argumento falha com
`unit_mode=argv_forbidden`. Variável do envelope sem `UNIT_ID` falha com
`unit_envelope=partial`, inclusive um `TENANT_ID` herdado de um `.env` de agente. Todos esses
erros viram `processor_entrypoint_failed` e exit 1.

## `recover-once` agendado

- Mesma imagem, com `command: ["recover-once"]`, nenhuma das sete variáveis e o conjunto
  inteiro de settings. Outro comando falha com `command=recover_once_required`.
- Uma passada limitada: lê até `AWS_PROCESSOR_RECOVERY_BATCH_SIZE` candidatos (índice `gsi4`,
  revalidados na chave base) e retoma os runs `PROCESSING`, `PUBLISHING` e
  `CANCEL_REQUESTED` pelo `PipelineCoordinator`. Em `PROCESSING`, o dispatch `STARTED` é
  liquidado por `DescribeExecution`: se `RUNNING`, fica; se terminal, fecha, e a próxima onda
  (ou a mesma, em generation+1) é reservada, iniciada e ligada. `PUBLISHING` é publicado.
- Cadência de no máximo `AWS_PROCESSOR_LEASE_SECONDS / 2`. O código não impõe isso, o
  agendador impõe. Passadas sobrepostas são seguras (CAS do dispatch).
- Exit 0 com a passada toda ok. Exit 1 se algum run falhou (`RecoveryFailed`, #314), depois de
  logar o resumo, inclusive quando uma passada sobreposta perde uma corrida CAS benigna.
  Alarmar em falha repetida, não em uma.
- É o `recover-once` que dispara a onda seguinte depois que a execução da anterior termina, e
  que retoma runs cuja última task morreu. Sem ele, os runs param em `PROCESSING` ou
  `PUBLISHING`. A última unidade de um run também publica, via `after_persist`.
- Referência de produção, a fixar pela spec de deployment: lease 3600, a cada 30 minutos,
  batch 100 e sem retry do agendador.

## Ações IAM por componente

O documento de policy exato é da spec de deployment. As ações abaixo saem das chamadas boto3
de hoje; ninguém na aplicação chama ECS.

- API (`central_api`): no DynamoDB, o conjunto de escrita na tabela e em `index/*`; no bucket
  de dados, `s3:GetObject`, `s3:PutObject` e `s3:ListBucket`; no bucket de audit, o conjunto de
  audit; em Step Functions, `DescribeStateMachine`, `StartExecution`, `DescribeExecution` e
  `StopExecution`.
- Task de unidade: o mesmo da API, exceto no bucket de audit, onde só lê
  `s3:GetBucketObjectLockConfiguration`.
- `recover-once`: o mesmo da task de unidade.
- Role da state machine, que inicia as tasks: `ecs:RunTask`, `ecs:StopTask`,
  `ecs:DescribeTasks`, `events:PutTargets`, `events:PutRule`, `events:DescribeRule` e
  `iam:PassRole` das roles da task.
- Conjunto de escrita do DynamoDB: `GetItem` (sempre com leitura consistente), `Query`,
  `PutItem`, `TransactWriteItems` e, pelos itens das transações, `UpdateItem`, `DeleteItem` e
  `ConditionCheckItem`. Não há `Scan`, `Batch*` nem `DescribeTable`.
- Task de unidade e `recover-once` precisam das quatro ações de Step Functions porque passam
  por `PipelineCoordinator.resume` (na unidade, via `after_persist`): liquidam o dispatch
  (`DescribeExecution`), iniciam a onda seguinte (`StartExecution`) e param a execução quando o
  bind falha (`StopExecution`). O plano da Task 8 previa só control plane e
  `DescribeExecution` para o recovery; o código de hoje exige as quatro.
- `DescribeExecution` e `StopExecution` usam o ARN de execução
  (`arn:aws:states:<região>:<conta>:execution:<máquina>:*`), não o da state machine.
- `s3:ListBucket` no bucket de dados é obrigatório: sem ela, `GetObject` de chave ausente
  devolve 403 em vez de 404, e `stat()` (que só reconhece 404, `NoSuchKey` e `NotFound`)
  propaga erro. Isso quebra o upload raw, a checagem de serving e o replay de put.
- Conjunto de audit: `s3:GetBucketObjectLockConfiguration` no bucket; `s3:PutObject`,
  `s3:PutObjectRetention`, `s3:GetObject` e `s3:GetObjectRetention` em `audit/*`. Nada de
  delete, list ou bypass. Todos os processos leem a configuração de Object Lock no boot, mas só
  a API grava audit.
- A URL de serving vale com a permissão de quem assina: a role da API precisa de
  `s3:GetObject` em `serving/*` no momento do uso. `s3:DeleteObject` não é usado em runtime.

## Audit: Object Lock e versioning

- O bucket de audit nasce com Object Lock habilitado, o que liga o versioning de forma
  definitiva. Sem isso, API e processor não sobem.
- Cada evento do outbox vira dois objetos em retenção `COMPLIANCE` até
  `created_at + AWS_AUDIT_RETENTION_DAYS`: `audit/<tenant>/<AAAA>/<MM>/<DD>/<event_id>.json` e o
  marcador `audit/.event-id/<event_id>.json`
  (`packages/cnes_infra/src/cnes_infra/audit/s3_object_lock_sink.py`).
- O put usa `IfNoneMatch: *` e checksum SHA-256. Replay só é aceito com conteúdo idêntico e
  retenção suficiente (`GetObjectRetention`); senão, `Conflict`.
- Quem grava hoje é o dispatcher de outbox dentro da API, a cada 30 s. A entrega é
  at-least-once por `event_id`.
- Retenção `COMPLIANCE` não pode ser encurtada nem apagada por ninguém, root incluído; o valor
  de `AWS_AUDIT_RETENTION_DAYS` é decisão da spec de deployment.

## Autenticação e serving

- Rotas `/api/v1/dashboard/*` exigem Bearer e `X-Tenant-Id`. Sem token, 401 `auth_required`;
  token inválido, 401 `token_invalid`; sem header, 400 `tenant_header_required`; sem
  membership, 403 `tenant_not_allowed`. Do token só saem `iss` e `sub`. O tenant é autorizado
  por leitura consistente da membership na chave base; o índice `gsi1` só lista candidatos.
- `GET /api/v1/dashboard/serving/{dataset}/{documento}` responde 307 para uma URL assinada de
  `serving/<tenant>/<run_id>/<documento>.json`, com `X-Dataset-Version` e
  `Cache-Control: private, no-store`, válida por `AWS_SERVING_URL_TTL_SECONDS`. Só chaves do
  grant ativo são assinadas. Fora do grant, 404 `serving_document_not_found`; objeto ausente ou
  falha de assinatura, 503 `active_serving_unavailable`.

## Logs e métricas

- O processor escreve uma linha JSON por evento no stdout, para o driver `awslogs`: `timestamp`
  (UTC, `Z`), `level`, `service` (`data-processor`), `logger`, `event` (mensagem formatada) e
  os extras no topo. Exceção sai só como `exception_type`, sem mensagem nem stack. Os campos
  `authorization`, `token`, `signed_url`, `aws_access_key_id`, `aws_secret_access_key` e
  `email`, e qualquer campo terminado em `_<nome>` (por exemplo `refresh_token`), viram
  `[REDACTED]` (`packages/cnes_infra/src/cnes_infra/observability/json_logging.py`).
- A API não configura logging da aplicação no profile `aws`. Eventos INFO
  (`aws_profile_composed`, `outbox_dispatched`) não saem. WARNING e acima
  (`dispatch_start_compensated`, `oidc_token_invalid`, `tenant_access_denied`,
  `outbox_dispatch_error`) saem em texto puro no stderr, sem o formatter JSON e com traceback.
  Filtrar a API só por texto.

Métricas a derivar por metric filter (criação, limiares e alarmes são do deployment):

| Métrica | Origem |
|---|---|
| `ProcessorRecoveryScanned` | `$.event = "processor_recovery_scanned"`, valor `$.scanned` |
| `ProcessorRecoveryRecovered` | `$.event = "processor_recovery_completed"`, valor `$.recovered` |
| `ProcessorRecoveryFailed` | `$.event = "processor_recovery_completed"`, valor `$.failed` |
| `ExecutionBindingFailures` | prefixo `dispatch_start_compensated` (em `$.event`; texto na API) |
| `ExecutionRedispatches` | sem evento de origem hoje (abaixo) |

`ExecutionRedispatches` não tem filtro possível: nenhum log marca a reserva de generation+1 nem
carrega `generation`.

Dimensões só `Environment`, `Service` e um `Reason` limitado. Tenant, run, onda e dispatch são
campos de correlação, nunca dimensão. Não alarmar em `execution_lost`: só o executor local o
produz.

## Sintomas

Backlog de recovery:

- `processor_recovery_scanned` com `scanned` igual a `AWS_PROCESSOR_RECOVERY_BATCH_SIZE` em
  passadas seguidas: há mais candidatos do que uma passada cobre.
- `processor_recovery_completed` com `failed > 0` repetido e `recover_run_error tenant_id=…
  run_id=…` para os mesmos runs.
- `recover_run_error` com `exception_type` `ProcessorExecutionUnavailable` num run com dispatch
  `STARTED`: o `DescribeExecution` falhou. O run só sai disso quando o lease do dispatch expira e
  uma passada reserva generation+1.
- `processor_execution_probe_failed` com `reason`: a passada falhou antes de retomar runs.
- `processor_entrypoint_failed` em toda passada: a task agendada roda, mas não passa do boot.

Falha de binding:

- `dispatch_start_compensated tenant_id=… run_id=… dispatch_id=…`: `StartExecution` funcionou,
  mas o bind do dispatch ou o callback `started` falhou. A execução é parada, o dispatch fecha
  `CANCELED` e o erro original segue. A passada seguinte reserva generation+1 com outro
  `dispatch_id`.
- Não há log de confirmação do `StopExecution`: conferir o status da execução. Uma execução
  `ABORTED` com o nome do dispatch antigo é esperada; não reiniciá-la.

## Procedimento de recovery

1. Identificar `TENANT_ID` e `RUN_ID` pelos campos de correlação dos logs.
2. Ler o dispatch ativo na chave base, com leitura consistente e só leitura:

   ```bash
   hex() { printf %s "$1" | od -An -tx1 | tr -d ' \n'; }
   pk="TENANT#$(hex "$TENANT_ID")#RUN#$(hex "$RUN_ID")"
   aws dynamodb get-item --table-name "$AWS_CONTROL_PLANE_TABLE" --consistent-read \
     --key "{\"pk\":{\"S\":\"$pk\"},\"sk\":{\"S\":\"DISPATCH#ACTIVE\"}}" \
     --projection-expression payload --query Item.payload.S --output text \
     | jq '{generation, state, lease_until, execution_ref, terminal_outcome}'
   ```

   O item continua lá depois do terminal. O dispatch só está ativo com `state` diferente de
   `TERMINAL` e `lease_until` no futuro.
3. Com `execution_ref`, ler o status da execução Standard:

   ```bash
   aws stepfunctions describe-execution --execution-arn "$EXECUTION_REF" \
     --query '{status: status, stopDate: stopDate}'
   ```

4. Execução `RUNNING` com lease no futuro: nada a fazer. Execução terminal, dispatch
   `RESERVED` sem `execution_ref` ou lease vencido: invocar **uma vez** a task definition
   agendada do `recover-once`, com a mesma imagem, role, rede e comando. A forma exata da
   invocação é do deployment.
5. Conferir `processor_recovery_completed` e reler o dispatch. Generation+1 com outro
   `dispatch_id` indica redispatch. Um exit 1 isolado pode ser corrida CAS benigna; julgar
   por `failed` em passadas seguidas.

Nunca editar nem apagar item do DynamoDB, iniciar ou fazer redrive de uma execução nomeada à
mão, ou parar uma execução para destravar o run. O CAS do dispatch e o fencing das unidades
são o que impede replay de execução terminal, e a edição manual os contorna.

## Rollback

- Rollback da aplicação é voltar a imagem e a revisão anterior da task definition da API e do
  processor. A imagem antiga passa pelo mesmo `validate_state_machine`: a state machine precisa
  continuar compatível com o `AWS_PROCESSOR_CONTAINER_NAME` e o literal `LEASE_SECONDS`
  daquela revisão, senão o boot falha fechado.
- Não há rollback de dados. O pointer do dataset só muda por CAS, o audit é imutável pela
  retenção `COMPLIANCE`, e claims e dispatches de generation antiga são recusados pelo
  fencing. Esperado, mas não coberto por este gate: runs em voo seguem pela próxima passada do
  `recover-once` na imagem restaurada.
- O caminho legado (Postgres, `S3PresignedStorage`, RLS) segue nos mesmos binários até o
  MIG-014 (#280). Voltar de profile é decisão de deploy, fora deste runbook.

## Fora do escopo deste gate

- O plano de runtime-amendments troca o serving 307 por um envelope 200 (Task 5) e exige
  `AssignPublicIp=ENABLED` com subnets públicas exatas em produção (Task 6). Hoje o validador
  exige `DISABLED`. O mesmo plano introduz `dispatch-outbox-once`, o deadline no PID 1 e o
  runbook `runtime-acceptance.md`, que ainda não existem.
- Tabela, buckets, state machine, roles, KMS, rede, agendador e alarmes: plano de
  infraestrutura de 2026-08-31.
