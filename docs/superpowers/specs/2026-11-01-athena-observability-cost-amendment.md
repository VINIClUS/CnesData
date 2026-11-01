# Amendment de escopo #388 — Athena, alarmes, política OPA e custo

**Status:** amendment da issue #388 (PROD-INFRA-011, Task 11 do plano de infraestrutura e Task 14
do plano de backlog). Não executa apply, plan com provider real, deploy ou chamada AWS.
O Budget e a ação de freeze continuam com ownership externo (`personal-infra-live`).

**Data:** 2026-11-01
**Base:** `origin/develop@a1c18f2a` (#443). A #387 (canary/fence) ainda não está em `develop`;
esta issue integra depois dela e registra como limite o que não existir no plan.

## Paths adicionais

Aprovados pelo coordenador em 2026-11-01, além da lista da issue.

- `infra/opentofu/env/prod/versions.tf` e `.terraform.lock.hcl`: provider `hashicorp/archive`,
  para zipar `cutoff_handler.py` com `data "archive_file"`. O lock só ganha a entrada nova,
  com as mesmas 15 plataformas da entrada aws; a entrada aws fica byte a byte igual
  (`-lockfile=readonly` no job nativo e no floor 1.10.0). O `output_path` do zip fica em
  `${path.root}/.terraform/`, nunca na árvore versionada.
- `infra/opentofu/modules/schedulers/{variables,main}.tf`: variável `alarm_actions` (lista,
  default `[]`) aplicada aos alarmes da #386. O plan isolado do módulo continua sem ações; a raiz
  passa o tópico de operador.
- `.github/workflows/infra-native.yml`: passo "Install conftest" no job `opentofu-native` (runner
  hospedado `ubuntu-latest`), versão e sha256 fixados, no padrão do passo do Trivy. Nunca em runner
  self-hosted. Os filtros de `pull_request` e `push` ganham `policies/**`, `schemas/**` e
  `tests/fixtures/**`.
- `docs/development.md`: conftest local e no CI.
- `tests/production/infra/_root_policy_inventory.py` (entrada da policy de cutoff), testes de
  composição/inventário da raiz que quebrarem por recursos novos, helpers `_athena_*.py` e
  `_cost_manifest_model.py` e os `test_*.py` novos. Nenhum arquivo existente perto de 500 linhas,
  `conftest.py` nem `_processing_native.py` é estendido.
- `infra/opentofu/modules/observability/cost-manifest.json`: o manifesto fica no módulo e não em
  `env/prod/`, porque a raiz aplicada não deve conter dado de revisão e o módulo é path permitido.
  Nenhum recurso o lê; testes e rego o recomputam.
- `docs/runbooks/athena-operator.md`: runbook do operador (premissa k, reativação manual,
  precondição de ativação).
- Este amendment.

## Fatos AWS verificados (2026-11-01)

- PutMetricAlarm: período × avaliações ≤ 604.800 s; período < 1 h limita a 1 dia; no máximo 5
  ações de alarme. Alarme com período × avaliações > 1 dia é avaliado uma vez por hora
  (guia CloudWatch, página alarm-evaluation).
  Janela alinhada ao relógio só existe para hora, dia e semana ISO e reflete o dado após o período
  fechar; não há janela mensal. Usa-se janela deslizante.
- Métricas Athena só existem com `publish_cloudwatch_metrics_enabled = true`. `ProcessedBytes` é só
  DML e inclui queries `CANCELED`. Um alarme só com a dimensão `WorkGroup` nunca casa: três
  `metric_query` (`SUCCEEDED`, `FAILED`, `CANCELED`; `QueryType=DML`) somados com `FILL(x, 0)`.
- O limite agregado do workgroup não é nativo: alarme → SNS → Lambda → `UpdateWorkGroup`.
- DynamoDB publica `SystemErrors` só no par `TableName`+`Operation`; alarme só com `TableName`
  fica em `INSUFFICIENT_DATA`.
- Plano CloudFront Free cobre a web ACL, regras gerenciadas e requisições do WAF associado
  (guia CloudFront, página flat-rate-pricing-plan),
  com limite de 5 regras (o static-web usa 2). A conta não pode estar usando o AWS Free Tier para
  assinar o plano. Sem o plano ACTIVE, a web ACL custa USD 5 + USD 1 por regra por mês (Price
  List, SKUs `2R9CM7WJ2KQ6AWDQ` e `D2BYWPHKSXKTQSXA`): USD 7, que estouraria o envelope. O
  gate de cutover da spec (binding ACTIVE) é o que protege; o manifesto registra a exposição.

## Cutoff mensal: derivação de W_d

Histórico: janela de 7 dias (W = 16 GB, avaliação horária), depois 1 dia com W_d = 2,5 GB, que
estourava (102,5 GB) por não contar a query que cruza o limiar; W_d = 2 GB fecha em 87 GB.

Na doc AWS (alarm-evaluation), alarme com período × avaliações > 1 dia é avaliado a cada hora;
até 1 dia é avaliado a cada minuto. O alarme usa as 3 métricas DML (`SUCCEEDED`, `FAILED`,
`CANCELED`), `period = 86400` em cada `metric_query`, `evaluation_periods = 1` e janela deslizante.

Prova: 31 janelas disjuntas de 1 dia cobrem qualquer mês de 31 dias. Se nenhuma janela alcança
W_d, o mês soma menos que 31·W_d. Se uma alcança, o disable trava (reativação manual), de modo que
o mês soma menos que 30·W_d (janelas anteriores) + W_d + sobra. Logo
mês ≤ 31·W_d + (k+1)·cutoff = 31·2 + 5·5 = 62 + 25 = 87 GB ≤ 100 GB. O +1 é a query que
cruza o limiar: ela conclui, publica a métrica e só então o alarme dispara; as k seguintes são as
que completam durante a latência ou estão em voo no disable.

- Decimais: W_d = 2.000.000.000 bytes (2 GB; com 2,5 GB a soma daria 102,5 GB); cutoff por query
  5.000.000.000 (menor que 5 GiB, então conservador sob as duas leituras de "5 GB"); limite
  100.000.000.000.
- Latência de detecção, teto `detection_latency_seconds = 300`: avaliação a cada 60 s, mais o atraso
  de publicação da métrica (que só sai na conclusão da query), mais SNS e Lambda.
- k = 4 é o número de queries que podem completar dentro dessa latência mais as que estiverem em
  voo no disable. Cada uma lê no máximo 5 GB porque `enforce_workgroup_configuration = true` força
  o cutoff por query. A AWS não oferece limite de concorrência por workgroup sem capacity
  reservation, que é paga e proibida pela política. Por isso k é um parâmetro operacional fixado no
  contrato (o operador roda queries em série), não uma garantia da AWS.
- Contrato imposto e testado: W_d, período, cutoff, k, `windows_per_month = 31`,
  `crossing_queries = 1`, `detection_latency_s` e o limite mensal são locals do módulo
  `athena`, saem no output
  `cutoff_contract` (raiz: `athena_cutoff_contract`). O teste lê threshold e period do recurso do
  alarme e o cutoff do recurso do workgroup, exige igualdade com o contrato, exige período ×
  avaliações ≤ 86400 e prova 31·W_d + (k+1)·cutoff ≤ 100e9 com valores lidos do plan. Mutações
  cobertas: W_d 2,5 GB, k 5, tirar o +1, período 172800, cutoff 6 GB e `evaluation_periods = 2`.
- **Desvio** do texto "100 GB/mês": o limite vira "teto de 87 GB por 31·W_d + (k+1)·cutoff com
  disable travado", não um contador de mês-calendário. Custo máximo do Athena ~USD 0,44 (USD 5/TB).

## Decisões

- **Workgroup** `cnesdata-prod-operator`: `enforce_workgroup_configuration = true`, publicação de
  métricas ligada, `requester_pays` desligado, resultado em
  `s3://cnesdata-prod-athena-results/results/` com SSE-S3 e `expected_bucket_owner`.
  `lifecycle { ignore_changes = [state] }`, para que um apply qualquer não reative o workgroup
  desabilitado. É o único `ignore_changes` do repositório (a
  proibição da spec é sobre roteamento) e um teste varre a árvore.
- **Bucket de resultados**: PAB completo, `BucketOwnerEnforced`, política só-TLS, expiração de 7
  dias, abort-multipart de 1 dia, `force_destroy = false`. `AVD-AWS-0132` (AES256) com
  `#trivy:ignore:...:exp:` datado, como no repo.
- **Dois tópicos SNS**. `cnesdata-prod-athena-cutoff`: Publish só de `cloudwatch.amazonaws.com`
  com `aws:SourceArn` = ARN determinístico do alarme (`format()`) e de `budgets.amazonaws.com`
  com `aws:SourceArn` = `shared_outputs.budget_freeze.budget_arn`, ambos com `aws:SourceAccount`.
  O segundo é o gancho de freeze do Athena, porque não há role de operador Athena no spec; o
  handler desabilita em qualquer invocação, então qualquer notificação do Budget ligada ao
  tópico (ownership externo) desabilita o workgroup. O formato exato do `SourceArn` do Budgets
  não foi provado em AWS real. `cnesdata-prod-operator-alarms`: todos os outros alarmes, incluindo
  os da #386; Publish só do CloudWatch da conta com `ArnLike` `alarm:cnesdata-prod-*`.
  Assinatura humana do tópico de operador é handoff operacional; nenhum endereço no repo.
- **Sem KMS nos tópicos** (`AVD-AWS-0095` com ignore datado): o CloudWatch não publica em tópico
  cifrado com `aws/sns`, e a CMK compartilhada tem key policy externa não verificável aqui. O
  payload de alarme não tem segredo.
- **Lambda** `cnesdata-prod-athena-cutoff`, python3.13, `ATHENA_WORKGROUP` por env var (nunca do
  evento; ausente levanta `RuntimeError`), `update_work_group(WorkGroup=..., State="DISABLED")`,
  `ClientError` re-levantado (fail-closed; SNS async re-tenta). Sem reserved concurrency (contas
  novas podem ter quota 10). Role com uma policy inline: `athena:UpdateWorkGroup` em
  `workgroup/cnesdata-prod-operator`; trust `lambda.amazonaws.com` + `aws:SourceAccount`
  (`aws:SourceArn` do trust não incluído: comportamento não verificável sem apply). **Sem
  permissão de logs** (opção a): observabilidade por alarmes de `AWS/Lambda Errors` e
  `AWS/SNS NumberOfNotificationsFailed` no tópico de operador.
- **Freeze**. Alvos (`freeze_target_role_arns`): API compartilhada, task recovery, scheduler
  recovery, Step Functions. Excluídos (`freeze_excluded_role_arns`): task audit, scheduler audit,
  execution role (o audit precisa dela para subir), task unit e a role da Lambda de cutoff.
  A role unit não está no spec porque o novo custo é o `RunTask` da Step Functions, que já é
  alvo; a unit só roda dentro de tasks em voo. Teste: alvo ∪ excluído = todas as `aws_iam_role`
  do plan da raiz mais a API compartilhada; interseção vazia, então role nova não escapa da
  classificação. As roles de GitHub são compartilhadas; a role do fence operator da #387 nasce com
  `count = 0` e entra aqui depois.
- **OPA**: `policies/cnesdata.rego`, `package main`, para que `conftest test` sem `--namespace`
  funcione. Rejeita NAT, LB, RDS/Aurora, Redshift, OpenSearch, ElastiCache, réplica de tabela
  global, replicação S3, região ≠ us-east-2 (exceto ACM e WAF `CLOUDFRONT` em us-east-1),
  Container Insights, planos pagos e custo acima de 8 (produto) e 15 (agregado). Mensagens:
  `forbidden_resource reason=... type=... address=...`, `forbidden_region ...`,
  `cost_over_limit scope=... scenario=... total_usd=... limit_usd=...`.
  O comando da issue com fixtures de rejeição sai com código 1 por construção; o verificador é
  `conftest test tests/fixtures/plans --policy policies --ignore rejected` (aceitas) mais o teste
  pytest, que afirma as mensagens exatas das rejeitadas e zero falhas no plan vivo da raiz.
- **Manifesto**: IDs de linha, limites 8/15 e totais recomputados (nunca armazenados), preço
  unitário com URL da Price List e SKU, `min/base/max` por linha, cenário `max` sem franquia de
  free tier (a franquia é da conta, compartilhada com outros projetos). Quantidades amarradas ao
  plan: Fargate = `monthly_limit_hours` × vCPU/memória reais das task definitions; métricas de
  alarme contadas do plan; Athena = 31·W_d + (k+1)·cutoff. `unverified`: franquia do Cognito e
  gasto de outros projetos da conta, sem número. Schema próprio validado por um subconjunto de
  JSON Schema no teste (sem nova dependência).

## Alarmes: sinais reais e limitações

Alarmes da #388 (sem criar novo shipper): cutoff Athena; Step Functions failed/timed out/aborted
no ARN `arn:aws:states:us-east-2:<conta>:stateMachine:cnesdata-prod`; DynamoDB throttles do teto
on-demand (`Read/WriteMaxOnDemandThroughputThrottleEvents`, nível de tabela; a tabela é
on-demand com teto 100/25), `SystemErrors` (GetItem, Query, UpdateItem) e consumo sustentado
de escrita (80% do teto de 25 WRU/s por 1 h); falha de task unit e proxy semanal de tasks unit
(EventBridge `TriggeredRules`); erros da Lambda e falhas de entrega do SNS de cutoff; dois
metric filters sobre o log group
`/ecs/cnesdata-prod-processor`, com campos reais do `JsonLogFormatter` (`event` + `extra`):
`processor_execution_probe_failed`, `processor_entrypoint_failed`, `processor_recovery_completed`
(`failed > 0` ou `reconcile_failed`) e `dispatch_deferred ... reason=monthly_attempts_exhausted`
(contador de 200 tentativas da #373, que roda no Fargate via `DispatchAdmission`).

Limitações registradas:

- API/tunnel: sem shipper do VPS não há sinal no CloudWatch; fica só como referência.
- Negação S3: métricas de request do S3 são pagas e `4xxErrors` inclui o 404 esperado da #441; o
  código não loga 403 distinto. A cobertura indireta é `processor_entrypoint_failed`.
- 100 task-h: sem Container Insights só existe proxy por contagem de tasks unit (limiar 10 por 7
  dias, assumindo 5 min faturados por task); recovery e audit têm horas fixas pelo schedule. É só
  monitoramento.
- 200 tentativas: o alarme dispara quando o contador esgota, não antes.
- Throttles dos GSIs exigem a dimensão `GlobalSecondaryIndexName` por índice e não são cobertos.
- `SystemErrors` cobre três operações; PutItem, DeleteItem e transações não são cobertas.
- Cada métrica em alarme custa ~USD 0,10/mês; o conjunto soma 25 métricas de alarme (contado do
  plan) e 2 métricas customizadas.

## Edição autorizada de `_processing_native._provider_schema`

Com a entrada `hashicorp/archive` no lock da raiz, a raiz de ferramenta da suíte nativa (lock
somente leitura, `init -lockfile=readonly`, só `aws`) falhava com "Provider dependency changes
detected". O coordenador autorizou a edição mínima em `tests/production/infra/_processing_native.py`
(2026-11-01): a raiz de ferramenta exige todos os providers de `provider_selections` do artefato,
nas versões exatas; a checagem final compara a igualdade com as seleções da raiz; o schema aws e a
checagem byte a byte do lock ficam. Nada mais nesse arquivo muda. O lock ganha só a entrada
`archive` (mesmas 15 plataformas, zero remoções).

## Largura de linha em dados

Dado JSON e fixtures ficam isentos do limite de 100 colunas por causa das URLs da Price List
(`cost-manifest.json`, `tests/fixtures/plans/**`). Código, HCL e docs respeitam o limite.

## Gate de aceitação do conftest

`conftest test tests/fixtures/plans/accepted --policy policies` (ou a árvore inteira com
`--ignore rejected`) sai com 0. O comando literal da issue sobre `tests/fixtures/plans` inclui as
fixtures rejeitadas e sai com código diferente de 0 por construção; as rejeições são afirmadas pelo
pytest com as mensagens exatas. O CI instala conftest 0.71.1 com sha256 fixo, só no job hospedado.

## WAF: precondição bloqueia a ativação

A linha `waf_dedicated` vale 0 com `precondition: "cloudfront_free_plan_active"`, `blocking:
"activation"`, `exposure_usd: 7` e `evidence: "pending"`; `cloudfront_free_plan` também declara
`blocking: "activation"` e `evidence: "pending"`, porque o plano Free ACTIVE não está provado. O
schema aceita `blocking` em `activation` ou `advisory` e `evidence` só em `pending`. O rego rejeita
manifesto sem a linha `waf_dedicated` (`waf_line_missing`), linha WAF sem a precondição, a
exposição, `blocking: "activation"` ou `evidence: "pending"` (`waf_unbound_exposure`), linha com
`precondition` sem `blocking` (`precondition_without_blocking`) e linha cujo máximo do produto mais
`exposure_usd` passa de 8 sem `blocking: "activation"` (`exposure_requires_activation_block`); o
teste do manifesto repete as regras. A raiz publica `activation_blocking_preconditions =
["cloudfront_free_plan_active"]`, consumido pela ativação da delivery (#382). Limitação: o manifesto
não prova o plano ACTIVE; a ativação deve esperar a confirmação humana.

## Base da quantidade máxima por linha (`max_basis`)

Cada linha declara `max_basis`: `cap` quando o máximo é um teto imposto ou derivado do plan
(testes amarram aos valores do plan), ou `hypothesis` quando é uma estimativa documentada.
Hipóteses, todas de uso estimado sem teto imposto pelo código: `cloudwatch_logs_ingestion`,
`cloudwatch_logs_storage`, `lambda_requests`, `lambda_compute`, `sns_requests`,
`step_functions_transitions`, `dynamodb_read_units`, `dynamodb_write_units`,
`dynamodb_pitr_storage`, `dynamodb_storage`, `s3_data_storage`, `s3_audit_storage`,
`s3_athena_results_storage`, `s3_requests`, `cognito_allowance`, `data_transfer_out`,
`github_actions`. Compartilhadas, fora do controle do produto: `state_backend`, `budget_actions`,
`other_projects_spend`. O produto fecha em 2,54 (min), 3,91 (base) e 7,84 (max) e o agregado em
3,54, 4,91 e 11,95, sob 8 e 15; o máximo do produto fica perto do limite por causa das hipóteses,
não de tetos, e a margem é consumida primeiro por elas.

## Actions pagas: USD 0

A linha `github_actions` vale 0 em min, base e max: o repositório é público e a documentação de
billing do GitHub declara gratuitos os runners hospedados padrão em repositório público e os
self-hosted. A premissa é `hypothesis`: vale enquanto o repositório for público e nenhum workflow
usar label de runner maior. O teste da linha lê todo `runs-on:` de `.github/workflows` e só aceita
`ubuntu-latest`, `windows-latest` e listas que começam com `self-hosted`.
