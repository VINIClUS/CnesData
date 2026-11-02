# Gate de aceite da infraestrutura (PROD-INFRA-012)

Gate não-mutante das tasks PROD-INFRA-001 a 011 (#378 a #388) mais o invariante S3 da #441.
Nada aqui aplica, destrói ou lê state; nenhuma credencial AWS entra no gate.

## Comando único

Da raiz do repositório, com `tofu`, `conftest` 0.71.1 e `trivy` 0.74.0 no `PATH`:

```bash
uv run python -I tests/production/infra/bind_processing_sources.py ~/.cache/bindings.json
CI=true PROCESSING_SOURCE_BINDINGS=~/.cache/bindings.json uv run pytest -q tests/production/infra
```

`CI=true` troca skip por falha quando falta ferramenta. O resultado válido é `0 failed`,
`0 skipped`, `0 error`. Rode no SHA exato de `develop` pós-merge e cole a saída na #389.
Uma execução por vez por checkout: a suíte usa `TF_DATA_DIR` próprio e grava em `.terraform/`.

## Lista expandida

O pytest acima já executa os itens 4 e 5. Para depurar um por vez:

1. `tofu -chdir=infra/opentofu fmt -check -recursive`
2. `tofu -chdir=infra/opentofu/env/prod init -backend=false -input=false -lockfile=readonly`
3. `tofu -chdir=infra/opentofu/env/prod validate -no-color`
4. `tofu test` do `production.tftest.hcl`, com `mock_provider` e sem credencial:

   ```bash
   tofu -chdir=infra/opentofu/env/prod init -backend=false -input=false \
     -lockfile=readonly -test-directory=../../tests
   tofu -chdir=infra/opentofu/env/prod test -test-directory=../../tests
   ```

5. `conftest test tests/fixtures/plans --policy policies --ignore rejected` (sai 0) e cada
   fixture de `rejected/` com a mensagem exata, que o pytest confere em
   `test_fixtures_de_rejeicao_falham_a_politica`.
6. `CI=true uv run pytest -q tests/production/infra`
7. `git diff --check`

### Desvios dos comandos da issue

- `tofu -chdir=infra/opentofu test` falha: `infra/opentofu` não tem `.tf`, e o init de diretório
  vazio não instala os módulos do teste. O equivalente é o item 4, que roda contra `env/prod`.
- `conftest test tests/fixtures/plans --policy policies` sem `--ignore rejected` sai 1 sempre,
  porque as fixtures de rejeição estão sob o mesmo caminho. O item 5 separa as duas metades.
- `-lockfile=readonly` evita que o init reescreva `env/prod/.terraform.lock.hcl`.

## Job de CI

Job `opentofu-native` do workflow `infra-native` (runner hospedado, `contents: read`, sem
secrets, sem apply). Roda a suíte acima e valida o JUnit; `opentofu-floor` roda fmt, init e
validate no OpenTofu 1.10.0. O gatilho de `pull_request` não filtra caminhos: um step de
detecção, só com `git`, compara `HEAD^1` com `HEAD` do merge commit (`--no-renames`, para que
deleções e o caminho antigo de uma renomeação também contem) e pula os demais steps quando
nada de infra mudou. Assim o job pode ser check obrigatório sem ficar "Expected". O
ruleset de `develop` é decisão separada.

## Antes de qualquer apply (pré-apply)

A raiz recebe a revisão dos schedules por variável (`scheduler_recovery_task_definition_arn` e
`scheduler_audit_task_definition_arn`). Essa revisão não é imposta no plan mockado; regra rego
sobre plan real (ARN do schedule entre os ARNs de task definition do processing em
`prior_state`/`planned_values`) fica como follow-up. Até lá, conferir à mão:

1. Ler a saída `processor_task_definition_arns` do state aplicado.
2. Confirmar que as duas variáveis são exatamente as revisões `recovery` e `audit` dessa saída.
3. Registrar a conferência (sem ARN completo) na evidência do apply.

## Capability checks em AWS real

Cada check exige aprovação explícita do dono, retenção mínima, evidência redigida e limpeza com
inventário final vazio. Formato comum de evidência: status, códigos de erro, contagens e
booleanos; conta, ARNs, IDs, domínios e tokens trocados por `<conta>`, `<arn>`, `<id>`. Nunca
registrar access key, secret, session token, senha, JWT, URL assinada nem o conteúdo de state.

### Executados pela #398 (não reexecutar)

Procedimento e matriz de divergências em `docs/runbooks/floci-and-aws-capability.md`.

| Check | Cobertura |
|---|---|
| Cognito (resource server, escopos, PKCE) | perfis `cognito-*` da #398 |
| DynamoDB: condição, TTL e teto 100/25 das transações | perfis `ddb-*` da #398 |
| S3 Object Lock GOVERNANCE | perfis `s3-hold` e `s3-retention` |
| IAM/STS: matrizes de role e probes | perfil `s3-retention` e suíte Floci |

### Só documentados (nenhum executado)

| Check | Procedimento |
|---|---|
| Object Lock COMPLIANCE do audit | `docs/runbooks/object-lock-capability.md` |
| Aceite e revogação do Roles Anywhere | `docs/runbooks/roles-anywhere-evidence.md` |
| CloudFront Free ACTIVE | `docs/runbooks/cloudfront-free.md` |
| IAM de serviço do Step Functions | seção abaixo |
| Digest exato no ECR | seção abaixo |
| `TriggeredRules` de regra sem target | seção abaixo |
| 404 de chave ausente com as três roles reais (#441) | seção abaixo |

#### IAM de serviço do Step Functions

- Aprovação: dono, depois do primeiro apply da role `step-functions`.
- Retenção mínima: uma execução da state machine de teste; apagar a execução ao fim.
- Procedimento: `aws stepfunctions start-execution` com entrada mínima válida, e conferir que
  `ecs:RunTask` só tem sucesso na revisão ativa e que `iam:PassRole` recusa outra role.
- Evidência: status da execução e códigos de erro das negações, sem ARNs.
- Limpeza: `aws stepfunctions stop-execution` se houver execução aberta; sem recurso novo.

#### Digest exato no ECR

- Aprovação: dono, com a imagem já publicada por digest.
- Retenção mínima: nenhuma imagem extra; só leitura.
- Procedimento: `aws ecr describe-images --repository-name <repo> --image-ids imageDigest=<d>`
  e comparar com `image_digest` informado ao plan; a task definition registrada deve citar
  `@sha256:` com o mesmo valor, e a tag mutável deve ser recusada pelo repositório.
- Evidência: igualdade booleana de digests e `imageTagMutability=IMMUTABLE`, sem URI completa.
- Limpeza: nada a limpar.

#### `TriggeredRules` de regra sem target

As regras `task_outcome` dos schedulers não têm `aws_cloudwatch_event_target` (o gate fixa a
ausência no plan). Falta provar na AWS que a métrica `TriggeredRules` é emitida assim.

- Aprovação: dono, depois do apply dos schedulers.
- Retenção mínima: um evento de teste; sem recurso novo.
- Procedimento: `aws events put-events` com um evento que case a regra de falha, e
  `aws cloudwatch get-metric-statistics --metric-name TriggeredRules` na janela seguinte.
- Evidência: contagem da métrica (zero ou maior que zero), sem nomes de regra reais.
- Limpeza: nada. Se a métrica não aparecer, abrir issue para adicionar target mínimo.

#### 404 de chave ausente (#441, critério 3)

`GetObject` de chave ausente devolve 404 só se a role tem `s3:ListBucket` aplicável; sem isso,
403. A sonda com role na forma da matriz está em
https://github.com/VINIClUS/CnesData/issues/441#issuecomment-6559506298. A prova com as três
roles reais (unit, recovery e API) depende de apply.

- Aprovação: dono, depois do apply das roles.
- Retenção mínima: nenhum objeto criado; só `get-object` de chave inexistente.
- Procedimento: assumir cada role com sessão temporária e pedir uma chave inexistente sob
  `reconciliation/` ou `serving/`.
- Evidência: código HTTP por role (`404` esperado), sem ARN nem chave.
- Limpeza: encerrar as sessões; nada a apagar.
