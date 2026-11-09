# IAM dedicado ao seed demo (#481)

O módulo `runtime-iam` cria uma role e uma inline policy quando `demo_seed` recebe
`tenant_partition_key` e `trusted_principal_arn`. O default `null` cria zero recursos demo
e devolve contrato nulo. A raiz prod encaminha input e output sem alterar `shared_outputs`.
A tabela vem exclusivamente do `runtime_grants.table_arn` já validado pelo módulo.

Exemplo **sintético**, sem identidade de produção:

```hcl
demo_seed = {
  tenant_partition_key  = "TENANT#64656d6f"
  trusted_principal_arn = "arn:aws:iam::123456789012:role/synthetic/demo-operator"
}
```

A PK é a saída canônica de `tenant_partition("demo")`: `TENANT#` seguido de hexadecimal
minúsculo de comprimento par e não vazio. Não aceita global, curingas, prefixos run/raw,
members nulos ou objetos parciais. O principal deve ser uma IAM role ARN ASCII exata da
mesma conta; root, usuário, sessão STS, OIDC, outra conta e curingas são recusados.
O nome `${name_prefix}-demo-seed` deve caber em 64 caracteres.

## Permissões e fronteiras

| Operação | Recurso / condição | Resultado |
|---|---|---|
| GetItem e PutItem | Tabela exata, todas as LeadingKeys iguais à PK demo | Permitido |
| Outra tabela, índice, canary ou outra PK | Fora da concessão | Sem Allow |
| Chave ausente, vazia ou conjunto misto | Null=false e ForAllValues:StringEquals | Sem Allow |
| Scan, Query, Update, Delete, batch, ConditionCheckItem, IAM | Ação ausente | Sem Allow |
| AssumeRole | Principal exato, sessão `demo-seed` | Trust dedicado |

O `Null` impede que a quantificação universal de `ForAllValues` permita um contexto vazio.
A policy concede GetItem/PutItem; transações DynamoDB são autorizadas pelas ações de cada
item (por exemplo, PutItem), sem inventar uma ação IAM de envelope TransactWriteItems.
Uma transação com item fora da PK demo não recebe a concessão desse item.

LeadingKeys restringe somente a PK. Qualquer SK e payload dentro dessa PK permanece
alcançável por GetItem/PutItem, inclusive itens que não sejam do seed. Não há garantia IAM
de tipo de entidade, atributos, condition expression, idempotência ou proteção contra
sobrescrita de um item existente na mesma PK. O consumer #476 deve validar tenant,
membership, dataset version/pointer, colisões, conteúdo e escrita condicional antes de
executar. O teste inclui quatro tipos de item como exemplos da mesma fronteira de PK.

O limite configurável da role é **3600 segundos**, mínimo permitido pela AWS. O consumer
#476 deve solicitar `DurationSeconds=900`, usar `RoleSessionName=demo-seed`, verificar a
expiração devolvida e descartar credenciais ao terminar. A role permite solicitação até
3600 segundos; este módulo não impõe um cap IAM de 900 segundos.

O trust exige principal e nome de sessão. Uma role que recebe concessões adicionais no
futuro exige nova revisão: o Allow desta policy não é um permissions boundary nem um
Deny global. As policies, trust, outputs e concessões do canary permanecem independentes.

## Contrato para #476

`demo_seed_iam_contract` tem exatamente estes nove campos:

| Campo | Conteúdo |
|---|---|
| contract_version | `cnesdata.demo-seed-iam/v1` |
| role_arn | ARN da role dedicada |
| policy_name | Nome da inline policy dedicada |
| policy_sha256 | SHA-256 do JSON de policy renderizado pelo módulo |
| table_arn | `runtime_grants.table_arn` |
| tenant_partition_key | PK canônica exata fornecida |
| role_session_name | `demo-seed` |
| max_session_duration_seconds | `3600` |
| allowed_actions | `dynamodb:GetItem`, `dynamodb:PutItem` |

Referências reais de tenant e principal serão entradas privadas da futura operação.
O hash identifica os bytes da policy deste módulo; não constitui readback da AWS.

## Verificação sem infraestrutura real

Seguir as ferramentas e bindings de [infrastructure-plan.md](infrastructure-plan.md):

```bash
uv run python -I tests/production/infra/bind_processing_sources.py /tmp/demo-bindings.json
CI=true PROCESSING_SOURCE_BINDINGS=/tmp/demo-bindings.json \
  uv run pytest -q tests/production/infra/test_demo_seed_iam.py \
  tests/production/infra/test_canary_fence_iam.py \
  tests/production/infra/test_root_canary_wiring.py \
  tests/production/infra/test_step_functions_iam.py
tofu -chdir=infra/opentofu fmt -check -recursive
```

Os testes usam provider mockado, `command=plan` e JSON nativo renderizado. A matriz nega
ações, recursos, tenants, contextos e sessões fora do contrato. Contraprovas removem Null
ou ampliam ação/recurso/PK/principal/sessão e devem ser detectadas pela matriz.
Os runs demo de `infra/opentofu/tests/production.tftest.hcl` verificam o encaminhamento
do contrato na raiz, com canary desativado; a regressão separada cobre canary ativo.

Nenhum plan mockado é candidato a apply. Código e mocks não provam propagação IAM,
STS real, login PKCE, isolamento efetivo nem aceite de Produção. Apply dedicado, revisão
da freeze allowlist, readbacks e seed real pertencem ao controller e à etapa de #476.
Sem operação cloud, state, DNS, CA ou credenciais nesta entrega.

Referências AWS:

- [LeadingKeys e condições DynamoDB](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/specifying-conditions.html)
- [ForAllValues e Null](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_condition_operators.html)
- [CreateRole MaxSessionDuration](https://docs.aws.amazon.com/IAM/latest/APIReference/API_CreateRole.html)
- [AssumeRole DurationSeconds](https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html)
