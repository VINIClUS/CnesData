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

A entrega usa `infra-plan.yml` e `infra-apply.yml`, ambos somente `workflow_dispatch`
em `develop`. Nenhum merge aplica infraestrutura. O source informado deve ser o SHA
exato da execução, contido em develop, com os dez checks obrigatórios verdes no próprio
SHA. O registro dos dois YAMLs na default main é um PR mínimo posterior ao Código verde,
com bytes/pins idênticos, auditoria e CI de main; não é autorização para executar em main
nem para habilitar Deploy develop.

Os callers e a constante de produtor usam o pin EXT1 integrado/auditado/verde
`97f8e7e086e3f107401b5eb35ff4017dde21ff34`, que inclui P1/P2. A CI de main
42535291459 foi conferida e o controlador liberou esse pin para Código. Proposta/HEAD
não integrado não substitui release. Toda atualização de produtor exige pins coerentes
nos dois callers e no helper, conferência de contratos, CI e nova auditoria do HEAD.
A liberação do pin não concede Ready/Produção: registro em main, allowlists, identidades,
transporte privado e demais fatos operacionais continuam obrigatórios.

### Entradas e admissão

- Plan: `expected_source_sha`, `purpose`, `request_descriptor`,
  `request_descriptor_sha256`. Phase A exige também `phase_inputs_run_id`,
  `phase_inputs_artifact_id`, `phase_inputs_artifact_digest` autenticados da #469.
- Apply: os mesmos campos mais `plan_run_id`, `artifact_id`, `artifact_digest` do índice
  planned. Em Phase A, o transporte dos inputs deve coincidir com a referência original
  do proof planned; não pode substituir os inputs no apply.
- Purpose fechado: `acm-certificate`, `bootstrap`, `phase-a`. Não existem inputs de
  role, backend, target, URL, tfvars arbitrário, lock ou destroy.

O descriptor canônico contém repository/source, pins imutáveis, seis referências de
baseline (backend, cost, drift, policy, shared-outputs, toolchain) e payload específico.
Seu SHA-256 cobre os bytes UTF-8 canônicos, inclusive um único LF final. Referências
trazem repository, workflow path/SHA, run/attempt e artifact ID/digest separados.
Metadados reais e bytes externos são autenticados; Artifact REST não possui run_attempt.
Created_at deve ficar estritamente dentro do intervalo do attempt autenticado: bordas
inteiras ambíguas falham fechadas. Não escolher latest, confiar em nome ou em hash declarado.

O job admit não acessa AWS. O reusable fixado repete a admissão pública completa contra
allowlists/provas originais governadas antes de OIDC e compara os bytes privados depois
da credencial temporária, antes de qualquer mutação. São oito inputs e quatro outputs
`evidence_*`, sem secrets:inherit. Roles plan/deploy distintas vêm do fragmento de dois
ARNs github do shared_outputs v2; o fragmento não é um objeto v2 completo. Governance e
transporte privado pertencem aos produtores; os 43 fatos ausentes não viram exemplos ready.

### Plan, checkpoint e apply exatos

O único schema proprietário é `release/plan-apply-evidence.schema.json`. A3 planned e
applied são instâncias imutáveis distintas do mesmo modelo; applied repete o plan e
seu binding. Identidade externa do artifact e digest ZIP ficam fora do envelope. O
aggregate cobre a lista de arquivos de conteúdo, sem envelope/selfhash. Binário, JSON,
inputs gerados, fontes, dependências, lock/providers, backend, policy/cost/drift e validade
ficam vinculados. O índice GitHub é só transporte sanitizado; binário, state, ARNs e
reports integrais permanecem privados no canal governado, nunca em artifact público.

Antes do apply, o produtor exige scheduler recovery/audit resolvidos do binário iguais
às revisões recovery/audit de processor_task_definition_arns do state anterior. Não usar
o candidate pós-apply para antecipar rotas. Mapa parcial/unknown ou divergência falha;
ausência/{} só no estágio inicial comprovado com inputs null. Retry concluído apenas lê
e verifica, sem novo apply. O diagnóstico público registra resultado/commitments, sem ARN.

Capturar VersionId real, ETag comprometido, lineage, serial e hash do state sob lease;
comparar o checkpoint planned autenticado ao estado atual antes do exact apply e guardar
checkpoint pós-operação. Mesmos bytes não provam mesma versão. State realmente ausente
é distinto de metadata ausente em objeto existente. Restore por versão é operação manual
auditada do owner/backend, não privilégio novo do produto nem recuperação de dados de
aplicação. Dados dos protótipos são descartáveis; state compartilhado, proveniência,
identidade/isolamento e recuperação de infraestrutura continuam protegidos.

Lease S3 CAS e .tflock nativo são controles distintos; ambos permanecem ativos. Validade
máxima do plan: 3600s e menor expiração dos insumos. Admitir execução só com margem de
1980s; até 1800s para execução, 15s para término comprovado e 45s compartilhados para
finalização/readback, antes de expirar menos 120s. Falha/ambiguidade exige recovery revisado;
não replan, takeover por TTL, -lock=false, destroy ou restauração automática. O apply
executa exclusivamente o binário auditado e verifica state/drift/publicação ao final.

### Ordem inicial e fases posteriores

1. Par plan/apply acm-certificate: seleção interna fixa aws_acm_certificate.static_web
   apenas na geração do plan; apply nunca recebe target. Publicar o CNAME observado do
   apex pelo handoff privado/redigido. PENDING_VALIDATION não significa ISSUED.
2. #476 Produção 1 publica DNS pelo owner EXT4 e confirma ISSUED do MESMO certificado.
3. Par plan/apply bootstrap normal do restante da raiz: image/active/release/principal
   nulos, retained vazio, canary storage false e sem state machine/Schedulers. Exigir
   distribuição Deployed, vínculo WAF/certificado e invariantes observados. Não emitir
   novo bootstrap para resetar release estabelecida. Sem DNS ou waiter no grafo do produto.
4. #476 Produção 2 comprova assinatura CloudFront Free ACTIVE da distribuição/WAF exatas
   antes do corte. Só então remover o bloqueio de ativação em seu PR governado.
5. Phase A transporta a allowlist #469, sem publicar ECR/registrar ECS por fora do plan;
   #472 Código/freeze e demais gates continuam exigidos antes da operação real.

Custos brutos: produto <=USD8, agregado <USD15 (15 congela), Actions pagos USD0.
PRE só inicial e monotônico, com activation/DNS/workloads blocked comprovados e projeção
unbound CF/WAF/KMS/evidence/requests/versões. Phase A, pós-P2 e cutover exigem ACTIVE real,
sem créditos, Free Tier da conta, evidência de outro app ou fallback pago. Manifest e
report financeiro são separados e não incluem seus próprios hashes.

Inputs/artefatos/credenciais e recuperação devem estar prontos antes do fence. Cada plan
B/C é gerado contra o checkpoint REAL precedente e auditado antes de seu exact apply;
nenhum C stale/serial pré-calculado/replan no apply. Propósitos B/C exigem amendment
próprio, não reutilizam phase-a. A janela permanece close confirmado → reopen verificado
<=1800s; GC/rollback estático/CSP ficam fora dela. IAM D1-A é readback/evaluation, não
propagação ou execução real; canaries/saúde/isolamento e aceites de Produção permanecem.

Antes de qualquer operação são necessários EXT1/5/6 reais, catálogo/allowlists, backend/
versões/locks/restore, canal EXT2, OIDC/roles efetivas e demais handoffs. LimnoPulse é
aceito primeiro e recebe smoke antes/depois de mudança compartilhada. Código ou fixtures
não concedem Ready/Produção. certificate_domain_validated segue false até a validação;
ligá-lo depois aplica HSTS e pertence à #476. IAM demo #481 exige amendment próprio,
sem ampliar canary_seeder ou os inventários fechados 0 ACM/9 bootstrap/7 Phase A.

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

- Este item é capability check posterior, não o gate pré-assinatura da Phase A. O contrato
  [Phase A](../superpowers/specs/2026-11-07-phase-a-contract-amendment.md) exige antes da
  EXT-2 duas leituras concordantes de policy/trust/PassRole e avaliação tipada por principal,
  registradas como `proof_kind="iam-readback-and-evaluation"`. Isso não é simulação de
  propagação nem prova runtime.
- Aprovação: dono, depois do primeiro apply da role `step-functions` e somente quando o
  controller abrir o handoff real. A execução é real, com o principal/contexto governado,
  e não pode ser usada para antecipar assinatura, alterar trust/rota ou criar máquina.
  Este check posterior é acompanhado pelas issues operacionais #473 (dispatcher/host),
  #474 (capability IAM/runtime) e #476 (aceite de Produção).
- Retenção mínima: uma execução real da state machine de teste; apagar a execução ao fim.
- Procedimento: `aws stepfunctions start-execution` com entrada mínima válida, conferindo
  `ecs:RunTask` somente na revisão ativa e `iam:PassRole` recusando outra role. Usar o
  principal, state machine e revisão governados pelo handoff; não inventar trust, máquina,
  rota ou principal alternativo.
- Evidência: status da execução e códigos de erro das negações, sem ARNs, segredos ou state.
- Limpeza: `aws stepfunctions stop-execution` se houver execução aberta; sem recurso novo.
  Canaries e gates reais das Phases B/C permanecem obrigatórios antes do aceite de Produção.

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
