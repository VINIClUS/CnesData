# Amendment de contrato Phase A — entrega de produção

**Status:** contrato documental aprovado pelo controller em 2026-11-07.
**Base:** `develop@db4ebd7883de3fcd352bb01c6ade21304cabc5a1` (verde, imutável para o
despacho desta alteração). **Escopo:** contrato e auditoria documental; não executa
plan/apply, AWS, host, DNS, promoção, assinatura, canary ou mudança de código.

Este amendment prevalece, nos pontos abaixo, sobre redação anterior conflitante nos
amendments #384/#385, no plano delivery e no design de operações. Resultados históricos,
aceites e bases antigas continuam registrados e não são reescritos. A sequência governante
é: procedência e promoção de bytes → Phase A → evidência pré-assinatura → assinatura EXT-2
→ canary/ativação. A assinatura criptográfica ocorre somente depois da Phase A.

## 1. Limites, ownership e precedência

- #469 consome o contrato versionado offline e é o proprietário futuro dos novos inputs e
  wiring de raiz canary; não depende de merge futuro de #468, workflow fictício ou segunda
  cópia de schema. #468 é proprietário do schema único
  `release/plan-apply-evidence.schema.json`, workflows plan/apply e produtor do envelope;
  apenas valida e transporta os inputs de #469. Quando #468 integrar, #469 confere
  compatibilidade e valida os campos consumidos antes de Produção.
- Novos paths de #468 e alterações nas issues #468/#469/#384/#470/#471/#472/#477 são
  publicados pelo controller depois da integração e auditoria deste amendment. Este PR não
  fecha nem autoclose nenhuma issue e não marca checkbox de Produção.
- Não há replan/apply no merge, tag mutável, branch consumidora empilhada, alteração de
  trust/rota ad hoc, registro ECS fora do plan ou `fargate-write` nesta entrega.
- A1–A5 e D1-A abaixo são o contrato fechado. O caminho real usa os gates e matrizes
  existentes; nenhum booleano de propagação substitui evidência.

## 2. A1 — procedência, integridade e assinatura em momentos distintos

Antes da promoção, conferir procedência autenticada no GitHub e integridade de todos os
insumos: repositório e SHA contido em `develop`, checks e run/attempt exigidos, candidato e
sidecar, IDs/digests/expiração dos artifacts, vínculos de dashboard/config/locks/fontes
ECR/SBOM e bytes OCI. O receipt deve identificar os metadados autenticados e os artifacts
efetivamente conferidos; fixture offline é teste, nunca receipt de produção.

Antes do apply, confirmar identidade, validade, políticas, custo, backend, drift e lease do
plan exato. Conferir manifest OCI, config digest, `linux/amd64`, media types e blobs sem
recompressão; ECR deve ser a conta/região governadas, repositório esperado, IMMUTABLE, e
`describe-images` deve igualar o digest do candidato e do plan. Tag não é autoridade.
Falha, expiração ou mistura de source/run/attempt fecha o gate.

Checksums não são assinatura. EXT-2 produz a única assinatura criptográfica da activation
manifest depois da Phase A; ela liga candidato, release/SHA, digest ECR, revisões exatas e
evidência Phase A. Não criar assinatura candidata nem inferir KMS pré-Phase A. Severidade
de segurança referencia resultado e revisão da mesma release, com identidade/digest da
evidência; `vulnerabilities_reviewed=true` sem origem é inválido.

## 3. A2 — identidade canary curta e vínculo completo

`release_id` R2 permanece byte-idêntico. O delivery também carrega `canary_id`, derivado
deterministicamente da candidata já verificada:

1. `candidate_sha256` é SHA-256 dos bytes canônicos do candidato.
2. Objeto UTF-8 tem exatamente as chaves string ordenadas `repository`, `release_id`,
   `source_sha`, `candidate_sha256`; JSON usa separators comma/colon, sem espaço ou newline,
   sem normalizar valores e sem números/NaN.
3. `binding_sha256 = sha256(b"cnesdata:prod:canary:v1\\0" + bytes_objeto)`; `\\0` denota
   um byte NUL. `canary_id` é os primeiros 32 hex minúsculos do binding.

Somente `runtime_iam.canary_release.release_id` recebe o `canary_id`; o principal confiável
e os ARNs de tabela, índice e bucket são campos distintos e exatos. Sessões, eventos e
object IDs derivam do mesmo `canary_id`, vinculados a esses campos. Evidência e activation preservam
`release_id`, `canary_id`, `binding_sha256`, versão de derivação e `candidate_sha256`.
Cada verificador recompõe o vínculo; ID curto independente é rejeitado. Uma `release_id`
fica vinculada a um único candidato/binding depois do primeiro prepare. Divergência em retry,
colisão ou material residual sem vínculo completo exige abort/recovery; não usar contador,
horário, novo salt ou reescrever evidência. O truncamento de 128 bits exige esse fail-closed;
não é alegação de equivalência ao SHA-256 completo.

Vetor determinístico de prova, não release final (a entrada é histórica e não autoriza cloud):

```json
{
  "derivation": "cnesdata:prod:canary:v1",
  "input": {
    "repository": "VINIClUS/CnesData",
    "release_id": "candidate-db4ebd7883de3fcd352bb01c6ade21304cabc5a1-42416377192-1",
    "source_sha": "db4ebd7883de3fcd352bb01c6ade21304cabc5a1",
    "candidate_sha256": "9a509eb987f01ba56d2f32066f41161ff295e459a71ca56c5c0d5e2327bb43c7"
  },
  "binding_sha256": "d155e7af91747d96917453791d3c21251f96edfd545d7c61bf49577d8e2d3b44",
  "canary_id": "d155e7af91747d96917453791d3c2125",
  "note": "Proof vector only; not the final production release."
}
```

#470 assina os mesmos campos; #471 deriva session/event/object identity do `canary_id`;
#472 mantém roles fixas, sem novos nomes por release.

## 4. A3 — envelope plan/apply único, propriedade #468

### 4.1 Envelope, enum e package

O envelope tem `schema_version=1`, UTF-8 canônico, datas UTC, SHAs minúsculos,
`additionalProperties=false` e limite de 262144 bytes. `status` é enum fechado:
`planned` ou `applied`; planned exige `apply=null`, applied repete o mesmo objeto `plan`
lógico, liga `plan_binding_sha256` e acrescenta receipt `apply`.

`plan.purpose` é enum fechado: `acm-certificate`, `bootstrap` ou `phase-a`. Não aceitar
novo purpose sem amendment consumidor. Há no máximo 32 `plan.dependencies`, 32 arquivos
de conteúdo no package, 128 revisões retained e 64 fingerprints de recursos/grants/routes;
listas são ordenadas e sem duplicatas.

`plan.package` lista somente arquivos de conteúdo com `name`, `size` e `sha256`, incluindo
binário e plan JSON, sem o envelope, arquivo extra, duplicata, link ou traversal.
`plan.package.aggregate_sha256` é o SHA-256 da lista canônica ordenada por `name`, não do
ZIP, envelope ou state. O planned artifact nunca é reescrito.

### 4.2 Campos fechados do plan e do apply

`plan` exige `repository`, `source_sha`, `producer.workflow_path`,
`producer.workflow_sha`, `producer.run_id`, `producer.run_attempt`, `package`, `purpose`,
`created_at`, `expires_at`, `binary_sha256`, `json_sha256`, `inputs_sha256`,
`dependencies`, `toolchain`, `backend`, `state_before`, `policy`, `cost`, `drift`,
`release` e `phase_a_inputs`. `dependencies` usa refs/digests imutáveis, nunca `latest`;
`toolchain` registra OpenTofu, selections de providers e hash do lock; `backend` registra
`identity_sha256` e configuração não secreta exata protegida.

`plan.created_at` e `plan.expires_at` são UTC; a validade não ultrapassa o insumo que
expira primeiro. `plan.policy`, `plan.cost` e `plan.drift` ligam verdict e digest de
evidência real, com pass obrigatório. `plan.release` é null em ACM/bootstrap; em phase-a
contém exatamente `release_id`, `source_sha`, `candidate_sha256`, `canary_id`,
`binding_sha256` e `image_digest`.

O `apply` exige `producer.workflow_path`, `producer.workflow_sha`, `producer.run_id`,
`producer.run_attempt`, `consumed_plan_artifact.artifact_id`,
`consumed_plan_artifact.artifact_sha256`, `consumed_plan_artifact.run_id`,
`consumed_plan_artifact.run_attempt`, `plan_binding_sha256`, `started_at`, `completed_at`,
`result`, `lease`, `principal`, `state_after`, `drift` e `outputs`. `result` só pode ser
`success` em receipt aceito; falhas ficam como diagnóstico, nunca applied.

`apply.started_at >= plan.created_at`, `apply.completed_at > apply.started_at`, ambos
ficam dentro da validade do plan com a margem exigida e não ultrapassam o insumo que expira
primeiro. `apply.lease` liga owner/run identity e prova CAS/renovação/liberação; `principal`
liga identidade temporária aprovada, conta/região e gate. `apply.drift` exige `clean` e
digest de readback pós-apply; não gera nem aplica novo plan.

O artifact planned e o applied receipt têm identidade externa própria autenticada pela
GitHub/Actions API. O digest externo é conferido separadamente; nenhum artifact contém a
própria identidade e não existe hash circular ou envelope concorrente.

### 4.3 Entrada Phase A e handoff

Fora de `purpose=phase-a`, `plan.phase_a_inputs=null`. Em phase-a, o objeto fechado é:

- `image_digest`: digest ECR promovido e conferido;
- `processor_active_unit_task_definition_arn`: ARN anterior ou null no bootstrap;
- `processor_retained_unit_task_definition_arns`: conjunto anterior, ordenado, único,
  validado e limitado a 128 ARNs;
- `canary_storage_enabled`: booleano true;
- `canary_release_id`: o `canary_id` derivado no §3;
- `canary_trusted_principal_arn`: ARN do principal canary aprovado no mesmo contexto.

O artifact de inputs é produzido por #469 e #468 apenas verifica e transporta origem,
digest e validade antes de OIDC. O transporte exige `phase_inputs_run_id`,
`phase_inputs_artifact_id`, `phase_inputs_artifact_digest` e `expected_source_sha`.
Não aceita tfvars arbitrário, URL ou override de backend, provider, target, parallelism,
destroy ou lock; as demais raízes vêm do handoff `shared_outputs v2`.

O wiring futuro de #469 não usa ARN literal: `canary_id` →
`runtime_iam.canary_release.release_id`; principal →
`runtime_iam.canary_release.trusted_principal_arn`; outputs de storage processing →
`runtime_iam.canary_release.table_arn/index_arn/bucket_arn`.
`processing.canary_storage_enabled = var.canary_storage_enabled`, nova var de raiz de #469.
`processing.canary_verifier_role_id = module.runtime_iam.canary_verifier_role_id`.
Bootstrap mantém release/principal/ARNs/role canary nulos e storage desabilitado; não há
`depends_on` global entre módulos.

### 4.4 Estado, outputs e hash

`plan.state_before` contém `lineage`, `serial` e `state_sha256`; `apply.state_after` contém
`lineage`, `serial`, `state_sha256` e `outputs_sha256`.
No bootstrap sem state existente, os três valores de `state_before` são null juntos.
`state_sha256` é SHA-256 da serialização canônica UTF-8 do JSON integral obtido do backend
exato sob lease, incluindo secrets/outputs no cálculo; parser rejeita duplicatas/NaN e o
plaintext nunca entra em artifact público. Mudança de state, lineage, drift ou vínculo
pre/post falha fechada.

Para Phase A, `apply.outputs` é objeto fechado:

- `processor_task_definition_arns`: objeto exato com `unit`, `recovery` e `audit`;
- `image_digest`: digest aplicado;
- `active_before`: ARN anterior ou null;
- `retained_before`: lista única, ordenada, de ARNs anteriores, máximo 128;
- `retained_after`: lista única, ordenada, pós-apply, máximo 128;
- `canary_id` e `binding_sha256`: identidade curta e vínculo completo do §3;
- `resource_fingerprints`, `grant_fingerprints` e `route_fingerprints`: listas de no
  máximo 64 objetos `{address, fingerprint_sha256}`, únicas e ordenadas por `address`.

Cada fingerprint é SHA-256 da representação canônica do endereço e campos observados;
endereços, listas, mapa de outputs e fingerprints são allowlisted e determinísticos.
`apply.state_after.outputs_sha256` é SHA-256 canônico de `apply.outputs` completo,
excluindo o próprio `outputs_sha256`; o hash não é autorreferente. Phase A exige essa forma
fechada e não amplia outputs Terraform sem necessidade concreta.

## 5. A4 — sequência, retained, retry e evidência IAM

No bootstrap, `active=null`, `retained_before=[]`, `image_digest` pode iniciar null e não
há state machine, Scheduler ou `states:*`; Phase A registra as três revisões quando recebe
digest, mantém active null e produz `retained_after` com as revisões reais. Em promoção,
active A e retained anterior são preservados; B é unido e ordenado em
`retained_after=unique(retained_before ∪ {B})`. Active não vira B na Phase A. `skip_destroy=true`
e `track_latest=false` permanecem; antigas não são apagadas/deregistradas. Phase B/C recebem
o conjunto real e não fazem apply extra apenas para preenchê-lo.

Retry é identificado por candidato/release/digest/`plan_binding_sha256`. Se o apply já
concluiu, apenas ler e verificar. Sem efeito confirmado, aplicar somente plan ainda válido
com state prévio exato. Efeito AWS sem receipt/state, timeout ambíguo ou mais de uma revisão
compatível produz `recovery_required`: parar antes de novo registro, sem import, replan ou
registro automático; recuperação é humana e auditada.

Antes da assinatura, D1-A exige duas leituras completas concordantes, separadas por pelo
menos 10 s, dentro de prazo global de 300 s por tentativa (relógio monotônico, intervalo
inicial 2 s, backoff até 10 s e jitter limitado). O resultado tipado é
`proof_kind="iam-readback-and-evaluation"`: readbacks de policy/trust/PassRole/roles e
avaliação de autorização por principal com contexto exato, incluindo positivos e negativos
da matriz. Isso demonstra estabilidade observada do contrato IAM; não é prova de execução,
propagação end-to-end ou runtime, e não chama simulação como substituto de propagação real.
Timeout, divergência, trust incorreta ou fingerprint diferente falham fechados.

Execução real é obrigatória nos canaries e nos gates das Phases B/C e smoke operacional,
antes do aceite de Produção. O canary real ocorre depois da activation assinada. Não criar
state machine temporária, ampliar trust, trocar principal ou mover rota para obter uma
“prova” antecipada. A matriz preserva: SFN só candidata unit ∪ retained com PassRole ECS
exato; verifier só audit candidate no cluster exato; Scheduler recovery/audit apenas
revisões anteriores; seeder/fence sem widening. Rede (cluster, subnets, SG, public IP e
platform) é conferida separadamente, pois IAM não a impõe.

## 6. A5 — delta exato e invariantes

Allowlist exclusiva da Phase A:

- task definitions `module.processing.aws_ecs_task_definition.processor[unit|recovery|audit]`;
- policy SFN `module.processing.aws_iam_role_policy.step_functions_ecs[0]`, somente RunTask
  candidate unit ∪ retained, preservando Describe/Stop e demais grants;
- roles/policies canary de task, execution, seeder, verifier e promotion-fence, somente
  criação ou vínculo ReleaseId/trust/sessão governados;
- policy `canary_verifier_run_task[0]` para audit candidate, cluster e verifier exatos;
- tabela DynamoDB e bucket/ownership/versioning/encryption/object-lock/lifecycle/policy S3
  canary existentes, apenas criação/configuração inicial protegida e sem reduzir retenção.

Em replacement ECS, delete só é aceitável com `skip_destroy=true` conferido e revisão antiga
preservada; `track_latest=false` também é obrigatório. Qualquer endereço managed fora da lista
é no-op. São invariantes: zero mudança em ASL/state machine/active/Schedulers/routes,
cluster/network, roles runtime/API/SFN de serviço, ECR, backend/provider/locks, grants
`states:*` de bootstrap e campos funcionais ECS além do digest. Futuros inputs de raiz
canary são propriedade de #469; seus campos são mapeados no §4 e
validados/transportados por #468. ARNs nunca são literais. Defaults mantêm bootstrap nulo
e storage isolado; não criar `depends_on` global nem novo oráculo IAM.

## 7. Compatibilidade e auditoria

O contrato fecha a distinção entre Código e Produção: #469 pode validar campos e vetores
offline no SHA base, mas nenhum fixture ou receipt sintético é aceito como produção. A
integração real depende de #468 auditada/default, #472, EXT-1/5/6, criação de candidato
final no SHA completo e readbacks/execuções reais correspondentes. Auditoria independente,
sem esta cadeira de implementação, revisará o amendment e o diff antes do despacho.

Referências de precedência:
[#384 processor amendment](2026-10-31-processor-provisioning-amendment.md),
[#385 IAM amendment](2026-10-31-step-functions-iam-amendment.md),
[closure #385](2026-11-01-step-functions-iam-closure.md),
[operations design](2026-08-29-cnesdata-production-operations-design.md).
