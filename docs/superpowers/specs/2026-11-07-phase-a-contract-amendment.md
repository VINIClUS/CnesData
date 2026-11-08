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

- #469 consome o contrato versionado offline; não depende de merge futuro de #468, de
  workflow fictício ou de uma segunda cópia de schema. #468 é o proprietário futuro do
  schema único `release/plan-apply-evidence.schema.json`, dos workflows plan/apply e do
  produtor do envelope. Quando #468 integrar, #469 confere compatibilidade e valida os
  campos efetivamente consumidos antes de Produção.
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

O input raiz `canary_release_id` e os campos existentes `runtime_iam.canary_release.*`,
sessões, eventos e object IDs usam o mesmo `canary_id`. Evidência e activation preservam
`release_id`, `canary_id`, `binding_sha256`, versão de derivação e `candidate_sha256`.
Cada verificador recompõe o vínculo; ID curto independente é rejeitado. Uma `release_id`
fica vinculada a um único candidato/binding depois do primeiro prepare. Divergência em retry,
colisão ou material residual sem vínculo completo exige abort/recovery; não usar contador,
horário, novo salt ou reescrever evidência. O truncamento de 128 bits exige esse fail-closed;
não é alegação de equivalência ao SHA-256 completo.

O vetor `phase-a-canary-vector.json` ao lado do draft é apenas vetor de derivação de prova,
não release final. #470 assina os mesmos campos; #471 deriva session/event/object identity
do `canary_id`; #472 mantém roles fixas, sem novos nomes por release.

## 4. A3 — envelope plan/apply único, propriedade #468

O contrato futuro tem `schema_version=1`, UTF-8 canônico, datas UTC, SHAs minúsculos,
`additionalProperties=false` e limite de 262144 bytes. `status=planned` contém `apply=null`;
`status=applied` repete o mesmo objeto `plan` lógico, liga seu
`plan_binding_sha256` e acrescenta receipt `apply`. O artifact planned não é reescrito;
planned e applied têm identidade externa própria autenticada pelo GitHub.

O package lista apenas arquivos de conteúdo `name/size/sha256`, ordenados, sem envelope,
duplicata, extra, link ou traversal. `aggregate_sha256` é o hash dessa lista canônica, não
ZIP, envelope ou state; não existe hash circular. O digest do artifact é conferido
separadamente via API/receipt autenticado.

Campos mínimos: `plan.repository`, `source_sha`, `producer` (workflow path/SHA, run_id,
run_attempt), package/aggregate, purpose, created/expires, binary/json/inputs SHA,
dependencies imutáveis, toolchain/lock, backend protegido, state_before, policy/cost/drift,
release e `phase_a_inputs`; e, no applied, producer, artifact consumido e digest, binding,
timestamps, result success, lease, principal, state_after, drift clean e outputs allowlisted.
Falhas de apply não são receipts applied.

`state_sha256` é SHA-256 da serialização canônica UTF-8 do JSON integral retornado pelo
backend exato sob lease, incluindo secrets/outputs no cálculo, sem persistir plaintext em
artifact público; parser rejeita duplicatas/NaN. Evidência guarda só digest, lineage/serial
e outputs permitidos. `outputs_sha256` cobre o mapa allowlisted completo. Mudança de state,
lineage, drift ou vínculo pre/post falha fechada.

Para `purpose=phase-a`, #469 produz artifact fechado com `image_digest`, active anterior
(inclusive null), retained anterior ordenado/deduplicado, `canary_storage_enabled=true`,
`canary_release_id=canary_id` e principal canary aprovado. O transporte exige
`phase_inputs_run_id`, `phase_inputs_artifact_id`, `phase_inputs_artifact_digest` e
`expected_source_sha`; não aceita tfvars arbitrário, override de backend/provider/target/
parallelism/destroy/lock ou URL. As demais raízes vêm do handoff shared_outputs v2.

Saídas Phase A são as três task-definition ARNs, digest, active/retained pre e post,
canary/binding e fingerprints de recursos/grants/routes lidos. Não ampliar outputs Terraform
sem necessidade concreta.

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
`states:*` de bootstrap e campos funcionais ECS além do digest. Raiz recebe somente
`canary_release_id`, `canary_trusted_principal_arn` e `canary_storage_enabled`; defaults
mantêm canary nulo/isolado. Não criar novo oráculo IAM.

## 7. Compatibilidade e auditoria

O contrato fecha a distinção entre Código e Produção: #469 pode validar campos e vetores
offline no SHA base, mas nenhum fixture ou receipt sintético é aceito como produção. A
integração real depende de #468 auditada/default, #472, EXT-1/5/6, criação de candidato
final no SHA completo e readbacks/execuções reais correspondentes. Auditoria independente,
sem esta cadeira de implementação, revisará o amendment e o diff antes do despacho.

Referências de precedência: [#384 processor amendment](2026-10-31-processor-provisioning-amendment.md),
[#385 IAM amendment](2026-10-31-step-functions-iam-amendment.md), [closure #385](2026-11-01-step-functions-iam-closure.md),
[operations design](2026-08-29-cnesdata-production-operations-design.md) e [delivery plan](../plans/2026-08-31-cnesdata-production-delivery-and-operations-implementation-plan.md).
