# PROD-DELIVERY: ownership e pré-requisitos externos

Status: decisões aprovadas pelo dono em 2026-11-07; integração sujeita à auditoria do controller.
Escopo: EPIC #465, tracker #93 e PROD-DELIVERY-001…012 (#466–#477).

## Autoridade e limites

Este amendment governa ownership, produtores externos, dependências e execução da delivery.
Prevalece nesses pontos sobre as specs deployment/operations de 2026-08-29, o plano delivery
de 2026-08-31, a seção Ownership do amendment static-web de 2026-10-31, o snapshot do closure
de 2026-11-05 e D2/D6 da EPIC de 2026-11-06. Não altera contratos de domínio ou as fases A/B/C.
As demais decisões e transferências desses documentos continuam obrigatórias.

As referências antigas a gates em `personal-infra-live` passam a significar os produtores
de `infra-ansible` abaixo. Não autorizam ampliar o escopo de `personal-infra-live`.
As tarefas externas passam a ser rastreadas nos repositórios responsáveis. Seus corpos e os
ajustes do backlog são preparados como drafts locais; o controller publica após auditoria.
Draft não é issue publicada, evidência de produção ou gate satisfeito.

Este PR entrega documentação, sem apply, promoção, merge ou mutação AWS/VPS/DNS. A autorização
do dono para execução futura não elimina auditoria, gates ou o aceite de cada estágio.

## Refs reconciliados em 2026-11-07

Leituras remotas imutáveis; nenhum checkout de trabalho preexistente foi modificado:

| Repositório | Ref examinada |
|---|---|
| CnesData | `develop@7a07fdd14ef858b87ecf40af633869581eecd422` |
| infra-ansible | `main@c4bf6cae7f6941d3a07b0bd7d3f93b60af32ae72` |
| infra-ansible-inventory | `main@165e413b66e8d028963ce5a133f35a3c730c8ac2` |
| personal-infra-live | `main@1428d058fa6da045945803b0b9531283ba6c4749` |

`infra-ansible#22` está merged (`9b4f5cb543e76df1156096fe84f3727a06e4555a`). Seu bootstrap
não fornece por si os reusable gates/OIDC da delivery, assinatura/evidência ou host gate;
não satisfaz EXT-1, EXT-2 ou EXT-3. Código de bootstrap também não comprova aplicação real.
O README atual de `personal-infra-live` já limita seu escopo a backend e Budget/freeze;
o import/reconcile USD 20 → USD 15 continua exigindo evidência real.

O transporte é o do perfil pessoal de `infra-ansible/docs/architecture.md` e das specs
`2026-08-29-personal-infra-live-composition-design.md` (§§4–6, 10) e
`2026-08-29-personal-vps-platform-and-cd-design.md` (§deployment protocol): SSM SecureString,
KMS e Roles Anywhere para segredos/identidade temporária; S3 versionado para state/evidência.
O inventário municipal usa ansible-vault; Infisical foi desativado e MinIO está desativado
sem data de retorno, conforme `infra-ansible/docs/safety.md`. A menção histórica a MinIO em
`infra-ansible-inventory/docs/environments.md` não autoriza usá-lo nesta delivery.
Não copiar inventário, credenciais ou artefatos municipais para o perfil pessoal.

O novo catálogo pessoal privado do inventário é uma extensão explícita de escopo deste
amendment. Guarda valores/topologia/identidades e referências, sem voltar a usar Infisical.
Segredos pessoais em claro, CA privada, folhas privadas e valores de SecureString ficam
fora de Git, plans e state. Valores iniciais/rotação usam bootstrap seguro separado, com
readback por versão/checksum; não usar `aws_ssm_parameter` para transportar seu plaintext.
Antes de operar, reconciliar novamente os refs e registrar qualquer mudança de contrato.

## Ownership aprovado

| Responsável | Produz e mantém |
|---|---|
| `infra-ansible` | EXT-1 reusable gates/OIDC; EXT-2 KMS/assinatura/evidência; EXT-3 host |
| `infra-ansible` | Provisionamento compartilhado Roles Anywhere, profiles/trust anchor/CRL |
| `infra-ansible-inventory` | Valores, topologia, identidades e configuração DNS privados |
| `personal-infra-live` | Apenas backend governado e Budget/freeze agregado de USD 15 |
| `CnesData` | Produto, consumidores de contratos e IAM dedicado do seed de demo |
| `VINIClUS` | Dono de DNS e CA offline; agentes são executores delegados |

`infra-ansible` tem raiz própria `opentofu/cnesdata-delivery/`, com chave exclusiva
`shared/cnesdata-delivery/prod/opentofu.tfstate` no backend governado. Nenhum outro root
gerencia esses recursos ou usa essa chave. Isso não transfere os recursos delivery para
o state de Budget/freeze, bootstrap, produto ou LimnoPulse. A liberação do prefixo exato,
locking, cifra e recuperação do backend é parte de EXT-5, sem ampliar seu ownership.

O produto continua dono de S3 web/dados/auditoria, CloudFront/ACM/WAF, Cognito,
DynamoDB, Step Functions/ECS/ECR e Athena. Não adota DNS nem recursos compartilhados.
O código compartilhado de host/deployer permanece em `infra-ansible`; o inventário contém
os valores privados. Um recurso tem um único produtor e um único state.

VINIClUS controla zona/conta DNS, aprovação da mudança e emissão/revogação/recuperação da CA.
Chave da CA privada nunca entra em Git, state, SSM ou artefato: duas cópias cifradas offline
em locais separados. Só PEM público/CRL assinada e metadados não secretos entram nos inputs.
Sem AWS Private CA. Instalação e rotação das folhas privadas usam canal seguro separado.

## Contratos consumidos, sem novo formato concorrente

`infra/opentofu/env/prod/shared-outputs.schema.json` v2 permanece a interface canônica.
Os produtores entregam fragmentos compatíveis e o handoff privado valida o objeto completo;
não renomear campos nem inventar um `shared_outputs` v3 para contornar um produtor ausente.

| Gate | Contrato que precisa produzir | Consumidor/gate |
|---|---|---|
| EXT-1 | `github.infrastructure_plan_role_arn` e `infrastructure_deploy_role_arn` | #468 inteira |
| EXT-1/3 | `github.host_deploy_role_arn` e reusable host gate específico | #473 inteira |
| EXT-2 | `kms_key_arn`, `storage.evidence_bucket_name`, `evidence_bucket_arn` | #470 inteira |
| EXT-4 | Operador, zona, configuração e evidência de DNS/CA privados | #476 Produção 1/2 |
| EXT-5 backend | `storage.state_bucket_name`, `state_bucket_arn`, backend exato | Todo Produção |
| EXT-5 budget | `budget_freeze.{budget_arn,action_arn,policy_arn}` | Todo Produção |
| EXT-5 RA | `roles_anywhere.{api_role_arn,profile_arn,trust_anchor_arn,crl}` | Todo Produção |
| EXT-6 | Aceite LimnoPulse e smoke antes/depois da mudança compartilhada | Todo Produção |

Os três ARNs `github.*` pertencem a `shared_outputs.github.*`, sem alterar sua forma.
O handoff inclui `schema_version=2`, `account_id` e o contrato `account_baseline` completo;
fragmentos parciais não podem fingir completude. Reconciliar a origem/ownership do baseline
existente antes do readback, sem adotar recurso de outro projeto implicitamente.

EXT-1 valida antes de OIDC o caller/repo/ref/workflow exato, source SHA, dependências,
artifact/run/digest/expiração, backend, drift, policy e custo. Trust vincula `aud` e o
`sub` customizado com repo/context/job_workflow_ref do reusable gate fixado por SHA.
Plan e deploy são roles distintas, sem cross-assume; PR não recebe credencial/id-token,
segredo, packages-write ou runner self-hosted. O gate aplica só o binário exato auditado,
sem replan, `-lock=false`, destroy workflow ou apply/promoção em merge.

O `.tflock` nativo e o lease S3 de coordenação são diferentes. O lease usa token de dono,
CAS e renovação antes da metade do intervalo, cobre a troca de credencial até a evidência
final e aborta antes da margem de segurança. Expiração só não autoriza takeover: provar
run terminal ou fazer recuperação humana revisada. Nenhum lock nativo é pré-criado.

EXT-2 assina a activation manifest só após Phase A; vincula candidato SHA-256, source/release,
digest ECR e revisões exatas/evidência conforme operations §2.4. Não reutilizar a CMK de
bootstrap para `kms:Sign` por inferência: provar KeyUsage/algoritmo/policy apropriados.
Se exigir chave de assinatura distinta da CMK de cifra, documentar o vínculo no gate sem
redefinir `kms_key_arn` nem alargar o schema. Evidência S3 tem objeto/version ID/digest,
retenção, acesso restrito e readback; nada de listagem arbitrária ou segredo nos logs.

EXT-3 implementa o protocolo existente da spec VPS compartilhada: três frames com tamanho
prefixado, (1) envelope v1 não secreto com `ci_evidence` e `release_manifest`, (2) archive
exato, limitado e hash-conferido com manifest embutida idêntica, (3) `GITHUB_TOKEN` efêmero
do produto para verificação GitHub independente ao vivo. Não criar protocolo alternativo.
O archive vincula SHA/imagens por digest/Compose/config/plano de plataforma/expiração.
Recusa traversal, links, devices, duplicatas, arquivos não declarados e limites excedidos.
O token só tem leitura necessária, nunca vai em argv/env/log/state/evidência, e é descartado
em todo exit. Segredos runtime são renderizados de SSM em `/run` depois da validação.

Todo modificador do VPS, inclusive humano, renovação de credencial e outro app, usa o mesmo
lease host-wide com dono, watchdog e margem válida até decisão de saúde/switch/rollback.
Recuperação stale exige prova de processo ausente e run terminal/sessão encerrada, com CAS
e registro redigido. Dispatcher forced-command e deployer só executam ações allowlisted,
sem shell arbitrário, troca de outro app ou poda global. Candidate/rollback do API e o
switch Nginx obedecem à evidência/plataforma exatas; CnesData não edita o host na #473.

## Produtor IAM próprio para demo

O `canary_seeder` atual (`modules/runtime-iam/canary.tf`) só tem `PutItem` na tabela canary
com `dynamodb:LeadingKeys = TENANT#_GLOBAL`. Não atende tenant/membership/dataset de demo.
Não ampliar essa role, a #471 ou os allowed paths da #476 silenciosamente.

Criar tarefa própria em CnesData, anterior ao consumer de seed da #476, com estes paths:

- criar `infra/opentofu/modules/runtime-iam/demo-seed.tf`;
- alterar `infra/opentofu/modules/runtime-iam/{variables,outputs}.tf`;
- alterar `infra/opentofu/env/prod/{main,variables,outputs}.tf` só para ligar IAM demo;
- criar `tests/production/infra/test_demo_seed_iam.py` e `tests/fixtures/demo-seed-iam/**`;
- alterar `infra/opentofu/tests/production.tftest.hcl` só para o contrato demo;
- criar `docs/runbooks/demo-seed-iam.md`.

Inputs obrigatórios: tabela/recursos reais, tenant demo exato, partições canônicas derivadas
dos contratos existentes, principal temporário e sessão limitada. Saídas: role/policy demo
e contrato de consumo privado, sem ID real em documentação pública. O produtor deriva a
matriz de ações dos adapters/seed contratado; não concede write geral por GitHub, Scan,
bulk delete, semáforo ou acesso a outro tenant. Testes de IAM renderizado provam negativas
para outro tenant/tabela/principal e o canary sem mudança. O consumer continua na #476.
Seed idempotente, sem sobrescrever tenant não demo, com evidência redigida de IDs/hashes;
o teste real de login PKCE e isolamento fica no aceite Produção 3 da #476.

## Execução, aceites e custo

- Código não requer apply. Cada produtor externo tem PR auditado, paths/inputs/outputs/testes,
  ref imutável e CI verde na branch de integração de seu repo (`main` nos refs acima).
  Consumer só nasce do último `develop` verde com dependências CnesData integradas e refs
  externos auditados fixados. Sem branches consumidoras empilhadas ou interfaces provisórias.
- Cada tarefa tem duas sessões: implementador e auditor independente. No máximo duas
  implementações, um auditor e um controller. Base/head auditados são registrados; mudança
  de base/head invalida a auditoria do delta afetado e exige reauditoria antes de merge.
- Hotspots são por recurso cross-repo: raiz/state delivery e outputs, OIDC subjects/trust,
  KMS/evidence, backend/policies, freeze allowlist, CA/CRL, DNS e lease/dispatcher/Nginx.
  Um PR modificador aberto por hotspot; controller serializa integração e operações.
- Só o controller executa plans binários exatos já auditados, após preflight de conta/região,
  principal/ownership, drift, locks/lease, custo e checkpoints. Recuperação usa versões de
  state/evidência e checkpoints verificados; não destroy, recriação ou replan implícito.
- Preservar a exceção estrita já contratada em `infra-ansible#22`: apenas a foundation inicial
  usa principal local root com MFA e janela/autorização/checkpoint exatos, como validado em
  `tools/aws_bootstrap/planner.py`. Ela não autoriza root em CI/cloud apply delivery/runtime,
  nem chaves AWS estáticas. Demais operações usam executores temporários com escopo mínimo.
- Produção exige readbacks e evidência real: backend/lock/restore, OIDC negativo, assinatura,
  acesso/retenção de evidência, Roles Anywhere/CRL, host gate/lease/saúde/rollback, DNS e freeze.
  Mock, bootstrap merged ou Código verde não satisfazem esses critérios.
- EXT-6 permanece duro: LimnoPulse aceito primeiro. Registrar sua evidência de aceite sem
  absorver roadmap funcional. O deploy manual LimnoPulse `41521975392` de 2026-11-02,
  source `a019ec5b5db1f8eda203079f1de4e7821500ec81`, deployment `production7502943323`
  em `success`, é candidato a verificar: tem zero artifacts e não prova o aceite compartilhado.
  Toda mudança compartilhada exige smoke LimnoPulse antes/depois;
  falha bloqueia a operação/requer recovery revisado. Não habilitar CnesData para contornar isso.
- Produto <= USD 8/mês; freeze agregado USD 15. Manifesto financeiro inclui KMS (cifra e eventual
  assinatura), S3/evidence/versões/requests, backend, SNS/Budget Actions e transição WAF.
  Sem supor custo zero/free tier. WAF sem FREE durante #468 fase 2 → #476 Produção 2 exige
  janela curta e custo revisado. FREE da distribuição/WAF exatos deve estar `ACTIVE` antes
  do corte DNS; sem fallback pago. `BILLING_MODE=disabled`, Actions pagos USD 0.
- `deploy-develop` permanece disabled. Nenhum workflow novo aplica/promove no merge.
  Tasks com estágio Produção só fecham com Código e Produção aceitos (#468, #469, #473,
  #474, #475 e #476). #477 pode fechar seu gate de Código com matriz real pendente; a EPIC não.

## Rastreabilidade a publicar após auditoria

| Draft lógico / repo responsável | Entradas/dependências | Saída / consumidor |
|---|---|---|
| INVENTORY / infra-ansible-inventory | Refs/ownership acima | Catálogo privado para EXT-1…5 |
| BACKEND-FREEZE / personal-infra-live | #1, #22 e catálogo privado | EXT-5 backend/Budget USD 15 |
| RA / infra-ansible | #22, backend/catálogo, CA offline | EXT-5 identidade |
| EXT-1 / infra-ansible | Backend/catálogo, #22 | Gates/OIDC para #468 |
| EXT-2 / infra-ansible | Backend/catálogo, #22 | Assinatura/evidência para #470 |
| EXT-3 / infra-ansible | EXT-1/2, RA, catálogo | Host gate/dispatcher para #473 |
| DNS-CA / infra-ansible-inventory | Dono, catálogo, outputs ACM/CloudFront | EXT-4 para #476 |
| EXT-6 / limnopulse | Aceite existente e plataforma compartilhada | Evidência de ordem/smoke |
| DEMO-IAM / CnesData | Contratos tenant/adapters e EXT-1 | Role dedicada antes de #476 |

Backend/RA/OIDC/KMS/host podem ter Código sem cloud; seus aceites Produção ficam bloqueados
por EXT-6 e pela ordem de bootstrap auditada. O catálogo pode declarar inputs pendentes,
sem inventar ARNs/DNS ou alegar readiness. Produção DNS consome outputs reais #468 fase 1/2;
não bloqueia o Código do catálogo, dos gates ou da #476 por ciclo de valores ainda não criados.
O estado inicial de #22 continua exclusivo; não depende da raiz delivery para criar seu backend.

Controller publica drafts completos com allowed paths, testes, critérios Código/Produção e
links dos predecessores, substitui os IDs lógicos pelos issues reais e atualiza #465/#93 e
#468/#470/#473/#476/#477. Nenhum checkbox/fechamento automático. #476 passa a depender do
produtor DEMO-IAM; isso não lhe concede permissão para editar IAM.
