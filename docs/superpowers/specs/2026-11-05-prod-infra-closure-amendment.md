# Fechamento de PROD-INFRA-005 e PROD-INFRA-007

Status: adotado pelo dono em 2026-11-05. Emenda #382 e #384. Não abre tracker, roadmap ou permissão
de deploy. As demais obrigações dos amendments de 2026-10-31 continuam valendo, salvo o que está
transferido abaixo.

## Decisões

**D9. Transferir e fechar.** Esta rodada entrega as correções declarativas e o contrato puro de
release estática. O que depende de produção passa para o plano delivery (tabela "Transferências"),
e #382, #384 e #365 são fechadas. Fechar não desbloqueia o MIG-014 (#280).

**D10. Prova no Fargate real.** O perfil `fargate-write` do harness `aws_capability` roda a imagem
atual (RED) e a corrigida (GREEN) em recursos efêmeros `cnesdata-cap-*`. É um quinto check real,
somado aos quatro do D6. Continua proibido o `apply` de produção.

**D11. AWS-0132.** A exceção do static-web é renovada por 90 dias, até 2027-02-03 00:00 UTC, com
revisão do novo SHA256 do `main.tf`. Mantém SSE-S3: o build é público e sem segredos.

## Estado do gate do delivery em 2026-11-05

[Ownership amendment](2026-11-07-prod-delivery-external-ownership-amendment.md) atualiza os
produtores e os refs externos deste snapshot. Bootstrap merged não comprova EXT-1/2/3 nem
readiness real. As transferências de Produção abaixo continuam obrigatórias.

O item 1 do corpo da #365 está desatualizado: o amendment static-web de 2026-10-31 limita o
`personal-infra-live` a Budget/freeze e backend. Estado observado:

- `personal-infra-live` existe (PR #1 em `main` hoje), sem nada aplicado;
- na AWS não há o bucket de estado dele nem OIDC provider;
- em `us-east-2` não há trust anchor do Roles Anywhere nem alias KMS próprio;
- o aceite do LimnoPulse não foi verificado.

O gate segue fechado. `personal-infra-live`, `infra-ansible` e `infra-ansible-inventory` são só
leitura nesta rodada.

## Defeito investigado na #384

A documentação da AWS define que um bind mount no Fargate nasce `root:root 0755`, salvo quando o
Dockerfile declara `VOLUME` no mesmo caminho do `containerPath`:
<https://docs.aws.amazon.com/AmazonECS/latest/developerguide/bind-mounts.html>.

O Dockerfile do processor não tem `VOLUME`, e a task roda como `app`, raiz read-only e
`TMPDIR=/tmp`. Docker local não serve de prova, porque volume nomeado copia o dono da imagem.

Regra de decisão:

1. Rodar `fargate-write` na imagem atual e registrar o resultado.
2. Se a escrita falhar, adicionar `VOLUME` ao Dockerfile, depois do ajuste de dono, e repetir até
   GREEN. Isso revoga, só nesse ponto, o "não mudar Dockerfiles" da #384.
3. Se a escrita passar, o Dockerfile fica como está e a prova fecha o critério.
4. Se o GREEN falhar por outro motivo, parar e reportar. `user=app`, raiz read-only e `drop ALL`
   nunca são relaxados.

Impacto do `VOLUME`: todo consumidor da imagem herda a declaração. O único fora do Fargate é o
`data-processor-local`, do profile `local`, que passa a criar volumes anônimos em `/tmp` e `/logs`
(`/data` já é o volume nomeado `local_data`), com o mesmo uid e a mesma escrita medidos com e sem
`VOLUME`. Esses volumes persistem em `up --force-recreate` e em `down` sem `-v`, então arquivos
deixados em `/tmp` e `/logs` sobrevivem ao container. `docker compose down -v` os remove. Nenhum
compose de `deploy/` usa essa imagem.

## Resultado do ensaio D10 (2026-11-05)

O passo 2 da regra de decisão se aplicou: o RED confirmou o defeito, e o `VOLUME` deu GREEN.

| Imagem | Run | `/tmp` | `/logs` | `/data` | `tempfile` |
|---|---|---|---|---|---|
| Sem `VOLUME` | `485eefc2` | `0:0 0o755 EACCES` | `0:0 0o755 EACCES` | `0:0 0o755 EACCES` | `FileNotFoundError` |
| Com `VOLUME` | `f06a29b9`, `8a5f5168` | `0:0 0o1777 ok` | `999:999 0o755 ok` | `999:999 0o755 ok` | `ok:/tmp` |

Nas três rodadas: `user=app`, `uid=999`, raiz `EROFS`, plataforma 1.4.0, 3 subnets, 1 regra de
egress, 0 de ingress, status `succeeded`, `remaining=[]` e revisões `ACTIVE=0`, `INACTIVE=0`.
`/tmp` com `root:root 1777` é o correto; o critério é a escrita, não o dono. `recover-once` termina
igual nas duas imagens (exit 1), então não separa RED de GREEN.

Runs são identificadores do ensaio, não commits. As duas imagens saíram da árvore de `8bc05769`
(pré-squash do #459), sem diferença para `2da01faa` em `apps/data_processor/`, `packages/` e
`uv.lock`. A RED usou esse Dockerfile sem a linha `VOLUME`. Digests do push: RED
`sha256:23f87153c6797ca7076ca84392b6246b2dbe3f62fb64cf1f8d63e2a31cf7cc0b`; GREEN, nas duas rodadas,
`sha256:0f4d8a7e48cf5b0ea5c3f3f6293e0bdc3539baf6c6fdd445748117c41a1bf5ea`.

## Limites do ensaio real

- Root da AWS só localmente, para criar e remover a identidade temporária `/cnesdata-cap/` com
  permissions boundary e para remover o `AWSServiceRoleForECS` descrito abaixo. O harness continua
  recusando root e perfis.
- Recursos só com nome `cnesdata-cap-*` e tags `Project`, `Purpose`, `Run` e `ExpiresAt`. Exceção:
  o `AWSServiceRoleForECS`, que a própria AWS cria na primeira task ECS da conta, com nome fixo. Foi
  removido com root ao final dos ensaios, e a AWS o recria na próxima task.
- Roda na VPC default de `us-east-2`, com IP público, zero ingress e egress TLS, como a rede de
  produção.
- Termina com inventário vazio em ECR, ECS, logs, security group, IAM, S3, DynamoDB e Cognito.
  A evidência `final_counts_all_cap` só conta os serviços do perfil em execução: perfis antigos não
  enxergam ECR, ECS, logs e security group. O inventário completo veio de leitura separada, depois
  do último ensaio.
- Evidência redigida: sem conta, ARN, credencial ou URL assinada.
- A imagem é build local do repositório. A prova com o digest promovido é a transferência da Task 4.
- Se o prazo de trabalho acaba durante o `docker push`, ou se o próprio `rmi` falha depois de um push
  bem-sucedido, a tag local com a conta fica para trás, sem credencial e sem aviso, e precisa de
  `docker rmi` manual. O `rmi` não usa a faixa `cleanup` porque o relógio
  dela é único: com o daemon travado, ele gastaria esse relógio, e a remoção dos recursos reais
  terminaria em `cleanup_timeout`. Com o daemon saudável, o `rmi` é imediato.

## Paths autorizados

PR da #384:

- `apps/data_processor/Dockerfile`, só pela regra de decisão acima;
- `tests/integration/aws_capability/**` e `docs/runbooks/floci-and-aws-capability.md`;
- `tests/production/infra/test_processing_security.py`, `_dockerfile_volumes.py`,
  `test_fargate_task_mirror.py` (espelho da task) e `_acceptance_gate.py`, para onde a tabela do
  `test_acceptance.py` foi movida.

PR da #382:

- `infra/opentofu/modules/static-web/main.tf`;
- `tests/production/infra/test_static_web.py`, `_static_web_controls.py`,
  `test_static_web_composition.py`, `test_acceptance.py` e `_acceptance_gate.py`;
- `packages/cnes_domain/src/cnes_domain/orchestration/static_release.py`, `test_static_release.py` e
  `test_static_release_retention.py`;
- `.trivyignore.yaml` e a seção 6.1 da deployment design;
- `docs/roadmap.md`.

PR de correção da auditoria independente:

- `.github/workflows/infra-native.yml` e `.github/workflows/ci.yml`;
- `tests/production/infra/test_gate_triggers.py`, `test_acceptance.py`,
  `test_athena_delivery_contracts.py` e `_acceptance_gate.py`;
- `tests/scripts/test_ci_opentofu_runner.py`;
- `tests/integration/aws_capability/_fargate_images.py` e `test_fargate_images.py`;
- `packages/cnes_domain/src/cnes_domain/orchestration/static_release.py`, `test_static_release.py` e
  `test_static_release_retention.py`;
- este amendment, `docs/runbooks/infrastructure-plan.md`, `docs/runbooks/floci-and-aws-capability.md`,
  `docs/roadmap.md` e o plano delivery.

## Decisões da #382

- **CSP.** Entram `object-src 'none'` e `base-uri 'self'`, exigidas pelo amendment de 2026-10-13 e
  ausentes do código. `style-src 'unsafe-inline'`, `img-src data:` e a origem `cognito-idp` em
  `connect-src` só ficam se provadas contra o `dist/` de produção. A raiz segue `report_only`.
- **Multipart.** O amendment de 2026-10-13 pede o abort, e o de 2026-10-31 o adiou. Esta rodada
  ratifica o abort declarativo em regra própria e sempre ativa, de 1 dia. Prazos de upload ficam no
  delivery. A regra `static-web-gc` continua `Disabled`.
- **Contrato puro.** `static_release.py`, no molde de `processor_retention.py`, sem I/O. É o oráculo
  que o publicador da Task 8 consome: cabeçalhos por objeto, inventário sem segredos, referências
  atual e anterior, retirada após 7 dias, teto estrito de 5.000.000.000 bytes e alvo de rollback.

### Prova da CSP contra o `dist/` de produção

Build em modo produção com env sintético de Cognito (14 arquivos, 1.200.315 bytes), servido
localmente com a CSP em enforce e exercitado com Playwright. O domínio real só existe depois do
`apply`, e a CSP segue `report_only` até a verificação da Task 8. Resultado:

- o login completo gera só `document authorize`, `fetch token` e `fetch API`. O browser não chama
  `cognito-idp`, porque os metadados são estáticos e `getSigningKeys` não é chamado. A origem sai do
  `connect-src`;
- nenhum asset usa `data:`, que sai do `img-src`;
- `style-src 'self'` bloqueia o `<style>` injetado pelo Radix Select em `/login`, então
  `'unsafe-inline'` fica provado;
- a única violação restante é `script-src eval`, do `new Function("")` do zod dentro de `try/catch`.
  É benigna, e `script-src 'self'` fica.

### Regras do contrato fixadas na revisão dos testes

- **Entrypoints.** Só os do build atual: `index.html` e `theme-init.js`. O manifest e o service worker
  citados na design §6.2 não existem no app, e entram no contrato quando existirem.
- **Referências.** Só as do `index.html` são exigidas no candidato. O restante, como fontes referidas
  pelo CSS, fica para o readback da Task 8.
- **Capacidade.** A recusa ainda devolve `retire` e `retire_latest`, sem upload nem rollback. Sem isso,
  um bucket cheio de itens já elegíveis nunca voltaria a aceitar release. `projected_bytes` não
  desconta esses itens, porque os bytes só saem depois da retirada real.
- **Delete marker.** Só em `assets/`, e nunca em chave referenciada por atual ou anterior, mesmo em
  outra versão: o CloudFront lê a versão mais recente da chave.
- **Sem `current`.** Nem `retire` nem `retire_latest`: um inventário sem referência atual não prova
  que um objeto saiu de uso, e o alvo real de rollback poderia ser removido. A release segue
  `PLANNED`, para não travar a primeira publicação nem a retomada depois de publicação interrompida.
  Com o bucket cheio e sem `current`, a recusa por capacidade não devolve o que retirar, e a saída
  é manual.
- **Segredos.** Chaves `sk_test_` e `rk_test_` também recusam a release.
- **Publicação idempotente.** Objeto com SHA256 igual ao da versão mais recente vai para
  `already_published`, seja asset ou entrypoint.

Pré-condições que o publicador da Task 8 precisa respeitar:

- `current` e `prior` listam todos os objetos de cada release, a partir do manifesto dela. O contrato
  não deriva esses objetos do `index.html`: um `current` parcial exporia os assets omitidos a delete
  marker.
- Delete marker não entra em `versions`: ele só define o `superseded_at` da versão anterior.
  Modelado como versão, faria `_overwrites` recusar para sempre o asset que voltasse num rollback
  de código, e `retire_latest` proporia outro marker sobre ele. `sha256=None` significa hash
  desconhecido e recusa a sobrescrita.
- `current` pode apontar para versão que não é a mais recente, como depois de publicação interrompida.
  O alvo de rollback continua sendo a referência em `current`.
- `retire` não presume a regra `static-web-gc`, que retém uma versão não corrente a mais
  (`newer_noncurrent_versions = 1`). Ao ativar a regra, os dois critérios seguem independentes.
- O contrato só reconhece referência do `index.html` em atributo entre aspas com caminho absoluto
  (`src="/assets/x.js"`). Isso exige manter o `base` do Vite em `/`: com `./assets/x.js` ou atributo
  sem aspas, o asset deixa de ser exigido no candidato.

### Exceção de teste do PR #460

`test_asset_com_delete_marker_fica_sem_versao_recente_e_pode_voltar` e
`test_asset_com_hash_desconhecido_na_versao_recente_recusa_sobrescrita` são testes de
caracterização: foram escritos depois do código e passam contra `2da01faa`, porque o PR só mudou o
docstring de `StoredVersion`. Não há execução falha-depois-passa contra o código anterior. A prova
de que detectam quebra veio de mutantes em memória: contar versão substituída como recente derruba
o primeiro, e tratar `sha256=None` como ausente derruba o segundo. O mutante do segundo rodou antes
do merge; o do primeiro, depois do merge e do fechamento das issues.

### Renovação da AWS-0132 (D11)

- `main.tf` revisado: SHA256 `b94212cb1c9d65c04e02f7cb6550b108f8b3ced71613c28ace7c3eaec3ce8e17`;
- Trivy 0.74.0, sem ignores, em `modules/static-web`, severidade CRITICAL e HIGH: um único achado,
  AWS-0132 em `aws_s3_bucket_server_side_encryption_configuration.this`. SHA256 do JSON bruto:
  `1b0b9b80f68f080f82bb18b76f3f031377fac8d8154ce47faa4b46279a551c16`;
- com `.trivyignore.yaml`, na raiz do repositório como no CI: 0 achados.

## Gatilho do gate

A auditoria independente achou o gate PROD-INFRA-012 fora dos filtros do `infra-native` quando mudam
arquivos que ele lê. Os filtros passam a cobrir: Dockerfile e código do `data_processor`, os três
pacotes, `production_values.py`, os dois arquivos do dashboard, os dois módulos do harness, os dois
scripts que o próprio workflow executa, os documentos citados por nome, `conftest.py`, `pytest.ini`,
`.gitignore`, `CODEOWNERS` e `deploy-main.yml`.

`test_gate_triggers.py` protege essa lista por leitura estática. Ele enxerga `REPO_ROOT / ...`,
`parents[3] / ...`, literais `apps/` e `packages/`, `from ... import` de `apps`, `packages` e `tests`,
e arquivos `tests/**/*.py` citados nos blocos `run` do workflow, fora a detecção de mudanças, que só
tem as listas de filtro. Cada forma tem um caso próprio no teste. Não enxerga, entre outros, `import a.b`,
`import_module`, `runpy.run_path`, `joinpath`, `parents[N]` com outro N, caminho relativo, f-string,
outro nome de raiz, `from scripts...`, `scripts/` no workflow, nem o que `tofu`, `conftest`, `trivy`
e processos filhos do pytest leem. Um arquivo lido dessas formas pode ficar fora dos filtros sem o
teste avisar.

Varreduras gerais ficam de fora, de propósito: segredos em `docs/runbooks`, `tests/production` e
`.github/workflows`, e rótulo de runner em todos os workflows. Mudança só nesses arquivos não dispara
o gate, e um problema neles aparece no próximo gate que rodar. PRs nos paths novos passam a esperar o
gate, cerca de 10 minutos; o repositório é público, sem custo de Actions.

A suíte offline de `tests/integration/aws_capability` não rodava em nenhum workflow. Agora roda no
`ci.yml`.

## Transferências para o plano delivery

| Obrigação | Origem | Destino |
|---|---|---|
| Promoção auditada e `fargate-write` com o digest promovido | #384 | Task 4 |
| Publicador sobre `static_release`, prazos de upload, readback e rollback reais | #382 | Task 8 |
| Ativação da regra `static-web-gc` | #382 | Task 8 |
| `prune_revisions.py` sobre `processor_retention`, inventário e poda de digests | #384 | Task 10 |
| FREE ACTIVE, ACM emitido, DNS | #382 | Task 11 |
| Enforce da CSP depois da primeira publicação real | #382 | Task 8 |
| Teste do mapeamento de `ListObjectVersions` para `StoredVersion` | #382 | Task 8 |
| Rollback restaura também `theme-init.js`; o contrato só devolve `index.html` | #382 | Task 8 |
| Apply do ACM em duas fases: `ISSUED` antes de a distribuição usar o ARN | #382 | Task 3 |
| `certificate_domain_validated = true` (HSTS) depois da validação do domínio | #382 | Task 11 |

Ordem de execução: a Task 3 entrega o apply em duas fases, e a Task 11 emite o certificado. A
publicação real da Task 8 depende da distribuição, então só roda depois do certificado `ISSUED`.

## Nomes de teste das issues

Os nomes pedidos nas issues divergiram na implementação. Mapeamento vigente:

| Issue | Pedido | Vigente |
|---|---|---|
| #382 | `test_bucket_lido_so_pela_distribuicao` | `test_origem_privada_separa_allow_oac_e_deny_tls` |
| #382 | `test_csp_lista_cognito_e_origem_s3_exatas` | `test_composicao_consume_cognito_serving_e_certificado` |
| #382 | `test_assets_imutaveis_e_index_sem_cache` | mesmo nome, em `test_static_release.py` |
| #382 | `test_lifecycle_retem_atual_e_anterior` | mesmo nome, em `test_static_release.py` |
| #384 | `test_task_definitions_fargate_read_only_sem_root` | `test_container_exige_raiz_somente_leitura_usuario_app_e_sem_privilegios` |
| #384 | `test_task_role_nasce_sem_politica` | `test_root_anexa_inventario_exato_de_policies_as_roles` (a #385 estendeu a role) |
| #384 | `test_plan_agenda_roles_e_task_definitions` | `test_outro_digest_altera_imagem_dos_tres_modos` |

## Fora do escopo

`tofu apply` de produção, DNS, emissão de ACM, plano FREE, publicação real, poda real, promoção para
`main` e reativação do `Deploy develop`. `BILLING_MODE` continua `disabled`, e a #60 segue dependente
da IANA.
