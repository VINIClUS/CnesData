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

## Limites do ensaio real

- Root da AWS só localmente, para criar e remover a identidade temporária `/cnesdata-cap/` com
  permissions boundary. O harness continua recusando root e perfis.
- Recursos só com nome `cnesdata-cap-*` e tags `Project`, `Purpose`, `Run` e `ExpiresAt`.
- Roda na VPC default de `us-east-2`, com IP público, zero ingress e egress TLS, como a rede de
  produção.
- Termina com inventário vazio em ECR, ECS, logs, security group, IAM, S3, DynamoDB e Cognito.
- Evidência redigida: sem conta, ARN, credencial ou URL assinada.
- A imagem é build local do repositório. A prova com o digest promovido é a transferência da Task 4.

## Paths autorizados

PR 1 (#384):

- `apps/data_processor/Dockerfile`, só pela regra de decisão acima;
- `tests/integration/aws_capability/**` e `docs/runbooks/floci-and-aws-capability.md`;
- `tests/production/infra/test_processing_security.py`, `tests/production/infra/test_acceptance.py`
  e um teste novo de espelho da task em `tests/production/infra/`.

PR 2 (#382):

- `infra/opentofu/modules/static-web/main.tf`;
- `tests/production/infra/test_static_web.py`, `test_static_web_composition.py` e
  `test_acceptance.py`;
- `packages/cnes_domain/src/cnes_domain/orchestration/static_release.py` e seu teste;
- `.trivyignore.yaml` e a seção 6.1 da deployment design;
- `docs/roadmap.md`.

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

## Transferências para o plano delivery

| Obrigação | Origem | Destino |
|---|---|---|
| Promoção auditada e `fargate-write` com o digest promovido | #384 | Task 4 |
| Publicador sobre `static_release`, prazos de upload, readback e rollback reais | #382 | Task 8 |
| Ativação da regra `static-web-gc` | #382 | Task 8 |
| `prune_revisions.py` sobre `processor_retention`, inventário e poda de digests | #384 | Task 10 |
| FREE ACTIVE, ACM emitido, DNS | #382 | Task 11 |
| Enforce da CSP depois da primeira publicação real | #382 | Task 8 |

## Nomes de teste das issues

Os nomes pedidos nas issues divergiram na implementação. Mapeamento vigente:

| Issue | Pedido | Vigente |
|---|---|---|
| #382 | `test_bucket_lido_so_pela_distribuicao` | `test_origem_privada_separa_allow_oac_e_deny_tls` |
| #382 | `test_csp_lista_cognito_e_origem_s3_exatas` | `test_raiz_agenda_static_web_e_acm_do_produto` |
| #382 | `test_assets_imutaveis_e_index_sem_cache` | teste novo do contrato `static_release` |
| #382 | `test_lifecycle_retem_atual_e_anterior` | teste novo do contrato `static_release` |
| #384 | `test_task_definitions_fargate_read_only_sem_root` | `test_container_exige_raiz_somente_leitura_usuario_app_e_sem_privilegios` |
| #384 | `test_task_role_nasce_sem_politica` | `test_root_anexa_inventario_exato_de_policies_as_roles` (a #385 estendeu a role) |
| #384 | `test_plan_agenda_roles_e_task_definitions` | `test_outro_digest_altera_imagem_dos_tres_modos` |

## Fora do escopo

`tofu apply` de produção, DNS, emissão de ACM, plano FREE, publicação real, poda real, promoção para
`main` e reativação do `Deploy develop`. `BILLING_MODE` continua `disabled`, e a #60 segue dependente
da IANA.
