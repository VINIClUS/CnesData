# Floci e checks de capacidade na AWS real (PROD-013)

Camada extra de emulação (Floci, MIT) para Cognito, DynamoDB, S3 Object Lock e IAM/STS com
enforcement. Não substitui o `dynamodb-local` nem o `localstack` do profile `aws-test` (AWS-014,
CND-025). Os mesmos cenários rodam depois como checks efêmeros na AWS real (PR 13b); a matriz
abaixo registra onde o emulador diverge.

## Rodar a suíte Floci

```bash
docker compose --profile floci up -d --wait floci
FLOCI_ENDPOINT_URL=http://127.0.0.1:4577 uv run --no-build pytest -rs -m floci tests/integration/floci
docker compose --profile floci down -v
```

- Serviço `floci`: `floci/floci:2.2.0` fixado por digest, só em `127.0.0.1:4577` (nunca colide
  com o LocalStack em 4566), `FLOCI_STORAGE_MODE=memory`, sem socket Docker e sem volume.
  `FLOCI_BASE_URL=http://127.0.0.1:4577` é o que issuer, JWKS e URLs assinadas embutem.
  `FLOCI_SERVICES_IAM_ENFORCEMENT_ENABLED=true` e `FLOCI_AUTH_VALIDATE_SIGNATURES=true`.
- Healthcheck: o script da própria imagem (`/usr/local/bin/healthcheck.sh`), que faz
  `GET /_floci/health` por `/dev/tcp` em bash, porque a imagem ubi9-micro não tem `curl`. O boot
  leva ~15 s (o Floci tenta o socket Docker ausente antes de ficar pronto). A fixture `floci`
  ainda exige `version=2.2.0` e `cognito-idp`, `dynamodb`, `iam` e `s3` em `running`.
- Sem o emulador, os testes **falham** com `floci_unready endpoint=… problem=…`; nunca pulam. A
  suíte rápida exclui o marcador com `and not floci`.
- Projeto compose: o padrão é o nome do diretório; use `-p <nome>` para não tocar outra stack.
- Trocar o digest exige rodar a suíte: teste `test_floci_*` que quebrar indica mudança de
  comportamento do emulador; atualize a matriz no mesmo PR.

## Regras de isolamento

- Endpoint só de `FLOCI_ENDPOINT_URL` (padrão `http://127.0.0.1:4577`). `AWS_ENDPOINT_URL` e
  `DYNAMODB_ENDPOINT_URL` são ignorados. Host fora de loopback, credencial na URL, caminho ou
  esquema diferente de http/https falham fechado (`floci_endpoint=rejected reason=…`).
- Sessão botocore dedicada (`tests/integration/floci/_floci.py`): `AWS_PROFILE`,
  `AWS_CONFIG_FILE` e `AWS_SHARED_CREDENTIALS_FILE` não são lidos, a cadeia de credenciais é vazia
  (sem env nem IMDS), proxies do ambiente são ignorados e não há retry. Cada cliente recebe
  credenciais explícitas: `test`/`test` (chave que o Floci documenta como bypass do enforcement)
  faz o papel de operador; usuários e roles criados no emulador exercitam as matrizes.
  `test_isolation.py` prova isso com perfil, config, credenciais e proxy hostis no ambiente.
- HTTP de discovery, JWKS e PKCE usa `httpx` com `trust_env=False`.
- Nomes `cnesdata-cap-<uuid8>`; teardown por `ExitStack` (teste ou módulo). COMPLIANCE usa
  janelas de 2 a 10 s para a limpeza esperar o vencimento e apagar tudo.

## CI

Job `floci-integration` em `.github/workflows/python-quality.yml`: `ubuntu-latest`,
`permissions: contents: read`, sem `secrets`, `id-token` ou `environment`. Ordem: contrato
(`tests/scripts/test_ci_floci.py`), `up -d --wait floci`, suíte com `--junitxml=floci.xml`, falha
se `skipped > 0` e `down -v` sob `if: always()`. Herda os gatilhos do workflow; PR que muda só o
`docker-compose.yml` não o dispara, então rode-o por `workflow_dispatch` depois de trocar o digest.

## Checks na AWS real (PR 13b)

Executados pelo conector AWS MCP, em `us-east-2`, sem `apply` de produção e com custo ~US$0.

1. **Identificação.** Run id `<uuid8>`; nomes `cnesdata-cap-<uuid8>[-sufixo]`; tags
   `Project=cnesdata`, `Purpose=capability-check` e `Run=<uuid8>` onde o serviço aceita tag.
2. **Inventário inicial.** Listar `cnesdata-cap-*` em DynamoDB, S3, IAM (usuários e roles) e
   Cognito (pools e domínios) e registrar as contagens.
3. **Cenários.** Os mesmos da suíte Floci. Reaproveitar `_cognito.provision` (tier em
   `pool_options`), `_dynamodb.create_canonical_table`, `_iam` e `_oauth_pkce` com
   `HostedUi("https://<prefixo>.auth.us-east-2.amazoncognito.com")` sem prefixo de caminho. O
   helper PKCE lê formulário HTML: se o login gerenciado v2 não expuser um, criar o domínio com
   `ManagedLoginVersion=1`. D-B: se o tier LITE não transformar `resource` em `aud`, a escolha
   volta ao usuário.
4. **Object Lock só GOVERNANCE**, com retenção curta, removida no teardown com
   `x-amz-bypass-governance-retention`. COMPLIANCE nunca; por isso o `S3ObjectLockAuditSink`
   (COMPLIANCE fixo) só é exercitado no Floci.
5. **Teardown na mesma execução:** domínio Cognito e pool; versões S3 e bucket; desligar
   `DeletionProtection` e apagar a tabela; chaves, políticas inline e usuários; políticas inline e
   roles.
6. **Inventário final vazio.** Repetir a listagem; qualquer `cnesdata-cap-*` remanescente reprova
   o check.
7. **Redação da evidência.** Registrar só status, códigos de erro, contagens e claims não
   sensíveis (`token_use`, `scope`, presença de `aud`). Trocar account ID, ARNs, IDs de pool e
   client e domínios por `<conta>`, `<arn>`, `<pool>`. Nunca registrar access key, secret, session
   token, senha, JWT, código de autorização ou URL assinada.

## Matriz de divergências

Floci 2.2.0 observado pela suíte (cada linha tem teste). A coluna AWS fica pendente até a 13b; o
texto entre parênteses é a expectativa pela documentação AWS.

### Cognito (`test_cognito_jwt.py`)

| Cenário | Floci | AWS (a preencher na 13b) | Impacto | Mitigação |
|---|---|---|---|---|
| `aud` do access token (SDK e PKCE) | ausente; ID token traz `aud=client_id` | pendente (sem `aud` salvo resource binding) | `OidcVerifier` exige `aud`: recusa access token | check real com `resource`; D-B |
| `resource` (RFC 8707) no authorize/token | ignorado em silêncio; escopos iguais | pendente (D-B, tier LITE) | não dá para provar binding no emulador | só a 13b decide |
| issuer e `jwks_uri` | `http://127.0.0.1:4577/<pool>` | pendente (`https://cognito-idp.us-east-2.amazonaws.com/<pool>`) | `OidcVerifier` recusa `jwks_uri` http | teste reescreve só o esquema via `MockTransport` |
| domínio de prefixo | `ACTIVE`, não roteado; discovery segue em `/cognito-idp/oauth2/*` | pendente (`https://<prefixo>.auth…/oauth2/*`) | URL do login gerenciado difere | `HostedUi(base_url, path_prefix)` |
| login gerenciado | form HTML com `_csrf` + cookie `XSRF-TOKEN`; senha errada = 400 | pendente (v1 HTML; v2 SPA) | scraping depende do formulário | `ManagedLoginVersion=1` na 13b |
| PKCE e callback | só S256; verifier errado consome o código; código não reutilizável; callback exato | pendente (igual) | nenhum | — |

### DynamoDB (`test_dynamodb_semantics.py`)

| Cenário | Floci | AWS (a preencher na 13b) | Impacto | Mitigação |
|---|---|---|---|---|
| transações | 100 aceitas; 101 `ValidationException`; chave duplicada rejeitada; `CancellationReasons` por item; `ClientRequestToken` idempotente e `IdempotentParameterMismatch` | pendente (igual) | nenhum | — |
| condição e CAS | `ConditionalCheckFailed`; `ALL_OLD` devolve o item | pendente (igual) | nenhum | — |
| TTL na leitura | expirado some de GetItem consistente, Query, GSI e Scan antes da varredura de 60 s | pendente (segue legível até a exclusão, que leva até dias) | teste que lê expirado diverge | filtrar `expires_at` na leitura; TTL só coleta lixo |
| TTL nas condições | expirado oculto ainda conta para `attribute_(not_)exists` | pendente (leitura e condição veem o item) | ler e depois gravar condicional discorda no Floci | condicionar sobre `expires_at` |
| consistência de GSI | imediata | pendente (eventual) | atraso de GSI não reproduz | dublê `StaleQueryClient` da AWS-014 |
| GSI `ConsistentRead` | `ValidationException` | pendente (igual) | nenhum | — |
| PITR | habilita e descreve 35 dias; restore não testado | pendente | nenhum | — |
| `DeletionProtection` | bloqueia `DeleteTable` (`ValidationException`) | pendente | nenhum | — |
| `OnDemandThroughput` | aceito e descrito; teto 5/5 não segura 100 escritas | pendente (teto gera throttling) | teto 100/25 (PROD-INFRA-002) não testável | check real |
| item > 400 KB | `ValidationException` | pendente (igual) | nenhum | — |

### S3 Object Lock (`test_s3_object_lock.py`)

| Cenário | Floci | AWS (a preencher na 13b) | Impacto | Mitigação |
|---|---|---|---|---|
| COMPLIANCE | delete de versão negado mesmo com bypass; não encurta nem rebaixa; vencida libera; delete sem versão cria marcador | não testável (D6 proíbe COMPLIANCE) | COMPLIANCE só tem evidência no Floci | GOVERNANCE na 13b |
| GOVERNANCE sem bypass | delete e encurtamento negados | pendente (igual) | nenhum | — |
| `s3:BypassGovernanceRetention` | não avaliado: sem permissão ou com Deny explícito o cabeçalho basta | pendente (`AccessDenied` sem a permissão) | alto: gate de bypass não testável | check real; audit em COMPLIANCE e dispatch sem delete |
| delete de versão | autorizado por `s3:DeleteObject`; `s3:DeleteObjectVersion` sozinha é negada | pendente (exige `s3:DeleteObjectVersion`) | política de delete não validável | check real |
| cabeçalhos de lock sem `s3:PutObjectRetention` | aceitos | pendente (exige a permissão) | política da role de dispatch não validável | check real |
| legal hold | bloqueia delete, mesmo com bypass, até `OFF` | pendente (igual) | nenhum | — |
| sobrescrita | nova versão; a retida continua; retenção da versão nova = `{}` | pendente (sem retenção = 404 `NoSuchObjectLockConfiguration`) | baixo: `S3ObjectStore` trata ausência como insuficiente | — |
| retenção padrão do bucket | GOVERNANCE aplicada a objeto novo | pendente (igual) | nenhum | — |
| versionamento em bucket com lock | suspensão aceita | pendente (`InvalidBucketState`) | médio | não suspender; check real |
| lock em bucket sem Object Lock | cabeçalhos aceitos e aplicados | pendente (`InvalidRequest`) | médio: bucket mal configurado passa | sink valida `ObjectLockEnabled` no boot |
| `S3ObjectLockAuditSink` | COMPLIANCE até `created_at + dias`; reenvio idempotente; divergente = `Conflict`; bucket sem lock recusado | não testável (COMPLIANCE) | evidência só no Floci | — |

### IAM/STS com enforcement (`test_iam_sts.py`, `test_iam_permission_matrix.py`)

| Cenário | Floci | AWS (a preencher na 13b) | Impacto | Mitigação |
|---|---|---|---|---|
| trust exato | principal nomeado assume; outro é negado | pendente (igual) | nenhum | — |
| `sts:AssumeRole` na identidade | exige Allow com `Resource: "*"`; sem política ou com o ARN da role, nega | pendente (mesma conta: ARN exato no trust basta) | políticas reais não validáveis | check real |
| `DurationSeconds` > `MaxSessionDuration` | 3601 e 43200 aceitos com máximo 3600 | pendente (`ValidationError`) | limite de 3600 s não testável | check real |
| session tags e `SourceIdentity` | aceitos sem `sts:TagSession`/`sts:SetSourceIdentity` no trust | pendente (`AccessDenied`) | médio | check real |
| propagação IAM | imediata | pendente (eventual) | corrida de propagação não reproduz | esperar/retentar na 13b |
| chave `test` | ignora enforcement e bucket policy | — (só emulador) | por design | operador dos testes |
| chave desconhecida | rejeitada por DynamoDB, S3 e IAM; `GetCallerIdentity` responde root | pendente (`InvalidClientTokenId`) | baixo | — |
| assinatura SigV4 | verificada só no S3; segredo errado aceito em DynamoDB e STS | pendente (rejeita) | alto: posse do segredo não é testável fora do S3 | check real |
| sessão sem token ou com token forjado | aceita fora do S3 | pendente (rejeita) | alto | check real |
| chave inativa | aceita fora do S3 | pendente (rejeita) | alto: revogação e fail-closed (spec ops §6) não testáveis | check real |
| dispatch: GetItem/UpdateItem por `LeadingKeys` | outbox e cursor permitidos; fence, semáforo e run negados | pendente (igual) | nenhum | — |
| dispatch: PutItem, DeleteItem, Scan, Query na base | negados | pendente (igual) | nenhum | — |
| Query no GSI exato (ARN `…/index/gsi6`) | negada: o Floci avalia o ARN da tabela | pendente (permitida) | política "exact-GSI" não validável | check real |
| Query de índice pelo ARN da tabela | permitida; `LeadingKeys` = partição do índice; `gsi1` também liberado | pendente (exige ARN do índice) | alto: over-permit no emulador | check real |
| transações | `TransactWriteItems` avaliada como ação única, sem `LeadingKeys`: dispatch não transaciona nem na partição permitida; Deny por `LeadingKeys` não bloqueia | pendente (ações por item com `LeadingKeys` e `dynamodb:EnclosingOperation`) | alto: guardrail contornável no emulador | check real |
| S3 por prefixo | dispatch grava, lê e estende retenção só em `audit/*`, sem delete, bypass, list, `raw/` ou `serving/`; API lê só `serving/*`, sem escrita; URL assinada 200 em `serving/`, 403 em `raw/` | pendente (igual) | nenhum | — |
| bucket policy | avaliada: Deny explícito vence Allow de identidade; Allow só na bucket policy concede | pendente (igual) | a doc do Floci dizia "não suportado"; não se confirma | — |
