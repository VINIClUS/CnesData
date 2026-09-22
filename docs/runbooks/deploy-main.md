# Deploy de `main` — cnesdata.com.br

## Visão geral

`.github/workflows/deploy-main.yml` promove uma imagem para produção. Ao
contrário de `deploy-develop.yml`, **não dispara em `push`** — só em
`workflow_dispatch`, com aprovação manual obrigatória (`environment:
production` tem required reviewer). Nenhum deploy de produção acontece
automaticamente após um merge.

1. `gate` — reexecuta `ci.yml` via `workflow_call` (pulado se `tag` for
   informado no dispatch).
2. `build` — builda as 4 imagens em matriz e publica em
   `ghcr.io/viniclus/cnesdata/<app>:main-<sha>` (+ tag móvel `main`).
3. `deploy` — aguarda aprovação do reviewer do environment `production`,
   depois roda no runner self-hosted `runner-cnes-prod` (homelab Proxmox,
   labels `self-hosted, cnesdata, deploy-prod`); dali dispara SSH com
   forced-command até a VPS, que roda `docker compose pull`, `migrator`,
   `up -d`, aguarda o healthcheck do `central-api`. Falhou? restaura a tag
   anterior automaticamente.
4. `smoke` — `apps/web_dashboard/scripts/smoke.sh` + `GET /api/v1/system/health`
   (`ubuntu-latest`: valida a URL pública de fora da rede).

O stack roda em `/opt/cnesdata`, na mesma VPS do staging
(`103.199.184.166`), com Postgres/MinIO/Keycloak próprios. Só o Caddy é
compartilhado — o mesmo container que serve produção também serve
`dev.cnesdata.com.br` (ver `deploy/prod/caddy/Caddyfile` e
`docs/runbooks/deploy-develop.md`).

## Migração do modelo antigo (contexto histórico)

Até 2026-09-18, produção era implantada manualmente: código copiado
(git-archive, não clone) para `/opt/cnesdata/src` e imagens buildadas
localmente na VPS (`docker compose build`), sem tag versionada e sem
GHCR. `deploy/prod/` traz produção para o mesmo modelo de `deploy/dev/` —
pull de imagem por tag, com rollback automático. `deploy/prod/docker-compose.prod.yml`
e `deploy/prod/caddy/Caddyfile` neste repo são a captura do que já rodava
na VPS em 2026-09-18, com `build:` trocado por `image: ...:${IMAGE_TAG}`.

## Provisionamento único (referência)

```bash
# 1. Gerar a chave SSH da CI (uma vez, localmente)
ssh-keygen -t ed25519 -f ~/.ssh/cnesdata_prod_ci -C cnesdata-prod-ci -N ""

# 2. Copiar deploy/prod/{bootstrap.sh,deploy.sh} para a VPS e rodar como root.
#    Diferente do bootstrap de dev, este NÃO cria .env, CA nem stack
#    dir — /opt/cnesdata já existe e está servindo tráfego real.
scp deploy/prod/{bootstrap.sh,deploy.sh} root@103.199.184.166:/root/deploy-prod/
ssh root@103.199.184.166 '
  CI_PUBLIC_KEY="'"$(cat ~/.ssh/cnesdata_prod_ci.pub)"'" \
    bash /root/deploy-prod/bootstrap.sh
'

# 3. Trocar docker-compose.prod.yml (build local → pull GHCR) — passo
#    supervisionado, não automatizado. Ver "Migração" acima.
scp deploy/prod/docker-compose.prod.yml root@103.199.184.166:/opt/cnesdata/
# NÃO faça `scp deploy/prod/caddy/Caddyfile` — o arquivo vivo em
# /opt/cnesdata/caddy/Caddyfile carrega vhosts de um site de produção não
# relacionado que compartilha o mesmo container Caddy; um scp sobrescreve e
# derruba esse outro site. Editar in-place + diff + `caddy validate` +
# `caddy reload` — ver "Migração de domínio" abaixo para o procedimento
# completo (mesmo destino explícito .../caddy/Caddyfile importa lá: um scp
# para /opt/cnesdata/ sozinho copiaria para /opt/cnesdata/Caddyfile, fora do
# mount, e o reload seguinte não aplicaria nada — Codex P1, PR #243).

# 4. Instalar a chave privada no runner self-hosted (homelab Proxmox), NÃO
#    em secrets do GitHub.
ssh <runner-cnes-prod-host> '
  install -d -m 700 /etc/cnesdata
  install -m 600 /dev/stdin /etc/cnesdata/deploy-prod.key < ~/.ssh/cnesdata_prod_ci
  ssh-keyscan -t ed25519 103.199.184.166 >> /etc/cnesdata/known_hosts
  chmod 600 /etc/cnesdata/known_hosts
'

# 5. Environment "production" no GitHub — required reviewer + branch policy
#    já configurados via API; só as variáveis (não-segredo) faltam.
gh variable set PROD_SSH_HOST --env production --body 103.199.184.166
gh variable set PROD_SSH_USER --env production --body cnesdeployprod
```

## Operação

### Primeiro deploy pós-migração

Publicar `main-<sha atual>` a partir das imagens já capturadas e implantar,
para confirmar que o pull funciona antes de depender dele:

```bash
gh workflow run deploy-main.yml
gh run watch "$(gh run list -w deploy-main.yml -L1 --json databaseId -q '.[0].databaseId')"
```

O workflow para no gate de aprovação do environment `production` — aprove
manualmente antes do job `deploy` rodar.

### Deploy manual / redeploy de uma tag específica

```bash
gh workflow run deploy-main.yml -f tag=main-<sha7>
```

O job `deploy` só executa quando `runner-cnes-prod` (Proxmox) está online;
se ficar offline, o job fica `queued` e falha após `timeout-minutes: 15`.
Saída de emergência sem o runner (chave local de operador):

```bash
ssh -i ~/.ssh/cnesdata_prod_ci cnesdeployprod@103.199.184.166 main-<sha7>
```

### Rollback

Mesmo comando acima com a tag anterior conhecida-boa. O `deploy.sh` também
faz rollback automático se o healthcheck do `central-api` não passar em
120s após um deploy.

### Logs

```bash
ssh root@103.199.184.166 'tail -100 /var/log/cnesdata-prod-deploy.log'
ssh root@103.199.184.166 'cd /opt/cnesdata && docker compose -f docker-compose.prod.yml logs -f central-api'
```

### Se o Caddy cair

O mesmo container `caddy` serve produção **e** o vhost de dev
(`dev.cnesdata.com.br`). Subir de novo com:

```bash
ssh root@103.199.184.166 'cd /opt/cnesdata && docker compose -f docker-compose.prod.yml up -d caddy'
```

## Migração MinIO → S3 real (concluída em 2026-09-22)

`docker-compose.prod.yml` removeu os serviços `minio`/`minio-init` inteiramente. Como
`deploy.sh` roda `up -d --remove-orphans`, o deploy que aplicou essa mudança removeu o
container `minio` em produção — o volume `minio_data` sobreviveu órfão (orphan removal não
apaga volumes; nenhum dado real existia nele, prod nunca tinha ido ao ar). Sequência
usada, para uma migração equivalente futura:

1. Provisionar o bucket S3 real e o IAM user **antes** do deploy — ver
   "Provisionamento do bucket S3 (prod)" abaixo. Não existia runbook para isso; passo
   feito manualmente via `aws` CLI no cutover de domínio.
2. Adicionar em `/opt/cnesdata/.env` (chaves que não existiam antes, `docker-compose.prod.yml`
   passa a lê-las diretamente, sem indireção via `MINIO_ROOT_USER`/`PASSWORD` como em dev):
   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_REGION` (`sa-east-1`), `S3_BUCKET`. Ver
   `deploy/prod/.env.example`. **Não** setar `S3_ENDPOINT_URL` em prod — vazio = S3 real.
3. `scp docker-compose.prod.yml` (passo já supervisionado — ver "Provisionamento único"
   acima) antes de disparar o workflow; ele não se autoatualiza no VPS.

**Bug conhecido (não bloqueou o cutover, corrigido separadamente):** com
`S3_ENDPOINT_URL` vazio e `S3_ADDRESSING_STYLE` no default (`auto`), o boto3 assinava
presigns contra o endpoint global (`s3.amazonaws.com`) em vez do regional — S3 devolvia
`307 TemporaryRedirect` em todo PUT para um bucket fora de `us-east-1`. Corrigido em
`packages/cnes_infra/src/cnes_infra/storage/s3_presigned.py` (`build_s3_client` força
`addressing_style="virtual"` quando não há `endpoint_url`); ver o commit que introduziu
esta seção do runbook.

## Provisionamento do bucket S3 (prod)

Conta AWS de produção, região `sa-east-1`:

```bash
aws s3api create-bucket --bucket cnesdata-landing --region sa-east-1 \
  --create-bucket-configuration LocationConstraint=sa-east-1
aws s3api put-public-access-block --bucket cnesdata-landing \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket cnesdata-landing \
  --server-side-encryption-configuration '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
aws iam create-user --user-name cnesdata-prod
aws iam put-user-policy --user-name cnesdata-prod --policy-name cnesdata-landing-rw \
  --policy-document file://cnesdata-prod-s3-policy.json  # ver policy mínima abaixo
aws iam create-access-key --user-name cnesdata-prod
```

Policy mínima — só `Get`/`Put` em `cnesdata-landing/*`, sem `s3:ListBucket` nem
`s3:DeleteObject`. Acesso é sempre por URL presignada; a role da aplicação nunca
lista/cria buckets, e `object_exists` (`s3_presigned.py`) já trata o 403 que
`head_object` devolve para uma chave ausente sem `s3:ListBucket` na raiz. O único
adapter S3 conectado a essa credencial em prod é `S3PresignedStorage`
(`central_api/deps.py:get_object_storage`), que não expõe delete — o `.delete()` de
`ObjectStorePort`/`S3ObjectStore` é de um port separado, hoje ligado a
`FilesystemObjectStore`, não a esse bucket. Conceder list/delete além do necessário só
aumenta o raio de dano de uma credencial comprometida (enumerar ou apagar objetos de
qualquer tenant):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Sid": "ObjectRW", "Effect": "Allow",
     "Action": ["s3:GetObject", "s3:PutObject"],
     "Resource": "arn:aws:s3:::cnesdata-landing/*"}
  ]
}
```

Antes de colocar a access key em `/opt/cnesdata/.env`, validar put→presign→GET
localmente (sem depender do container) — pega erro de policy/região antes do deploy,
não no healthcheck. **Não** inclua delete nessa validação: a policy acima
deliberadamente não concede `s3:DeleteObject`, então um `delete_object` com essa
credencial falha por design, não por policy errada. Limpar o objeto de teste com uma
credencial administrativa separada (ex.: a sua própria, via `aws s3api delete-object`).

## Migração de domínio (vinisantana.com → cnesdata.com.br, concluída em 2026-09-22)

Trocou hostnames hardcoded em `deploy/prod/caddy/Caddyfile`,
`.github/workflows/deploy-main.yml` (build-args OIDC, URL de smoke) e
`deploy/prod/.env.example` de `vinisantana.com` para `cnesdata.com.br`. **Nenhum desses
arquivos se autoaplica na VPS** — `deploy.sh` só troca `IMAGE_TAG`, nunca copia Caddyfile,
compose ou `.env`. Sem os passos abaixo, a imagem nova builda com
`VITE_OIDC_AUTHORITY=https://cnesdata.com.br/idp/realms/cnesdata` mas roda atrás do Caddy
antigo (ainda só serve `cnesdata.vinisantana.com`) e do Keycloak com client OIDC que só
autoriza o redirect URI antigo — login quebra com invalid redirect URI mesmo com toda a
infra HTTP funcionando. Sequência usada, para uma migração de domínio equivalente futura:

1. **Nunca `scp deploy/prod/caddy/Caddyfile` para a VPS.** O arquivo vivo em
   `/opt/cnesdata/caddy/Caddyfile` tem, depois do bloco `storage.dev.cnesdata.com.br`, três
   vhosts de um site de produção não relacionado (`limnopulse.com` e afins) que compartilha
   o mesmo container Caddy — um `scp` sobrescreve e derruba esse outro site. Editar
   in-place na VPS, sempre conferindo com `diff` contra o arquivo do repo antes (a única
   diferença esperada é esse apêndice). `docker-compose.prod.yml` **é** seguro de `scp` —
   o limnopulse usa seu próprio compose file.

   **`caddy validate`/`caddy reload` reportando sucesso não prova que o container aplicou
   o conteúdo novo.** `docker-compose.prod.yml` bind-monta o arquivo como
   `./caddy/Caddyfile:/etc/caddy/Caddyfile:ro` — um bind mount de **arquivo único**. Se a
   edição usa `sed -i` (comportamento GNU padrão) ou qualquer editor que salva por
   write-temp+rename, o inode antigo fica órfão: o mount do container continua preso a
   ele, `caddy validate`/`reload` seguem lendo e validando esse conteúdo velho (que
   também é sintaticamente válido, então "sucesso" não distingue nada), e o container só
   veria o arquivo novo numa recriação futura. Reproduzido ao vivo em 2026-09-22: o
   container tinha 4 dias rodando (`docker inspect` mostrando `Created` de dias atrás),
   `stat` dentro do container mostrava um inode diferente do arquivo no host mesmo depois
   de reescrever com `cat old > Caddyfile` (truncate-in-place não ajudou — o mount já
   estava desconectado do path havia dias, de uma edição anterior). Procedimento correto:
   1. Editar o arquivo (qualquer método).
   2. Conferir de **dentro do container**, não só no host:
      `docker compose --env-file .env --env-file .env.image -f docker-compose.prod.yml
      exec caddy stat -c "%i" /etc/caddy/Caddyfile` — comparar com `stat -c "%i"` do
      arquivo no host. Inodes diferentes = mount desatualizado, `validate`/`reload`
      não vão ajudar.
   3. Se os inodes divergirem, recriar o container (única forma de recarregar um bind
      mount de arquivo único): `docker compose --env-file .env --env-file .env.image
      -f docker-compose.prod.yml up -d --force-recreate caddy` — os dois `--env-file`
      são obrigatórios (`IMAGE_TAG` vive só em `.env.image`; sem eles o compose falha
      resolvendo a imagem do `cnes_db_migrator`, não só do caddy). Recriar o container
      derruba prod/dev/limnopulse por ~1-2s (mesmo container compartilhado) — aceitável,
      já é o comando de recuperação documentado em "Se o Caddy cair" acima.
   4. Só então `caddy validate` + `caddy reload`, e verificar o conteúdo servido de
      dentro do container (`exec caddy grep ...`), não só via `curl` no domínio (DNS
      pode já estar apontando para outro lugar ou já ter sido removido, mascarando se o
      Caddy em si está correto).
2. Durante a janela de transição, servir cada hostname novo junto com o antigo no mesmo
   bloco (`cnesdata.com.br, cnesdata.vinisantana.com { ... }`) — nunca editar para
   substituir um pelo outro nesse meio-tempo. Incidente real em 2026-09-22: fazer isso
   antes do restante da migração estar pronto derrubou prod por ~30min (TLS handshake
   failure em todo request para `cnesdata.vinisantana.com`/`api.vinisantana.com`, sem
   bloco correspondente no Caddy). Remover os hostnames antigos só depois que `.env`,
   imagem deployada e DNS de prod também tiverem migrado — feche essa janela rápido
   (ver ponto 6): uma vez a imagem nova no ar, o hostname legado ainda resolvendo serve o
   build novo e quebra login iniciado por lá (state OIDC guardado na origem errada).
3. Atualizar `/opt/cnesdata/.env`: `PUBLIC_DOMAIN=cnesdata.com.br`,
   `API_DOMAIN=api.cnesdata.com.br`,
   `DASHBOARD_OIDC_ISSUER=https://cnesdata.com.br/idp/realms/cnesdata`,
   `AUTH_DEVICE_VERIFICATION_URI=https://cnesdata.com.br/activate`.
4. **Migrar o client OIDC no realm do Keycloak prod pela Admin API/console** — o estado do
   realm vive no volume persistente `keycloak_data`; reiniciar o Keycloak com
   `--import-realm` **não substitui** um realm já importado. Adicionar (não substituir)
   `https://cnesdata.com.br/*` aos redirect URIs, `https://cnesdata.com.br` aos web
   origins, **e** `https://cnesdata.com.br/*` a `attributes["post.logout.redirect.uris"]`
   do client `cnesdata-dashboard` — esse último campo é uma **string única separada por
   `##`**, não uma lista JSON (Keycloak guarda post-logout separado de redirect URI — sem
   isso o login funciona mas o logout é rejeitado). Manter as entradas antigas até
   confirmar login **e logout** funcionando, depois remover. Sem o redirect URI, o
   dashboard novo recebe `invalid redirect_uri` do Keycloak mesmo com DNS/TLS/Caddy
   corretos.
5. `docker compose -f docker-compose.prod.yml exec caddy caddy reload --config
   /etc/caddy/Caddyfile` (não precisa recriar o container — `up -d caddy` só é necessário
   se a imagem/volumes mudaram) para o Caddy emitir os certs LE novos no primeiro
   request.
6. Só então disparar `deploy-main.yml`. Rodar
   `smoke.sh https://cnesdata.com.br https://api.cnesdata.com.br` logo depois, e conferir
   o header `content-security-policy` da resposta — `connect-src` só deve citar os hosts
   novos; enquanto citar os antigos, a imagem no ar ainda é a pré-cutover.
7. Assim que o smoke passar (ver ponto 2): remover DNS legado primeiro. Nessa ordem —
   Caddy antes do DNS reproduz o incidente de 2026-09-22 (bloco removido, nome ainda
   resolvendo). Mas apagar o registro **não** faz resolvers recursivos/clientes pararem
   de resolver o hostname na hora — eles podem reter a resposta em cache até o TTL
   anterior do registro expirar. Só remover o bloco do Caddy depois de esperar pelo
   menos esse TTL **e** confirmar via um resolvedor público
   (`dig @1.1.1.1 <host-legado>` sem retornar nada) — removê-lo antes disso reproduz o
   mesmo TLS handshake failure para os clientes com cache ainda válido. Keycloak por
   último, depois de Caddy. Atualizar também `RELEASES_PUBLIC_BASE_URL` (GitHub Actions
   variable) e os manifestos publicados em R2 que tiverem URL absoluta para o host
   antigo (ver `docs/runbooks/dumpagent-release.md`).

## Pendências conhecidas (fora do escopo desta entrega)

- `src/` antigo em `/opt/cnesdata` (código copiado manualmente, usado pelo
  modelo de build local) fica obsoleto após a migração — remover depois de
  confirmar alguns deploys via GHCR bem-sucedidos.
- Variável `GESTOR_PASSWORD` existe em `/opt/cnesdata/.env` mas não é
  referenciada por `docker-compose.prod.yml` — origem não identificada
  nesta migração; investigar antes de removê-la.

## Host da API (`api.cnesdata.com.br`)

Mesma estrutura do dev (ver `deploy-develop.md`, seção "Host da API"): DNS A para o VPS,
bloco `api.cnesdata.com.br` no Caddyfile apontando para `central-api:8000` (só `/api/*`),
`CORS_ALLOWED_ORIGINS=https://${PUBLIC_DOMAIN}` no `central-api` e `API_ORIGIN=https://${API_DOMAIN}`
no `web-dashboard` (definir `API_DOMAIN` e `PRECOS_NOINDEX` em `/opt/cnesdata/.env`, ver
`deploy/prod/.env.example`). A imagem do dashboard de `main` é compilada com
`VITE_API_BASE_URL=https://api.cnesdata.com.br/api/v1`. Antes do primeiro deploy de `main` com
essa mudança, `scp` o compose atualizado e aplicar o Caddyfile atualizado (edição in-place
na VPS, nunca `scp` — ver "Migração de domínio" acima) e conferir
`smoke.sh https://cnesdata.com.br https://api.cnesdata.com.br`.

