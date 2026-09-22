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
scp deploy/prod/docker-compose.prod.yml deploy/prod/caddy/Caddyfile \
  root@103.199.184.166:/opt/cnesdata/  # caddy/Caddyfile mantém os 2 vhosts

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

## Pré-requisito manual: migração MinIO → S3 real (PR storage)

`docker-compose.prod.yml` desta PR remove os serviços `minio`/`minio-init` inteiramente.
Como `deploy.sh` roda `up -d --remove-orphans`, o **primeiro** deploy pós-merge remove o
container `minio` em produção. O volume `minio_data` sobrevive à remoção do container
(orphan removal não apaga volumes) e fica órfão, mas persiste com os dados intactos até
alguém rodar `down -v` — a migração dos objetos não é destrutiva por si só, mas deve
acontecer antes do deploy de qualquer forma: depois do `up -d`, nenhum container fala mais
com o MinIO para servir os dados de lá.

Antes do primeiro `gh workflow run deploy-main.yml` com esta mudança:

1. Copiar objetos do bucket `cnesdata-landing` (MinIO atual) para o bucket S3 real via
   `mc mirror` ou `aws s3 sync` — **antes** do deploy, não como follow-up.
2. Adicionar em `/opt/cnesdata/.env` (chaves que não existem hoje, `docker-compose.prod.yml`
   passa a lê-las diretamente, sem indireção via `MINIO_ROOT_USER`/`PASSWORD` como em dev):
   `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_REGION` (`sa-east-1`), `S3_BUCKET`. Ver
   `deploy/prod/.env.example`. **Não** setar `S3_ENDPOINT_URL` em prod — vazio = S3 real.
3. `scp docker-compose.prod.yml` (passo já supervisionado — ver "Provisionamento único"
   acima) antes de disparar o workflow; ele não se autoatualiza no VPS.

## Pré-requisito manual: migração de domínio (vinisantana.com → cnesdata.com.br)

Esta PR troca hostnames hardcoded em `deploy/prod/caddy/Caddyfile`,
`.github/workflows/deploy-main.yml` (build-args OIDC, URL de smoke) e
`deploy/prod/.env.example` de `vinisantana.com` para `cnesdata.com.br`. **Nenhum desses
arquivos se autoaplica na VPS** — `deploy.sh` só troca `IMAGE_TAG`, nunca copia Caddyfile,
compose ou `.env`. Sem os passos abaixo, a imagem nova builda com
`VITE_OIDC_AUTHORITY=https://cnesdata.com.br/idp/realms/cnesdata` mas roda atrás do Caddy
antigo (ainda só serve `cnesdata.vinisantana.com`) e do Keycloak com client OIDC que só
autoriza o redirect URI antigo — login quebra com invalid redirect URI mesmo com toda a
infra HTTP funcionando.

Antes do primeiro `gh workflow run deploy-main.yml` com esta mudança:

1. `scp` o `Caddyfile` e o `docker-compose.prod.yml` novos para `/opt/cnesdata/` (mesmo
   passo supervisionado de "Provisionamento único"). **O Caddyfile já serve cada hostname
   novo junto com o antigo no mesmo bloco** (`cnesdata.com.br, cnesdata.vinisantana.com
   { ... }`) — nunca edite para substituir um pelo outro. Incidente real em 2026-09-22:
   fazer isso derrubou prod por ~30min (TLS handshake failure em todo request para
   `cnesdata.vinisantana.com`/`api.vinisantana.com`, sem bloco correspondente no Caddy).
   Só remover os hostnames antigos depois que `.env`, imagem deployada e DNS de prod
   também tiverem migrado.
2. Atualizar `/opt/cnesdata/.env`: `PUBLIC_DOMAIN=cnesdata.com.br`,
   `API_DOMAIN=api.cnesdata.com.br`,
   `DASHBOARD_OIDC_ISSUER=https://cnesdata.com.br/idp/realms/cnesdata`,
   `AUTH_DEVICE_VERIFICATION_URI=https://cnesdata.com.br/activate`.
3. **Migrar o client OIDC no realm do Keycloak prod pelo console** — o estado do realm
   vive no volume persistente `keycloak_data`; reiniciar o Keycloak com `--import-realm`
   **não substitui** um realm já importado. Adicionar (não substituir)
   `https://cnesdata.com.br/auth/callback` aos redirect URIs, `https://cnesdata.com.br`
   aos web origins, **e** `https://cnesdata.com.br/*` a
   `attributes["post.logout.redirect.uris"]` do client `cnesdata-dashboard` (Keycloak
   guarda post-logout separado de redirect URI — sem isso o login funciona mas o logout é
   rejeitado). Manter as entradas antigas até confirmar login **e logout** funcionando,
   depois remover. Sem o redirect URI, o dashboard novo recebe `invalid redirect_uri` do
   Keycloak mesmo com DNS/TLS/Caddy corretos.
4. `docker compose -f docker-compose.prod.yml exec caddy caddy reload --config
   /etc/caddy/Caddyfile` (não precisa recriar o container — `up -d caddy` só é necessário
   se a imagem/volumes mudaram) para o Caddy emitir os certs LE novos no primeiro
   request.
5. Só então disparar `deploy-main.yml`. Rodar
   `smoke.sh https://cnesdata.com.br https://api.cnesdata.com.br` logo depois.

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
essa mudança, copiar o compose e o Caddyfile atualizados e conferir
`smoke.sh https://cnesdata.com.br https://api.cnesdata.com.br`.

