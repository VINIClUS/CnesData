# Deploy contínuo de `develop` — dev.cnesdata.vinisantana.com

## Visão geral

Todo push em `develop` que toca `apps/**`, `packages/**` ou `deploy/**` dispara
`.github/workflows/deploy-develop.yml`:

1. `gate` — reexecuta `ci.yml` (lint + testes + coverage) via `workflow_call`.
2. `build` — builda as 4 imagens Python/nginx em matriz e publica em
   `ghcr.io/viniclus/cnesdata/<app>:develop-<sha>` (+ tag móvel `develop`).
3. `deploy` — roda no runner self-hosted `runner-cnes-dev` (homelab Proxmox,
   labels `self-hosted, cnesdata, deploy-dev`); dali dispara SSH com
   forced-command até a VPS, que roda `docker compose pull`, `migrator`,
   `up -d`, aguarda o healthcheck do `central-api`. Falhou? restaura a tag
   anterior automaticamente.
4. `smoke` — `apps/web_dashboard/scripts/smoke.sh` + `GET /api/v1/system/health`
   (permanece em `ubuntu-latest`: precisa validar a URL pública de fora da rede).

`gate`, `build` e `smoke` continuam em runners hospedados do GitHub — só
`deploy` usa o self-hosted, e só é alcançado por `push`/`workflow_dispatch`
em `develop`, nunca por `pull_request`.

O ambiente roda ao lado do stack de produção (`main`) na mesma VPS
(`103.199.184.166`), em `/opt/cnesdata-dev`, com Postgres/MinIO/Keycloak
próprios. Só o Caddy é compartilhado — ver `docs/architecture.md` e o
compose em `deploy/dev/docker-compose.dev.yml`.

## Provisionamento único (já feito, referência)

```bash
# 1. Gerar a chave SSH da CI (uma vez, localmente)
ssh-keygen -t ed25519 -f ~/.ssh/cnesdata_dev_ci -C cnesdata-dev-ci -N ""

# 2. Copiar deploy/dev/*.sh para a VPS e rodar como root
#    GHCR_USER/GHCR_PAT são opcionais — só necessários se os packages
#    ghcr.io/viniclus/cnesdata/* forem privados. Preferimos torná-los
#    públicos após o primeiro push a reter um PAT novo na VPS.
scp deploy/dev/{bootstrap.sh,deploy.sh,docker-compose.dev.yml} root@103.199.184.166:/root/deploy-dev/
ssh root@103.199.184.166 '
  CI_PUBLIC_KEY="'"$(cat ~/.ssh/cnesdata_dev_ci.pub)"'" \
    bash /root/deploy-dev/bootstrap.sh
'

# 3. Instalar a chave privada no runner self-hosted (homelab Proxmox), NÃO
#    em secrets do GitHub — o job `deploy` roda nesse runner, não numa VM
#    hospedada descartável.
ssh <runner-cnes-dev-host> '
  install -d -m 700 /etc/cnesdata
  install -m 600 /dev/stdin /etc/cnesdata/deploy-dev.key < ~/.ssh/cnesdata_dev_ci
  ssh-keyscan -t ed25519 103.199.184.166 > /etc/cnesdata/known_hosts
  chmod 600 /etc/cnesdata/known_hosts
'

# 4. Environment "develop" no GitHub — só variáveis (não-segredo); a chave
#    não passa mais por secrets do GitHub.
gh api -X PUT repos/VINIClUS/CnesData/environments/develop
gh variable set DEV_SSH_HOST --env develop --body 103.199.184.166
gh variable set DEV_SSH_USER --env develop --body cnesdeploy
```

## Caddy de produção — vhost dev (ajuste manual, feito uma vez)

Backup antes de editar, e conferir produção depois de cada passo:

```bash
ssh root@103.199.184.166 '
  cd /opt/cnesdata &&
  cp docker-compose.prod.yml docker-compose.prod.yml.bak &&
  cp caddy/Caddyfile caddy/Caddyfile.bak
'
```

Em `docker-compose.prod.yml`, serviço `caddy` — declarar as duas redes
(ao declarar `networks:` num serviço a rede `default` implícita some, por
isso precisa ser listada):

```yaml
  caddy:
    ...
    networks: [default, cnesdata_edge]

networks:
  cnesdata_edge:
    external: true
```

Em `caddy/Caddyfile`, acrescentar um bloco novo sem tocar no existente:

```
dev.cnesdata.vinisantana.com {
    handle /idp/* {
        reverse_proxy dev-keycloak:8080
    }
    handle {
        reverse_proxy dev-web-dashboard:80
    }
}
```

Aplicar e validar produção:

```bash
ssh root@103.199.184.166 'cd /opt/cnesdata && docker compose -f docker-compose.prod.yml up -d caddy'
curl -I https://cnesdata.vinisantana.com   # deve continuar 200
```

## Operação

### Deploy manual / redeploy de uma tag específica

```bash
gh workflow run deploy-develop.yml -f tag=develop-<sha7>
```

O job `deploy` só executa quando `runner-cnes-dev` (Proxmox) está online; se
ficar offline, o job fica `queued` e falha após `timeout-minutes: 15` em vez
de travar. Saída de emergência sem o runner (chave local de operador):

```bash
ssh -i ~/.ssh/cnesdata_dev_ci cnesdeploy@103.199.184.166 develop-<sha7>
```

### Rollback

Mesmo comando acima com a tag anterior conhecida-boa. O `deploy.sh` também
faz rollback automático se o healthcheck do `central-api` não passar em
120s após um deploy.

### Logs

```bash
ssh root@103.199.184.166 'tail -100 /var/log/cnesdata-dev-deploy.log'
ssh root@103.199.184.166 'cd /opt/cnesdata-dev && docker compose -f docker-compose.dev.yml logs -f central-api'
```

### Recriar o ambiente do zero

```bash
ssh root@103.199.184.166 'cd /opt/cnesdata-dev && docker compose -f docker-compose.dev.yml down -v'
# .env e secrets/ca.* NÃO são apagados por bootstrap.sh — remova manualmente
# se quiser segredos novos, depois rode bootstrap.sh de novo.
```

### Se o Caddy do main cair

O vhost de dev depende do container `caddy` do stack **main**. Se ele cair,
suba-o normalmente (`docker compose -f /opt/cnesdata/docker-compose.prod.yml
up -d caddy`); isso não afeta os containers de dev, que continuam rodando —
só ficam sem HTTPS/roteamento até o Caddy voltar.

## Pendências conhecidas (fora do escopo desta entrega)

- Firewall Hostinger (grupo `358236`) tem uma regra `TCP any/any` liberada —
  hardening recomendado, não incluído aqui.
- Conta AWS opera com chaves de **root**; recomendo criar um IAM user
  dedicado e remover as chaves de root pelo console (a CLI não remove
  chaves da própria conta root).

## Host da API (`api.dev.vinisantana.com`)

Desde a entrega das páginas públicas o dashboard de dev é compilado com
`VITE_API_BASE_URL=https://api.dev.vinisantana.com/api/v1` (build-arg em
`deploy-develop.yml`), portanto o navegador fala com a API cross-origin. Três peças
precisam estar alinhadas, todas versionadas em `deploy/`:

1. **DNS**: registro A de `api.dev.vinisantana.com` apontando para o VPS (Hostinger DNS,
   fora do repo). Caddy emite o certificado automaticamente na primeira requisição.
2. **Caddy** (`deploy/prod/caddy/Caddyfile`, copiado manualmente para
   `/opt/cnesdata/caddy/Caddyfile`): bloco `api.dev.vinisantana.com` → `dev-central-api:8000`,
   só `/api/*`; o resto responde 404. Aplicar com
   `docker compose -f docker-compose.prod.yml up -d caddy` em `/opt/cnesdata`.
3. **Compose dev** (`deploy/dev/docker-compose.dev.yml`, copiado para `/opt/cnesdata-dev`):
   `central-api` entra na rede `cnesdata_edge` com alias `dev-central-api` e recebe
   `CORS_ALLOWED_ORIGINS=https://dev.cnesdata.vinisantana.com`; `dashboard` recebe
   `API_ORIGIN=https://api.dev.vinisantana.com` (CSP `connect-src`) e `PRECOS_NOINDEX`.

Validação (também no job `smoke`): `apps/web_dashboard/scripts/smoke.sh
https://dev.cnesdata.vinisantana.com https://api.dev.vinisantana.com` confere health no host
da API, `connect-src` na CSP, preflight CORS aceito só para a origem do dashboard e `/docs`
inacessível pelo host público.
