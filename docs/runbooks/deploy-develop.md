# Deploy contínuo de `develop` — dev.cnesdata.vinisantana.com

## Visão geral

Todo push em `develop` que toca `apps/**`, `packages/**` ou `deploy/**` dispara
`.github/workflows/deploy-develop.yml`:

1. `gate` — reexecuta `ci.yml` (lint + testes + coverage) via `workflow_call`.
2. `build` — builda as 4 imagens Python/nginx em matriz e publica em
   `ghcr.io/viniclus/cnesdata/<app>:develop-<sha>` (+ tag móvel `develop`).
3. `deploy` — SSH com forced-command até a VPS; roda `docker compose pull`,
   `migrator`, `up -d`, aguarda o healthcheck do `central-api`. Falhou?
   restaura a tag anterior automaticamente.
4. `smoke` — `apps/web_dashboard/scripts/smoke.sh` + `GET /api/v1/system/health`.

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

# 3. Environment "develop" no GitHub (secrets + variables)
gh api -X PUT repos/VINIClUS/CnesData/environments/develop
gh secret set DEV_SSH_KEY --env develop < ~/.ssh/cnesdata_dev_ci
gh secret set DEV_SSH_KNOWN_HOSTS --env develop < <(ssh-keyscan -t ed25519 103.199.184.166)
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
