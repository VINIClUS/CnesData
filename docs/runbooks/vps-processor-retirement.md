# Aposentadoria do data-processor na VPS (MIG-012, #278)

A partir do MIG-012 o write-fence é incondicional.

- O `central_api` responde 410 `legacy_ingestion_retired` em `/api/v1/jobs/upload-url`,
  `/api/v1/jobs/register`, `/api/v1/jobs/{job_id}/fail`, `/api/v1/extractions/enqueue` e
  `/api/v1/admin/reap-leases`, e não roda mais o reaper de leases.
- O `data_processor` sai com `profile_required` fora de `PROFILE=local|aws`.

A VPS não define `PROFILE`. Com a imagem nova, o serviço `data-processor` (`restart: on-failure`)
entraria em crash-loop. Por isso ele sai do Compose **antes** do deploy da imagem com o fence.

Este runbook não apaga dados legados:
- o MIG-013 (#279) atesta e faz o snapshot;
- o MIG-014 (#280) remove o runtime.

O deploy por forced-command (`/usr/local/bin/cnesdata-{prod,dev}-deploy`, cópia de
`deploy/{prod,dev}/deploy.sh`) só troca `IMAGE_TAG` e roda `pull`, `migrator` e
`up -d --remove-orphans`. Ele não sincroniza o Compose. Por isso a edição desta página é manual e
segue esta ordem:

| Stack | Diretório | Arquivo | Quando |
|---|---|---|---|
| dev | `/opt/cnesdata-dev` | `docker-compose.dev.yml` | logo antes do merge em `develop` |
| prod | `/opt/cnesdata` | `docker-compose.prod.yml` | com o `deploy-main` aguardando aprovação |

Nos comandos abaixo, use os valores da stack:
- `STACK`: o diretório da stack.
- `FILE`: o arquivo Compose da stack.
- `COMPOSE`: `docker compose --env-file .env --env-file .env.image -f $FILE`, executado em
  `$STACK`.

## 1. Quiesce

```bash
cd "$STACK"
$COMPOSE exec -T postgres sh -c 'psql -U "${POSTGRES_USER:-cnesdata}" \
  -d "${POSTGRES_DB:-cnesdata}" -At -c "select status, count(*) from landing.extractions
  group by 1 order by 1"'
```

Prossiga só com zero linhas nos estados não terminais: `PENDING`, `UPLOADED`,
`REGISTERED`, `CLAIMED` e `PROCESSING`. `CLAIMED` com lease vencido conta como trabalho
elegível a retry. Se houver linhas nesses estados, faça uma das duas coisas:
- aguarde o processor drenar;
- registre na issue a decisão de abandoná-las.

## 2. Backup

```bash
dest=/root/cnesdata-backups/$(basename "$STACK")-$(date -u +%Y%m%dT%H%M%SZ)
install -d -m 700 "$dest"
cp -p "$STACK/$FILE" "$STACK/.env" "$STACK/.env.image" "$dest/"
chmod 600 "$dest/.env"
```

## 3. Remover o serviço

Entre este passo e o deploy do passo 4, o `central_api` antigo ainda aceita as rotas legadas e
roda o reaper, sem worker para drenar. Por isso:
- rode este passo só com a imagem do passo 4 a caminho: em dev, logo antes do merge, com o CI
  da PR verde; em prod, com o job `deploy` do `deploy-main` aguardando aprovação;
- confirme que nenhum chamador alcança as rotas legadas no intervalo: zero certificados de agente
  ativos com `AGENT_MTLS_REQUIRED` ligado (as rotas de jobs exigem mTLS) e nenhum uso do
  `X-Admin-Token` até o fim do passo 4;
- no T0, repita a checagem do passo 1. Linha não terminal criada no intervalo é registrada na
  issue e abandonada.

1. Instale no host o Compose versionado da PR que remove o serviço, preservando dono e modo
   (`cnesdeploy` em dev, `cnesdeployprod` em prod; modo 644):

   ```bash
   install -o "$OWNER" -g "$OWNER" -m 644 /tmp/$FILE "$STACK/$FILE"
   ```

   Antes de instalar, confirme que o arquivo do host é idêntico ao versionado que está sendo
   substituído (`sha256sum`). Se não for, remova só o bloco `data-processor` à mão e registre a
   divergência.
2. Valide e aplique:

   ```bash
   cd "$STACK" && $COMPOSE config --quiet && $COMPOSE up -d --remove-orphans
   ```

3. Confira o resultado:
   - o container `data-processor` sumiu de `$COMPOSE ps`;
   - `central-api` continua `healthy`.

## 4. Deploy da imagem com o fence

- **dev:** merge em `develop`. O `Deploy develop` faz gate, deploy e smoke.
- **prod:**
  1. Faça o merge da PR de promoção `develop` → `main` e confirme que `main` contém o merge do
     MIG-012 (`git merge-base --is-ancestor <sha-do-merge> origin/main`).
  2. Rode `gh workflow run deploy-main.yml --ref main` e espere o build. O job `deploy` fica
     aguardando a aprovação do environment `production`.
  3. Rode o passo 3 nesse momento.
  4. Aprove o environment `production`.

Depois do deploy, confirme no host que a tag nova está ativa (`cat .env.image`, `$COMPOSE ps`). Se
o `deploy.sh` restaurou a tag anterior sozinho (health do `central-api` falhou), o fence **não**
está ativo. Nesse caso, corrija e refaça o deploy antes de coletar o T0.

Smoke do fence:
- As rotas da lista acima respondem 410 `legacy_ingestion_retired` a um chamador autenticado.
  Use, por exemplo, `POST /api/v1/admin/reap-leases` com `X-Admin-Token` lido do ambiente do
  container, sem ecoar o token.
- Nenhum `extractions_repo` é tocado.

## 5. Marco T0 (evidência)

Com o serviço removido e a imagem com o fence ativa, registre na #278:
- o horário UTC;
- `pg_current_snapshot()` e `pg_current_wal_lsn()`;
- `n_tup_ins`, `n_tup_upd` e `n_tup_del` de `pg_stat_user_tables` para os schemas `landing` e
  `gold`;
- contagens, `max(created_at)` e `max(registered_at)` de `landing.extractions`, e zero linhas
  nos estados não terminais do passo 1;
- o prazo das URLs de upload legadas. Cada URL nasceu com uma linha nova em
  `landing.extractions` e vale 3600 s, e o fence não revoga URL já emitida. A listagem do
  storage legado só entra no T0 depois de `max(created_at)` + 1 h. Se o prazo ainda não passou,
  espere-o antes de listar;
- o último `LastModified` e o total de objetos do storage legado de landing, sem imprimir
  segredos:
  - dev: bucket do AIStor, listado de dentro do container `central-api` com as credenciais que
    ele já tem;
  - prod: bucket S3 `cnesdata-landing`. As credenciais do storage legado na VPS não têm
    `s3:ListBucket` nele (`AccessDenied` em 2026-10-11), então a listagem exige uma identidade
    que não seja root e tenha esse direito. Sem ela, registre a prova indireta: zero linhas em
    `landing.extractions` e o presign cercado, já que todo upload legado dependia do presign de
    `/api/v1/jobs/upload-url`.

O `xact_commit` do banco continua subindo, porque auth, leads e health ainda escrevem em `public`.
Por isso a prova de ausência de escrita usa os contadores por tabela de `landing` e `gold`.

## 6. Janela de observação (≥ 1 h)

Repita a coleta do passo 5. O critério de aceite tem três partes:
- delta zero de inserções, atualizações e remoções em `landing.*` e `gold.*`;
- nenhum objeto novo no storage legado;
- logs do `central-api` só com 410 nas rotas legadas.

Os contadores de `pg_stat` zeram se o Postgres reiniciar. Nesse caso, compare as contagens e os
`max(created_at)`/`max(registered_at)`, e registre o reinício.

Publique T0, T1 e o veredito na #278.

## Rollback

- **Compose:** nunca restaure o arquivo do passo 2. Ele ainda define o `data-processor`, e o
  `up -d` recriaria o worker com escrita legada em `landing.*` e `gold.*`. O backup serve só
  para auditoria e `diff`. Se o passo 3 falhar, corrija para frente a partir do Compose
  versionado, sem o serviço.
- **Imagem:** volte só para uma tag que já tenha o fence (a do merge do MIG-012 em `develop` ou
  posterior). Troque só a tag e mantenha o Compose atual, o mesmo caminho do rollback
  automático do `deploy.sh`:

  ```bash
  cd "$STACK" && echo "IMAGE_TAG=<tag-com-fence>" > .env.image
  $COMPOSE pull && $COMPOSE up -d --remove-orphans
  ```

  Nunca volte manualmente para uma tag anterior ao fence: ela reabre as rotas legadas e o reaper
  no `central_api`. Se nenhuma tag com o fence funcionar, corrija para frente. Na primeira
  implantação do fence, o rollback automático do `deploy.sh` volta sozinho para a tag anterior,
  sem o fence (passo 4). Trate isso como incidente: corrija e refaça o deploy antes de qualquer
  outra ação, e colete o T0 de novo.
- **Escritas legadas:** nunca reative. Ou seja:
  - não religue o `data-processor`;
  - não crie flag de runtime que desfaça o fence.
