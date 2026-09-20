# Profile Local (SQLite + filesystem, sem Postgres/MinIO/Keycloak/AWS)

## Visão Geral

`--profile local` sobe `central-api-local` + `data-processor-local` +
`web-dashboard-local` contra um único volume nomeado (`local_data`), sem
nenhuma dependência de infra externa. Control plane e credenciais vivem em
SQLite (`state/cnesdata.sqlite3`); objetos (artefatos raw/serving) em
filesystem (`objects/`); auditoria em JSONL + índice SQLite derivável
(`audit/`). Layout completo: `docs/architecture.md#docker-compose-local`.

## Bring-up

```bash
docker compose --profile local up -d --build
```

O volume `local_data` é inicializado pelo Docker a partir do `/data`
já criado e `chown`ado para o usuário `app` em ambos os Dockerfiles — não é
necessário `mkdir`/`chmod` manual no host.

## Primeiro usuário

Um `data_dir` novo só recebe a linha `Tenant` no boot; nenhum login é
possível até existir usuário + membership. Bootstrap roda dentro do
container, como o usuário `app`:

```bash
docker compose exec central-api-local \
  python -m central_api.bootstrap --email gestor@epitacio.sp.gov.br
```

Pede senha via prompt (ou defina `LOCAL_BOOTSTRAP_PASSWORD` no ambiente do
container — nunca em argv). `--user-id` e `--role` são opcionais
(`user-1` / `gestor` por padrão). Idempotente: reexecutar com o mesmo
`--user-id` atualiza a credencial em vez de duplicar.

Login em `http://localhost:5173`.

## Backup

```bash
docker compose exec central-api-local \
  python -m central_api.local_backup create --target /data/backups/b1.tar
docker cp cnesdata_central_api_local:/data/backups/b1.tar ./b1.tar
```

O `docker cp` para fora do container é obrigatório: um backup que só existe
dentro do volume que ele protege não sobrevive à perda desse volume.

## Restore

**Precisa da stack parada.** `central-api-local` e `data-processor-local`
semeiam o tenant no boot (`_seed_tenant`, roda a cada start), então um
`docker compose exec` contra um container já em execução sempre encontra o
`data_dir` não-vazio e falha com `target_not_empty`. O restore roda como um
container avulso, com o entrypoint (`uvicorn`) trocado pelo próprio CLI, para
nunca disparar essa seed:

```bash
docker compose --profile local down -v
docker compose --profile local run --rm --no-deps \
  --entrypoint python \
  --volume "$PWD/b1.tar:/restore.tar:ro" \
  central-api-local -m central_api.local_backup restore --archive /restore.tar
docker compose --profile local up -d
```

`restore_backup` verifica hash de cada arquivo e o `tenant_id` antes de
publicar — nada é escrito se qualquer verificação falhar. Um restore
interrompido no meio deixa um diretório `.<token>.restore` dentro de
`data_dir`; a próxima tentativa recusa `target_not_empty` até um novo
`down -v`.

## Pendências conhecidas (fora do escopo desta entrega)

- **`LocalWorkerPool._unit_execution_forbidden` em `central_api` sempre
  levanta.** É intencional: `central_api` só planeja/despacha
  (`RunPlanningService`), nunca executa units — quem executa é o
  `PipelineCoordinator` do `data_processor`. Não é um bug a corrigir.
- **`restore_backup` alarga permissões para `0o666`/`0o777`
  (`_widen_permissions`).** Defensivo: o fluxo documentado acima já roda
  dentro do container, como `app`, e não depende disso. Mantido para
  qualquer restore executado por um principal com UID diferente do `app`
  — permissão `other` é a única garantia robusta de acesso cross-UID
  nesse cenário.
