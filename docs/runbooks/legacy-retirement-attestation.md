# Atestado de aposentadoria do legado e snapshot (MIG-013, #279)

Antes da remoção do runtime legado (MIG-014, #280), registre três coisas:
- **Atestado.** O Postgres legado (Gold/landing) e o storage legado de landing não guardam histórico
  real a preservar.
- **Snapshot.** Um `pg_dump` imutável de cada stack da VPS, guardado no object store alvo.
- **Verificação.** O hash do snapshot, conferido em outro processo com credenciais só do alvo.

O exportador Parquet do plano original saiu do escopo em 2026-09-24. Motivo: prod nunca operou com
dados reais e os dados da VPS são sintéticos.

Se alguma consulta abaixo mostrar dado real de tenant em `gold.*` ou `landing.*`, **pare**. Nesse
caso o exportador do plano volta, e a decisão precisa ser reaberta na #279.

## Pré-condições

- **Write-fence aceito.** O fence do MIG-012 (#278) está em vigor e foi observado: T0 e a janela de
  ≥ 1 h estão publicados na #278. Isso garante que o snapshot é o estado final.
- **Acesso.** Há acesso root à VPS. As stacks ficam em:
  - prod: `/opt/cnesdata`, arquivo `docker-compose.prod.yml`;
  - dev: `/opt/cnesdata-dev`, arquivo `docker-compose.dev.yml`.
- **Variável `COMPOSE`.** Nos comandos abaixo, `COMPOSE` vale `docker compose --env-file .env
  --env-file .env.image -f <arquivo>` e é executado no diretório da stack.

## 1. Consultas somente leitura

Rode tudo em uma transação `READ ONLY` com `ON_ERROR_STOP`. As tabelas com dado pessoal entram
**só por contagem**:

```bash
$COMPOSE exec -T postgres sh -c 'psql -U "${POSTGRES_USER:-cnesdata}" \
  -d "${POSTGRES_DB:-cnesdata}" -At -F "|" -v ON_ERROR_STOP=1' <<'SQL'
begin transaction read only;
select 'server', version();
select 'database', datname, pg_size_pretty(pg_database_size(datname)) from pg_database
  where not datistemplate order by 2;
select 'alembic', version_num from alembic_version;
select format('select %L, %L, count(*) from %I.%I', 'count', schemaname || '.' || tablename,
  schemaname, tablename)
  from pg_tables where schemaname in ('dashboard', 'gold', 'landing', 'marketing', 'public')
  order by schemaname, tablename
\gexec
select 'landing.extractions', tenant_id, source_type, status, min(competencia)::text,
  max(competencia)::text, count(*)
  from landing.extractions group by 2, 3, 4 order by 2, 3, 4;
select format('select %L, %L, tenant_id::text, min(competencia)::text, max(competencia)::text, '
  'count(*) from %I.%I group by 3 order by 3', 'gold_by_tenant', schemaname || '.' || tablename,
  schemaname, tablename)
  from pg_tables t where schemaname = 'gold' and tablename like 'fato%'
  and exists (select 1 from information_schema.columns c where c.table_schema = t.schemaname
    and c.table_name = t.tablename and c.column_name = 'competencia')
  and exists (select 1 from information_schema.columns c where c.table_schema = t.schemaname
    and c.table_name = t.tablename and c.column_name = 'tenant_id')
\gexec
rollback;
SQL
docker volume ls --format '{{.Name}}'
```

O que conferir na saída:
- **Gold e landing:** contagem exata por tabela, e por tenant e range de competência em
  `landing.extractions` e em cada `gold.fato_*` com linhas.
- **Dado pessoal:** `dashboard.users`, `dashboard.user_tenants`, `dashboard.access_requests`,
  `dashboard.audit_log`, `marketing.leads`, `public.auth_provisioned_certs` e
  `public.auth_refresh_tokens` aparecem só como contagem, nunca por conteúdo. Em prod, `leads`
  pode ter contatos reais das páginas públicas. Isso é dado pessoal, não histórico de tenant.
- **Bancos do cluster:** o dump do passo 3 cobre só o banco da aplicação. Se o Keycloak usar outro
  banco no mesmo cluster, registre que ele ficou fora do dump por decisão do plano: o export do
  realm pertence aos pré-requisitos do MIG-014. Em 2026-10-10 o Keycloak de dev usava volume
  próprio, fora do Postgres.
- **Volumes:** liste os órfãos (por exemplo `minio_data` em prod) sem removê-los.

Liste também o storage legado de landing: contagem, bytes e último `LastModified`. Não imprima
segredos nem nomes de objeto que contenham dados.
- **dev:** o bucket do AIStor, de dentro do container `central-api`, com as credenciais que ele
  já tem.
- **prod:** o bucket S3 `cnesdata-landing`. As credenciais do storage legado na VPS não têm
  `s3:ListBucket` nesse bucket (`AccessDenied` em 2026-10-11). A listagem exige uma identidade
  que não seja root e que tenha esse direito, executada pelo responsável. Sem ela, este item do
  aceite fica aberto. A prova indireta complementa a listagem, mas não a substitui: zero linhas
  em `landing.extractions`, contadores de `pg_stat` zerados e presign cercado desde o T0. Como
  toda URL legada nasceu com uma linha nova e valia 3600 s, zero linhas também significa
  nenhuma URL pendente.

## 2. Decisão registrada

Na #279, registre:
- a decisão "dados sintéticos, sem histórico a exportar";
- data e responsável;
- referência às decisões do usuário de 2026-09-22 e 2026-09-24;
- as contagens do passo 1.

O realm do Keycloak só é atestado aqui. O export final dele pertence aos pré-requisitos do MIG-014.

## 3. Snapshot

O bloco roda num subshell com `errexit`. Se o `pg_dump` falhar ou o arquivo não restaurar por
inteiro, ele para e avisa, e o passo 4 não começa:

```bash
dir=/root/cnesdata-snapshots/$(basename "$PWD")-$(date -u +%Y%m%dT%H%M%SZ)
(
  set -euo pipefail
  install -d -m 700 "$dir"
  $COMPOSE exec -T postgres sh -c 'pg_dump -U "${POSTGRES_USER:-cnesdata}" \
    -d "${POSTGRES_DB:-cnesdata}" --format=custom' > "$dir/cnesdata.dump"
  chmod 600 "$dir/cnesdata.dump"
  docker run --rm --network none -v "$dir:/snap:ro" postgres:16-alpine \
    pg_restore --file=/dev/null /snap/cnesdata.dump
  sha256sum "$dir/cnesdata.dump"; stat -c %s "$dir/cnesdata.dump"
) || echo "snapshot_failed dir=$dir: não siga para o passo 4"
```

O `pg_restore --file=/dev/null` lê o arquivo inteiro, inclusive os blocos de dados, e falha em um
dump truncado. Não basta conferir só o índice com `--list`.

O dump contém dados pessoais (`dashboard.users`, `marketing.leads`). Trate-o como segredo:
- diretório com modo 700, arquivo com modo 600;
- bucket privado e criptografado;
- apague a cópia local depois da verificação do passo 5.

## 4. Upload create-if-absent para o alvo

- **Destino:** `s3://$RAW_S3_BUCKET/raw/354130/LEGACY_SNAPSHOT/<AAAA-MM-DD>/`. É o bucket raw da
  própria stack (`RAW_S3_BUCKET` no `.env`). Nenhum código lista esse prefixo, porque as cadeias
  raw passam pelo control plane. O nome do bucket carrega o ID da conta AWS: na issue, publique
  a URI com o ID omitido.
- **Objetos:** `cnesdata-<env>.dump` e `manifest.json`. O manifest leva `sha256`, `size_bytes`,
  o `VersionId` do dump (`object_version_id`), `alembic_version`, `pg_dump_version`,
  `created_at`, `source_stack` e o marcador T0 do write-fence publicado na #278 (horário,
  snapshot de transação e LSN).
- **Credenciais:** só `RAW_AWS_ACCESS_KEY_ID`, `RAW_AWS_SECRET_ACCESS_KEY`, `RAW_AWS_REGION` e
  `RAW_S3_BUCKET` do `.env` da stack, copiadas para um env-file temporário com modo 600, sem
  ecoar valores. Nunca as credenciais da estação de trabalho.
- **Onde roda:** um container descartável da imagem `central_api` que a stack já usa. Ela traz o
  boto3, então nenhuma imagem nova é baixada. A imagem roda como `app` (uid 999) e o snapshot é
  só do root, então o container roda com `--user 0:0`.
- **Create-if-absent:** cada `PutObject` usa `IfNoneMatch='*'` e `ChecksumSHA256`. Uma segunda
  tentativa falha com `PreconditionFailed` (412) em vez de sobrescrever, e o S3 recusa bytes que
  não batem com o checksum.
- **Imutabilidade.** O `IfNoneMatch` protege só este upload. A credencial raw continua com
  `PutObject` em qualquer chave, e o bucket não tem Object Lock. Por isso o snapshot é ancorado no
  `VersionId` de cada objeto, que vai para o manifest e para a #279.
  - `deploy/aws/raw/main.tf` liga versionamento e AES256, bloqueia acesso público e não declara
    regra de lifecycle. O IAM raw não tem nenhum Delete, então uma sobrescrita cria uma versão
    nova e a versão registrada continua no bucket.
  - O IAM raw também não tem `s3:GetObjectVersion`. A verificação do passo 5 confere que a
    versão **corrente** de cada chave é a registrada. Se um dia não for, recupere a versão
    registrada com uma credencial administrativa.

```bash
cd "$STACK"
env=prod   # ou dev
day=$(date -u +%F)
alembic=$($COMPOSE exec -T postgres sh -c 'psql -U "${POSTGRES_USER:-cnesdata}" \
  -d "${POSTGRES_DB:-cnesdata}" -Atc "select version_num from alembic_version"')
pg_dump_version=$($COMPOSE exec -T postgres pg_dump --version)
t0='{"at":"<T0 da #278>","txid_snapshot":"<snapshot>","wal_lsn":"<LSN>","issue":"#278"}'
img=ghcr.io/viniclus/cnesdata/central_api:$(grep -oP '(?<=^IMAGE_TAG=).*' .env.image)
envf=$(mktemp) && chmod 600 "$envf"
grep -E '^RAW_(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_REGION|S3_BUCKET)=' .env > "$envf"
docker run --rm -i --user 0:0 --env-file "$envf" -v "$dir:/snap" \
  -e SNAP_ENV="$env" -e SNAP_DATE="$day" -e ALEMBIC="$alembic" -e PG_DUMP="$pg_dump_version" \
  -e FENCE_T0="$t0" --entrypoint python "$img" - <<'PY'
import base64, hashlib, json, os
from datetime import UTC, datetime
from boto3.session import Session

e = os.environ
s3 = Session(e["RAW_AWS_ACCESS_KEY_ID"], e["RAW_AWS_SECRET_ACCESS_KEY"],
             region_name=e["RAW_AWS_REGION"]).client("s3")
bucket, prefix = e["RAW_S3_BUCKET"], f"raw/354130/LEGACY_SNAPSHOT/{e['SNAP_DATE']}/"


def put(key, body):
    digest = hashlib.sha256(body)
    resp = s3.put_object(Bucket=bucket, Key=key, Body=body, IfNoneMatch="*",
                         ChecksumSHA256=base64.b64encode(digest.digest()).decode())
    print(f"uploaded key={key} bytes={len(body)} sha256={digest.hexdigest()} "
          f"version_id={resp.get('VersionId')}")
    return resp.get("VersionId")


dump_key = f"{prefix}cnesdata-{e['SNAP_ENV']}.dump"
with open("/snap/cnesdata.dump", "rb") as f:
    dump = f.read()
dump_version = put(dump_key, dump)
manifest = json.dumps({
    "source_stack": e["SNAP_ENV"], "tenant_id": "354130", "object_key": dump_key,
    "object_version_id": dump_version, "sha256": hashlib.sha256(dump).hexdigest(),
    "size_bytes": len(dump), "format": "pg_dump --format=custom",
    "pg_dump_version": e["PG_DUMP"], "alembic_version": e["ALEMBIC"],
    "write_fence_t0": json.loads(e["FENCE_T0"]),
    "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
}, indent=2, sort_keys=True).encode()
with open("/snap/manifest.json", "xb") as f:
    f.write(manifest)
put(f"{prefix}manifest.json", manifest)
PY
rm -f "$envf"
```

O `t0` copia o marcador T0 que a #278 publicou para a stack. Guarde a saída: o `version_id` do
manifest entra no passo 5 e na #279. O env-file temporário é apagado ao fim; o passo 5 cria
outro.

## 5. Verificação por outro processo

Abra **outro** container descartável, com um env-file novo igual ao do passo 4 e o diretório do
snapshot montado só para leitura. `MANIFEST_VERSION_ID` é o `version_id` do manifest impresso
no passo 4:

```bash
envf=$(mktemp) && chmod 600 "$envf"
grep -E '^RAW_(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_REGION|S3_BUCKET)=' .env > "$envf"
docker run --rm -i --user 0:0 --env-file "$envf" -v "$dir:/snap:ro" \
  -e SNAP_DATE="$day" -e MANIFEST_VERSION_ID="<version_id do manifest>" \
  --entrypoint python "$img" - <<'PY'
import base64, hashlib, json, os
from boto3.session import Session
from botocore.exceptions import ClientError

e = os.environ
s3 = Session(e["RAW_AWS_ACCESS_KEY_ID"], e["RAW_AWS_SECRET_ACCESS_KEY"],
             region_name=e["RAW_AWS_REGION"]).client("s3")
bucket, prefix = e["RAW_S3_BUCKET"], f"raw/354130/LEGACY_SNAPSHOT/{e['SNAP_DATE']}/"
with open("/snap/manifest.json", "rb") as f:
    local = f.read()
manifest, errors = json.loads(local), []
expected = (
    (manifest["object_key"], manifest["object_version_id"], manifest["size_bytes"],
     manifest["sha256"]),
    (f"{prefix}manifest.json", e["MANIFEST_VERSION_ID"], len(local),
     hashlib.sha256(local).hexdigest()),
)
for key, version, size, sha in expected:
    head = s3.head_object(Bucket=bucket, Key=key, ChecksumMode="ENABLED")
    checksum = base64.b64encode(bytes.fromhex(sha)).decode()
    if head.get("VersionId") != version:
        errors.append(f"current_version_mismatch key={key} got={head.get('VersionId')}")
    if head["ContentLength"] != size:
        errors.append(f"size_mismatch key={key}")
    if head.get("ChecksumSHA256") != checksum:
        errors.append(f"checksum_mismatch key={key} got={head.get('ChecksumSHA256')}")
remote = s3.get_object(Bucket=bucket, Key=f"{prefix}manifest.json")["Body"].read()
if remote != local:
    errors.append("manifest_bytes_mismatch")
digest, body = hashlib.sha256(), s3.get_object(Bucket=bucket, Key=manifest["object_key"])["Body"]
for chunk in iter(lambda: body.read(1 << 20), b""):
    digest.update(chunk)
if digest.hexdigest() != manifest["sha256"]:
    errors.append("dump_sha256_mismatch")
try:
    s3.put_object(Bucket=bucket, Key=f"{prefix}manifest.json", Body=local, IfNoneMatch="*")
    errors.append("create_if_absent_not_enforced")
except ClientError as exc:
    if exc.response["Error"]["Code"] != "PreconditionFailed":
        errors.append(f"probe_error code={exc.response['Error']['Code']}")
print(f"verified dump_sha256={digest.hexdigest()} errors={len(errors)} {' '.join(errors)}")
PY
rm -f "$envf"
```

O script confere cinco coisas:
1. A versão corrente de cada chave é a registrada (`VersionId`).
2. `HeadObject` com `ChecksumMode=ENABLED`: `ContentLength` e `ChecksumSHA256` batem com o
   manifest.
3. O manifest remoto tem os mesmos bytes da cópia local.
4. O SHA-256 do dump, calculado no stream do `GetObject`, bate com o do manifest.
5. Um `PutObject` com `IfNoneMatch='*'` dos **mesmos bytes** do manifest falha com
   `PreconditionFailed`. Use os mesmos bytes: se a condição não valer, nada muda.

O IaC (`deploy/aws/raw/main.tf`) não declara regra de lifecycle; cite isso na issue. A credencial
alvo não tem `s3:GetLifecycleConfiguration`, e o atestado não usa credencial mais ampla para ler a
configuração viva.

Registre na #279 o comando exato, as URIs (sem o ID da conta), os `VersionId`, os hashes, os
bytes e `errors=0`. Depois apague as cópias locais (`rm -rf "$dir"`).

## Restauração (sem reativar writes)

Restaure só em um Postgres isolado e descartável, sem rede, nunca na stack viva. Use a mesma
versão da stack (`postgres:16-alpine`; 16.15 em 2026-10-10). A fonte é a cópia verificada no
alvo, porque o passo 5 apaga a local:

```bash
restore_dir=$(mktemp -d) && chmod 700 "$restore_dir"
envf=$(mktemp) && chmod 600 "$envf"
grep -E '^RAW_(AWS_ACCESS_KEY_ID|AWS_SECRET_ACCESS_KEY|AWS_REGION|S3_BUCKET)=' .env > "$envf"
docker run --rm -i --user 0:0 --env-file "$envf" -v "$restore_dir:/out" \
  -e KEY="raw/354130/LEGACY_SNAPSHOT/<AAAA-MM-DD>/cnesdata-<env>.dump" \
  --entrypoint python "$img" - <<'PY'
import os
from boto3.session import Session

e = os.environ
s3 = Session(e["RAW_AWS_ACCESS_KEY_ID"], e["RAW_AWS_SECRET_ACCESS_KEY"],
             region_name=e["RAW_AWS_REGION"]).client("s3")
s3.download_file(e["RAW_S3_BUCKET"], e["KEY"], "/out/cnesdata.dump")
PY
rm -f "$envf"
sha256sum "$restore_dir/cnesdata.dump"   # tem de ser igual ao sha256 do manifest
docker run -d --name restore-check --network none -e POSTGRES_HOST_AUTH_METHOD=trust \
  postgres:16-alpine
until docker exec restore-check pg_isready -U postgres -q; do sleep 1; done
docker exec restore-check createdb -U postgres restore_check
docker exec -i restore-check pg_restore -U postgres -d restore_check --no-owner \
  < "$restore_dir/cnesdata.dump"
```

Se o SHA-256 não bater, a chave foi sobrescrita depois do snapshot. Nesse caso, recupere o
`VersionId` registrado na #279 com uma credencial administrativa, porque a credencial raw não lê
versões antigas.

A restauração serve para auditoria e consulta. Ela não religa `data-processor`, rotas legadas nem
storage de landing. Apague o container e o diretório ao terminar (`docker rm -f restore-check`,
`rm -rf "$restore_dir"`).

## Fora de escopo (MIG-014)

- Apagar volumes ou serviços. Inclui os volumes órfãos, como o `minio_data` de prod: liste-os
  (`docker volume ls`) e ateste aqui, mas não remova.
- Remover runtime, config e docs legados.
- Fazer o export final do realm Keycloak.
