# Edge Jobs raw (#294)

O caminho raw é opt-in no agente (`dumpagent run --raw` ou `AGENT_RAW_MODE=true`).
Sem a opção, `dumpagent run` continua no protocolo legado `/api/v1/jobs`.
O upload raw passa sempre por `central_api`; o agente não recebe chaves AWS.

## Infraestrutura AWS

`deploy/aws/raw/main.tf` cria tabelas e buckets separados para `dev` e `prod`,
seis GSIs, TTL `expires_at`, versionamento, criptografia e IAM por ambiente.
Aplicar com backend remoto de estado protegido; o estado contém as chaves IAM.
Antes do apply, confirmar `aws sts get-caller-identity` = `836651842853` e
`terraform plan` sem alterações em recursos fora de `cnesdata-raw-*`.

As variáveis de `central-api` são `RAW_BACKEND=aws`, `RAW_DYNAMODB_TABLE`,
`RAW_S3_BUCKET`, `RAW_AWS_REGION`, `RAW_AWS_ACCESS_KEY_ID` e
`RAW_AWS_SECRET_ACCESS_KEY`. As credenciais de `dev` e `prod` são diferentes.
Não substituem `AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY` do storage legado.
Validar, com cada identidade raw, `dynamodb describe-table` e `s3api
head-bucket`; o boot da API repete essas verificações e falha se não houver
acesso.

## Instalação na VPS

Os workflows de deploy só alteram a tag da imagem; antes de acioná-los,
instalar explicitamente Compose e variáveis raw na VPS. Para cada stack:

1. Salvar cópias de `docker-compose.{dev,prod}.yml`, `.env` e `.env.image`
   fora do diretório da stack, com timestamp e permissões de acesso restritas.
2. Copiar o Compose versionado de `deploy/dev` para `/opt/cnesdata-dev` e o de
   `deploy/prod` para `/opt/cnesdata`, preservando propriedade e modo.
3. Acrescentar as seis variáveis raw ao `.env` existente de cada stack,
   usando a chave IAM do ambiente correspondente. Não reescrever os segredos
   já existentes. Confirmar `docker compose ... config --quiet` nas duas
   stacks antes de reiniciar qualquer serviço.
4. Executar `deploy-develop` em `develop`, verificar health e smoke de claim,
   upload e registro. Só depois publicar `main` pelo workflow protegido e
   repetir a verificação. O workflow de release do agente aceita
   `workflow_dispatch` com `version=v0.1.2` para dry-run; a tag
   `dumpagent-go-v0.1.2` faz a publicação real depois do servidor.

Em rollback, restaurar os arquivos Compose, `.env` e `.env.image` salvos e
rodar `docker compose up -d`. Para o agente, restaurar o canal anterior no
manifesto de release. Manter tabelas e objetos raw para replay posterior.

## Enqueue e smoke local

No profile `local`, definir `RAW_LOCAL_TOKEN` na API e no agente. O agente
envia `X-Raw-Token` e `X-Raw-Agent-Id`; a API associa o tenant configurado.
Na VPS, a identidade vem apenas do certificado mTLS validado pelo Caddy.

Criar os dez jobs com:

```bash
curl -fsS -X POST "$CENTRAL_API_URL/api/v1/admin/raw-jobs/enqueue" \
  -H "X-Admin-Token: $ADMIN_TOKEN" \
  -H 'Idempotency-Key: smoke-2026-09' \
  -H 'Content-Type: application/json' \
  -d '{"tenant_id":"354130","agent_id":"AGENT_ID","competencia":"2026-09"}'
```

Repetir a mesma chave devolve os mesmos dez IDs. A opção `sources` limita
as fontes (`CNES_LOCAL`, `SIHD`, `BPA_MAG`, `SIA_LOCAL`);
`cnes_snapshot_mode=DELTA` só altera CNES. Reutilizar a chave com outro
pedido durante 24 horas retorna 409.

O smoke opt-in do CI Windows usa a label `run-raw-smoke`. Ele cria as fixtures
CNES, SIHD, BPA e SIA, inicia `central_api` real no profile `local`, executa
o binário com `--raw`, aguarda os dez jobs em `SUCCEEDED`, reproduz o enqueue
e reinicia o agente. Os testes Go de outbox cobrem interrupções antes e depois
do upload, replay do spool, erro transitório e perda de fence.
