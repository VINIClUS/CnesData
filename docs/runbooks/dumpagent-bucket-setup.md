# Bucket de Releases do dumpagent — Setup

## Objetivo

Bucket S3-compatible isolado da infra de tenants (MinIO produção). Armazena artefatos
`.zip` do dumpagent Go, o manifesto de update por canal e a cópia imutável por versão.
Distribuição pública (leitura via domínio custom); publicação segue restrita ao workflow
de release. Ver `dumpagent-release.md` para o contrato do manifesto e o corte de release.

## Provider — decisão

Candidatos (decisão por custo+egress+latência):

| Provider | Prós | Contras |
|---|---|---|
| **Cloudflare R2** (recomendado) | Egress zero, S3-compatible, barato | SLA menos maduro que AWS |
| AWS S3 | Maduro, OIDC federation com GitHub | Custo de egress |
| DO Spaces | S3-compatible, simples | CDN menos polido |

**Default escolhido:** Cloudflare R2 — egress zero ajuda com N tenants baixando.

## Provisioning

### Cloudflare R2

1. Dashboard Cloudflare → R2 → Create bucket `cnesdata-releases`
2. Access Keys → Create API Token:
   - Permissions: `Object Read & Write` em `cnesdata-releases` apenas
   - Token name: `github-actions-dumpagent-release`
3. Copiar `Access Key ID` + `Secret Access Key` + endpoint URL

### GitHub Secrets (repo)

Adicionar em `Settings → Secrets and variables → Actions`:

- `RELEASES_S3_ACCESS_KEY` = access key do token
- `RELEASES_S3_SECRET_KEY` = secret
- `RELEASES_S3_ENDPOINT` = `https://<account>.r2.cloudflarestorage.com`
- `RELEASES_S3_BUCKET` = `cnesdata-releases`
- `RELEASES_S3_REGION` = `auto` (R2 aceita)

> Não-sensíveis (`_BUCKET`, `_ENDPOINT`, `_REGION`) estão hoje como `secrets.*` por
> conveniência histórica — inconsistente com `vars.DEV_SSH_HOST`/`vars.PROD_SSH_HOST`.
> Migrar para `vars.*` é PR mecânico separado, não bloqueia este setup.

### GitHub Variables (repo)

- `RELEASES_PUBLIC_BASE_URL` = `https://releases.cnesdata.com.br` — usada pelo
  workflow de release para compor as URLs do manifesto e pelos agentes/operadores para
  baixar artefatos. É `vars.*`, não secret: aparece em todo manifesto servido
  publicamente, e mascarar o valor tornaria os logs do workflow ilegíveis.

## Acesso público — reversão da decisão original

**Esta seção reverte a decisão anterior deste runbook** ("Public access: Disabled,
leitura só via presigned URL"). Motivo: agentes em rede municipal não têm como carregar
uma credencial R2, e o binário + checksum são públicos por natureza (repo público,
`.zip` sem segredo embutido). Sem leitura pública, não há update check nem self-update
possível na ponta.

1. R2 → `cnesdata-releases` → Settings → Public access → **Connect Domain** →
   `releases.cnesdata.com.br` (a zona `cnesdata.com.br` já precisa estar na
   Cloudflare). O CNAME proxied é criado automaticamente pelo R2.
2. **Não habilitar a URL `r2.dev`** em produção — é rate-limited e não-cacheável por
   design. Só serve como smoke test temporário se o DNS do domínio custom não estiver
   pronto ainda.
3. **Antes de habilitar, confirmar que o bucket só contém artefatos de release** —
   conectar um domínio custom torna **o bucket inteiro** legível pelo mundo:

   ```bash
   aws s3 ls s3://cnesdata-releases/ --recursive --endpoint-url "$ENDPOINT" | head -50
   ```

   Esperado: só chaves sob `dumpagent/**`. Se houver qualquer outro prefixo (dump de
   tenant, credencial, backup), remover antes de conectar o domínio.
4. Cache Rules na zona Cloudflare: uma regra, hostname = `releases.cnesdata...`,
   ação "Eligible for cache", Edge TTL = "Use cache-control header from origin". Regras
   por path não são necessárias — o workflow de release define `Cache-Control` por
   objeto (ver `dumpagent-release.md`).
5. Opcional: regra de rate-limiting no WAF para o hostname (ex.: 100 req/min/IP) —
   seguro barato num endpoint público sem autenticação.
6. R2 → Settings → Object lifecycle rules: apagar objetos sob `dumpagent/_dryrun/` após
   7 dias (saída de `workflow_dispatch` em modo dry-run, sem valor após verificação).
7. O escopo do API token (`Object Read & Write` neste bucket, passo 2) **não muda** —
   leitura pública é propriedade do bucket, não do token, que continua sendo usado só
   pelo workflow de release para escrever.

## Layout de chaves

```
cnesdata-releases/
└── dumpagent/
    ├── v0.1.0/
    │   ├── release.json                       # manifesto imutável desta versão
    │   └── windows-amd64/
    │       ├── dumpagent-v0.1.0.zip
    │       └── dumpagent-v0.1.0.zip.sha256
    ├── stable/
    │   └── latest.json                        # canal stable — releases sem sufixo rc
    ├── rc/
    │   └── latest.json                        # canal rc — releases -rc./-beta./-alpha.
    └── _dryrun/<run_id>/...                    # saída de workflow_dispatch, lifecycle 7d
```

Schema do manifesto (`release.json` e `latest.json` têm o mesmo formato — o canal é uma
cópia server-side do release.json da versão promovida): ver
`docs/contracts/dumpagent-update-manifest.schema.json`. Detalhes de canal, promoção e
kill switch de rollback: `dumpagent-release.md`.

## Verificação

Após um release publicado (ver `dumpagent-release.md` para o fluxo completo):

```bash
curl -s https://releases.cnesdata.com.br/dumpagent/stable/latest.json | jq .
```

Expected: manifesto válido, `artifacts."windows-amd64".url` respondendo 200.

## Rotação de chave

Anual, ou imediato em caso de leak:

1. Dashboard R2 → Access Keys → Revoke token antigo
2. Criar token novo
3. Atualizar GitHub Secrets
4. Verificar próximo release workflow green

## Contatos

Ops/Infra: `<nome>` — <email>
