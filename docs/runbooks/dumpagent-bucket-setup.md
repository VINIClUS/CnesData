# Bucket de Releases do dumpagent — Setup

## Objetivo

Bucket S3-compatible isolado da infra de tenants (MinIO produção). Armazena
artefatos `.zip` do dumpagent Go para distribuição manual (MVP1) e OTA (future).

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
2. Settings → Public access: **Disabled** (read só via presigned URL)
3. Access Keys → Create API Token:
   - Permissions: `Object Read & Write` em `cnesdata-releases` apenas
   - Token name: `github-actions-dumpagent-release`
4. Copiar `Access Key ID` + `Secret Access Key` + endpoint URL

### GitHub Secrets (repo)

Adicionar em `Settings → Secrets and variables → Actions`:

- `RELEASES_S3_ACCESS_KEY` = access key do token
- `RELEASES_S3_SECRET_KEY` = secret
- `RELEASES_S3_ENDPOINT` = `https://<account>.r2.cloudflarestorage.com`
- `RELEASES_S3_BUCKET` = `cnesdata-releases`
- `RELEASES_S3_REGION` = `auto` (R2 aceita)

## Layout de chaves

```
cnesdata-releases/
└── dumpagent/
    ├── v0.1.0-rc1/
    │   └── windows-amd64/
    │       ├── dumpagent-v0.1.0-rc1.zip
    │       └── dumpagent-v0.1.0-rc1.zip.sha256
    ├── v0.1.0/
    │   └── windows-amd64/
    │       ├── dumpagent-v0.1.0.zip
    │       └── dumpagent-v0.1.0.zip.sha256
    └── latest.json                   # { "version": "v0.1.0", "url": "..." }
```

`latest.json` atualizado por release workflow — base para futura rotina OTA.

## Verificação

Após push de tag `dumpagent-go-v0.1.0-rc1`:

```bash
aws s3 ls s3://cnesdata-releases/dumpagent/v0.1.0-rc1/windows-amd64/ \
  --endpoint-url https://<account>.r2.cloudflarestorage.com
```

Expected: `.zip` + `.sha256` listados.

## Rotação de chave

Anual, ou imediato em caso de leak:

1. Dashboard R2 → Access Keys → Revoke token antigo
2. Criar token novo
3. Atualizar GitHub Secrets
4. Verificar próximo release workflow green

## Contatos

Ops/Infra: `<nome>` — <email>
