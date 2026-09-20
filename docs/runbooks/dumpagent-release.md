# dumpagent — Corte de Release, Dry-run, Canais, Rollback

## Visão geral

O release do dumpagent Go tem GitHub Releases como registro canônico de versões (tag,
notas, checksums) e R2 como camada primária de distribuição, com o mesmo binário também
publicado como asset do Release (fallback). Workflow:
`.github/workflows/dump-agent-go-release.yml`, grafo
`resolve → preflight → gate → build → github-release → publish-r2 → promote`.

O manifesto de update (`release.json` / `latest.json`) é o contrato consumido por um
futuro cliente de update check / self-update (fora de escopo deste PR — ver "Fora de
escopo" abaixo). Schema: `docs/contracts/dumpagent-update-manifest.schema.json`.

## Convenção de tag e canal

Tag: `dumpagent-go-v<versão>`. A versão publicada é a tag menos o prefixo
`dumpagent-go-`. Canal derivado por regex, nunca por substring:

| Padrão da versão | Canal | Comportamento |
|---|---|---|
| `v<major>.<minor>.<patch>` | `stable` | promove `dumpagent/stable/latest.json` |
| `v<major>.<minor>.<patch>-(rc\|beta\|alpha).<n>` | `rc` | `--prerelease`; promove `dumpagent/rc/latest.json` |
| qualquer outro formato | — | workflow falha em `resolve` com `version_invalid` |

Um release `stable` **não** move o manifesto `rc` (e vice-versa). Uma máquina no canal
`rc` não rola para `stable` automaticamente — é o que "canal" significa. Limpeza de um
`rc` obsoleto (apontar `rc/latest.json` de volta para o último `stable`) é ação manual do
operador, não automática.

## `min_supported_version`

Lido de `apps/dump_agent_go/MIN_SUPPORTED_VERSION` (arquivo versionado, uma linha) a cada
release. Representa a versão mínima de agente que pode aplicar este update
automaticamente — abaixo dela, presume-se mudança de config/protocolo incompatível e o
agente deve seguir o runbook manual (`dumpagent-install-windows.md`). Bump deliberado,
revisado em PR, junto com a mudança que quebra compatibilidade — nunca calculado
automaticamente pelo workflow.

## Cortando um release

### 1. Dry-run primeiro (sempre)

Actions → `dump-agent-go-release` → Run workflow → `version`: `v0.0.1-rc.1` (ou a próxima
versão real em formato rc). Não cria tag, não cria Release, não publica em
`dumpagent/<canal>/latest.json` — escreve em `dumpagent/_dryrun/<run_id>/` no R2.

Verifica: credenciais R2, comportamento de checksum do AWS CLI contra o endpoint R2,
geração e validação do manifesto contra o schema — com raio de impacto zero.

```bash
BASE=https://releases.cnesdata.vinisantana.com
Z=dumpagent-v0.0.1-rc.1-windows-amd64.zip
curl -sI "$BASE/dumpagent/_dryrun/<run_id>/v0.0.1-rc.1/windows-amd64/$Z"
```

### 2. Tag real

```bash
git tag dumpagent-go-v0.1.0
git push origin dumpagent-go-v0.1.0
```

Isso dispara `preflight` (bloqueia se a versão já existe no R2), `gate` (reusa os gates
de PR do dump-agent-go: lint, vet, race tests, cobertura ≥65%, drift do OpenAPI client,
integração Firebird no Windows — ~15-20min), `build`, `github-release` (Release criado
como **draft**), `publish-r2`, e por fim `promote` (undraft do Release + escreve o
manifesto do canal — **último passo do pipeline**: nada é descobrível antes disso).

### 3. Verificação pós-release

```bash
BASE=https://releases.cnesdata.vinisantana.com
CH=stable   # ou rc

curl -sI "$BASE/dumpagent/v0.1.0/windows-amd64/dumpagent-v0.1.0-windows-amd64.zip" \
  | grep -i cache-control        # esperado: public, max-age=31536000, immutable
curl -sI "$BASE/dumpagent/$CH/latest.json" | grep -i cache-control
  # esperado: public, max-age=60, must-revalidate

M=$(curl -s "$BASE/dumpagent/$CH/latest.json")
curl -sLo /tmp/dumpagent.zip "$(jq -r '.artifacts["windows-amd64"].url' <<<"$M")"
echo "$(jq -r '.artifacts["windows-amd64"].sha256' <<<"$M")  /tmp/dumpagent.zip" | sha256sum -c -

gh release view dumpagent-go-v0.1.0 --json isDraft,isPrerelease,assets,body
  # esperado: isDraft=false, assets com 3 itens (.zip, .sha256, release.json)
```

### Primeiro release: notas geradas

`gh release create --generate-notes` calcula as notas contra o Release anterior. Como
não existe nenhum Release anterior ao primeiro corte, as notas geradas abrangem todo o
histórico do repositório — editar manualmente via
`gh release edit dumpagent-go-v0.1.0 --notes-file notas.md` antes de divulgar.
Releases seguintes não têm esse problema.

## Idempotência / re-execução

| Passo | Comportamento em re-run |
|---|---|
| `preflight` | Falha com `version_already_published` se a versão já existe no R2 |
| `gh release create` | Guardado por `gh release view` — não duplica |
| `gh release upload` | `--clobber` — substitui os assets |
| Objetos versionados no R2 | Guardados por `preflight`; forçar sobrescrita exige apagar o prefixo à mão |
| `gh release edit --draft=false` | Idempotente |
| Cópia do manifesto de canal | Idempotente (mesma origem) |

Publicar uma versão nova sob a mesma tag **não é suportado** — o `preflight` existe
justamente para impedir isso, já que municípios podem já ter baixado o binário anterior.

## Rollback — kill switch operacional

Não requer rebuild. Reescrever o manifesto do canal para apontar para a versão anterior:

```bash
BUCKET=cnesdata-releases
ENDPOINT=https://<account>.r2.cloudflarestorage.com
aws s3 cp "s3://$BUCKET/dumpagent/v0.1.0/release.json" \
  "s3://$BUCKET/dumpagent/stable/latest.json" \
  --cache-control 'public, max-age=60, must-revalidate' \
  --content-type application/json \
  --endpoint-url "$ENDPOINT"
```

Propaga em até 60s (TTL do manifesto do canal). Para rollback de Go para Python (crash
loop, corrupção de dados), ver `dumpagent-rollback.md` — esse caminho reinstala o agente
Python e é o recurso de último caso; a reescrita de manifesto acima é preferível sempre
que o binário Go em si não for o problema.

## Fora de escopo (deste PR)

- Cliente Go de update check / self-update (`internal/updater`) — consome este contrato,
  mas não existe ainda. O que este runbook cobre é só a distribuição.
- Assinatura de código (Authenticode) — binários seguem não-assinados; whitelist de AV
  por município continua necessária (ver `dumpagent-install-windows.md`).
- Endpoint de update no `central_api` / pin de versão por tenant.
