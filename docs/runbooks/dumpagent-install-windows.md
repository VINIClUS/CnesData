# dumpagent — Instalação Windows (MVP1 manual)

## Pré-requisitos

- Windows 10/11 ou Windows Server 2016+
- Privilégios de administrador na máquina
- Relógio sincronizado (NTP < 1min skew) — ver `edge-ntp-setup.md`
- Acesso de rede HTTPS ao `central_api` + bucket MinIO de uploads
- `fbclient.dll` 64-bit presente (caso driver CGO — verificar com Spec 0)

## Arquivos entregues

- `dumpagent-v<versão>-windows-amd64.zip` — contém `dumpagent.exe`

Distribuição pelo domínio público do R2 (`releases.cnesdata.com.br`) — sem
credencial. A versão corrente do canal `stable` está em `dumpagent/stable/latest.json`;
para uma versão específica, usar `dumpagent/<versão>/`. Ver `dumpagent-release.md` para o
contrato do manifesto.

> **Ainda não ativo (2026-09-22):** o custom domain do R2 continua apontando para
> `releases.cnesdata.vinisantana.com` — a migração (`dumpagent-bucket-setup.md`, seção
> "Acesso público") e a atualização da variável de repo `RELEASES_PUBLIC_BASE_URL` são
> passos externos (Cloudflare R2 Connect Domain + `gh variable set`), não cobertos por
> nenhuma PR de código. Até isso acontecer, use `releases.cnesdata.vinisantana.com` abaixo.

```powershell
$base = "https://releases.cnesdata.vinisantana.com/dumpagent"  # trocar para
                                                                 # releases.cnesdata.com.br
                                                                 # quando o R2 migrar
$manifest = Invoke-RestMethod "$base/stable/latest.json"
$art = $manifest.artifacts.'windows-amd64'
Invoke-WebRequest -Uri $art.url -OutFile "dumpagent-$($manifest.version)-windows-amd64.zip"
```

## Verificação de integridade

```powershell
$hash = (Get-FileHash ".\dumpagent-$($manifest.version)-windows-amd64.zip" -Algorithm SHA256).Hash
if ($hash.ToLower() -ne $art.sha256) { throw "sha256_mismatch expected=$($art.sha256) got=$hash" }
```

Se o R2 estiver inacessível, `$art.fallback_url` aponta para o mesmo `.zip` como asset do
GitHub Release — mesmo SHA256, verificação idêntica.

## Extração + posicionamento

```powershell
Expand-Archive -Path ".\dumpagent-$($manifest.version)-windows-amd64.zip" `
  -DestinationPath "C:\Program Files\CnesAgent"
```

## Configuração

Criar `C:\Program Files\CnesAgent\config.env`. **As chaves de banco usam o
prefixo da fonte** (`CNES_`/`SIHD_`/`BPA_`) — `resolveFB` em
`cmd/dumpagent/path_config.go` não lê `DB_HOST`/`DB_USER`/etc sem prefixo; um
agente configurado com os nomes sem prefixo sobe com `DB_PATH` vazio e senha
`masterkey` padrão (achado H6, ver `docs/edge-agent-audit-2026-09-20.md`).
**Nunca coloque a senha aqui** — `config.env` vira uma chave de registro
(`Environment` do serviço), legível por qualquer usuário autenticado na
máquina (ACL padrão de `HKLM\SYSTEM\...\Services`). Use
`dumpagent set-secret cnes` para a senha (armazenamento DPAPI).

`INTENT` seleciona o tipo de extração CNES enviado ao `central_api` — default
`cnes_estabelecimentos`, o único valor que `_FATO_SUBTYPE_FOR`
(`central_api/routes/jobs.py`) mapeia sem erro 422 (H10, ver findings). Omitir
é seguro; só sobrescreva para `cnes_profissionais`/`cnes_equipes`.
`COD_MUN_IBGE` é o código IBGE de 6 dígitos anexado ao manifest de upload —
default `TENANT_ID` quando omitido, só precisa divergir onde o tenant não for
o próprio código IBGE:

```env
CENTRAL_API_URL=https://api.cnesdata.gov.br
TENANT_ID=354130
COMPETENCIA_YYYYMM=202601
INTENT=cnes_estabelecimentos
COD_MUN_IBGE=354130
CNES_DB_HOST=localhost
CNES_DB_PORT=3050
CNES_DB_PATH=C:\Programa CNES\database\CNES.GDB
CNES_DB_USER=SYSDBA
CNES_DB_CHARSET=WIN1252
DUMP_MAX_JITTER_SECONDS=1800
FIREBIRD_DLL=C:\Programa CNES\fbclient.dll
```

## Senha do Firebird

Antes ou depois do install, como Administrator:

```powershell
cd "C:\Program Files\CnesAgent"
.\dumpagent.exe set-secret cnes
```

Prompt interativo (não ecoa); armazena `secrets/cnes.dpapi` sob a raiz de
estado do agente (`%ProgramData%\CnesAgent`, machine-wide — ver H5 no
findings).

## Instalação como Serviço

Abrir PowerShell como Administrator:

```powershell
cd "C:\Program Files\CnesAgent"
.\dumpagent.exe install --config "C:\Program Files\CnesAgent\config.env"
```

`--config` escreve as chaves não-secretas do arquivo no valor de registro
`Environment` do próprio serviço (`HKLM\SYSTEM\CurrentControlSet\Services\
CnesDumpAgent`), que o SCM injeta no processo ao iniciar. Chaves com nome
sugerindo segredo (`PASSWORD`, `SECRET`, `TOKEN`, `APIKEY`) são rejeitadas
com um aviso — use `set-secret` para essas.

Output esperado: `installed service=CnesDumpAgent exe=C:\...\dumpagent.exe`

Verificar:

```powershell
Get-Service CnesDumpAgent
```

## Iniciar

```powershell
Start-Service CnesDumpAgent
```

Aguardar 30s e verificar logs (raiz de estado é **machine-wide**, não
`%LOCALAPPDATA%` — mesma para o serviço e para qualquer sessão admin):

```powershell
Get-Content "$env:ProgramData\CnesAgent\logs\dumpagent.log" -Tail 30
```

Procurar por: `boot version=v0.1.0 mode=run` + `machine_id_resolved` + `worker_started`.

## Desinstalação

`dumpagent.exe uninstall` para o serviço se estiver rodando, remove o
registro do SCM e desregistra a fonte de eventlog — nessa ordem. É
idempotente (rodar duas vezes não é erro). **Por padrão preserva o estado**
(certs, secrets, fila, delta store, audit log) — mesmo comportamento que o
runbook de rollback espera ao copiar `logs/*` depois do uninstall
(`docs/runbooks/dumpagent-rollback.md`, passo 8).

```powershell
.\dumpagent.exe uninstall
Remove-Item "C:\Program Files\CnesAgent" -Recurse -Force
```

Não é necessário `Stop-Service` manual antes — o comando já para o serviço
se necessário.

### Descomissionamento completo (residual zero)

Para remover também o estado do agente (ex.: trocando de município/tenant,
ou desativando o posto), use `--purge`:

```powershell
.\dumpagent.exe uninstall --purge
Remove-Item "C:\Program Files\CnesAgent" -Recurse -Force
```

`--purge` remove a raiz de estado inteira (`%ProgramData%\CnesAgent`):
certs, secrets DPAPI, fila (`queue/outbox.db`), delta store, audit log,
`machine_id`, `config.yaml`, `CLOCK_FATAL.txt` se presente.

### Checklist de verificação pós-desinstalação

```powershell
Get-Service CnesDumpAgent -ErrorAction SilentlyContinue   # espera: erro/ausente
Test-Path "HKLM:\SYSTEM\CurrentControlSet\Services\CnesDumpAgent"  # espera: False
Get-Process dumpagent -ErrorAction SilentlyContinue        # espera: nada
Test-Path "$env:ProgramData\CnesAgent"                      # False só após --purge
```

Se `Get-Service` ainda retornar o serviço com `Status=Running` mesmo após
`uninstall` reportar sucesso, o processo não foi parado a tempo — pare-o
manualmente (`Stop-Process`) e reexecute `uninstall` (idempotente).

## Troubleshooting

| Sintoma | Causa provável | Ação |
|---|---|---|
| `already_running lock=dumpagent` | Outra instância rodando | `Get-Process dumpagent*` + kill |
| `CLOCK_FATAL.txt` aparece | Skew > 60min | `w32tm /resync` + restart service |
| `firebird_open` erro | `CNES_DB_PATH` errado (nome sem prefixo é ignorado) ou fbclient.dll ausente | Verificar config.env usa `CNES_DB_*` + `Test-Path` |
| Logs vazios, service parado | Config.env malformado | Executar `dumpagent.exe run` em foreground para ver erro |
| AV bloqueia exe | Whitelist necessária | Ver seção "AV whitelist" abaixo |

## AV whitelist

Alguns antivírus municipais tratam executáveis Go não-assinados como suspeitos.
Whitelist path `C:\Program Files\CnesAgent\dumpagent.exe` no AV do município.
Binary signing fica para uma release futura — ver "Fora de escopo" em
`dumpagent-release.md`.
