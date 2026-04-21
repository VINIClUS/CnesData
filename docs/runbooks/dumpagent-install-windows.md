# dumpagent — Instalação Windows (MVP1 manual)

## Pré-requisitos

- Windows 10/11 ou Windows Server 2016+
- Privilégios de administrador na máquina
- Relógio sincronizado (NTP < 1min skew) — ver `edge-ntp-setup.md`
- Acesso de rede HTTPS ao `central_api` + bucket MinIO de uploads
- `fbclient.dll` 64-bit presente (caso driver CGO — verificar com Spec 0)

## Arquivos entregues

- `dumpagent-v<versão>-windows-amd64.zip` — contém `dumpagent.exe`

Download via link presigned enviado pelo operador central ou direto do bucket:

```powershell
# Operador com credencial válida para o bucket:
aws s3 cp s3://cnesdata-releases/dumpagent/v0.1.0/windows-amd64/dumpagent-v0.1.0-windows-amd64.zip . `
  --endpoint-url https://<account>.r2.cloudflarestorage.com
```

## Verificação de integridade

```powershell
# baixar .sha256 do mesmo bucket
(Get-FileHash .\dumpagent-v0.1.0-windows-amd64.zip -Algorithm SHA256).Hash
# comparar com conteúdo do .sha256
```

## Extração + posicionamento

```powershell
Expand-Archive -Path .\dumpagent-v0.1.0-windows-amd64.zip -DestinationPath "C:\Program Files\CnesAgent"
```

## Configuração

Criar `C:\Program Files\CnesAgent\config.env`:

```env
CENTRAL_API_URL=https://api.cnesdata.gov.br
TENANT_ID=354130
DB_HOST=localhost
DB_PORT=3050
DB_PATH=C:\Programa CNES\database\CNES.GDB
DB_USER=SYSDBA
DB_PASSWORD=<senha Firebird>
DB_CHARSET=WIN1252
DUMP_MAX_JITTER_SECONDS=1800
FIREBIRD_DLL=C:\Programa CNES\fbclient.dll
```

**Segurança:** proteger o arquivo (ACL apenas Administrators + SYSTEM):

```powershell
icacls "C:\Program Files\CnesAgent\config.env" `
  /inheritance:r /grant:r "Administrators:(R,W)" "SYSTEM:(R,W)"
```

## Instalação como Serviço

Abrir PowerShell como Administrator:

```powershell
cd "C:\Program Files\CnesAgent"
.\dumpagent.exe install --config "C:\Program Files\CnesAgent\config.env"
```

Output esperado: `installed service=CnesDumpAgent exe=C:\...\dumpagent.exe`

Verificar:

```powershell
Get-Service CnesDumpAgent
```

## Iniciar

```powershell
Start-Service CnesDumpAgent
```

Aguardar 30s e verificar logs:

```powershell
Get-Content "$env:LOCALAPPDATA\CnesAgent\logs\dumpagent.log" -Tail 30
```

Procurar por: `boot version=v0.1.0 mode=run` + `machine_id_resolved` + `worker_started`.

## Desinstalação

```powershell
Stop-Service CnesDumpAgent
.\dumpagent.exe uninstall
Remove-Item "C:\Program Files\CnesAgent" -Recurse -Force
```

## Troubleshooting

| Sintoma | Causa provável | Ação |
|---|---|---|
| `already_running lock=dumpagent` | Outra instância rodando | `Get-Process dumpagent*` + kill |
| `CLOCK_FATAL.txt` aparece | Skew > 60min | `w32tm /resync` + restart service |
| `firebird_open` erro | DB_PATH errado ou fbclient.dll ausente | Verificar config.env + `Test-Path` |
| Logs vazios, service parado | Config.env malformado | Executar `dumpagent.exe run` em foreground para ver erro |
| AV bloqueia exe | Whitelist necessária | Ver seção "AV whitelist" abaixo |

## AV whitelist

Alguns antivírus municipais tratam executáveis Go não-assinados como suspeitos.
Whitelist path `C:\Program Files\CnesAgent\dumpagent.exe` no AV do município.
Binary signing será adicionado em release futura (OTA spec).
