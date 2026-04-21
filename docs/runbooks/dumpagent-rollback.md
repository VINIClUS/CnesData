# dumpagent Rollback — Go → Python

## Quando executar

- Taxa de `FailJob(retryable=false)` Go > 20% em janela 1h após cutover
- Corrupção de Parquet detectada (diff shadow > 0 em produção)
- Crash-loop do service (panic threshold atingido)
- Decisão operacional em coordenação com central

## Tempo estimado

15 min por máquina. Sem downtime significativo — Python retoma fila do mesmo ponto.

## Pré-requisitos

- Acesso admin à máquina
- ZIP da última versão Python salvo localmente em `C:\CnesAgent_Backup\`
- Senha Firebird + caminho CNES.GDB preservados

## Procedimento

### 1. Parar serviço Go

```powershell
Stop-Service CnesDumpAgent -Force
# aguarda até 30s para SCM reportar STOPPED
Get-Service CnesDumpAgent
```

Confirmar `Status: Stopped`.

### 2. Desinstalar serviço Go

```powershell
cd "C:\Program Files\CnesAgent"
.\dumpagent.exe uninstall
```

Output esperado: `uninstalled service=CnesDumpAgent`.

### 3. Reinstalar Python

Opção A — wheel instalado via pip:

```powershell
# Se pip + Python ainda presentes:
pip install dump-agent==<ultima_py_stable>
# ou:
pip install C:\CnesAgent_Backup\dump_agent-<versao>-py3-none-any.whl
```

Opção B — PyInstaller frozen (recomendado se pip foi removido):

```powershell
Expand-Archive "C:\CnesAgent_Backup\dump_agent_py-<versao>.zip" "C:\Program Files\CnesAgentPy"
```

### 4. Configurar env Python

Criar `C:\Program Files\CnesAgentPy\.env` com mesmas vars da instalação Go
(TENANT_ID, DB_HOST, DB_PATH, DB_PASSWORD etc).

### 5. Reinstalar serviço Python

Windows com NSSM (mecanismo usado pelo Python):

```powershell
# nssm.exe baixado previamente em C:\CnesAgent_Backup\
C:\CnesAgent_Backup\nssm.exe install CnesDumpAgent `
  "C:\Python313\python.exe" "-m dump_agent.main"
C:\CnesAgent_Backup\nssm.exe set CnesDumpAgent AppDirectory "C:\Program Files\CnesAgentPy"
C:\CnesAgent_Backup\nssm.exe set CnesDumpAgent AppEnvironmentExtra (Get-Content "C:\Program Files\CnesAgentPy\.env" -Raw)
```

Ou se instalação original usa outro método (InstallAnywhere etc), seguir
documentação do instalador original.

### 6. Iniciar Python

```powershell
Start-Service CnesDumpAgent
Get-Content "$env:LOCALAPPDATA\CnesAgent\logs\dump_agent.log" -Tail 30
```

Procurar por boot Python esperado (formato `YYYY-MM-DD HH:MM:SS INFO ...`).

### 7. Notificar central

Via e-mail / Telegram para operador central:
- Tenant rolled back: `PE/354130`
- Timestamp: `<ISO8601>`
- Motivo resumido
- Versão Python ativa

### 8. Preservar logs Go para post-mortem

```powershell
$backup = "C:\CnesAgent_Backup\rollback-$(Get-Date -Format 'yyyyMMdd-HHmm')"
New-Item -ItemType Directory $backup -Force
Copy-Item "$env:LOCALAPPDATA\CnesAgent\logs\*" $backup -Recurse
Copy-Item "$env:LOCALAPPDATA\CnesAgent\CLOCK_FATAL.txt" $backup -ErrorAction SilentlyContinue
```

Upload para central para análise.

## Verificação pós-rollback

- [ ] `Get-Service CnesDumpAgent` → Running
- [ ] Últimos logs mostram `worker_started` Python
- [ ] Próximos 30min: pelo menos 1 job completed no central dashboard
- [ ] `central_api` dashboard /agents/status retorna `agent_version` Python
